from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url


class ConcurrentEODError(RuntimeError):
    """Raised when a second EOD is started while another session holds the EOD advisory lock."""

# Session-level advisory lock: one EOD at a time across all app/scheduler clients.
# Must match eod.run_eod_for_date (import this constant from here only).
EOD_SESSION_ADVISORY_LOCK_KEY = 9_238_471

def _get_conn():
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


def _get_advisory_conn():
    """
    Connection for session advisory locks only.

    Use the default (tuple) cursor so pg_try_advisory_lock results are read as row[0].
    RealDictCursor does not support row[0] — it raises KeyError(0) and breaks EOD startup.
    """
    return psycopg2.connect(get_database_url())


def clear_stale_eod_audit_runs(*, stale_after_minutes: int = 120) -> int:
    """
    Mark old RUNNING audit rows as ABORTED when no finish signal arrived (e.g. process crash).
    Returns number of rows updated.
    """
    if stale_after_minutes < 1:
        stale_after_minutes = 120
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE eod_runs
                SET run_status = 'ABORTED',
                    finished_at = NOW(),
                    error_message = COALESCE(
                        NULLIF(TRIM(error_message), ''),
                        'Run did not complete (stale RUNNING cleared).'
                    )
                WHERE run_status = 'RUNNING'
                  AND finished_at IS NULL
                  AND started_at < (NOW() - (%s * INTERVAL '1 minute'))
                """,
                (stale_after_minutes,),
            )
            n = cur.rowcount
        conn.commit()
    return int(n or 0)


def is_another_eod_session_active() -> bool:
    """
    True if another DB session currently holds the EOD advisory lock (EOD work in progress).

    Uses try-lock + immediate unlock as a non-blocking probe; does not contend with the running EOD.
    """
    conn = _get_advisory_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (EOD_SESSION_ADVISORY_LOCK_KEY,))
            row = cur.fetchone()
            got = bool(row[0]) if row else False
            if got:
                cur.execute("SELECT pg_advisory_unlock(%s)", (EOD_SESSION_ADVISORY_LOCK_KEY,))
        conn.commit()
        return not bool(got)
    finally:
        conn.close()


@contextmanager
def eod_exclusive_session_lock() -> Iterator[None]:
    """
    Acquire the global EOD session advisory lock for the duration of the context.

    Only one EOD may run at a time (manual UI, scheduler, or backfill). The lock is
    released when the context exits (including on failure); if the process crashes,
    the DB session ends and the lock is released automatically.
    """
    conn = _get_advisory_conn()
    acquired = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (EOD_SESSION_ADVISORY_LOCK_KEY,))
            row = cur.fetchone()
            acquired = bool(row[0]) if row else False
        conn.commit()
        if not acquired:
            raise ConcurrentEODError(
                "Another End-of-Day run is already in progress. "
                "Wait for it to finish before starting again."
            )
        yield
    finally:
        if acquired:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (EOD_SESSION_ADVISORY_LOCK_KEY,))
                conn.commit()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


def start_run(
    *,
    run_id: str,
    as_of_date: date,
    tasks_cfg: dict[str, Any],
    policy_mode: str,
    advance_on_degraded: bool,
) -> None:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO eod_runs (
                    run_id, as_of_date, run_status, policy_mode, advance_on_degraded, tasks_cfg
                )
                VALUES (%s, %s, 'RUNNING', %s, %s, %s::jsonb)
                """,
                (run_id, as_of_date, policy_mode, advance_on_degraded, json.dumps(tasks_cfg or {})),
            )
        conn.commit()


def finish_run(
    *,
    run_id: str,
    run_status: str,
    failed_stage: str | None = None,
    error_message: str | None = None,
) -> None:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE eod_runs
                SET run_status = %s,
                    finished_at = NOW(),
                    failed_stage = %s,
                    error_message = %s
                WHERE run_id = %s
                """,
                (run_status, failed_stage, error_message, run_id),
            )
        conn.commit()


def log_stage_event(
    *,
    run_id: str,
    stage_name: str,
    is_blocking: bool,
    status: str,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO eod_stage_events (
                    run_id, stage_name, is_blocking, status, error_message, details
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    run_id,
                    stage_name,
                    is_blocking,
                    status,
                    error_message,
                    json.dumps(details or {}),
                ),
            )
        conn.commit()

