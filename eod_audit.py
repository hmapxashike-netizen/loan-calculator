from __future__ import annotations

import json
from datetime import date
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url


def _get_conn():
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


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

