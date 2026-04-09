"""
End-of-day workflow: database and orchestration only.

Callers (e.g. Streamlit UI) handle presentation, session state, and spinners.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from eod.core import run_eod_for_date, run_single_loan_eod
from eod.audit import is_another_eod_session_active
from loan_management import (
    _connection,
    get_repayment_ids_for_loan_and_date,
    load_system_config_from_db,
    reallocate_repayment,
)
from psycopg2.extras import RealDictCursor
from eod.system_business_date import (
    get_system_business_config,
    run_eod_process,
)


def get_eod_business_context() -> dict[str, Any]:
    """Current system business date and next calendar day (UI headers)."""
    sb_cfg = get_system_business_config()
    current = sb_cfg["current_system_date"]
    return {
        "current_system_date": current,
        "next_system_date": current + timedelta(days=1),
    }


def run_full_eod_advance_system_date(*, skip_tick: bool = False) -> dict[str, Any]:
    """EOD for current system date; advances system date on success (unless skip_tick)."""
    return run_eod_process(skip_tick=skip_tick)


def run_backfill_eod_for_date(as_of_date: date):
    """Backfill EOD for a date; does not advance system date."""
    return run_eod_for_date(as_of_date, replay_refresh_allocations=True)


def is_another_eod_session_active_safe() -> bool:
    """True if probe reports another session; False if probe fails or says no."""
    try:
        return bool(is_another_eod_session_active())
    except Exception:
        return False


def count_loans_with_daily_state_vs_active(as_of_date: date) -> tuple[int, int]:
    """
    (loans_with_state_for_date, active_loan_count) for EOD pre-run warnings.
    On DB errors returns (0, 0), matching prior UI behavior.
    """
    loans_with_state = 0
    active_loans = 0
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT loan_id) AS n
                    FROM loan_daily_state
                    WHERE as_of_date = %s
                    """,
                    (as_of_date,),
                )
                row = cur.fetchone() or {}
                loans_with_state = int(row.get("n") or 0)
                cur.execute("SELECT COUNT(*) AS n FROM loans WHERE status = 'active'")
                row2 = cur.fetchone() or {}
                active_loans = int(row2.get("n") or 0)
    except Exception:
        pass
    return loans_with_state, active_loans


def parse_repayment_id_lines(text: str) -> tuple[list[int], str | None]:
    """
    Parse one-per-line or comma-separated repayment IDs.
    Returns (ids, bad_token) where bad_token is set on first invalid token.
    """
    raw = (text or "").replace(",", "\n").splitlines()
    parsed: list[int] = []
    for line in raw:
        s = line.strip()
        if not s:
            continue
        try:
            parsed.append(int(s))
        except ValueError:
            return [], s
    return parsed, None


def reallocate_repayments_for_ids(
    repayment_ids: list[int],
    *,
    system_config: dict[str, Any] | None = None,
) -> tuple[list[int], list[tuple[int, str]]]:
    cfg = system_config if system_config is not None else load_system_config_from_db() or {}
    ok: list[int] = []
    err: list[tuple[int, str]] = []
    for rid in repayment_ids:
        try:
            reallocate_repayment(rid, system_config=cfg)
            ok.append(rid)
        except Exception as ex:
            err.append((rid, str(ex)))
    return ok, err


def list_repayment_ids_for_loan_value_date(loan_id: int, value_date: date) -> list[int]:
    """Posted receipt IDs for loan + value date (empty if none)."""
    return get_repayment_ids_for_loan_and_date(int(loan_id), value_date)


def recompute_single_loan_daily_state(
    loan_id: int,
    as_of_date: date,
    *,
    system_config: dict[str, Any] | None = None,
) -> None:
    cfg = system_config if system_config is not None else load_system_config_from_db() or {}
    run_single_loan_eod(int(loan_id), as_of_date, sys_cfg=cfg)
