"""
System Business Date: decouples business date from physical calendar.

All loan accrual and "Amount Due" logic must use get_effective_date() instead
of datetime.now() or date.today(). The current_system_date is advanced by +1
day only when EOD completes successfully.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from config import get_database_url
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

_CONFIG_ROW_ID = 1


def _get_conn():
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


def get_effective_date() -> date:
    """
    Returns the current system business date from config.
    Never use datetime.now() or date.today() for accrual or Amount Due logic.
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT current_system_date FROM system_business_config WHERE id = %s",
                    (_CONFIG_ROW_ID,),
                )
                row = cur.fetchone()
                if row and row.get("current_system_date"):
                    d = row["current_system_date"]
                    return d.date() if hasattr(d, "date") else d
    except Exception as e:
        logger.warning("get_effective_date failed, falling back to today: %s", e)
    return date.today()


def get_system_business_config() -> dict[str, Any]:
    """Load system business config: current_system_date, eod_auto_run_time, is_auto_eod_enabled."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT current_system_date, eod_auto_run_time, is_auto_eod_enabled, updated_at
                    FROM system_business_config WHERE id = %s
                    """,
                    (_CONFIG_ROW_ID,),
                )
                row = cur.fetchone()
                if row:
                    d = row["current_system_date"]
                    return {
                        "current_system_date": d.date() if hasattr(d, "date") else d,
                        "eod_auto_run_time": row["eod_auto_run_time"],
                        "is_auto_eod_enabled": bool(row["is_auto_eod_enabled"]),
                        "updated_at": row["updated_at"],
                    }
    except Exception as e:
        logger.warning("get_system_business_config failed: %s", e)
    return {
        "current_system_date": date(2025, 11, 2),
        "eod_auto_run_time": time(23, 0, 0),
        "is_auto_eod_enabled": False,
        "updated_at": None,
    }


def set_current_system_date(new_date: date) -> bool:
    """Update current_system_date. Returns True on success."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE system_business_config
                    SET current_system_date = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (new_date, _CONFIG_ROW_ID),
                )
            conn.commit()
        return True
    except Exception as e:
        logger.error("set_current_system_date failed: %s", e)
        return False


def set_system_business_config(
    *,
    current_system_date: date | None = None,
    eod_auto_run_time: time | None = None,
    is_auto_eod_enabled: bool | None = None,
) -> bool:
    """Update one or more config fields. Returns True on success."""
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                updates = []
                params = []
                if current_system_date is not None:
                    updates.append("current_system_date = %s")
                    params.append(current_system_date)
                if eod_auto_run_time is not None:
                    updates.append("eod_auto_run_time = %s")
                    params.append(eod_auto_run_time)
                if is_auto_eod_enabled is not None:
                    updates.append("is_auto_eod_enabled = %s")
                    params.append(is_auto_eod_enabled)
                if not updates:
                    return True
                updates.append("updated_at = NOW()")
                params.append(_CONFIG_ROW_ID)
                cur.execute(
                    f"UPDATE system_business_config SET {', '.join(updates)} WHERE id = %s",
                    params,
                )
            conn.commit()
        return True
    except Exception as e:
        logger.error("set_system_business_config failed: %s", e)
        return False


def run_eod_process(*, skip_tick: bool = False) -> dict[str, Any]:
    """
    Run EOD for the current system date, then tick +1 day on success.

    1. Performs daily interest accruals for current_system_date.
    2. Processes payments/fees scheduled for that date (via existing EOD logic).
    3. On success: increments current_system_date by +1 day.
    4. Logs completion with both current_system_date and datetime.now().

    Safety: current_system_date does NOT tick if any part of EOD fails.
    Returns dict with keys: success, as_of_date, new_system_date, real_world_time, error.
    """
    from eod import ConcurrentEODError, run_eod_for_date

    cfg = get_system_business_config()
    as_of = cfg["current_system_date"]
    real_before = datetime.now(timezone.utc)

    result = {
        "success": False,
        "as_of_date": as_of,
        "new_system_date": None,
        "real_world_time": real_before.isoformat(),
        "error": None,
        "run_id": None,
        "run_status": None,
        "failed_stage": None,
        "concurrent_eod": False,
    }

    eod_result = None
    try:
        # Guardrail: if the system date was advanced forward by more than +1 day
        # (e.g. a manual update), prevent running EOD in a way that could leave
        # entire dates with blank `loan_daily_state`.
        #
        # We use EOD audit history to determine the last date that would have
        # advanced the system business date, then check whether any skipped
        # days have zero `loan_daily_state` rows.
        try:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT as_of_date
                        FROM eod_runs
                        WHERE finished_at IS NOT NULL
                          AND (
                                run_status = 'SUCCESS'
                             OR (run_status = 'DEGRADED' AND advance_on_degraded = TRUE)
                          )
                        ORDER BY as_of_date DESC, started_at DESC
                        LIMIT 1
                        """
                    )
                    row = cur.fetchone()

                # If eod_runs doesn't exist yet (fresh DB), allow EOD to proceed.
                last_advanced = row["as_of_date"] if row else None

            if last_advanced is not None:
                expected_next = last_advanced + timedelta(days=1)
                if as_of > expected_next:
                    # Check whether any skipped date has zero daily state rows at all.
                    missing_days: list[date] = []
                    missing_start = expected_next
                    missing_end = as_of - timedelta(days=1)
                    if missing_start <= missing_end:
                        with _get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    SELECT d::date
                                    FROM generate_series(%s::date, %s::date, interval '1 day') AS gs(d)
                                    LEFT JOIN loan_daily_state lds
                                           ON lds.as_of_date = d::date
                                    WHERE lds.loan_id IS NULL
                                    ORDER BY d::date
                                    """,
                                    (missing_start, missing_end),
                                )
                                missing_days = [r["d"] for r in cur.fetchall()]  # type: ignore[misc]

                        if missing_days:
                            result["error"] = (
                                "EOD would skip over unprocessed date(s) and could leave blank "
                                "`loan_daily_state` rows. "
                                f"Last advancing EOD was {last_advanced.isoformat()}; "
                                f"current system date is {as_of.isoformat()} (skipped). "
                                f"Missing daily-state dates: {', '.join(d.isoformat() for d in missing_days)}. "
                                "Run EOD sequentially (no gaps) or backfill those dates first."
                            )
                            return result
        except Exception:
            # If audit schema or daily-state query fails, do not block EOD;
            # fallback to current behavior.
            pass

        eod_result = run_eod_for_date(as_of, allow_system_date_eod=True)
    except ConcurrentEODError as e:
        result["error"] = str(e)
        result["concurrent_eod"] = True
        logger.warning("EOD not started (another run in progress) for %s: %s", as_of, e)
        return result
    except Exception as e:
        # str(e) can be uninformative (e.g. KeyError(0) -> "0"); include exception type.
        result["error"] = f"{type(e).__name__}: {e}"
        if hasattr(e, "stage_name"):
            result["failed_stage"] = getattr(e, "stage_name")
        logger.error("EOD failed for %s, system date NOT advanced: %s", as_of, e)
        return result

    if eod_result is not None:
        result["run_id"] = getattr(eod_result, "run_id", None)
        result["run_status"] = getattr(eod_result, "run_status", None)
        result["failed_stage"] = getattr(eod_result, "failed_stage", None)
        if getattr(eod_result, "run_status", "SUCCESS") == "FAILED":
            result["error"] = getattr(eod_result, "error_message", None) or "EOD failed."
            return result
        # Hard guardrail: never advance business date if today's EOD date is
        # left blank for any eligible active loan (active + has at least one schedule row).
        try:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)::int AS n
                        FROM loans l
                        WHERE l.status = 'active'
                          AND EXISTS (
                              SELECT 1
                              FROM loan_schedules ls
                              WHERE ls.loan_id = l.id
                          )
                        """
                    )
                    eligible_active = int((cur.fetchone() or {}).get("n") or 0)

                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT lds.loan_id)::int AS n
                        FROM loan_daily_state lds
                        JOIN loans l ON l.id = lds.loan_id
                        WHERE lds.as_of_date = %s
                          AND l.status = 'active'
                          AND EXISTS (
                              SELECT 1
                              FROM loan_schedules ls
                              WHERE ls.loan_id = l.id
                          )
                        """,
                        (as_of,),
                    )
                    with_state = int((cur.fetchone() or {}).get("n") or 0)
            if eligible_active > with_state:
                result["error"] = (
                    "EOD completed but date advance is blocked: "
                    f"blank `loan_daily_state` detected for {as_of.isoformat()} "
                    f"({with_state}/{eligible_active} eligible active loan(s) populated). "
                    "Backfill/fix daily state first, then retry."
                )
                return result
        except Exception:
            # Non-blocking on metadata check failure; keep existing EOD behavior.
            pass
        if not skip_tick and not getattr(eod_result, "should_advance_date", True):
            result["error"] = (
                f"EOD completed as {getattr(eod_result, 'run_status', 'DEGRADED')}; "
                "policy forbids system date advance."
            )
            return result

    if not skip_tick:
        next_date = as_of + timedelta(days=1)
        if set_current_system_date(next_date):
            result["success"] = True
            result["new_system_date"] = next_date
            real_after = datetime.now(timezone.utc)
            logger.info(
                "EOD completed: as_of=%s, new_system_date=%s, real_world=%s",
                as_of,
                next_date,
                real_after.isoformat(),
            )
        else:
            result["error"] = "Failed to advance system date after EOD"
    else:
        result["success"] = True
        result["new_system_date"] = as_of

    return result


def should_trigger_auto_eod() -> bool:
    """
    True if is_auto_eod_enabled and current real-world time has passed eod_auto_run_time.
    Uses server local time for comparison.
    """
    cfg = get_system_business_config()
    if not cfg["is_auto_eod_enabled"]:
        return False
    run_time = cfg["eod_auto_run_time"]
    if hasattr(run_time, "hour"):
        target = datetime.now().replace(
            hour=run_time.hour,
            minute=run_time.minute,
            second=run_time.second or 0,
            microsecond=0,
        )
    else:
        target = datetime.now().replace(hour=23, minute=0, second=0, microsecond=0)
    return datetime.now() >= target
