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
    from eod import run_eod_for_date

    cfg = get_system_business_config()
    as_of = cfg["current_system_date"]
    real_before = datetime.now(timezone.utc)

    result = {
        "success": False,
        "as_of_date": as_of,
        "new_system_date": None,
        "real_world_time": real_before.isoformat(),
        "error": None,
    }

    try:
        run_eod_for_date(as_of)
    except Exception as e:
        result["error"] = str(e)
        logger.error("EOD failed for %s, system date NOT advanced: %s", as_of, e)
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
