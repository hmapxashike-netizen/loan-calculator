"""
Persisted accrual-engine snapshots for incremental EOD.

``loan_daily_state`` merged columns reflect allocations and guards; they are not
safe for hydrating :class:`eod.loan_daily_engine.Loan`. This module defines a
versioned JSON contract for the raw engine state at end of ``as_of_date``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from eod.loan_daily_engine import Loan

ENGINE_RESUME_SCHEMA_VERSION = 1


def _dstr(v: Any) -> str:
    if isinstance(v, Decimal):
        return format(v, "f")
    return format(Decimal(str(v)), "f")


def serialize_engine_resume(
    engine_loan: Loan,
    *,
    product_code: str | None,
) -> dict[str, Any]:
    """Build JSON-serializable dict for ``loan_daily_state.engine_resume``."""
    vsched = getattr(engine_loan, "_eod_schedule_version", None)
    return {
        "v": ENGINE_RESUME_SCHEMA_VERSION,
        "product_code": (str(product_code).strip() if product_code else None),
        "eod_schedule_version": int(vsched) if vsched is not None else 1,
        "principal_not_due": _dstr(engine_loan.principal_not_due),
        "principal_arrears": _dstr(engine_loan.principal_arrears),
        "interest_accrued_balance": _dstr(engine_loan.interest_accrued_balance),
        "interest_arrears": _dstr(engine_loan.interest_arrears),
        "default_interest_balance": _dstr(engine_loan.default_interest_balance),
        "penalty_interest_balance": _dstr(engine_loan.penalty_interest_balance),
        "fees_charges_balance": _dstr(engine_loan.fees_charges_balance),
        "last_regular_interest_daily": _dstr(engine_loan.last_regular_interest_daily),
        "last_default_interest_daily": _dstr(engine_loan.last_default_interest_daily),
        "last_penalty_interest_daily": _dstr(engine_loan.last_penalty_interest_daily),
        "days_overdue_engine": int(engine_loan.days_overdue),
        "current_period_start": engine_loan.current_period_start.isoformat(),
        "regular_interest_period_to_date": _dstr(engine_loan.regular_interest_period_to_date),
        "penalty_interest_period_to_date": _dstr(engine_loan.penalty_interest_period_to_date),
        "default_interest_period_to_date": _dstr(engine_loan.default_interest_period_to_date),
    }


def _dec(payload: dict[str, Any], key: str) -> Decimal:
    return Decimal(str(payload[key]))


def parse_engine_resume_dict(raw: Any) -> dict[str, Any] | None:
    """Accept dict or JSON-parsed object; return None if unusable."""
    if raw is None:
        return None
    if isinstance(raw, str):
        import json

        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if not isinstance(raw, dict):
        return None
    return raw


def engine_resume_is_valid_schema(payload: dict[str, Any] | None) -> bool:
    if not payload or int(payload.get("v") or 0) != ENGINE_RESUME_SCHEMA_VERSION:
        return False
    keys = (
        "eod_schedule_version",
        "principal_not_due",
        "principal_arrears",
        "interest_accrued_balance",
        "interest_arrears",
        "default_interest_balance",
        "penalty_interest_balance",
        "fees_charges_balance",
        "last_regular_interest_daily",
        "last_default_interest_daily",
        "last_penalty_interest_daily",
        "days_overdue_engine",
        "current_period_start",
        "regular_interest_period_to_date",
        "penalty_interest_period_to_date",
        "default_interest_period_to_date",
    )
    return all(k in payload for k in keys)


def apply_engine_resume(engine_loan: Loan, payload: dict[str, Any]) -> None:
    """
    Overwrite accrual-engine fields after ``Loan`` construction (schedule loaded).

    Clears ``interest_accrued_table``; skipped days are not needed for forward
    ``process_day`` in the current engine.
    """
    engine_loan.principal_not_due = as_10dp(_dec(payload, "principal_not_due"))
    engine_loan.principal_arrears = as_10dp(_dec(payload, "principal_arrears"))
    engine_loan.interest_accrued_balance = as_10dp(_dec(payload, "interest_accrued_balance"))
    engine_loan.interest_arrears = as_10dp(_dec(payload, "interest_arrears"))
    engine_loan.default_interest_balance = as_10dp(_dec(payload, "default_interest_balance"))
    engine_loan.penalty_interest_balance = as_10dp(_dec(payload, "penalty_interest_balance"))
    engine_loan.fees_charges_balance = as_10dp(_dec(payload, "fees_charges_balance"))
    engine_loan.last_regular_interest_daily = as_10dp(_dec(payload, "last_regular_interest_daily"))
    engine_loan.last_default_interest_daily = as_10dp(_dec(payload, "last_default_interest_daily"))
    engine_loan.last_penalty_interest_daily = as_10dp(_dec(payload, "last_penalty_interest_daily"))
    engine_loan.days_overdue = int(payload["days_overdue_engine"])
    engine_loan.current_period_start = date.fromisoformat(str(payload["current_period_start"]))
    engine_loan.regular_interest_period_to_date = as_10dp(_dec(payload, "regular_interest_period_to_date"))
    engine_loan.penalty_interest_period_to_date = as_10dp(_dec(payload, "penalty_interest_period_to_date"))
    engine_loan.default_interest_period_to_date = as_10dp(_dec(payload, "default_interest_period_to_date"))
    setattr(engine_loan, "_eod_schedule_version", int(payload["eod_schedule_version"]))
    engine_loan.interest_accrued_table.clear()
    engine_loan.current_date = None


def product_code_matches_resume(payload: dict[str, Any], loan_row: dict[str, Any]) -> bool:
    """False if product changed since resume was written."""
    saved = payload.get("product_code")
    cur = loan_row.get("product_code")
    cur_s = str(cur).strip() if cur else None
    saved_s = str(saved).strip() if saved else None
    return saved_s == cur_s
