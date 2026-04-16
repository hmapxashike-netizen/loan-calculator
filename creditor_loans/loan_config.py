"""Build ``LoanConfig`` for the mirror engine from ``creditor_loan_types.behavior_json``."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from eod.loan_daily_engine import LoanConfig

_DEFAULT_WATERFALL: list[str] = [
    "interest_arrears_balance",
    "interest_accrued_balance",
    "principal_arrears",
    "principal_not_due",
    "default_interest_balance",
    "penalty_interest_balance",
    "fees_charges_balance",
]


def loan_config_from_behavior(behavior: dict[str, Any] | None) -> LoanConfig:
    b = behavior or {}
    order = b.get("waterfall_bucket_order")
    if isinstance(order, list) and order:
        wf: list[str] = [str(x) for x in order]
    else:
        wf = list(_DEFAULT_WATERFALL)
    return LoanConfig(
        regular_rate_per_month=Decimal(str(b.get("regular_rate_per_month", 0) or 0)),
        default_interest_absolute_rate_per_month=Decimal(
            str(b.get("default_interest_absolute_rate_per_month", 0) or 0)
        ),
        penalty_interest_absolute_rate_per_month=Decimal(
            str(b.get("penalty_interest_absolute_rate_per_month", 0) or 0)
        ),
        grace_period_days=int(b.get("grace_period_days", 0) or 0),
        penalty_on_principal_arrears_only=bool(b.get("penalty_on_principal_arrears_only", True)),
        waterfall_bucket_order=wf,
        flat_interest=bool(b.get("flat_interest", False)),
    )
