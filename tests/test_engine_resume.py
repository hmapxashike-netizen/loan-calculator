"""Engine resume JSON for incremental EOD (serialize/hydrate + invalidation helpers)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from eod.core import _bumps_invalidate_incremental_resume
from eod.engine_resume import (
    ENGINE_RESUME_SCHEMA_VERSION,
    apply_engine_resume,
    engine_resume_is_valid_schema,
    product_code_matches_resume,
    serialize_engine_resume,
)
from eod.loan_daily_engine import Loan, LoanConfig, ScheduleEntry


def _minimal_loan() -> Loan:
    cfg = LoanConfig(
        regular_rate_per_month=Decimal("0.1"),
        default_interest_absolute_rate_per_month=Decimal("0"),
        penalty_interest_absolute_rate_per_month=Decimal("0"),
        waterfall_bucket_order=[
            "fees_charges_balance",
            "penalty_interest_balance",
            "default_interest_balance",
            "interest_arrears_balance",
            "interest_accrued_balance",
            "principal_arrears",
            "principal_not_due",
        ],
    )
    entries = [
        ScheduleEntry(
            period_start=date(2025, 1, 1),
            due_date=date(2025, 2, 1),
            principal_component=Decimal("100"),
            interest_component=Decimal("10"),
        )
    ]
    loan = Loan(
        loan_id="1",
        disbursement_date=date(2025, 1, 1),
        original_principal=Decimal("1000"),
        config=cfg,
        schedule=entries,
    )
    setattr(loan, "_eod_schedule_version", 1)
    loan.process_day(date(2025, 1, 5))
    return loan


def test_serialize_roundtrip_restores_engine_fields():
    loan = _minimal_loan()
    payload = serialize_engine_resume(loan, product_code="TERM")
    assert payload["v"] == ENGINE_RESUME_SCHEMA_VERSION
    assert payload["product_code"] == "TERM"

    loan2 = _minimal_loan()
    apply_engine_resume(loan2, payload)

    assert loan2.principal_not_due == loan.principal_not_due
    assert loan2.interest_accrued_balance == loan.interest_accrued_balance
    assert loan2.days_overdue == loan.days_overdue
    assert loan2.current_period_start == loan.current_period_start
    assert getattr(loan2, "_eod_schedule_version") == getattr(loan, "_eod_schedule_version")


def test_engine_resume_schema_rejects_wrong_version():
    assert not engine_resume_is_valid_schema({"v": 0})
    assert not engine_resume_is_valid_schema(None)


def test_product_code_matches_resume():
    p = serialize_engine_resume(_minimal_loan(), product_code="A")
    assert product_code_matches_resume(p, {"product_code": "A"})
    assert not product_code_matches_resume(p, {"product_code": "B"})


def test_bumps_invalidate_mid_gap_only():
    resume_anchor = date(2025, 1, 9)
    as_of = date(2025, 1, 14)
    bumps = [(date(2025, 1, 11), 2)]
    assert _bumps_invalidate_incremental_resume(bumps, resume_anchor, as_of)

    bumps_edge = [(date(2025, 1, 9), 2)]
    assert not _bumps_invalidate_incremental_resume(bumps_edge, resume_anchor, as_of)

    bumps_today = [(date(2025, 1, 14), 2)]
    assert not _bumps_invalidate_incremental_resume(bumps_today, resume_anchor, as_of)
