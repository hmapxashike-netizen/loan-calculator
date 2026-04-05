"""Tests for accrual convention (canonical period-first schedule accrual) and engine windows."""
from datetime import date, timedelta
from decimal import Decimal

import pytest

from accrual_convention import (
    ACCRUAL_START_EFFECTIVE_DAY,
    accrual_start_convention_from_config,
    normalize_accrual_start_convention,
)
from eod.core import _validate_schedule_accrual_periods
from eod.loan_daily_engine import Loan, LoanConfig, ScheduleEntry


@pytest.mark.parametrize("raw", [None, "", "NEXT_DAY", "effective_day", "bogus"])
def test_normalize_always_effective_day(raw):
    assert normalize_accrual_start_convention(raw) == ACCRUAL_START_EFFECTIVE_DAY


def test_accrual_start_convention_from_config():
    assert accrual_start_convention_from_config(None) == ACCRUAL_START_EFFECTIVE_DAY
    assert accrual_start_convention_from_config({}) == ACCRUAL_START_EFFECTIVE_DAY
    assert (
        accrual_start_convention_from_config({"accrual_start_convention": "NEXT_DAY"})
        == ACCRUAL_START_EFFECTIVE_DAY
    )


def _minimal_loan() -> Loan:
    ps = date(2026, 1, 1)
    due = date(2026, 1, 31)
    cfg = LoanConfig(
        regular_rate_per_month=Decimal("0"),
        default_interest_absolute_rate_per_month=Decimal("0"),
        penalty_interest_absolute_rate_per_month=Decimal("0"),
        grace_period_days=999,
        waterfall_bucket_order=["principal_not_due"],
    )
    entry = ScheduleEntry(
        period_start=ps,
        due_date=due,
        principal_component=Decimal("0"),
        interest_component=Decimal("300"),
    )
    return Loan(
        loan_id="1",
        disbursement_date=ps,
        original_principal=Decimal("10000"),
        config=cfg,
        schedule=[entry],
    )


def test_disbursement_day_accrues():
    loan = _minimal_loan()
    assert loan._scheduled_interest_for_day(date(2026, 1, 1)) > 0
    assert loan._scheduled_interest_for_day(date(2026, 1, 30)) > 0
    assert loan._scheduled_interest_for_day(date(2026, 1, 31)) == Decimal("0")


def test_total_regular_over_period_matches_interest_component():
    """30 accrual days (1 Jan – 30 Jan) × daily = full interest_component."""
    loan = _minimal_loan()
    interest = Decimal("300")
    total = Decimal("0")
    d = date(2026, 1, 1)
    while d <= date(2026, 1, 31):
        loan.process_day(d)
        total += loan.last_regular_interest_daily
        d += timedelta(days=1)
    assert total == interest


def test_validate_schedule_rejects_period_gap():
    e1 = ScheduleEntry(
        date(2025, 1, 1),
        date(2025, 1, 31),
        Decimal("100"),
        Decimal("10"),
    )
    e2 = ScheduleEntry(
        date(2025, 2, 2),
        date(2025, 2, 28),
        Decimal("100"),
        Decimal("10"),
    )
    with pytest.raises(ValueError, match="chain"):
        _validate_schedule_accrual_periods([e1, e2], date(2025, 1, 1))


def test_validate_schedule_rejects_first_period_before_disbursement():
    e1 = ScheduleEntry(
        date(2024, 12, 1),
        date(2025, 1, 31),
        Decimal("100"),
        Decimal("10"),
    )
    with pytest.raises(ValueError, match="first period_start"):
        _validate_schedule_accrual_periods([e1], date(2025, 1, 1))


def test_validate_schedule_allows_first_period_on_or_after_disbursement():
    """Recast schedules may start the first engine period after the original disbursement date."""
    e1 = ScheduleEntry(
        date(2025, 2, 1),
        date(2025, 2, 28),
        Decimal("100"),
        Decimal("10"),
    )
    _validate_schedule_accrual_periods([e1], date(2025, 1, 1))
