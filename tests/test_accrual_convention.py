"""Tests for accrual start convention helpers and engine day windows."""
from datetime import date
from decimal import Decimal

import pytest

from accrual_convention import (
    ACCRUAL_START_EFFECTIVE_DAY,
    ACCRUAL_START_NEXT_DAY,
    accrual_start_convention_from_config,
    normalize_accrual_start_convention,
)
from eod.loan_daily_engine import Loan, LoanConfig, ScheduleEntry


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, ACCRUAL_START_NEXT_DAY),
        ("", ACCRUAL_START_NEXT_DAY),
        ("next_day", ACCRUAL_START_NEXT_DAY),
        ("NEXT_DAY", ACCRUAL_START_NEXT_DAY),
        ("OPTION_2", ACCRUAL_START_NEXT_DAY),
        ("effective_day", ACCRUAL_START_EFFECTIVE_DAY),
        ("EFFECTIVE_DAY", ACCRUAL_START_EFFECTIVE_DAY),
        ("OPTION_1", ACCRUAL_START_EFFECTIVE_DAY),
        ("bogus", ACCRUAL_START_NEXT_DAY),
    ],
)
def test_normalize_accrual_start_convention(raw, expected):
    assert normalize_accrual_start_convention(raw) == expected


def test_accrual_start_convention_from_config():
    assert accrual_start_convention_from_config(None) == ACCRUAL_START_NEXT_DAY
    assert accrual_start_convention_from_config({}) == ACCRUAL_START_NEXT_DAY
    assert (
        accrual_start_convention_from_config({"accrual_start_convention": "EFFECTIVE_DAY"})
        == ACCRUAL_START_EFFECTIVE_DAY
    )


def _minimal_loan(convention: str) -> Loan:
    ps = date(2026, 1, 1)
    due = date(2026, 1, 31)
    cfg = LoanConfig(
        regular_rate_per_month=Decimal("0"),
        default_interest_absolute_rate_per_month=Decimal("0"),
        penalty_interest_absolute_rate_per_month=Decimal("0"),
        grace_period_days=999,
        waterfall_bucket_order=["principal_not_due"],
        accrual_start_convention=convention,
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


def test_next_day_first_accrual_is_day_after_period_start():
    loan = _minimal_loan(ACCRUAL_START_NEXT_DAY)
    assert loan._scheduled_interest_for_day(date(2026, 1, 1)) == Decimal("0")
    assert loan._scheduled_interest_for_day(date(2026, 1, 2)) > 0
    assert loan._scheduled_interest_for_day(date(2026, 1, 31)) > 0


def test_effective_day_first_accrual_is_period_start():
    loan = _minimal_loan(ACCRUAL_START_EFFECTIVE_DAY)
    assert loan._scheduled_interest_for_day(date(2026, 1, 1)) > 0
    assert loan._scheduled_interest_for_day(date(2026, 1, 31)) == Decimal("0")
    assert loan._scheduled_interest_for_day(date(2026, 1, 30)) > 0


def test_same_total_accrued_interest_over_full_period():
    """Both conventions: 30 accrual days × daily = full interest_component for Jan 1–Jan 31 period."""
    from datetime import timedelta

    total_days = 30
    interest = Decimal("300")
    daily = interest / Decimal(total_days)

    def total_regular_through(loan: Loan, last: date) -> Decimal:
        t = Decimal("0")
        d = date(2026, 1, 1)
        while d <= last:
            loan.process_day(d)
            t += loan.last_regular_interest_daily
            d += timedelta(days=1)
        return t

    loan_n = _minimal_loan(ACCRUAL_START_NEXT_DAY)
    loan_e = _minimal_loan(ACCRUAL_START_EFFECTIVE_DAY)
    # Through Jan 30: effective has full 30 days accrued; next_day has 29 (Jan 2–Jan 30)
    t_n_30 = total_regular_through(loan_n, date(2026, 1, 30))
    t_e_30 = total_regular_through(loan_e, date(2026, 1, 30))
    assert t_e_30 == interest
    assert t_n_30 == 29 * daily

    loan_n2 = _minimal_loan(ACCRUAL_START_NEXT_DAY)
    t_n_31 = total_regular_through(loan_n2, date(2026, 1, 31))
    assert t_n_31 == interest

    loan_e2 = _minimal_loan(ACCRUAL_START_EFFECTIVE_DAY)
    t_e_31 = total_regular_through(loan_e2, date(2026, 1, 31))
    # Jan 31: no regular accrual; transition moves full instalment interest to arrears
    assert t_e_31 == interest
