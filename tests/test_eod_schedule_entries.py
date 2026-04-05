"""EOD schedule entry building (Period 0 / recast openings)."""

from datetime import date
from decimal import Decimal

from eod.core import _build_schedule_entries


def test_period_zero_sets_next_accrual_start_not_long_zero_interest_span():
    loan_row = {"disbursement_date": date(2025, 1, 1)}
    rows = [
        {
            "Period": 0,
            "Date": "06-Apr-2025",
            "principal": 0,
            "interest": 0,
        },
        {
            "Period": 1,
            "Date": "06-May-2025",
            "principal": Decimal("10"),
            "interest": Decimal("30"),
        },
    ]
    entries = _build_schedule_entries(loan_row, rows)
    assert len(entries) == 1
    assert entries[0].period_start == date(2025, 4, 6)
    assert entries[0].due_date == date(2025, 5, 6)
    assert entries[0].interest_component == Decimal("30")


def test_original_term_period_zero_at_disbursement_skipped():
    loan_row = {"disbursement_date": date(2025, 1, 1)}
    rows = [
        {"Period": 0, "Date": "01-Jan-2025", "principal": 0, "interest": 0},
        {
            "Period": 1,
            "Date": "01-Feb-2025",
            "principal": Decimal("100"),
            "interest": Decimal("10"),
        },
    ]
    entries = _build_schedule_entries(loan_row, rows)
    assert len(entries) == 1
    assert entries[0].period_start == date(2025, 1, 1)
    assert entries[0].due_date == date(2025, 2, 1)
