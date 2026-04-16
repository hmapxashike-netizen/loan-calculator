"""Creditor facility / drawdown: EOM interest helper and periodic EOD billing."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


def test_periodic_scheduled_interest_in_calendar_month_sums_matching_dates():
    from creditor_loans.creditor_eom_interest import periodic_scheduled_interest_in_calendar_month

    cur = MagicMock()
    cur.fetchall.return_value = [
        {"Date": "2024-01-15", "interest": "10"},
        {"Date": "2024-02-10", "interest": "20"},
        {"Date": "2024-03-05", "interest": "99"},
    ]
    m = date(2024, 2, 1)
    end = date(2024, 2, 29)
    got = periodic_scheduled_interest_in_calendar_month(cur, 1, m, end)
    assert got == Decimal("20")


@patch("creditor_loans.periodic_engine.save_creditor_loan_daily_state")
def test_run_periodic_creditor_drawdown_bills_principal_interest_on_due_date(mock_save):
    from creditor_loans.periodic_engine import run_periodic_creditor_drawdown_for_date

    conn = MagicMock()
    drawdown = {"id": 7, "disbursement_date": date(2024, 1, 1), "principal": 10000.0}
    schedule_rows = [{"Date": date(2024, 2, 1), "principal": 100.0, "interest": 25.0}]
    yesterday_saved = {
        "principal_not_due": 10000.0,
        "principal_arrears": 0.0,
        "interest_accrued_balance": 0.0,
        "interest_arrears_balance": 0.0,
        "default_interest_balance": 0.0,
        "penalty_interest_balance": 0.0,
        "fees_charges_balance": 0.0,
        "days_overdue": 0,
    }
    run_periodic_creditor_drawdown_for_date(
        conn,
        drawdown,
        schedule_rows,
        date(2024, 2, 1),
        yesterday=date(2024, 1, 31),
        alloc={},
        yesterday_saved=yesterday_saved,
        block_accruals=False,
    )
    mock_save.assert_called_once()
    args, kwargs = mock_save.call_args
    assert args[0] == 7
    assert pytest.approx(kwargs["principal_not_due"], rel=1e-9) == 9900.0
    assert pytest.approx(kwargs["principal_arrears"], rel=1e-9) == 100.0
    assert pytest.approx(float(kwargs["interest_arrears_balance"]), rel=1e-9) == 25.0


def test_schema_migration_90_file_exists():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    assert (root / "schema" / "90_creditor_facilities_drawdowns.sql").is_file()
    assert (root / "scripts" / "run_migration_90.py").is_file()
