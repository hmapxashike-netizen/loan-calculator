"""Unit tests for eod.loan_daily_engine due-date interest billing (gross accrued → arrears)."""

from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal

from eod.loan_daily_engine import Loan, LoanConfig, ScheduleEntry


def _minimal_config() -> LoanConfig:
    return LoanConfig(
        regular_rate_per_month=Decimal("0"),
        default_interest_absolute_rate_per_month=Decimal("0"),
        penalty_interest_absolute_rate_per_month=Decimal("0"),
        grace_period_days=999,
        waterfall_bucket_order=["principal_arrears"],
    )


class TestGrossInterestBillingOnDueDate(unittest.TestCase):
    def test_full_period_accrued_moves_to_arrears_on_due_date(self) -> None:
        """After daily accrual through day before due, T bills from accrued to interest_arrears."""
        disb = date(2024, 4, 1)
        due = date(2024, 4, 30)
        interest_component = Decimal("201")
        entry = ScheduleEntry(
            period_start=disb,
            due_date=due,
            principal_component=Decimal("0"),
            interest_component=interest_component,
        )
        loan = Loan(
            loan_id="1",
            disbursement_date=disb,
            original_principal=Decimal("10000"),
            config=_minimal_config(),
            schedule=[entry],
        )
        d = disb
        while d < due:
            loan.process_day(d)
            d += timedelta(days=1)
        self.assertGreater(loan.interest_accrued_balance, Decimal("0"))
        self.assertEqual(loan.interest_arrears, Decimal("0"))
        loan.process_day(due)
        self.assertEqual(loan.interest_arrears, interest_component)
        # Daily _q10 accrual sum can differ from contractual T by negligible dust.
        self.assertLessEqual(loan.interest_accrued_balance, Decimal("0.0000001"))

    def test_interest_arrears_grows_by_t_even_if_accrued_short(self) -> None:
        """T still bills to arrears; accrued floors at zero when balance < T."""
        disb = date(2024, 4, 1)
        due = date(2024, 4, 2)
        T = Decimal("100")
        entry = ScheduleEntry(
            period_start=disb,
            due_date=due,
            principal_component=Decimal("0"),
            interest_component=T,
        )
        loan = Loan(
            loan_id="1",
            disbursement_date=disb,
            original_principal=Decimal("5000"),
            config=_minimal_config(),
            schedule=[entry],
        )
        loan.process_day(disb)
        self.assertEqual(loan.interest_accrued_balance, T)
        loan.interest_accrued_balance = Decimal("30")
        loan.process_day(due)
        self.assertEqual(loan.interest_accrued_balance, Decimal("0"))
        self.assertEqual(loan.interest_arrears, T)


if __name__ == "__main__":
    unittest.main()
