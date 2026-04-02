"""Loan-management-specific exceptions."""

from __future__ import annotations

from datetime import date


class NeedOverpaymentDecision(Exception):
    """Reserved for future use. Standard waterfall now sends overpayment to unapplied by default (no raise)."""

    def __init__(self, repayment_id: int, loan_id: int, amount_remaining: float, effective_date: date):
        self.repayment_id = repayment_id
        self.loan_id = loan_id
        self.amount_remaining = amount_remaining
        self.effective_date = effective_date
        super().__init__(
            f"Overpayment at waterfall step 6: repayment_id={repayment_id} loan_id={loan_id} "
            f"amount_remaining={amount_remaining}"
        )
