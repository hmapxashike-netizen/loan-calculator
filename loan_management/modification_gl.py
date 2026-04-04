"""GL posting helpers for loan modification approval (write-off, top-up)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from .approval_journal import build_loan_approval_journal_payload
from .cash_gl import _post_event_for_loan


def post_principal_writeoff_for_loan(
    loan_id: int,
    amount: Decimal | float,
    *,
    entry_date: date,
    created_by: str = "system",
    unique_suffix: str = "",
) -> None:
    """Post PRINCIPAL_WRITEOFF journals (allowance vs loan principal) when templates exist."""
    if amount is None or float(amount) <= 0:
        return
    amt = as_10dp(Decimal(str(amount)))
    payload: dict[str, Any] = {
        "allowance_credit_losses": amt,
        "loan_principal": amt,
    }
    try:
        from accounting.service import AccountingService

        svc = AccountingService()
        _post_event_for_loan(
            svc,
            int(loan_id),
            event_type="PRINCIPAL_WRITEOFF",
            reference=f"MOD-WRITELOFF-{loan_id}",
            description=f"Loan modification principal write-off (loan {loan_id})",
            event_id=f"MOD-WO-{loan_id}-{entry_date.isoformat()}-{unique_suffix or 'x'}",
            created_by=created_by,
            entry_date=entry_date,
            payload=payload,
        )
    except Exception as e:
        print(f"post_principal_writeoff_for_loan failed: {e}")


def post_modification_topup_disbursement(
    loan_id: int,
    topup_amount: Decimal | float,
    *,
    entry_date: date,
    created_by: str = "system",
    unique_suffix: str = "",
) -> None:
    """
    Post additional disbursement as LOAN_APPROVAL-style entry (Dr principal, Cr cash).
    Fees zero; gross principal increase equals cash out.
    """
    if topup_amount is None or float(topup_amount) <= 0:
        return
    ta = float(as_10dp(Decimal(str(topup_amount))))
    details = {
        "principal": ta,
        "disbursed_amount": ta,
        "drawdown_fee": 0.0,
        "arrangement_fee": 0.0,
        "admin_fee": 0.0,
    }
    payload = build_loan_approval_journal_payload(details)
    try:
        from accounting.service import AccountingService

        svc = AccountingService()
        _post_event_for_loan(
            svc,
            int(loan_id),
            event_type="LOAN_APPROVAL",
            reference=f"MOD-TOPUP-{loan_id}",
            description=f"Loan modification top-up disbursement (loan {loan_id})",
            event_id=f"MOD-TOPUP-{loan_id}-{entry_date.isoformat()}-{unique_suffix or 'x'}",
            created_by=created_by,
            entry_date=entry_date,
            payload=payload,
        )
    except Exception as e:
        print(f"post_modification_topup_disbursement failed: {e}")
