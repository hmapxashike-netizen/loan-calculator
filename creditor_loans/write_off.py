"""Post creditor-specific write-off journals."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from decimal_utils import as_10dp

from .persistence import get_creditor_loan


def post_creditor_writeoff(
    creditor_loan_id: int,
    *,
    principal_amount: Decimal | float = 0,
    interest_amount: Decimal | float = 0,
    entry_date: date,
    created_by: str = "creditor_writeoff_ui",
    reference: str | None = None,
) -> None:
    row = get_creditor_loan(creditor_loan_id)
    if not row:
        raise ValueError("Creditor facility not found.")
    cf_id = int(row["creditor_facility_id"])

    pa = as_10dp(Decimal(str(principal_amount or 0)))
    ia = as_10dp(Decimal(str(interest_amount or 0)))
    if pa <= 0 and ia <= 0:
        raise ValueError("At least one of principal_amount or interest_amount must be positive.")

    from accounting.service import AccountingService

    svc = AccountingService()
    ref = (reference or "").strip() or None
    if pa > 0:
        svc.post_event(
            event_type="CREDITOR_PRINCIPAL_WRITEOFF",
            reference=ref,
            description=f"Creditor principal write-off CL-{creditor_loan_id}",
            event_id=f"CL-WO-P-{creditor_loan_id}-{entry_date.isoformat()}",
            created_by=created_by,
            entry_date=entry_date,
            amount=pa,
            payload={"borrowings_loan_principal": pa, "creditor_loan_forgiveness_income": pa},
            loan_id=None,
            creditor_drawdown_id=int(creditor_loan_id),
            creditor_facility_id=cf_id,
        )
    if ia > 0:
        svc.post_event(
            event_type="CREDITOR_INTEREST_WRITEOFF",
            reference=ref,
            description=f"Creditor interest write-off CL-{creditor_loan_id}",
            event_id=f"CL-WO-I-{creditor_loan_id}-{entry_date.isoformat()}",
            created_by=created_by,
            entry_date=entry_date,
            amount=ia,
            payload={"interest_payable": ia, "creditor_loan_forgiveness_income": ia},
            loan_id=None,
            creditor_drawdown_id=int(creditor_loan_id),
            creditor_facility_id=cf_id,
        )
