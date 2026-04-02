"""Retroactive GL reversal journals for a receipt's allocation buckets."""

from __future__ import annotations

from decimal import Decimal

from .allocation_queries import _get_allocation_sum_for_repayment
from .cash_gl import _post_event_for_loan
from .db import RealDictCursor, _connection
from .unapplied_refs import _repayment_journal_reference


def post_receipt_allocation_gl_reversals(original_repayment_id: int) -> None:
    """
    Retroactively post GL reversal journals for a receipt's allocation buckets.
    This is useful when a previous reversal created allocation rows/state but the
    GL journals did not get posted for the receipt's own PAYMENT_* allocations.
    """
    alloc_row = _get_allocation_sum_for_repayment(original_repayment_id)
    if not alloc_row:
        return

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT loan_id, COALESCE(value_date, payment_date) AS eff_date
                FROM loan_repayments
                WHERE id = %s
                """,
                (original_repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                return

            loan_id = int(row["loan_id"])
            eff_date = row.get("eff_date")
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date() if callable(getattr(eff_date, "date")) else eff_date

    try:
        from accounting_service import AccountingService

        svc_alloc = AccountingService()
    except Exception:
        return

    def _p(v):
        return float(v or 0)

    _rj = _repayment_journal_reference(loan_id, original_repayment_id)

    # Principal
    prin_arr = _p(alloc_row.get("alloc_principal_arrears"))
    if prin_arr > 1e-6:
        p = Decimal(str(prin_arr))
        _post_event_for_loan(
            svc_alloc,
            loan_id,
            repayment_id=original_repayment_id,
            event_type="PAYMENT_PRINCIPAL",
            reference=_rj,
            description=f"Reversal of principal (arrears) — {_rj}",
            event_id=f"REV-REPAY-{original_repayment_id}-PRIN-ARR",
            created_by="system",
            entry_date=eff_date,
            payload={"cash_operating": p, "principal_arrears": p},
            amount=p,
            is_reversal=True,
        )
    prin_nyd = _p(alloc_row.get("alloc_principal_not_due"))
    if prin_nyd > 1e-6:
        p = Decimal(str(prin_nyd))
        _post_event_for_loan(
            svc_alloc,
            loan_id,
            repayment_id=original_repayment_id,
            event_type="PAYMENT_PRINCIPAL_NOT_YET_DUE",
            reference=_rj,
            description=f"Reversal of principal (not yet due) — {_rj}",
            event_id=f"REV-REPAY-{original_repayment_id}-PRIN-NYD",
            created_by="system",
            entry_date=eff_date,
            payload={"cash_operating": p, "loan_principal": p},
            amount=p,
            is_reversal=True,
        )
    # Interest
    int_arrears = _p(alloc_row.get("alloc_interest_arrears"))
    if int_arrears > 1e-6:
        p = Decimal(str(int_arrears))
        _post_event_for_loan(
            svc_alloc,
            loan_id,
            repayment_id=original_repayment_id,
            event_type="PAYMENT_REGULAR_INTEREST",
            reference=_rj,
            description=f"Reversal of interest (arrears) — {_rj}",
            event_id=f"REV-REPAY-{original_repayment_id}-INT-ARR",
            created_by="system",
            entry_date=eff_date,
            payload={"cash_operating": p, "regular_interest_arrears": p},
            amount=p,
            is_reversal=True,
        )
    int_accrued = _p(alloc_row.get("alloc_interest_accrued"))
    if int_accrued > 1e-6:
        p = Decimal(str(int_accrued))
        _post_event_for_loan(
            svc_alloc,
            loan_id,
            repayment_id=original_repayment_id,
            event_type="PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
            reference=_rj,
            description=f"Reversal of interest (accrued / not billed) — {_rj}",
            event_id=f"REV-REPAY-{original_repayment_id}-INT-ACC",
            created_by="system",
            entry_date=eff_date,
            payload={"cash_operating": p, "regular_interest_accrued": p},
            amount=p,
            is_reversal=True,
        )
    # Penalty & Default
    pen = _p(alloc_row.get("alloc_penalty_interest"))
    if pen > 1e-6:
        p = Decimal(str(pen))
        _post_event_for_loan(
            svc_alloc,
            loan_id,
            repayment_id=original_repayment_id,
            event_type="PAYMENT_PENALTY_INTEREST",
            reference=_rj,
            description=f"Reversal of penalty interest — {_rj}",
            event_id=f"REV-REPAY-{original_repayment_id}-PEN",
            created_by="system",
            entry_date=eff_date,
            payload={
                "cash_operating": p,
                "penalty_interest_asset": p,
                "penalty_interest_suspense": p,
                "penalty_interest_income": p,
            },
            amount=p,
            is_reversal=True,
        )
    default_i = _p(alloc_row.get("alloc_default_interest"))
    if default_i > 1e-6:
        p = Decimal(str(default_i))
        _post_event_for_loan(
            svc_alloc,
            loan_id,
            repayment_id=original_repayment_id,
            event_type="PAYMENT_DEFAULT_INTEREST",
            reference=_rj,
            description=f"Reversal of default interest — {_rj}",
            event_id=f"REV-REPAY-{original_repayment_id}-DEF",
            created_by="system",
            entry_date=eff_date,
            payload={
                "cash_operating": p,
                "default_interest_asset": p,
                "default_interest_suspense": p,
                "default_interest_income": p,
            },
            amount=p,
            is_reversal=True,
        )
