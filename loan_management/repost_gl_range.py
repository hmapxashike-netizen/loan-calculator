"""Re-post deterministic GL journals for a loan over a date range."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from .cash_gl import _post_event_for_loan
from .db import RealDictCursor, _connection
from .unapplied_refs import (
    _repayment_journal_reference,
    _unapplied_original_reference,
    _unapplied_reversal_reference,
)


def repost_gl_for_loan_date_range(
    loan_id: int,
    start_date: date,
    end_date: date,
    *,
    created_by: str = "system",
) -> None:
    """
    Re-post deterministic GL journals so GL matches the latest allocation state.

    Why needed:
    - EOD replay can overwrite allocations/daily_state for later receipts/liquidations.
    - Original GL journals posted at capture time remain stale unless we re-post.

    Safety:
    - AccountingRepository.save_journal_entry replaces journals by (event_id, event_tag),
      so re-posting is idempotent and converges GL to the latest computed values.
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    try:
        from accounting.service import AccountingService
    except Exception as exc:
        raise RuntimeError(f"gl_repost: AccountingService unavailable: {exc}") from exc

    svc = AccountingService()

    def _p(v: Any) -> float:
        return float(v or 0)

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Teller receipts (and their reversal rows), excluding internal system liquidations.
            cur.execute(
                """
                SELECT
                    lr.id,
                    lr.status,
                    lr.original_repayment_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS eff_date,
                    lra.alloc_principal_not_due,
                    lra.alloc_principal_arrears,
                    lra.alloc_interest_accrued,
                    lra.alloc_interest_arrears,
                    lra.alloc_default_interest,
                    lra.alloc_penalty_interest,
                    lra.alloc_fees_charges,
                    lra.unallocated
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.reference, '') ILIKE '%%Reversal of unapplied funds%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%Reversal of unapplied funds%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%Reversal of unapplied funds%%'
                  )
                ORDER BY (COALESCE(lr.value_date, lr.payment_date))::date, lr.id
                """,
                (loan_id, start_date, end_date),
            )
            receipt_rows = cur.fetchall()

            for r in receipt_rows:
                rid = int(r["id"])
                eff = r["eff_date"]
                status = (r.get("status") or "").strip().lower()
                orig_id = r.get("original_repayment_id")
                orig_id_int = int(orig_id) if orig_id is not None else None

                apr = _p(r.get("alloc_principal_arrears"))
                pnd = _p(r.get("alloc_principal_not_due"))
                aiar = _p(r.get("alloc_interest_arrears"))
                aia = _p(r.get("alloc_interest_accrued"))
                adi = _p(r.get("alloc_default_interest"))
                api = _p(r.get("alloc_penalty_interest"))
                afc = _p(r.get("alloc_fees_charges"))
                unalloc = _p(r.get("unallocated"))

                # Base reference for receipt journals.
                base_ref = _repayment_journal_reference(loan_id, rid)

                # Original receipt journals (only meaningful for posted receipts).
                if status == "posted":
                    if unalloc > 1e-6:
                        ref_u = _unapplied_original_reference(
                            "credit", loan_id=loan_id, repayment_id=rid, value_date=eff
                        )
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=rid,
                            event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                            reference=ref_u,
                            description="Unapplied funds on overpayment",
                            event_id=ref_u,
                            created_by=created_by,
                            entry_date=eff,
                            amount=Decimal(str(unalloc)),
                        )
                    if apr > 1e-6:
                        p = Decimal(str(apr))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=rid,
                            event_type="PAYMENT_PRINCIPAL",
                            reference=base_ref,
                            description=f"Principal (arrears) — {base_ref}",
                            event_id=f"REPAY-{rid}-PRIN-ARR",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "principal_arrears": p},
                        )
                    if pnd > 1e-6:
                        p = Decimal(str(pnd))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=rid,
                            event_type="PAYMENT_PRINCIPAL_NOT_YET_DUE",
                            reference=base_ref,
                            description=f"Principal (not yet due) — {base_ref}",
                            event_id=f"REPAY-{rid}-PRIN-NYD",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "loan_principal": p},
                        )
                    if aiar > 1e-6:
                        p = Decimal(str(aiar))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=rid,
                            event_type="PAYMENT_REGULAR_INTEREST",
                            reference=base_ref,
                            description=f"Interest (arrears) — {base_ref}",
                            event_id=f"REPAY-{rid}-INT-ARR",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "regular_interest_arrears": p},
                        )
                    if aia > 1e-6:
                        p = Decimal(str(aia))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=rid,
                            event_type="PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
                            reference=base_ref,
                            description=f"Interest (accrued / not billed) — {base_ref}",
                            event_id=f"REPAY-{rid}-INT-ACC",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "regular_interest_accrued": p},
                        )
                    if api > 1e-6:
                        p = Decimal(str(api))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=rid,
                            event_type="PAYMENT_PENALTY_INTEREST",
                            reference=base_ref,
                            description=f"Penalty interest — {base_ref}",
                            event_id=f"REPAY-{rid}-PEN",
                            created_by=created_by,
                            entry_date=eff,
                            payload=None,
                        )
                    if adi > 1e-6:
                        p = Decimal(str(adi))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=rid,
                            event_type="PAYMENT_DEFAULT_INTEREST",
                            reference=base_ref,
                            description=f"Default interest — {base_ref}",
                            event_id=f"REPAY-{rid}-DEF",
                            created_by=created_by,
                            entry_date=eff,
                            payload=None,
                        )
                    if afc > 1e-6:
                        p = Decimal(str(afc))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=rid,
                            event_type="PASS_THROUGH_COST_RECOVERY",
                            reference=base_ref,
                            description=f"Fees/charges — {base_ref}",
                            event_id=f"REPAY-{rid}-FEES",
                            created_by=created_by,
                            entry_date=eff,
                            amount=p,
                            payload=None,
                        )

                # Reversal receipt journals (only meaningful when receipt is reversed).
                if status == "reversed" and orig_id_int is not None:
                    # Reverse the original receipt journals using deterministic REV ids.
                    if unalloc > 1e-6:
                        orig_ref_u = _unapplied_original_reference(
                            "credit", loan_id=loan_id, repayment_id=orig_id_int, value_date=eff
                        )
                        rev_ref_u = _unapplied_reversal_reference(orig_ref_u)
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=orig_id_int,
                            event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                            reference=rev_ref_u,
                            description=f"Reversal of unapplied overpayment: {orig_ref_u}",
                            event_id=rev_ref_u,
                            created_by=created_by,
                            entry_date=eff,
                            amount=Decimal(str(unalloc)),
                            is_reversal=True,
                        )
                    if apr > 1e-6:
                        p = Decimal(str(apr))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=orig_id_int,
                            event_type="PAYMENT_PRINCIPAL",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of principal (arrears) — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-PRIN-ARR",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "principal_arrears": p},
                            amount=p,
                            is_reversal=True,
                        )
                    if pnd > 1e-6:
                        p = Decimal(str(pnd))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=orig_id_int,
                            event_type="PAYMENT_PRINCIPAL_NOT_YET_DUE",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of principal (not yet due) — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-PRIN-NYD",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "loan_principal": p},
                            amount=p,
                            is_reversal=True,
                        )
                    if aiar > 1e-6:
                        p = Decimal(str(aiar))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=orig_id_int,
                            event_type="PAYMENT_REGULAR_INTEREST",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of interest (arrears) — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-INT-ARR",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "regular_interest_arrears": p},
                            amount=p,
                            is_reversal=True,
                        )
                    if aia > 1e-6:
                        p = Decimal(str(aia))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=orig_id_int,
                            event_type="PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of interest (accrued / not billed) — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-INT-ACC",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "regular_interest_accrued": p},
                            amount=p,
                            is_reversal=True,
                        )
                    if api > 1e-6:
                        p = Decimal(str(api))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=orig_id_int,
                            event_type="PAYMENT_PENALTY_INTEREST",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of penalty interest — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-PEN",
                            created_by=created_by,
                            entry_date=eff,
                            amount=p,
                            is_reversal=True,
                        )
                    if adi > 1e-6:
                        p = Decimal(str(adi))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=orig_id_int,
                            event_type="PAYMENT_DEFAULT_INTEREST",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of default interest — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-DEF",
                            created_by=created_by,
                            entry_date=eff,
                            amount=p,
                            is_reversal=True,
                        )
                    if afc > 1e-6:
                        p = Decimal(str(afc))
                        _post_event_for_loan(
                            svc,
                            loan_id,
                            repayment_id=orig_id_int,
                            event_type="PASS_THROUGH_COST_RECOVERY",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of fees/charges — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-FEES",
                            created_by=created_by,
                            entry_date=eff,
                            amount=p,
                            is_reversal=True,
                        )

            # Unapplied liquidations and liquidation reversals (system rows).
            cur.execute(
                """
                SELECT
                    (COALESCE(lr.value_date, lr.payment_date))::date AS eff_date,
                    lra.event_type,
                    lra.source_repayment_id,
                    lra.alloc_principal_not_due,
                    lra.alloc_principal_arrears,
                    lra.alloc_interest_accrued,
                    lra.alloc_interest_arrears,
                    lra.alloc_default_interest,
                    lra.alloc_penalty_interest,
                    lra.alloc_fees_charges
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lr.loan_id = %s
                  AND (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND lra.event_type IN ('unapplied_funds_allocation', 'unallocation_parent_reversed')
                  AND lra.source_repayment_id IS NOT NULL
                ORDER BY (COALESCE(lr.value_date, lr.payment_date))::date, lr.id
                """,
                (loan_id, start_date, end_date),
            )
            liq_rows = cur.fetchall()
            for r in liq_rows:
                eff = r["eff_date"]
                src = int(r["source_repayment_id"])
                evt = str(r.get("event_type") or "")
                is_rev = evt == "unallocation_parent_reversed"

                liq_ref = _unapplied_original_reference(
                    "liquidation", loan_id=loan_id, repayment_id=src, value_date=eff
                )
                event_id = _unapplied_reversal_reference(liq_ref) if is_rev else liq_ref

                # unapplied_funds_allocation rows store positive bucket amounts.
                # unallocation_parent_reversed rows store the same magnitudes NEGATIVE; GL repost
                # needs positive amounts with is_reversal=True (matches reverse_repayment posting).
                def _liq_bucket(v: Any) -> float:
                    x = _p(v)
                    return abs(x) if is_rev else x

                apr = _liq_bucket(r.get("alloc_principal_not_due"))
                apa = _liq_bucket(r.get("alloc_principal_arrears"))
                aia = _liq_bucket(r.get("alloc_interest_accrued"))
                aiar = _liq_bucket(r.get("alloc_interest_arrears"))
                adi = _liq_bucket(r.get("alloc_default_interest"))
                api = _liq_bucket(r.get("alloc_penalty_interest"))
                afc = _liq_bucket(r.get("alloc_fees_charges"))

                if apr > 1e-6:
                    _post_event_for_loan(
                        svc,
                        loan_id,
                        repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_NOT_YET_DUE",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: principal not yet due ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(apr)),
                        is_reversal=is_rev,
                    )
                if apa > 1e-6:
                    _post_event_for_loan(
                        svc,
                        loan_id,
                        repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: principal arrears ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(apa)),
                        is_reversal=is_rev,
                    )
                if aia > 1e-6:
                    _post_event_for_loan(
                        svc,
                        loan_id,
                        repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST_NOT_YET_DUE",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: interest accrued ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(aia)),
                        is_reversal=is_rev,
                    )
                if aiar > 1e-6:
                    _post_event_for_loan(
                        svc,
                        loan_id,
                        repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: interest arrears ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(aiar)),
                        is_reversal=is_rev,
                    )
                if adi > 1e-6:
                    _post_event_for_loan(
                        svc,
                        loan_id,
                        repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: default interest ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(adi)),
                        is_reversal=is_rev,
                    )
                if api > 1e-6:
                    _post_event_for_loan(
                        svc,
                        loan_id,
                        repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_PENALTY_INTEREST",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: penalty interest ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(api)),
                        is_reversal=is_rev,
                    )
                if afc > 1e-6:
                    _post_event_for_loan(
                        svc,
                        loan_id,
                        repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_PASS_THROUGH_COST_RECOVERY",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: fees/charges ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(afc)),
                        is_reversal=is_rev,
                    )
