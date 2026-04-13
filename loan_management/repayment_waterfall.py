"""Persist waterfall allocation for a receipt and refresh same-day ``loan_daily_state``."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from decimal_utils import as_10dp

from .allocation_queries import (
    _get_opening_balances_for_repayment,
    get_net_allocation_for_loan_date,
    get_unallocated_for_loan_date,
)
from .cash_gl import _post_event_for_loan
from .daily_state import save_loan_daily_state
from .db import RealDictCursor, _connection
from .product_catalog import load_system_config_from_db
from .serialization import _date_conv
from .unapplied_recast import _credit_unapplied_funds
from .unapplied_refs import _repayment_journal_reference, _unapplied_original_reference
from .waterfall_core import _get_waterfall_config, compute_waterfall_allocation


def allocate_repayment_waterfall(
    repayment_id: int,
    *,
    as_of: date | None = None,
    system_config: dict | None = None,
    preloaded_balances: dict | None = None,
    event_type: str = "new_allocation",
) -> None:
    """
    Allocate a repayment across loan buckets using the configured waterfall
    (Standard or Borrower-friendly) and loan_daily_state. Persists allocation
    and updates daily state. Called at save receipt (real-time allocation).

    **Opening balance policy:** The waterfall uses **opening** balances for the
    receipt value date — i.e. **closing(eff_date − 1)** minus net allocations from
    **earlier posted receipts** on the same value date (by `repayment_id`). It does
    **not** use intraday EOD accrual for eff_date before allocating (so paying
    arrears does not pre-bill default/penalty for that day). Reversed receipts are
    excluded (no longer `posted`).

    When reallocate_repayment calls this, it can pass preloaded_balances (engine - others)
    so we use that state instead of reading from DB, avoiding read-after-write visibility issues.

    Standard waterfall:
    - Allocate to buckets 1–5 (fees, penalty, default interest, interest arrears,
      principal arrears). Principal arrears are due and are paid.
    - Never allocate directly to interest accrued or principal not due.
    - Any remainder goes to Unapplied funds. Recast is a separate process
      (apply from Unapplied tab after funds are credited).
    """
    if as_of is None:
        from eod.system_business_date import get_effective_date

        as_of = get_effective_date()

    cfg = system_config or load_system_config_from_db() or {}
    profile_key, bucket_order = _get_waterfall_config(cfg)
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT lr.id, lr.loan_id, lr.amount,
                       COALESCE(lr.value_date, lr.payment_date) AS eff_date
                FROM loan_repayments lr
                WHERE lr.id = %s
                """,
                (repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Repayment {repayment_id} not found.")

            amount = float(row["amount"])
            if amount <= 0:
                # Negative repayments (reversals) must never get new_allocation.
                # They get unallocation_parent_reversed at creation (reverse_repayment)
                # or via clear script's fix-reversed section.
                return

            loan_id = int(row["loan_id"])
            eff_date = row["eff_date"] or as_of
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            # Always source balances from exact-date persisted state; ignore preloaded snapshots.
            _ = preloaded_balances  # backward compatibility only
            from eod.core import run_single_loan_eod

            # Ensure prior calendar day state exists (closing = opening for eff_date).
            prev_cal = eff_date - timedelta(days=1)
            run_single_loan_eod(loan_id, prev_cal, sys_cfg=system_config)

            # Serialize same-loan allocation (multiple receipts / concurrent saves).
            cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (4021001, loan_id))

            balances, st_prev, days_overdue = _get_opening_balances_for_repayment(
                cur, loan_id, eff_date, repayment_id,
            )
            state_as_of = eff_date

            alloc, unapplied = compute_waterfall_allocation(
                amount, balances, bucket_order, profile_key,
                state_as_of=state_as_of, repayment_id=repayment_id,
            )

            alloc_principal_not_due = alloc.get("alloc_principal_not_due", 0.0)
            alloc_principal_arrears = alloc.get("alloc_principal_arrears", 0.0)
            alloc_interest_accrued = alloc.get("alloc_interest_accrued", 0.0)
            alloc_interest_arrears = min(
                alloc.get("alloc_interest_arrears", 0.0),
                balances.get("interest_arrears_balance", 0.0),
            )
            alloc_default_interest = alloc.get("alloc_default_interest", 0.0)
            alloc_penalty_interest = alloc.get("alloc_penalty_interest", 0.0)
            alloc_fees_charges = alloc.get("alloc_fees_charges", 0.0)

            alloc_principal_total = alloc_principal_not_due + alloc_principal_arrears
            alloc_interest_total = (
                alloc_interest_accrued
                + alloc_interest_arrears
                + alloc_default_interest
                + alloc_penalty_interest
            )
            alloc_fees_total = alloc_fees_charges
            remaining_arrears = float(
                as_10dp(
                    max(0.0, balances.get("interest_arrears_balance", 0.0) - alloc_interest_arrears)
                    + max(0.0, balances.get("default_interest_balance", 0.0) - alloc_default_interest)
                    + max(0.0, balances.get("penalty_interest_balance", 0.0) - alloc_penalty_interest)
                    + max(0.0, balances.get("principal_arrears", 0.0) - alloc_principal_arrears)
                    + max(0.0, balances.get("fees_charges_balance", 0.0) - alloc_fees_charges)
                )
            )
            if unapplied > 1e-6 and remaining_arrears > 1e-6:
                raise ValueError(
                    f"Policy violation for repayment {repayment_id}: unapplied={unapplied} while "
                    f"arrears still outstanding={remaining_arrears}."
                )
            total_alloc = alloc_principal_total + alloc_interest_total + alloc_fees_total
            if abs((total_alloc + unapplied) - amount) > 0.01:
                raise ValueError(
                    f"Allocation mismatch for repayment {repayment_id}: "
                    f"amount={amount}, allocated={total_alloc}, unapplied={unapplied}"
                )

            cur2 = conn.cursor()
            cur2.execute(
                """
                INSERT INTO loan_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total,
                    alloc_total, unallocated, event_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    repayment_id,
                    alloc_principal_not_due,
                    alloc_principal_arrears,
                    alloc_interest_accrued,
                    alloc_interest_arrears,
                    alloc_default_interest,
                    alloc_penalty_interest,
                    alloc_fees_charges,
                    alloc_principal_total,
                    alloc_interest_total,
                    alloc_fees_total,
                    float(as_10dp(total_alloc)),
                    float(as_10dp(unapplied)),
                    event_type,
                ),
            )
            if unapplied > 1e-6:
                _credit_unapplied_funds(conn, loan_id, repayment_id, unapplied, eff_date)

            try:
                from accounting.service import AccountingService

                svc = AccountingService()

                # Each post_event uses transaction_templates for that event_type only.
                # Cash Dr must equal the sum of Cr lines for THAT journal — do not use full receipt
                # amount on PAYMENT_PRINCIPAL (only 2 lines: cash + principal_arrears); split not-yet-due
                # to PAYMENT_PRINCIPAL_NOT_YET_DUE, interest arrears vs accrued to separate events, etc.

                _rj = _repayment_journal_reference(loan_id, repayment_id)

                # Overpayment remainder: credit unapplied_funds and post GL using the
                # dedicated template (UNAPPLIED_FUNDS_OVERPAYMENT).
                if unapplied > 1e-6:
                    _post_event_for_loan(svc, loan_id, repayment_id=repayment_id,
                        event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                        reference=_unapplied_original_reference(
                            "credit",
                            loan_id=loan_id,
                            repayment_id=repayment_id,
                            value_date=eff_date,
                        ),
                        description="Unapplied funds on overpayment",
                        event_id=_unapplied_original_reference(
                            "credit",
                            loan_id=loan_id,
                            repayment_id=repayment_id,
                            value_date=eff_date,
                        ),
                        created_by="system",
                        entry_date=eff_date,
                        amount=Decimal(str(unapplied)),
                    )
                if alloc_principal_arrears > 0:
                    p = Decimal(str(alloc_principal_arrears))
                    _post_event_for_loan(svc, loan_id, repayment_id=repayment_id,
                        event_type="PAYMENT_PRINCIPAL",
                        reference=_rj,
                        description=f"Principal (arrears) — {_rj}",
                        event_id=f"REPAY-{repayment_id}-PRIN-ARR",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "principal_arrears": p,
                        },
                    )

                if alloc_principal_not_due > 0:
                    p = Decimal(str(alloc_principal_not_due))
                    _post_event_for_loan(svc, loan_id, repayment_id=repayment_id,
                        event_type="PAYMENT_PRINCIPAL_NOT_YET_DUE",
                        reference=_rj,
                        description=f"Principal (not yet due) — {_rj}",
                        event_id=f"REPAY-{repayment_id}-PRIN-NYD",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "loan_principal": p,
                        },
                    )

                if alloc_interest_arrears > 0:
                    p = Decimal(str(alloc_interest_arrears))
                    _post_event_for_loan(svc, loan_id, repayment_id=repayment_id,
                        event_type="PAYMENT_REGULAR_INTEREST",
                        reference=_rj,
                        description=f"Interest (arrears) — {_rj}",
                        event_id=f"REPAY-{repayment_id}-INT-ARR",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "regular_interest_arrears": p,
                        },
                    )

                if alloc_interest_accrued > 0:
                    p = Decimal(str(alloc_interest_accrued))
                    _post_event_for_loan(svc, loan_id, repayment_id=repayment_id,
                        event_type="PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
                        reference=_rj,
                        description=f"Interest (accrued / not billed) — {_rj}",
                        event_id=f"REPAY-{repayment_id}-INT-ACC",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "regular_interest_accrued": p,
                        },
                    )

                if alloc_penalty_interest > 0:
                    p = Decimal(str(alloc_penalty_interest))
                    _post_event_for_loan(svc, loan_id, repayment_id=repayment_id,
                        event_type="PAYMENT_PENALTY_INTEREST",
                        reference=_rj,
                        description=f"Penalty interest — {_rj}",
                        event_id=f"REPAY-{repayment_id}-PEN",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "penalty_interest_asset": p,
                            "penalty_interest_suspense": p,
                            "penalty_interest_income": p,
                        },
                    )

                if alloc_default_interest > 0:
                    p = Decimal(str(alloc_default_interest))
                    _post_event_for_loan(svc, loan_id, repayment_id=repayment_id,
                        event_type="PAYMENT_DEFAULT_INTEREST",
                        reference=_rj,
                        description=f"Default interest — {_rj}",
                        event_id=f"REPAY-{repayment_id}-DEF",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "default_interest_asset": p,
                            "default_interest_suspense": p,
                            "default_interest_income": p,
                        },
                    )

            except Exception as e:
                print(f"Failed to post repayment journals for {repayment_id}: {e}")

            new_interest_accrued = max(0.0, balances["interest_accrued_balance"] - alloc_interest_accrued)
            new_interest_arrears = max(0.0, balances["interest_arrears_balance"] - alloc_interest_arrears)
            new_principal_not_due = max(0.0, balances["principal_not_due"] - alloc_principal_not_due)
            new_principal_arrears = max(0.0, balances["principal_arrears"] - alloc_principal_arrears)
            new_default_interest = max(0.0, balances["default_interest_balance"] - alloc_default_interest)
            new_penalty_interest = max(0.0, balances["penalty_interest_balance"] - alloc_penalty_interest)
            new_fees_charges = max(0.0, balances["fees_charges_balance"] - alloc_fees_charges)
            new_reg_susp = max(
                0.0,
                float(
                    as_10dp(
                        float(balances.get("regular_interest_in_suspense_balance", 0) or 0)
                        - alloc_interest_accrued
                    )
                ),
            )
            new_pen_susp = max(0.0, float(as_10dp(new_penalty_interest)))
            new_def_susp = max(0.0, float(as_10dp(new_default_interest)))

            # Use daily/period columns from **closing(eff_date-1)** (st_prev). Intraday EOD accrual for
            # eff_date is not applied before allocation; night EOD will refresh this row.
            # Never recalculate daily columns from post-allocation balances: doing so breaks the
            # bucket identity (opening + daily - alloc = closing) and inflates statement charges.
            _sp = st_prev or {}
            reg_daily = float(_sp.get("regular_interest_daily", 0) or 0)
            pen_daily = float(_sp.get("penalty_interest_daily", 0) or 0)
            def_daily = float(_sp.get("default_interest_daily", 0) or 0)
            reg_period = float(_sp.get("regular_interest_period_to_date", 0) or 0)
            pen_period = float(_sp.get("penalty_interest_period_to_date", 0) or 0)
            def_period = float(_sp.get("default_interest_period_to_date", 0) or 0)

            if (
                new_interest_arrears + new_default_interest + new_penalty_interest + new_principal_arrears
                <= 1e-6
            ):
                days_overdue = 0

            total_exposure = (
                new_principal_not_due + new_principal_arrears + new_interest_accrued + new_interest_arrears
                + new_default_interest + new_penalty_interest + new_fees_charges
            )
            eff_date_val = _date_conv(eff_date) or (eff_date.date() if isinstance(eff_date, datetime) else eff_date)
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date_val, conn=conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date_val, conn=conn)

            # Update loan_daily_state in the same connection/cursor so it commits with the allocation.
            # Explicit UPDATE so we don't rely on ON CONFLICT; row must exist (from restore or EOD).
            cur2.execute(
                """
                UPDATE loan_daily_state SET
                    regular_interest_daily = %s,
                    principal_not_due = %s,
                    principal_arrears = %s,
                    interest_accrued_balance = %s,
                    interest_arrears_balance = %s,
                    default_interest_daily = %s,
                    default_interest_balance = %s,
                    penalty_interest_daily = %s,
                    penalty_interest_balance = %s,
                    fees_charges_balance = %s,
                    days_overdue = %s,
                    total_delinquency_arrears = %s,
                    total_exposure = %s,
                    regular_interest_period_to_date = %s,
                    penalty_interest_period_to_date = %s,
                    default_interest_period_to_date = %s,
                    net_allocation = %s,
                    unallocated = %s,
                    regular_interest_in_suspense_balance = %s,
                    penalty_interest_in_suspense_balance = %s,
                    default_interest_in_suspense_balance = %s,
                    total_interest_in_suspense_balance = %s
                WHERE loan_id = %s AND as_of_date = %s
                """,
                (
                    reg_daily,
                    new_principal_not_due,
                    new_principal_arrears,
                    new_interest_accrued,
                    new_interest_arrears,
                    def_daily,
                    new_default_interest,
                    pen_daily,
                    new_penalty_interest,
                    new_fees_charges,
                    days_overdue,
                    float(as_10dp(new_principal_arrears + new_interest_arrears + new_default_interest + new_penalty_interest + new_fees_charges)),
                    total_exposure,
                    reg_period,
                    pen_period,
                    def_period,
                    net_alloc,
                    unalloc,
                    new_reg_susp,
                    new_pen_susp,
                    new_def_susp,
                    float(
                        as_10dp(
                            Decimal(str(new_reg_susp))
                            + Decimal(str(new_pen_susp))
                            + Decimal(str(new_def_susp))
                        )
                    ),
                    loan_id,
                    eff_date_val,
                ),
            )
            if cur2.rowcount == 0:
                # Row missing (e.g. first receipt of the day); upsert so we don't leave allocation without state
                save_loan_daily_state(
                    loan_id=loan_id,
                    as_of_date=eff_date_val,
                    regular_interest_daily=reg_daily,
                    principal_not_due=new_principal_not_due,
                    principal_arrears=new_principal_arrears,
                    interest_accrued_balance=new_interest_accrued,
                    interest_arrears_balance=new_interest_arrears,
                    default_interest_daily=def_daily,
                    default_interest_balance=new_default_interest,
                    penalty_interest_daily=pen_daily,
                    penalty_interest_balance=new_penalty_interest,
                    fees_charges_balance=new_fees_charges,
                    days_overdue=days_overdue,
                    regular_interest_period_to_date=reg_period,
                    penalty_interest_period_to_date=pen_period,
                    default_interest_period_to_date=def_period,
                    net_allocation=net_alloc,
                    unallocated=unalloc,
                    regular_interest_in_suspense_balance=new_reg_susp,
                    penalty_interest_in_suspense_balance=new_pen_susp,
                    default_interest_in_suspense_balance=new_def_susp,
                    conn=conn,
                )
            cur2.close()
