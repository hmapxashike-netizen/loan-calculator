"""Recast path: apply unapplied via reclassification + shared unapplied credit insert helper."""

from __future__ import annotations

from datetime import date

from decimal_utils import as_10dp

from .allocation_queries import get_net_allocation_for_loan_date, get_unallocated_for_loan_date
from .cash_gl import _post_event_for_loan
from .daily_state import save_loan_daily_state
from .db import RealDictCursor, _connection
from .unapplied_refs import _unapplied_original_reference


def apply_unapplied_funds_recast(
    unapplied_funds_id: int,
    *,
    as_of: date | None = None,
) -> None:
    """
    Apply unapplied funds to the loan via recast logic (separate from allocation).
    Reclassifies accrued interest → interest arrears and principal not due → principal arrears
    (up to the unapplied amount), then applies the payment to those buckets. Call only after
    funds have been credited to Unapplied (e.g. from Unapplied tab).
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, amount, value_date, repayment_id
                FROM unapplied_funds
                WHERE id = %s
                """,
                (unapplied_funds_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} not found.")
            if float(row.get("amount") or 0) <= 0:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} has no positive balance (already consumed or debit).")
            # Prevent double-apply: check if debit already exists for this credit
            cur.execute(
                "SELECT 1 FROM unapplied_funds WHERE source_unapplied_id = %s LIMIT 1",
                (unapplied_funds_id,),
            )
            if cur.fetchone():
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} was already applied via recast.")
            loan_id = int(row["loan_id"])
            amount = float(as_10dp(row["amount"]))
            value_date = row["value_date"]
            if hasattr(value_date, "date"):
                value_date = value_date.date()
            eff_date = as_of or value_date
            source_repayment_id = int(row["repayment_id"]) if row.get("repayment_id") is not None else None
            if source_repayment_id is None:
                raise ValueError("Recast requires source repayment_id on the unapplied credit row.")

            # Ensure exact-date state exists before any mutation.
            from eod.core import run_single_loan_eod

            run_single_loan_eod(loan_id, eff_date)

            cur.execute(
                """
                SELECT as_of_date, principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance,
                       penalty_interest_balance, fees_charges_balance, days_overdue,
                       regular_interest_daily, default_interest_daily, penalty_interest_daily,
                       regular_interest_period_to_date, penalty_interest_period_to_date, default_interest_period_to_date,
                       COALESCE(regular_interest_in_suspense_balance, 0) AS regular_interest_in_suspense_balance,
                       COALESCE(penalty_interest_in_suspense_balance, 0) AS penalty_interest_in_suspense_balance,
                       COALESCE(default_interest_in_suspense_balance, 0) AS default_interest_in_suspense_balance
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date = %s
                FOR UPDATE
                """,
                (loan_id, eff_date),
            )
            st_row = cur.fetchone()
            if not st_row:
                raise ValueError(f"No exact loan_daily_state for loan_id={loan_id} on {eff_date}.")
            balances = {k: float(st_row.get(k, 0) or 0) for k in (
                "principal_not_due", "principal_arrears", "interest_accrued_balance",
                "interest_arrears_balance", "default_interest_balance",
                "penalty_interest_balance", "fees_charges_balance", "days_overdue"
            )}
            # Preserve accruals for eff_date: use existing row if it's for eff_date, else engine state
            st_row_as_of = st_row.get("as_of_date")
            if hasattr(st_row_as_of, "date"):
                st_row_as_of = st_row_as_of.date() if callable(getattr(st_row_as_of, "date")) else st_row_as_of
            if st_row_as_of == eff_date:
                acc_daily = (float(st_row.get("regular_interest_daily") or 0), float(st_row.get("default_interest_daily") or 0), float(st_row.get("penalty_interest_daily") or 0))
                acc_period = (float(st_row.get("regular_interest_period_to_date") or 0), float(st_row.get("penalty_interest_period_to_date") or 0), float(st_row.get("default_interest_period_to_date") or 0))
            else:
                from eod.core import get_engine_state_for_loan_date

                _eng = get_engine_state_for_loan_date(loan_id, eff_date)
                if _eng:
                    acc_daily = (_eng.get("regular_interest_daily", 0) or 0, _eng.get("default_interest_daily", 0) or 0, _eng.get("penalty_interest_daily", 0) or 0)
                    acc_period = (_eng.get("regular_interest_period_to_date", 0) or 0, _eng.get("penalty_interest_period_to_date", 0) or 0, _eng.get("default_interest_period_to_date", 0) or 0)
                else:
                    acc_daily = (0.0, 0.0, 0.0)
                    acc_period = (0.0, 0.0, 0.0)

            remaining = float(as_10dp(amount))
            move_accrued_to_arrears = float(
                as_10dp(min(balances["interest_accrued_balance"], remaining))
            )
            remaining = float(as_10dp(remaining - move_accrued_to_arrears))
            move_principal_not_due_to_arrears = 0.0
            if remaining > 1e-6:
                move_principal_not_due_to_arrears = float(
                    as_10dp(min(balances["principal_not_due"], remaining))
                )

            new_interest_accrued = float(
                as_10dp(balances["interest_accrued_balance"] - move_accrued_to_arrears)
            )
            new_interest_arrears = float(
                as_10dp(balances["interest_arrears_balance"] - move_accrued_to_arrears)
            )
            new_principal_not_due = float(
                as_10dp(balances["principal_not_due"] - move_principal_not_due_to_arrears)
            )
            new_principal_arrears = float(
                as_10dp(balances["principal_arrears"] - move_principal_not_due_to_arrears)
            )
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date, conn=conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date, conn=conn)
            liquidation_amount = move_accrued_to_arrears + move_principal_not_due_to_arrears
            # Safety: recast should not consume a different total than we allocate to
            # the liquidation buckets; otherwise reversal cascade would not reconcile.
            if abs(float(as_10dp(liquidation_amount)) - float(as_10dp(amount))) > 1e-6:
                raise ValueError(
                    "Recast allocation mismatch: unapplied amount does not equal "
                    "principal_not_due_to_arrears + interest_accrued_to_arrears."
                )

            rec_reg_susp = float(st_row.get("regular_interest_in_suspense_balance") or 0)
            rec_pen_susp = float(balances["penalty_interest_balance"])
            rec_def_susp = float(balances["default_interest_balance"])

            save_loan_daily_state(
                loan_id=loan_id,
                as_of_date=eff_date,
                regular_interest_daily=acc_daily[0],
                principal_not_due=new_principal_not_due,
                principal_arrears=new_principal_arrears,
                interest_accrued_balance=new_interest_accrued,
                interest_arrears_balance=new_interest_arrears,
                default_interest_daily=acc_daily[1],
                default_interest_balance=balances["default_interest_balance"],
                penalty_interest_daily=acc_daily[2],
                penalty_interest_balance=balances["penalty_interest_balance"],
                fees_charges_balance=balances["fees_charges_balance"],
                days_overdue=int(balances["days_overdue"]),
                regular_interest_period_to_date=acc_period[0],
                penalty_interest_period_to_date=acc_period[1],
                default_interest_period_to_date=acc_period[2],
                net_allocation=net_alloc,
                unallocated=unalloc,
                regular_interest_in_suspense_balance=rec_reg_susp,
                penalty_interest_in_suspense_balance=rec_pen_susp,
                default_interest_in_suspense_balance=rec_def_susp,
                conn=conn,
            )

            # Create a deterministic system repayment representing this recast liquidation leg.
            try:
                from accounting.service import AccountingService

                svc_unapplied = AccountingService()
            except Exception:
                svc_unapplied = None

            cur.execute(
                """
                INSERT INTO loan_repayments (
                    loan_id, amount, payment_date, reference, value_date, status
                ) VALUES (%s, %s, %s, %s, %s, 'posted')
                RETURNING id
                """,
                (
                    loan_id,
                    float(as_10dp(-liquidation_amount)),
                    eff_date,
                    "Unapplied funds allocation",
                    eff_date,
                ),
            )
            liquidation_repayment_id = int(cur.fetchone()[0])

            # Insert liquidation lineage allocation so reverse_repayment can cascade.
            cur.execute(
                """
                INSERT INTO loan_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total,
                    alloc_total, unallocated,
                    event_type,
                    source_repayment_id
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    'unapplied_funds_allocation', %s
                )
                """,
                (
                    liquidation_repayment_id,
                    float(as_10dp(0.0)),
                    float(as_10dp(move_principal_not_due_to_arrears)),
                    float(as_10dp(0.0)),
                    float(as_10dp(move_accrued_to_arrears)),
                    float(as_10dp(0.0)),
                    float(as_10dp(0.0)),
                    float(as_10dp(0.0)),
                    float(as_10dp(move_principal_not_due_to_arrears)),
                    float(as_10dp(move_accrued_to_arrears)),
                    float(as_10dp(0.0)),
                    float(as_10dp(liquidation_amount)),
                    float(as_10dp(0.0)),
                    source_repayment_id,
                ),
            )

            liq_ref = _unapplied_original_reference(
                "liquidation",
                loan_id=loan_id,
                repayment_id=source_repayment_id,
                value_date=eff_date,
            )

            # Insert debit row (ledger-style, no UPDATE)
            cur.execute(
                """
                INSERT INTO unapplied_funds (
                    loan_id, amount, value_date, entry_type, reference,
                    allocation_repayment_id, source_repayment_id, source_unapplied_id, currency
                )
                VALUES (%s, %s, %s, 'debit', 'Applied via recast', %s, %s, %s, 'USD')
                """,
                (
                    loan_id,
                    float(as_10dp(-liquidation_amount)),
                    eff_date,
                    liquidation_repayment_id,
                    source_repayment_id,
                    unapplied_funds_id,
                ),
            )

            # GL postings for recast liquidation.
            # For recast, liquidated amounts land in (principal_arrears, interest_arrears).
            if svc_unapplied is not None:
                from decimal import Decimal

                if move_principal_not_due_to_arrears > 1e-6:
                    _post_event_for_loan(
                        svc_unapplied,
                        loan_id,
                        repayment_id=source_repayment_id,
                        event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS",
                        reference=liq_ref,
                        description=f"Recast liquidation: principal arrears ({liq_ref})",
                        event_id=liq_ref,
                        created_by="system",
                        entry_date=eff_date,
                        amount=Decimal(str(move_principal_not_due_to_arrears)),
                    )
                if move_accrued_to_arrears > 1e-6:
                    _post_event_for_loan(
                        svc_unapplied,
                        loan_id,
                        repayment_id=source_repayment_id,
                        event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST",
                        reference=liq_ref,
                        description=f"Recast liquidation: interest arrears ({liq_ref})",
                        event_id=liq_ref,
                        created_by="system",
                        entry_date=eff_date,
                        amount=Decimal(str(move_accrued_to_arrears)),
                    )


def _credit_unapplied_funds(
    conn,
    loan_id: int,
    repayment_id: int,
    amount: float,
    value_date: date,
    currency: str = "USD",
) -> None:
    """
    Insert a credit row into unapplied_funds (ledger-style, append-only).

    Guardrail: credit rows for receipt overpayment must be backed by
    ``loan_repayment_allocation.unallocated`` for the same ``repayment_id``.
    """
    amt = float(as_10dp(amount))
    if amt <= 1e-6:
        raise ValueError(
            f"Refusing unapplied credit for repayment_id={repayment_id}: amount must be > 0."
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(unallocated, 0) AS unallocated
            FROM loan_repayment_allocation
            WHERE repayment_id = %s
            """,
            (repayment_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(
                f"Refusing unapplied credit for repayment_id={repayment_id}: no allocation row found."
            )
        unallocated = float(as_10dp(row[0] if isinstance(row, tuple) else row.get("unallocated", 0)))
        if unallocated + 1e-6 < amt:
            raise ValueError(
                f"Refusing unapplied credit for repayment_id={repayment_id}: "
                f"credit={amt} exceeds allocation.unallocated={unallocated}."
            )
        cur.execute(
            """
            INSERT INTO unapplied_funds (loan_id, repayment_id, amount, currency, value_date, entry_type, reference)
            VALUES (%s, %s, %s, %s, %s, 'credit', 'Overpayment')
            """,
            (loan_id, repayment_id, amt, currency, value_date),
        )
