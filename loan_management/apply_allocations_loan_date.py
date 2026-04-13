"""Batch/EOD: apply posted receipts for one loan and calendar date to allocation + unapplied."""

from __future__ import annotations

from datetime import date

from decimal_utils import as_10dp

from .db import RealDictCursor, _connection
from .unapplied_recast import _credit_unapplied_funds
from .waterfall_core import _get_waterfall_config, compute_waterfall_allocation


def apply_allocations_for_loan_date(
    loan_id: int,
    as_of_date: date,
    balances: dict[str, float],
    days_overdue: int,
    sys_cfg: dict,
) -> dict[str, float]:
    """
    Apply all posted receipts with value_date = as_of_date for this loan, in order.
    Writes to loan_repayment_allocation and unapplied_funds; returns updated balances only.
    Caller must persist the returned state to loan_daily_state (e.g. via save_loan_daily_state).

    Note: The primary allocation path is allocate_repayment_waterfall at receipt save time,
    which persists allocation and daily state in one transaction. This function is for
    batch/EOD flows that apply multiple receipts and then persist once.
    """
    profile_key, bucket_order = _get_waterfall_config(sys_cfg)
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, amount FROM loan_repayments
                WHERE loan_id = %s AND status = 'posted'
                  AND COALESCE(value_date, payment_date) = %s
                ORDER BY id
                """,
                (loan_id, as_of_date),
            )
            receipts = cur.fetchall()
    state = dict(balances)
    for rec in receipts:
        repayment_id = int(rec["id"])
        amount = float(rec["amount"] or 0)
        if amount <= 0:
            continue
        alloc, unapplied = compute_waterfall_allocation(
            amount,
            state,
            bucket_order,
            profile_key,
            state_as_of=as_of_date,
            repayment_id=repayment_id,
        )
        alloc_principal_not_due = alloc.get("alloc_principal_not_due", 0.0)
        alloc_principal_arrears = alloc.get("alloc_principal_arrears", 0.0)
        alloc_interest_accrued = alloc.get("alloc_interest_accrued", 0.0)
        alloc_interest_arrears = alloc.get("alloc_interest_arrears", 0.0)
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
                max(0.0, state.get("interest_arrears_balance", 0.0) - alloc_interest_arrears)
                + max(0.0, state.get("default_interest_balance", 0.0) - alloc_default_interest)
                + max(0.0, state.get("penalty_interest_balance", 0.0) - alloc_penalty_interest)
                + max(0.0, state.get("principal_arrears", 0.0) - alloc_principal_arrears)
                + max(0.0, state.get("fees_charges_balance", 0.0) - alloc_fees_charges)
            )
        )
        if unapplied > 1e-6 and remaining_arrears > 1e-6:
            raise ValueError(
                f"Policy violation for repayment {repayment_id}: unapplied={unapplied} while "
                f"arrears still outstanding={remaining_arrears}."
            )
        with _connection() as conn:
            with conn.cursor() as cur:
                alloc_total = alloc_principal_total + alloc_interest_total + alloc_fees_total
                cur.execute(
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
                        float(as_10dp(alloc_total)),
                        float(as_10dp(unapplied)),
                        "new_allocation",
                    ),
                )
                if unapplied > 1e-6:
                    _credit_unapplied_funds(conn, loan_id, repayment_id, unapplied, as_of_date)
        state["principal_not_due"] = max(0.0, state["principal_not_due"] - alloc_principal_not_due)
        state["principal_arrears"] = max(0.0, state["principal_arrears"] - alloc_principal_arrears)
        state["interest_accrued_balance"] = max(
            0.0, state["interest_accrued_balance"] - alloc_interest_accrued
        )
        state["interest_arrears_balance"] = max(
            0.0, state["interest_arrears_balance"] - alloc_interest_arrears
        )
        state["default_interest_balance"] = max(
            0.0, state["default_interest_balance"] - alloc_default_interest
        )
        state["penalty_interest_balance"] = max(
            0.0, state["penalty_interest_balance"] - alloc_penalty_interest
        )
        state["fees_charges_balance"] = max(0.0, state["fees_charges_balance"] - alloc_fees_charges)
    return state
