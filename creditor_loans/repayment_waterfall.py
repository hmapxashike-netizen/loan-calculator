"""Allocate creditor repayment across mirror buckets (one allocation row per repayment)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from .db import _connection

_BUCKET_TO_BALANCE_KEY: dict[str, str] = {
    "principal_not_due": "principal_not_due",
    "principal_arrears": "principal_arrears",
    "interest_accrued_balance": "interest_accrued_balance",
    "interest_arrears_balance": "interest_arrears_balance",
    "default_interest_balance": "default_interest_balance",
    "penalty_interest_balance": "penalty_interest_balance",
    "fees_charges_balance": "fees_charges_balance",
}

_BUCKET_TO_ALLOC_COL: dict[str, str] = {
    "principal_not_due": "alloc_principal_not_due",
    "principal_arrears": "alloc_principal_arrears",
    "interest_accrued_balance": "alloc_interest_accrued",
    "interest_arrears_balance": "alloc_interest_arrears",
    "default_interest_balance": "alloc_default_interest",
    "penalty_interest_balance": "alloc_penalty_interest",
    "fees_charges_balance": "alloc_fees_charges",
}


def allocate_creditor_repayment_waterfall(
    creditor_loan_id: int,
    repayment_id: int,
    amount: Decimal,
    *,
    balances: dict[str, float],
    waterfall_bucket_order: list[str],
    value_date: date,
    conn=None,
) -> dict[str, Decimal]:
    """
    Apply ``amount`` in waterfall order against ``balances`` (persisted bucket column names).

    Upserts ``creditor_repayment_allocation``; inserts ``creditor_unapplied_funds`` when remainder > 0.
    """
    amt = as_10dp(amount)
    if amt <= 0:
        raise ValueError("amount must be positive")

    remaining = amt
    alloc_cols: dict[str, Decimal] = {
        "alloc_principal_not_due": Decimal("0"),
        "alloc_principal_arrears": Decimal("0"),
        "alloc_interest_accrued": Decimal("0"),
        "alloc_interest_arrears": Decimal("0"),
        "alloc_default_interest": Decimal("0"),
        "alloc_penalty_interest": Decimal("0"),
        "alloc_fees_charges": Decimal("0"),
    }

    for bucket in waterfall_bucket_order:
        if remaining <= 0:
            break
        bkey = _BUCKET_TO_BALANCE_KEY.get(bucket)
        col = _BUCKET_TO_ALLOC_COL.get(bucket)
        if not bkey or not col:
            continue
        bucket_bal = Decimal(str(as_10dp(balances.get(bkey, 0) or 0)))
        take = min(remaining, bucket_bal)
        if take > 0:
            alloc_cols[col] = as_10dp(alloc_cols[col] + take)
            remaining = as_10dp(remaining - take)

    alloc_principal_total = as_10dp(alloc_cols["alloc_principal_not_due"] + alloc_cols["alloc_principal_arrears"])
    alloc_interest_total = as_10dp(
        alloc_cols["alloc_interest_accrued"]
        + alloc_cols["alloc_interest_arrears"]
        + alloc_cols["alloc_default_interest"]
        + alloc_cols["alloc_penalty_interest"]
    )
    alloc_fees_total = alloc_cols["alloc_fees_charges"]

    def _do(c):
        with c.cursor() as cur:
            cur.execute("DELETE FROM creditor_repayment_allocation WHERE repayment_id = %s", (repayment_id,))
            cur.execute(
                """
                INSERT INTO creditor_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    repayment_id,
                    alloc_cols["alloc_principal_not_due"],
                    alloc_cols["alloc_principal_arrears"],
                    alloc_cols["alloc_interest_accrued"],
                    alloc_cols["alloc_interest_arrears"],
                    alloc_cols["alloc_default_interest"],
                    alloc_cols["alloc_penalty_interest"],
                    alloc_cols["alloc_fees_charges"],
                    alloc_principal_total,
                    alloc_interest_total,
                    alloc_fees_total,
                ),
            )
            if remaining > 0:
                cur.execute(
                    """
                    INSERT INTO creditor_unapplied_funds (
                        creditor_drawdown_id, creditor_repayment_id, amount, value_date, entry_type, reference
                    ) VALUES (%s, %s, %s, %s, 'credit', %s)
                    """,
                    (
                        creditor_loan_id,
                        repayment_id,
                        float(remaining),
                        value_date,
                        "Partial payment — unapplied",
                    ),
                )

    if conn is not None:
        _do(conn)
    else:
        with _connection() as c:
            _do(c)

    return {
        "principal": alloc_principal_total,
        "interest": alloc_interest_total,
        "fees": alloc_fees_total,
        "unapplied": remaining,
        **{k: v for k, v in alloc_cols.items()},
    }
