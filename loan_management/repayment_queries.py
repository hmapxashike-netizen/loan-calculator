"""Lightweight read queries on ``loan_repayments`` (IDs by date / status)."""

from __future__ import annotations

from datetime import date

from .db import _connection


def get_repayment_ids_for_value_date(value_date: date) -> list[int]:
    """Repayment IDs with value_date (or payment_date) on the given date, posted only. Order by id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayments
                WHERE status = 'posted'
                  AND COALESCE(reference, '') <> 'Unapplied funds allocation'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY id
                """,
                (value_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def get_liquidation_repayment_ids_for_value_date(value_date: date) -> list[int]:
    """
    Posted system liquidation repayment IDs (unapplied funds allocation) on value_date.

    Same date rule as :func:`get_repayment_ids_for_value_date` (value_date or payment_date).
    Ordered by ``id`` for deterministic replay.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayments
                WHERE status = 'posted'
                  AND COALESCE(reference, '') = 'Unapplied funds allocation'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY id
                """,
                (value_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def get_batch_loan_ids_with_reversed_receipts_in_range(
    loan_ids: list[int],
    start_exclusive: date,
    end_inclusive: date,
) -> set[int]:
    """
    Loans among ``loan_ids`` with at least one reversed receipt whose value_date
    (or payment_date) is in (start_exclusive, end_inclusive].
    """
    if not loan_ids:
        return set()
    lids = [int(x) for x in loan_ids]
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT loan_id FROM loan_repayments
                WHERE loan_id = ANY(%s)
                  AND status = 'reversed'
                  AND (COALESCE(value_date, payment_date))::date > %s::date
                  AND (COALESCE(value_date, payment_date))::date <= %s::date
                """,
                (lids, start_exclusive, end_inclusive),
            )
            return {int(r[0]) for r in cur.fetchall()}


def get_loan_ids_with_reversed_receipts_on_date(value_date: date) -> list[int]:
    """
    Loan IDs that have at least one reversed receipt on the given value_date.
    Used by EOD to identify loans needing reallocation of other same-day receipts
    so the waterfall is preserved after reversals.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT loan_id FROM loan_repayments
                WHERE status = 'reversed'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY loan_id
                """,
                (value_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def get_repayment_ids_for_loan_and_date(loan_id: int, value_date: date) -> list[int]:
    """Posted repayment IDs for a specific loan on the given value_date. Order by id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayments
                WHERE loan_id = %s
                  AND status = 'posted'
                  AND COALESCE(reference, '') <> 'Unapplied funds allocation'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY id
                """,
                (loan_id, value_date),
            )
            return [int(r[0]) for r in cur.fetchall()]
