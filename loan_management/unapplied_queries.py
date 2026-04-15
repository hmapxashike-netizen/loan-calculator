"""Read queries for unapplied balances, ledger, and ``unapplied_funds``."""

from __future__ import annotations

from datetime import date
from typing import Any

from .db import RealDictCursor, _connection


def get_unapplied_repayment_ids(loan_id: int, as_of_date: date) -> set[int]:
    """Repayment IDs that created unapplied credits for this loan with value_date <= as_of_date."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT repayment_id FROM unapplied_funds
                WHERE loan_id = %s AND value_date <= %s AND amount > 0 AND repayment_id IS NOT NULL
                """,
                (loan_id, as_of_date),
            )
            return {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}


def get_unapplied_balance(loan_id: int, as_of_date: date) -> float:
    """Balance = SUM(amount) for the loan with value_date <= as_of_date (ledger-style)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM unapplied_funds
                WHERE loan_id = %s AND value_date <= %s
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)


def get_unapplied_ledger_balance(loan_id: int, as_of_date: date) -> float:
    """Balance = SUM(amount) for loan_id, value_date <= as_of_date (ledger-style single table)."""
    return get_unapplied_balance(loan_id, as_of_date)


def get_loans_with_unapplied_balance(as_of_date: date) -> list[int]:
    """Loan IDs with unapplied balance > 0 as of the given date (ledger-style)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT loan_id
                FROM unapplied_funds
                WHERE value_date <= %s
                GROUP BY loan_id
                HAVING COALESCE(SUM(amount), 0) > 0
                ORDER BY loan_id
                """,
                (as_of_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def get_loan_ids_with_unapplied_balance_and_arrears_for_eod(as_of_date: date) -> list[int]:
    """
    Subset of :func:`get_loans_with_unapplied_balance` that can actually consume
    unapplied toward arrears in EOD.

    Matches the early exits in ``apply_unapplied_funds_to_arrears_eod``:
    net unapplied (value_date <= as_of) > 0 and latest ``loan_daily_state`` on
    or before ``as_of_date`` has a positive sum of the five arrears-related buckets
    (interest arrears, default, penalty, principal arrears, fees).
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH unapplied_positive AS (
                    SELECT loan_id
                    FROM unapplied_funds
                    WHERE value_date <= %s
                    GROUP BY loan_id
                    HAVING COALESCE(SUM(amount), 0) > 0
                ),
                latest_daily AS (
                    SELECT DISTINCT ON (loan_id)
                        loan_id,
                        COALESCE(principal_arrears, 0) AS principal_arrears,
                        COALESCE(interest_arrears_balance, 0) AS interest_arrears_balance,
                        COALESCE(default_interest_balance, 0) AS default_interest_balance,
                        COALESCE(penalty_interest_balance, 0) AS penalty_interest_balance,
                        COALESCE(fees_charges_balance, 0) AS fees_charges_balance
                    FROM loan_daily_state
                    WHERE as_of_date <= %s
                    ORDER BY loan_id, as_of_date DESC
                )
                SELECT up.loan_id
                FROM unapplied_positive up
                INNER JOIN latest_daily ld ON ld.loan_id = up.loan_id
                WHERE (
                    ld.interest_arrears_balance
                    + ld.default_interest_balance
                    + ld.penalty_interest_balance
                    + ld.principal_arrears
                    + ld.fees_charges_balance
                ) > 0
                ORDER BY up.loan_id
                """,
                (as_of_date, as_of_date),
            )
            return [int(r[0]) for r in cur.fetchall()]


def get_unapplied_entries(loan_id: int, through_date: date) -> list[tuple[date, float]]:
    """
    Unapplied entries for statement from loan_repayment_allocation.unallocated only.
    Returns list of (value_date, amount) for in-memory aggregation.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    COALESCE(lra.unallocated, 0) AS amount
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND (COALESCE(lr.value_date, lr.payment_date))::date <= %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                  )
                ORDER BY value_date, lr.id
                """,
                (loan_id, through_date),
            )
            rows = cur.fetchall()
    out: list[tuple[date, float]] = []
    for r in rows:
        vd = r[0]
        if hasattr(vd, "date"):
            vd = vd.date() if callable(getattr(vd, "date")) else vd
        out.append((vd, float(r[1] or 0)))
    return out


def get_unapplied_ledger_entries_for_statement(
    loan_id: int,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """
    Statement-facing unapplied ledger lines from the unapplied_funds_ledger view.
    This view mirrors the export ledger logic:
      - Credits/reversals from receipt allocation (loan_repayment_allocation totals vs amount)
      - Liquidations from loan_repayment_allocation with event_type='unapplied_funds_allocation'
    Returns entry_kind in ('credit','reversal','liquidation') with unapplied_running_balance.
    Includes all entries with value_date <= end_date so running balance is correct.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    ufl.repayment_id,
                    ufl.repayment_key,
                    ufl.loan_id,
                    ufl.value_date,
                    ufl.entry_kind,
                    ufl.liquidation_repayment_id,
                    ufl.unapplied_delta,
                    ufl.alloc_prin_arrears,
                    ufl.alloc_int_arrears,
                    ufl.alloc_penalty_int,
                    ufl.alloc_default_int,
                    ufl.alloc_fees_charges,
                    ufl.unapplied_running_balance,
                    ufl.parent_repayment_id,
                    ufl.reversal_of_id,
                    lr.amount AS source_receipt_amount
                FROM unapplied_funds_ledger ufl
                LEFT JOIN loan_repayments lr ON lr.id = ufl.repayment_id
                WHERE ufl.loan_id = %s
                  AND ufl.value_date <= %s
                ORDER BY ufl.value_date, ufl.repayment_id, ufl.entry_kind
                """,
                (loan_id, end_date),
            )
            return [dict(r) for r in cur.fetchall()]
