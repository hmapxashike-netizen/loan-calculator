"""Read and update core loans table rows."""

from __future__ import annotations

from typing import Any

from .db import RealDictCursor, _connection


def get_loan(loan_id: int) -> dict | None:
    """Fetch loan details by id."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM loans WHERE id = %s", (loan_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def get_loans_by_customer(customer_id: int) -> list[dict]:
    """Fetch all loans for a customer."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, loan_type, principal, disbursed_amount, term, status, created_at FROM loans WHERE customer_id = %s ORDER BY created_at DESC",
                (customer_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def update_loan_details(loan_id: int, **kwargs: Any) -> None:
    """Update selected columns on loans. Keys must be valid column names."""
    if not kwargs:
        return
    allowed = {
        "principal",
        "disbursed_amount",
        "term",
        "annual_rate",
        "monthly_rate",
        "installment",
        "total_payment",
        "end_date",
        "first_repayment_date",
        "loan_type",
    }
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    sets = ", ".join(f"{k} = %s" for k in updates) + ", updated_at = NOW()"
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE loans SET {sets} WHERE id = %s",
                (*updates.values(), loan_id),
            )
