"""CRUD-style reads/updates for the ``loans`` table."""

from __future__ import annotations

from typing import Any

from .db import Json, RealDictCursor, _connection


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
                "SELECT id, loan_type, principal, disbursed_amount, term, status, created_at "
                "FROM loans WHERE customer_id = %s ORDER BY created_at DESC",
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


def update_loan_safe_details(
    loan_id: int,
    updates: dict[str, Any],
) -> None:
    """Update safe fields on an active loan without changing schedules or financials."""
    allowed_keys = {
        "collateral_security_subtype_id",
        "collateral_charge_amount",
        "collateral_valuation_amount",
        "metadata",
    }
    set_clauses = []
    params = []

    has_meta = False
    meta_val = None
    for k, v in updates.items():
        if k not in allowed_keys:
            continue
        if k == "metadata":
            has_meta = True
            meta_val = v
            continue
        set_clauses.append(f"{k} = %s")
        params.append(v)

    with _connection() as conn:
        with conn.cursor() as cur:
            if has_meta:
                cur.execute("SELECT metadata FROM loans WHERE id = %s", (loan_id,))
                row = cur.fetchone()
                existing_meta = row[0] if row and row[0] else {}
                if isinstance(existing_meta, str):
                    import json

                    try:
                        existing_meta = json.loads(existing_meta)
                    except Exception:
                        existing_meta = {}
                if isinstance(meta_val, dict):
                    existing_meta.update(meta_val)
                set_clauses.append("metadata = %s")
                params.append(Json(existing_meta))

            if set_clauses:
                set_clauses.append("updated_at = NOW()")
                query = f"UPDATE loans SET {', '.join(set_clauses)} WHERE id = %s"
                params.append(loan_id)
                cur.execute(query, tuple(params))
