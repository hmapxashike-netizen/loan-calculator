"""CRUD for creditor counterparties, facilities, drawdowns, and schedules."""

from __future__ import annotations

from typing import Any

import pandas as pd

from decimal_utils import as_10dp

from .db import RealDictCursor, _connection


def list_counterparties(*, active_only: bool = True) -> list[dict[str, Any]]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = "SELECT * FROM creditor_counterparties"
            if active_only:
                q += " WHERE status = 'active'"
            q += " ORDER BY name"
            cur.execute(q)
            return [dict(r) for r in cur.fetchall()]


def create_counterparty(
    name: str,
    *,
    reference_code: str | None = None,
    tax_id: str | None = None,
    status: str = "active",
) -> int:
    st = (status or "active").strip().lower()
    if st not in ("active", "inactive", "deleted"):
        raise ValueError("status must be active, inactive, or deleted")
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO creditor_counterparties (name, reference_code, tax_id, status)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (name.strip(), (reference_code or "").strip() or None, (tax_id or "").strip() or None, st),
            )
            return int(cur.fetchone()[0])


def update_counterparty_status(counterparty_id: int, status: str) -> None:
    st = (status or "").strip().lower()
    if st not in ("active", "inactive", "deleted"):
        raise ValueError("status must be active, inactive, or deleted")
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE creditor_counterparties SET status = %s, updated_at = NOW() WHERE id = %s",
                (st, int(counterparty_id)),
            )
        conn.commit()


def create_facility(
    creditor_counterparty_id: int,
    *,
    facility_limit: float,
    facility_expiry_date: Any = None,
    facility_fee_amount: float = 0.0,
    status: str = "active",
) -> int:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO creditor_facilities (
                    creditor_counterparty_id, facility_limit, facility_expiry_date,
                    facility_fee_amount, status
                ) VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(creditor_counterparty_id),
                    float(as_10dp(facility_limit)),
                    facility_expiry_date,
                    float(as_10dp(facility_fee_amount)),
                    (status or "active").strip(),
                ),
            )
            return int(cur.fetchone()[0])


def update_facility(
    facility_id: int,
    *,
    facility_limit: float,
    facility_expiry_date: Any = None,
    facility_fee_amount: float = 0.0,
    status: str = "active",
) -> None:
    st = (status or "active").strip().lower()
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE creditor_facilities
                SET facility_limit = %s,
                    facility_expiry_date = %s,
                    facility_fee_amount = %s,
                    status = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    float(as_10dp(facility_limit)),
                    facility_expiry_date,
                    float(as_10dp(facility_fee_amount)),
                    st,
                    int(facility_id),
                ),
            )
        conn.commit()


def list_facilities(
    *,
    creditor_counterparty_id: int | None = None,
    status: str | None = "active",
) -> list[dict[str, Any]]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = """
                SELECT f.*, cp.name AS counterparty_name
                FROM creditor_facilities f
                JOIN creditor_counterparties cp ON cp.id = f.creditor_counterparty_id
                WHERE 1=1
            """
            params: list[Any] = []
            if creditor_counterparty_id is not None:
                q += " AND f.creditor_counterparty_id = %s"
                params.append(int(creditor_counterparty_id))
            if status:
                q += " AND f.status = %s"
                params.append(status)
            q += " ORDER BY f.id DESC"
            cur.execute(q, params)
            return [dict(r) for r in cur.fetchall()]


def get_facility(facility_id: int) -> dict[str, Any] | None:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT f.*, cp.name AS counterparty_name
                FROM creditor_facilities f
                JOIN creditor_counterparties cp ON cp.id = f.creditor_counterparty_id
                WHERE f.id = %s
                """,
                (int(facility_id),),
            )
            r = cur.fetchone()
            return dict(r) if r else None


def list_creditor_loan_types() -> list[dict[str, Any]]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT code, label, behavior_json FROM creditor_loan_types ORDER BY label")
            return [dict(r) for r in cur.fetchall()]


def list_creditor_loans(*, status: str | None = "active") -> list[dict[str, Any]]:
    """List drawdowns (legacy name); includes counterparty via facility."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if status:
                cur.execute(
                    """
                    SELECT cd.*, cp.name AS counterparty_name
                    FROM creditor_drawdowns cd
                    JOIN creditor_facilities f ON f.id = cd.creditor_facility_id
                    JOIN creditor_counterparties cp ON cp.id = f.creditor_counterparty_id
                    WHERE cd.status = %s
                    ORDER BY cd.id DESC
                    """,
                    (status,),
                )
            else:
                cur.execute(
                    """
                    SELECT cd.*, cp.name AS counterparty_name
                    FROM creditor_drawdowns cd
                    JOIN creditor_facilities f ON f.id = cd.creditor_facility_id
                    JOIN creditor_counterparties cp ON cp.id = f.creditor_counterparty_id
                    ORDER BY cd.id DESC
                    """
                )
            return [dict(r) for r in cur.fetchall()]


def get_creditor_loan(creditor_loan_id: int) -> dict[str, Any] | None:
    """Load one drawdown by id (legacy parameter name)."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT cd.*, cp.name AS counterparty_name, lt.behavior_json AS type_behavior_json,
                       f.id AS creditor_facility_id, f.creditor_counterparty_id
                FROM creditor_drawdowns cd
                JOIN creditor_facilities f ON f.id = cd.creditor_facility_id
                JOIN creditor_counterparties cp ON cp.id = f.creditor_counterparty_id
                JOIN creditor_loan_types lt ON lt.code = cd.creditor_loan_type_code
                WHERE cd.id = %s
                """,
                (int(creditor_loan_id),),
            )
            r = cur.fetchone()
            return dict(r) if r else None


def update_creditor_drawdown(
    drawdown_id: int,
    *,
    status: str | None = None,
    accrual_mode: str | None = None,
) -> None:
    """Update safe operational fields on a drawdown (not principal or schedule)."""
    if status is None and accrual_mode is None:
        return
    if accrual_mode is not None:
        am = str(accrual_mode).strip()
        if am not in ("daily_mirror", "periodic_schedule"):
            raise ValueError("accrual_mode must be daily_mirror or periodic_schedule")
    with _connection() as conn:
        with conn.cursor() as cur:
            if status is not None and accrual_mode is not None:
                cur.execute(
                    """
                    UPDATE creditor_drawdowns
                    SET status = %s, accrual_mode = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (str(status).strip(), str(accrual_mode).strip(), int(drawdown_id)),
                )
            elif status is not None:
                cur.execute(
                    """
                    UPDATE creditor_drawdowns SET status = %s, updated_at = NOW() WHERE id = %s
                    """,
                    (str(status).strip(), int(drawdown_id)),
                )
            else:
                cur.execute(
                    """
                    UPDATE creditor_drawdowns SET accrual_mode = %s, updated_at = NOW() WHERE id = %s
                    """,
                    (str(accrual_mode).strip(), int(drawdown_id)),
                )
        conn.commit()


def get_creditor_schedule_lines(creditor_loan_id: int) -> list[dict[str, Any]]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT csl.*
                FROM creditor_schedule_lines csl
                JOIN creditor_loan_schedules s ON s.id = csl.creditor_loan_schedule_id
                WHERE s.creditor_drawdown_id = %s AND s.version = 1
                ORDER BY csl."Period"
                """,
                (int(creditor_loan_id),),
            )
            return [dict(r) for r in cur.fetchall()]


def insert_creditor_schedule_from_dataframe(
    conn,
    creditor_loan_schedule_id: int,
    schedule_df: pd.DataFrame,
) -> None:
    """Persist schedule lines (column names aligned with debtor schedule_lines)."""
    with conn.cursor() as cur:
        for _, row in schedule_df.iterrows():
            period = int(row.get("Period") or row.get("period") or 0)
            period_date = str(row.get("Date") or row.get("date") or "")
            payment = float(
                as_10dp(
                    row.get("Payment")
                    or row.get("payment")
                    or row.get("Monthly Installment")
                    or 0
                )
            )
            principal = float(as_10dp(row.get("Principal") or row.get("principal") or 0))
            interest = float(as_10dp(row.get("Interest") or row.get("interest") or 0))
            pb = float(as_10dp(row.get("Principal Balance") or row.get("principal_balance") or 0))
            to = float(as_10dp(row.get("Total Outstanding") or row.get("total_outstanding") or 0))
            cur.execute(
                """
                INSERT INTO creditor_schedule_lines (
                    creditor_loan_schedule_id, "Period", "Date", payment, principal, interest,
                    principal_balance, total_outstanding
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    creditor_loan_schedule_id,
                    period,
                    period_date,
                    payment,
                    principal,
                    interest,
                    pb,
                    to,
                ),
            )
