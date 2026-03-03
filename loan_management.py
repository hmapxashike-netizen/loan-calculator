"""
Loan management: persist loan details, schedules, and repayments to the database.
Uses loans.py for computation only; this module handles DB writes.
"""

from __future__ import annotations

import contextlib
import json
from datetime import date, datetime
from typing import Any

import pandas as pd

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor, Json
except ImportError:
    psycopg2 = None
    RealDictCursor = None
    Json = None

from config import get_database_url


def _get_conn():
    if psycopg2 is None:
        raise RuntimeError("Install psycopg2-binary to use loan_management.")
    return psycopg2.connect(get_database_url())


@contextlib.contextmanager
def _connection():
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _date_conv(v: Any) -> date | None:
    """Convert iso string or date to date for DB."""
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
    return None


def save_loan(
    customer_id: int,
    loan_type: str,
    details: dict[str, Any],
    schedule_df: pd.DataFrame,
    schedule_version: int = 1,
) -> int:
    """
    Persist loan details and schedule to DB.
    - Inserts one row into loans (loan details).
    - Inserts one row into loan_schedules (version).
    - Inserts one row per period into schedule_lines (instalments).

    details: facility, principal, term, annual_rate, monthly_rate (optional),
             drawdown_fee, arrangement_fee, admin_fee (optional),
             disbursement_date, start_date, end_date, first_repayment_date (optional),
             maturity_date (optional), installment (optional), total_payment (optional),
             grace_type (optional), moratorium_months (optional), bullet_type (optional),
             scheme (optional), payment_timing (optional), metadata (optional).

    Returns loan_id.
    """
    loan_type_db = {
        "Consumer Loan": "consumer_loan",
        "Term Loan": "term_loan",
        "Bullet Loan": "bullet_loan",
        "Customised Repayments": "customised_repayments",
    }.get(loan_type, loan_type.replace(" ", "_").lower())

    metadata = details.get("metadata") or {}
    if details.get("penalty_rate_pct") is not None:
        metadata["penalty_rate_pct"] = float(details["penalty_rate_pct"])
    if details.get("penalty_quotation"):
        metadata["penalty_quotation"] = details["penalty_quotation"]

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loans (
                    customer_id, loan_type, facility, principal, term,
                    annual_rate, monthly_rate, drawdown_fee, arrangement_fee, admin_fee,
                    disbursement_date, start_date, end_date, first_repayment_date, maturity_date,
                    installment, total_payment, grace_type, moratorium_months, bullet_type, scheme,
                    payment_timing, metadata, status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
                """,
                (
                    customer_id,
                    loan_type_db,
                    float(details.get("facility", 0)),
                    float(details.get("principal", 0)),
                    int(details.get("term", 0)),
                    details.get("annual_rate") if details.get("annual_rate") is not None else None,
                    details.get("monthly_rate") if details.get("monthly_rate") is not None else None,
                    details.get("drawdown_fee"),
                    details.get("arrangement_fee"),
                    details.get("admin_fee"),
                    _date_conv(details.get("disbursement_date") or details.get("start_date")),
                    _date_conv(details.get("start_date")),
                    _date_conv(details.get("end_date")),
                    _date_conv(details.get("first_repayment_date")),
                    _date_conv(details.get("maturity_date")),
                    float(details["installment"]) if details.get("installment") is not None else None,
                    float(details["total_payment"]) if details.get("total_payment") is not None else None,
                    details.get("grace_type"),
                    details.get("moratorium_months"),
                    details.get("bullet_type"),
                    details.get("scheme"),
                    details.get("payment_timing"),
                    Json(metadata) if metadata else None,
                    details.get("status", "active"),
                ),
            )
            loan_id = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO loan_schedules (loan_id, version) VALUES (%s, %s) RETURNING id",
                (loan_id, schedule_version),
            )
            schedule_id = cur.fetchone()[0]

        # Schedule lines: support both "Period"/"Date" and "Monthly Installment"/"Payment" column names
        with conn.cursor() as cur:
            for _, row in schedule_df.iterrows():
                period = int(row.get("Period", row.get("period", 0)))
                period_date = str(row.get("Date", row.get("Date", "")))[:32] if pd.notna(row.get("Date")) else None
                payment = float(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0)))) if pd.notna(row.get("Payment", row.get("Monthly Installment", 0))) else 0.0
                principal = float(row.get("Principal", row.get("principal", 0))) if pd.notna(row.get("Principal")) else 0.0
                interest = float(row.get("Interest", row.get("interest", 0))) if pd.notna(row.get("Interest")) else 0.0
                principal_balance = float(row.get("Principal Balance", row.get("principal_balance", 0))) if pd.notna(row.get("Principal Balance")) else 0.0
                total_outstanding = float(row.get("Total Outstanding", row.get("total_outstanding", 0))) if pd.notna(row.get("Total Outstanding")) else 0.0
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (schedule_id, period, period_date, payment, principal, interest, principal_balance, total_outstanding),
                )

    return loan_id


def record_repayment(
    loan_id: int,
    amount: float,
    payment_date: date | str,
    period_number: int | None = None,
    schedule_line_id: int | None = None,
    reference: str | None = None,
    customer_reference: str | None = None,
    company_reference: str | None = None,
    value_date: date | str | None = None,
    system_date: datetime | str | None = None,
    status: str = "posted",
) -> int:
    """
    Record an actual payment/receipt against a loan.
    customer_reference: appears on customer loan statement
    company_reference: appears in company general ledger
    value_date: effective date (default = payment_date)
    system_date: when captured (default = now)
    Returns repayment id.
    """
    pdate = _date_conv(payment_date) if payment_date else None
    if not pdate:
        raise ValueError("payment_date is required")
    vdate = _date_conv(value_date) if value_date else pdate
    sdate = system_date
    if sdate is None:
        sdate = datetime.now()
    elif isinstance(sdate, str):
        sdate = datetime.fromisoformat(sdate.replace("Z", "+00:00"))
    ref = customer_reference or reference
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO loan_repayments (
                    loan_id, schedule_line_id, period_number, amount, payment_date,
                    reference, customer_reference, company_reference, value_date, system_date, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (loan_id, schedule_line_id, period_number, amount, pdate, ref, customer_reference, company_reference, vdate, sdate, status),
            )
            return cur.fetchone()[0]


def record_repayments_batch(rows: list[dict]) -> tuple[int, int, list[str]]:
    """
    Record multiple repayments. Each row: loan_id, amount, payment_date, customer_reference, company_reference, value_date (optional), system_date (optional).
    Returns (success_count, fail_count, list of error messages).
    """
    success = 0
    fail = 0
    errors: list[str] = []
    for i, row in enumerate(rows):
        try:
            record_repayment(
                loan_id=int(row["loan_id"]),
                amount=float(row["amount"]),
                payment_date=row["payment_date"],
                customer_reference=row.get("customer_reference"),
                company_reference=row.get("company_reference"),
                value_date=row.get("value_date"),
                system_date=row.get("system_date"),
            )
            success += 1
        except Exception as e:
            fail += 1
            errors.append(f"Row {i + 1}: {e}")
    return success, fail, errors


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
            cur.execute("SELECT id, loan_type, facility, principal, term, status, created_at FROM loans WHERE customer_id = %s ORDER BY created_at DESC", (customer_id,))
            return [dict(r) for r in cur.fetchall()]


# -----------------------------------------------------------------------------
# System configurations (stored in config table)
# -----------------------------------------------------------------------------

CONFIG_KEY_SYSTEM = "system_config"


def load_system_config_from_db() -> dict | None:
    """
    Load system configurations from the config table.
    Returns None if not found or on error.
    """
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM config WHERE key = %s", (CONFIG_KEY_SYSTEM,))
                row = cur.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
    except Exception:
        pass
    return None


def save_system_config_to_db(config: dict) -> bool:
    """
    Save system configurations to the config table.
    Uses INSERT ... ON CONFLICT (key) DO UPDATE for upsert.
    Returns True on success, False on error.
    """
    try:
        value_json = json.dumps(config)
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO config (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """,
                    (CONFIG_KEY_SYSTEM, value_json),
                )
        return True
    except Exception:
        return False
