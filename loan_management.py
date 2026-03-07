"""
Loan management: persist loan details, schedules, and repayments to the database.
Uses loans.py for computation only; this module handles DB writes.
"""

from __future__ import annotations

import contextlib
import json
from datetime import date, datetime
from typing import Any, Literal

import pandas as pd

# Waterfall bucket name -> (alloc_* column name, loan_daily_state column name)
BUCKET_TO_ALLOC = {
    "fees_charges_balance": ("alloc_fees_charges", "fees_charges_balance"),
    "penalty_interest_balance": ("alloc_penalty_interest", "penalty_interest_balance"),
    "default_interest_balance": ("alloc_default_interest", "default_interest_balance"),
    "interest_arrears_balance": ("alloc_interest_arrears", "interest_arrears_balance"),
    "interest_accrued_balance": ("alloc_interest_accrued", "interest_accrued_balance"),
    "principal_arrears": ("alloc_principal_arrears", "principal_arrears"),
    "principal_not_due": ("alloc_principal_not_due", "principal_not_due"),
}


class NeedOverpaymentDecision(Exception):
    """Raised when Standard waterfall reaches bucket 6 with remaining amount; UI must ask Recast vs Unapplied."""

    def __init__(self, repayment_id: int, loan_id: int, amount_remaining: float, effective_date: date):
        self.repayment_id = repayment_id
        self.loan_id = loan_id
        self.amount_remaining = amount_remaining
        self.effective_date = effective_date
        super().__init__(
            f"Overpayment at waterfall step 6: repayment_id={repayment_id} loan_id={loan_id} "
            f"amount_remaining={amount_remaining}"
        )

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
    product_code: str | None = None,
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
    if details.get("currency"):
        metadata["currency"] = details["currency"]

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loans (
                    customer_id, loan_type, product_code, facility, principal, term,
                    annual_rate, monthly_rate, drawdown_fee, arrangement_fee, admin_fee,
                    disbursement_date, start_date, end_date, first_repayment_date, maturity_date,
                    installment, total_payment, grace_type, moratorium_months, bullet_type, scheme,
                    payment_timing, metadata, status, agent_id, relationship_manager_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
                """,
                (
                    customer_id,
                    loan_type_db,
                    product_code,
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
                    details.get("agent_id"),
                    details.get("relationship_manager_id"),
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


def reverse_repayment(
    original_repayment_id: int,
    *,
    system_date: datetime | str | None = None,
) -> int:
    """
    Insert a reversing repayment row, leaving the original immutable.
    The reversal carries a negative amount and links back to the original
    via original_repayment_id. Caller is responsible for choosing an
    appropriate value_date (typically same as original) if needed later.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM loan_repayments WHERE id = %s",
                (original_repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Repayment {original_repayment_id} not found.")

            if row["status"] == "reversed":
                raise ValueError(f"Repayment {original_repayment_id} is already reversed.")

            sdate = system_date
            if sdate is None:
                sdate = datetime.now()
            elif isinstance(sdate, str):
                sdate = datetime.fromisoformat(sdate.replace("Z", "+00:00"))

            cur.execute(
                """
                INSERT INTO loan_repayments (
                    loan_id, schedule_line_id, period_number, amount, payment_date,
                    reference, customer_reference, company_reference, value_date, system_date,
                    status, original_repayment_id
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    'reversed', %s
                )
                RETURNING id
                """,
                (
                    row["loan_id"],
                    row["schedule_line_id"],
                    row["period_number"],
                    -float(row["amount"]),
                    row["payment_date"],
                    row["reference"],
                    row["customer_reference"],
                    row["company_reference"],
                    row.get("value_date") or row["payment_date"],
                    sdate,
                    original_repayment_id,
                ),
            )
            new_id = cur.fetchone()[0]

            # Mark original as reversed for clarity (status only; no amount change)
            cur.execute(
                "UPDATE loan_repayments SET status = 'reversed' WHERE id = %s",
                (original_repayment_id,),
            )
            return new_id


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
            repayment_id = record_repayment(
                loan_id=int(row["loan_id"]),
                amount=float(row["amount"]),
                payment_date=row["payment_date"],
                customer_reference=row.get("customer_reference"),
                company_reference=row.get("company_reference"),
                value_date=row.get("value_date"),
                system_date=row.get("system_date"),
            )
            # Allocate per waterfall and update daily state.
            allocate_repayment_waterfall(repayment_id)
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


def get_latest_schedule_version(loan_id: int) -> int:
    """Return the latest schedule version number for a loan (1 = original)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(version), 1) FROM loan_schedules WHERE loan_id = %s",
                (loan_id,),
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] else 1


def get_schedule_lines(loan_id: int, schedule_version: int | None = None) -> list[dict]:
    """Fetch schedule lines for a loan. If schedule_version is None, use latest."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if schedule_version is None:
                cur.execute(
                    "SELECT id FROM loan_schedules WHERE loan_id = %s ORDER BY version DESC LIMIT 1",
                    (loan_id,),
                )
                row = cur.fetchone()
                if not row:
                    return []
                cur.execute(
                    'SELECT * FROM schedule_lines WHERE loan_schedule_id = %s ORDER BY "Period"',
                    (row["id"],),
                )
                return [dict(r) for r in cur.fetchall()]
            cur.execute(
                """
                SELECT sl.* FROM schedule_lines sl
                JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
                WHERE ls.loan_id = %s AND ls.version = %s
                ORDER BY sl."Period"
                """,
                (loan_id, schedule_version),
            )
            return [dict(r) for r in cur.fetchall()]


def get_loan_daily_state_balances(loan_id: int, as_of_date: date) -> dict[str, float] | None:
    """Get bucket balances for a loan as of a date (latest row on or before as_of_date)."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance,
                       penalty_interest_balance, fees_charges_balance, days_overdue
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date <= %s
                ORDER BY as_of_date DESC LIMIT 1
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "principal_not_due": float(row["principal_not_due"] or 0),
                "principal_arrears": float(row["principal_arrears"] or 0),
                "interest_accrued_balance": float(row["interest_accrued_balance"] or 0),
                "interest_arrears_balance": float(row["interest_arrears_balance"] or 0),
                "default_interest_balance": float(row["default_interest_balance"] or 0),
                "penalty_interest_balance": float(row["penalty_interest_balance"] or 0),
                "fees_charges_balance": float(row["fees_charges_balance"] or 0),
                "days_overdue": int(row["days_overdue"] or 0),
            }


def get_loan_daily_state_range(loan_id: int, start_date: date, end_date: date) -> list[dict]:
    """All loan_daily_state rows for a loan in [start_date, end_date] ordered by as_of_date."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT as_of_date, regular_interest_daily, principal_not_due, principal_arrears,
                       interest_accrued_balance, interest_arrears_balance,
                       default_interest_daily, default_interest_balance,
                       penalty_interest_daily, penalty_interest_balance,
                       fees_charges_balance, total_exposure
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date >= %s AND as_of_date <= %s
                ORDER BY as_of_date
                """,
                (loan_id, start_date, end_date),
            )
            return [dict(r) for r in cur.fetchall()]


def get_repayments_with_allocations(
    loan_id: int, start_date: date, end_date: date
) -> list[dict]:
    """Repayments with value_date in range (posted only) and their allocation breakdown."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT lr.id, lr.amount, lr.payment_date, lr.value_date, lr.customer_reference,
                       COALESCE(lra.alloc_interest_total, 0) AS alloc_interest_total,
                       COALESCE(lra.alloc_fees_total, 0) AS alloc_fees_total,
                       COALESCE(lra.alloc_principal_total, 0) AS alloc_principal_total
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s AND lr.status = 'posted'
                  AND COALESCE(lr.value_date, lr.payment_date) >= %s
                  AND COALESCE(lr.value_date, lr.payment_date) <= %s
                ORDER BY COALESCE(lr.value_date, lr.payment_date), lr.id
                """,
                (loan_id, start_date, end_date),
            )
            return [dict(r) for r in cur.fetchall()]


def get_unapplied_balance(loan_id: int, as_of_date: date) -> float:
    """Sum of pending unapplied_funds for the loan with value_date <= as_of_date."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM unapplied_funds
                WHERE loan_id = %s AND status = 'pending' AND value_date <= %s
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)


def get_unapplied_repayment_ids(loan_id: int, as_of_date: date) -> set[int]:
    """Repayment IDs that have a pending unapplied_funds row for this loan with value_date <= as_of_date."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT repayment_id FROM unapplied_funds
                WHERE loan_id = %s AND status = 'pending' AND value_date <= %s
                """,
                (loan_id, as_of_date),
            )
            return {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}


def save_new_schedule_version(loan_id: int, schedule_df: pd.DataFrame, version: int) -> int:
    """Insert a new schedule version and its lines. Returns the new loan_schedules.id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO loan_schedules (loan_id, version) VALUES (%s, %s) RETURNING id",
                (loan_id, version),
            )
            schedule_id = cur.fetchone()[0]
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
    return schedule_id


def update_loan_details(loan_id: int, **kwargs: Any) -> None:
    """Update selected columns on loans. Keys must be valid column names."""
    if not kwargs:
        return
    allowed = {"principal", "term", "annual_rate", "monthly_rate", "installment", "total_payment",
               "end_date", "maturity_date", "first_repayment_date", "loan_type"}
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


def get_amount_due_summary(loan_id: int, as_of: date | None = None) -> dict:
    """
    Compute a simple 'amount due today' view for a loan, based on:
      - total scheduled payments up to as_of (from schedule_lines)
      - total repayments up to as_of (from loan_repayments)
    Returns a dict with:
      {
        "amount_due": ...,
        "scheduled_total": ...,
        "repaid_total": ...,
      }
    """
    if as_of is None:
        as_of = date.today()

    with _connection() as conn:
        with conn.cursor() as cur:
            # Sum scheduled payments up to as_of
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(sl.payment), 0) AS scheduled_total
                FROM schedule_lines sl
                JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
                WHERE ls.loan_id = %s
                  AND (
                    sl."Date" IS NULL
                    OR TO_DATE(sl."Date", 'DD-Mon-YYYY') <= %s
                  )
                """,
                (loan_id, as_of),
            )
            row = cur.fetchone()
            scheduled_total = float(row[0]) if row and row[0] is not None else 0.0

            # Sum repayments up to as_of, using value_date when available
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM loan_repayments
                WHERE loan_id = %s
                  AND COALESCE(value_date, payment_date) <= %s
                """,
                (loan_id, as_of),
            )
            row = cur.fetchone()
            repaid_total = float(row[0]) if row and row[0] is not None else 0.0

    amount_due = max(scheduled_total - repaid_total, 0.0)
    return {
        "amount_due": amount_due,
        "scheduled_total": scheduled_total,
        "repaid_total": repaid_total,
    }


def save_loan_daily_state(
    loan_id: int,
    as_of_date: date,
    *,
    regular_interest_daily: float = 0.0,
    principal_not_due: float = 0.0,
    principal_arrears: float = 0.0,
    interest_accrued_balance: float = 0.0,
    interest_arrears_balance: float = 0.0,
    default_interest_daily: float = 0.0,
    default_interest_balance: float = 0.0,
    penalty_interest_daily: float = 0.0,
    penalty_interest_balance: float = 0.0,
    fees_charges_balance: float = 0.0,
    days_overdue: int = 0,
) -> None:
    """
    Upsert daily loan bucket balances into loan_daily_state.
    total_exposure is computed as the sum of all principal + interest + fees buckets.
    """
    total_exposure = (
        principal_not_due
        + principal_arrears
        + interest_accrued_balance
        + interest_arrears_balance
        + default_interest_balance
        + penalty_interest_balance
        + fees_charges_balance
    )

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_daily_state (
                    loan_id, as_of_date,
                    regular_interest_daily,
                    principal_not_due,
                    principal_arrears,
                    interest_accrued_balance,
                    interest_arrears_balance,
                    default_interest_daily,
                    default_interest_balance,
                    penalty_interest_daily,
                    penalty_interest_balance,
                    fees_charges_balance,
                    days_overdue,
                    total_exposure
                )
                VALUES (
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (loan_id, as_of_date) DO UPDATE
                SET
                    regular_interest_daily   = EXCLUDED.regular_interest_daily,
                    principal_not_due        = EXCLUDED.principal_not_due,
                    principal_arrears        = EXCLUDED.principal_arrears,
                    interest_accrued_balance = EXCLUDED.interest_accrued_balance,
                    interest_arrears_balance = EXCLUDED.interest_arrears_balance,
                    default_interest_daily   = EXCLUDED.default_interest_daily,
                    default_interest_balance = EXCLUDED.default_interest_balance,
                    penalty_interest_daily   = EXCLUDED.penalty_interest_daily,
                    penalty_interest_balance = EXCLUDED.penalty_interest_balance,
                    fees_charges_balance     = EXCLUDED.fees_charges_balance,
                    days_overdue             = EXCLUDED.days_overdue,
                    total_exposure           = EXCLUDED.total_exposure
                """,
                (
                    loan_id,
                    as_of_date,
                    regular_interest_daily,
                    principal_not_due,
                    principal_arrears,
                    interest_accrued_balance,
                    interest_arrears_balance,
                    default_interest_daily,
                    default_interest_balance,
                    penalty_interest_daily,
                    penalty_interest_balance,
                    fees_charges_balance,
                    days_overdue,
                    total_exposure,
                ),
            )


def _credit_unapplied_funds(
    conn,
    loan_id: int,
    repayment_id: int,
    amount: float,
    value_date: date,
    currency: str = "USD",
) -> None:
    """Insert a row into unapplied_funds (suspense) for overpayment."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO unapplied_funds (loan_id, repayment_id, amount, currency, value_date, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            """,
            (loan_id, repayment_id, amount, currency, value_date),
        )


def allocate_repayment_waterfall(
    repayment_id: int,
    *,
    as_of: date | None = None,
    overpayment_action: Literal["unapplied", "recast"] | None = None,
    system_config: dict | None = None,
) -> None:
    """
    Allocate a repayment across loan buckets using the configured waterfall
    (Standard or Borrower-friendly) and loan_daily_state. Persists allocation
    and updates daily state.

    When the active profile is Standard and there is remaining amount at
    waterfall step 6 (principal_arrears), raises NeedOverpaymentDecision unless
    overpayment_action is set:
      - overpayment_action="unapplied": credit remainder to unapplied_funds.
      - overpayment_action="recast": allocate remainder to principal_not_due
        (caller should then run loan recast to regenerate schedule).
    """
    if as_of is None:
        as_of = date.today()

    cfg = system_config or load_system_config_from_db() or {}
    profile_name = (cfg.get("payment_waterfall") or "Standard").strip().lower()
    if profile_name.startswith("borrower"):
        profile_key = "borrower_friendly"
    else:
        profile_key = "standard"
    profiles = cfg.get("waterfall_profiles") or {}
    bucket_order = profiles.get(profile_key)
    if not bucket_order:
        bucket_order = list(BUCKET_TO_ALLOC.keys())

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
                return

            loan_id = int(row["loan_id"])
            eff_date = row["eff_date"] or as_of
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            cur.execute(
                """
                SELECT principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance,
                       penalty_interest_balance, fees_charges_balance, days_overdue
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date <= %s
                ORDER BY as_of_date DESC LIMIT 1
                """,
                (loan_id, eff_date),
            )
            st_row = cur.fetchone()
            balances: dict[str, float] = {
                "principal_not_due": 0.0,
                "principal_arrears": 0.0,
                "interest_accrued_balance": 0.0,
                "interest_arrears_balance": 0.0,
                "default_interest_balance": 0.0,
                "penalty_interest_balance": 0.0,
                "fees_charges_balance": 0.0,
            }
            if st_row:
                for k in balances:
                    balances[k] = float(st_row.get(k, 0) or 0)
            days_overdue = int(st_row["days_overdue"]) if st_row else 0

            alloc: dict[str, float] = {k: 0.0 for k in BUCKET_TO_ALLOC}
            remaining = amount
            overpayment_at_step6: float | None = None

            for idx, bucket_name in enumerate(bucket_order):
                if bucket_name not in BUCKET_TO_ALLOC:
                    continue
                alloc_key, state_key = BUCKET_TO_ALLOC[bucket_name]
                bucket_balance = balances.get(state_key, 0.0)
                to_alloc = min(remaining, max(0.0, bucket_balance))
                alloc[alloc_key] = to_alloc
                remaining -= to_alloc
                if remaining <= 1e-6:
                    remaining = 0.0
                    break
                if profile_key == "standard" and idx == 5 and remaining > 1e-6:
                    overpayment_at_step6 = round(remaining, 2)
                    # When nothing is due, entire payment is unallocated: auto-credit to unapplied
                    # so it shows in Unapplied column without requiring a second step.
                    if remaining >= amount - 1e-6:
                        _credit_unapplied_funds(
                            conn, loan_id, repayment_id, round(remaining, 2), eff_date
                        )
                        remaining = 0.0
                        break
                    if overpayment_action is None:
                        raise NeedOverpaymentDecision(
                            repayment_id, loan_id, overpayment_at_step6, eff_date
                        )
                    if overpayment_action == "unapplied":
                        _credit_unapplied_funds(
                            conn, loan_id, repayment_id, overpayment_at_step6, eff_date
                        )
                        remaining = 0.0
                        break
                    if overpayment_action == "recast":
                        alloc["alloc_principal_not_due"] += remaining
                        remaining = 0.0
                        break

            if remaining > 1e-6 and profile_key == "standard":
                idx6 = next((i for i, b in enumerate(bucket_order) if b == "principal_arrears"), -1)
                if idx6 >= 0:
                    for i in range(idx6 + 1, len(bucket_order)):
                        b = bucket_order[i]
                        if b not in BUCKET_TO_ALLOC:
                            continue
                        ak, sk = BUCKET_TO_ALLOC[b]
                        bal = balances.get(sk, 0.0)
                        ta = min(remaining, max(0.0, bal))
                        alloc[ak] = ta
                        remaining -= ta
                        if remaining <= 1e-6:
                            break
                # When nothing was allocated (full amount remaining), auto-credit to unapplied.
                if remaining > 1e-6 and remaining >= amount - 1e-6:
                    _credit_unapplied_funds(conn, loan_id, repayment_id, round(remaining, 2), eff_date)
                    remaining = 0.0
                elif remaining > 1e-6 and overpayment_action is None:
                    raise NeedOverpaymentDecision(
                        repayment_id, loan_id, round(remaining, 2), eff_date
                    )
                elif remaining > 1e-6 and overpayment_action == "unapplied":
                    _credit_unapplied_funds(conn, loan_id, repayment_id, round(remaining, 2), eff_date)
                    remaining = 0.0
                elif remaining > 1e-6 and overpayment_action == "recast":
                    alloc["alloc_principal_not_due"] += remaining
                    remaining = 0.0

            if remaining > 1e-6:
                alloc["alloc_principal_not_due"] += remaining

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
            total_alloc = alloc_principal_total + alloc_interest_total + alloc_fees_total
            if abs(total_alloc - amount) > 0.01:
                raise ValueError(
                    f"Allocation mismatch for repayment {repayment_id}: "
                    f"amount={amount}, allocated={total_alloc}"
                )

            cur2 = conn.cursor()
            cur2.execute(
                """
                INSERT INTO loan_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (repayment_id) DO UPDATE SET
                    alloc_principal_not_due = EXCLUDED.alloc_principal_not_due,
                    alloc_principal_arrears = EXCLUDED.alloc_principal_arrears,
                    alloc_interest_accrued = EXCLUDED.alloc_interest_accrued,
                    alloc_interest_arrears = EXCLUDED.alloc_interest_arrears,
                    alloc_default_interest = EXCLUDED.alloc_default_interest,
                    alloc_penalty_interest = EXCLUDED.alloc_penalty_interest,
                    alloc_fees_charges = EXCLUDED.alloc_fees_charges,
                    alloc_principal_total = EXCLUDED.alloc_principal_total,
                    alloc_interest_total = EXCLUDED.alloc_interest_total,
                    alloc_fees_total = EXCLUDED.alloc_fees_total
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
                ),
            )
            cur2.close()

            new_principal_not_due = balances["principal_not_due"] - alloc_principal_not_due
            new_principal_arrears = balances["principal_arrears"] - alloc_principal_arrears
            new_interest_accrued = balances["interest_accrued_balance"] - alloc_interest_accrued
            new_interest_arrears = balances["interest_arrears_balance"] - alloc_interest_arrears
            new_default_interest = balances["default_interest_balance"] - alloc_default_interest
            new_penalty_interest = balances["penalty_interest_balance"] - alloc_penalty_interest
            new_fees_charges = balances["fees_charges_balance"] - alloc_fees_charges

            save_loan_daily_state(
                loan_id=loan_id,
                as_of_date=eff_date,
                regular_interest_daily=0.0,
                principal_not_due=new_principal_not_due,
                principal_arrears=new_principal_arrears,
                interest_accrued_balance=new_interest_accrued,
                interest_arrears_balance=new_interest_arrears,
                default_interest_daily=0.0,
                default_interest_balance=new_default_interest,
                penalty_interest_daily=0.0,
                penalty_interest_balance=new_penalty_interest,
                fees_charges_balance=new_fees_charges,
                days_overdue=days_overdue,
            )


# -----------------------------------------------------------------------------
# Products
# -----------------------------------------------------------------------------

def list_products(active_only: bool = True) -> list[dict]:
    """List products. Each dict: id, code, name, loan_type, is_active, created_at, updated_at."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                where = " WHERE is_active = TRUE" if active_only else ""
                cur.execute(
                    f"SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products{where} ORDER BY code"
                )
                return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []


def get_product(product_id: int) -> dict | None:
    """Get product by id."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products WHERE id = %s",
                    (product_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
    except Exception:
        return None


def get_product_by_code(code: str) -> dict | None:
    """Get product by code."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products WHERE code = %s",
                    (code.strip(),),
                )
                row = cur.fetchone()
                return dict(row) if row else None
    except Exception:
        return None


def create_product(code: str, name: str, loan_type: str) -> int:
    """Create a product. Returns product id."""
    code = code.strip().upper()
    name = name.strip()
    lt = {"Consumer Loan": "consumer_loan", "Term Loan": "term_loan", "Bullet Loan": "bullet_loan", "Customised Repayments": "customised_repayments"}.get(
        loan_type, loan_type.replace(" ", "_").lower()
    )
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products (code, name, loan_type, is_active) VALUES (%s, %s, %s, TRUE) RETURNING id",
                (code, name, lt),
            )
            return cur.fetchone()[0]


def update_product(product_id: int, *, name: str | None = None, loan_type: str | None = None, is_active: bool | None = None) -> None:
    """Update product name, loan_type, and/or is_active."""
    updates = []
    args = []
    if name is not None:
        updates.append("name = %s")
        args.append(name.strip())
    if loan_type is not None:
        lt = {"Consumer Loan": "consumer_loan", "Term Loan": "term_loan", "Bullet Loan": "bullet_loan", "Customised Repayments": "customised_repayments"}.get(
            loan_type, loan_type.replace(" ", "_").lower()
        )
        updates.append("loan_type = %s")
        args.append(lt)
    if is_active is not None:
        updates.append("is_active = %s")
        args.append(is_active)
    if not updates:
        return
    args.append(product_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE products SET updated_at = NOW(), {', '.join(updates)} WHERE id = %s",
                args,
            )


def delete_product(product_id: int) -> None:
    """Delete a product and its config. Raises ValueError if any loans reference this product."""
    CONFIG_KEY_PRODUCT_PREFIX = "product_config:"
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, code FROM products WHERE id = %s", (product_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("Product not found.")
            code = row["code"]
            cur.execute("SELECT COUNT(*) AS n FROM loans WHERE product_code = %s", (code,))
            n = cur.fetchone()["n"] or 0
            if n > 0:
                raise ValueError(f"Cannot delete: {n} loan(s) use this product. Deactivate it instead.")
            cur.execute("DELETE FROM config WHERE key = %s", (CONFIG_KEY_PRODUCT_PREFIX + code,))
            cur.execute("DELETE FROM products WHERE id = %s", (product_id,))


CONFIG_KEY_PRODUCT_PREFIX = "product_config:"


def get_product_config_from_db(code: str) -> dict | None:
    """Load product config JSON from config table."""
    try:
        key = CONFIG_KEY_PRODUCT_PREFIX + code.strip()
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM config WHERE key = %s", (key,))
                row = cur.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
    except Exception:
        pass
    return None


def save_product_config_to_db(code: str, config: dict) -> bool:
    """Save product config JSON."""
    try:
        key = CONFIG_KEY_PRODUCT_PREFIX + code.strip()
        value_json = json.dumps(config)
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO config (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """,
                    (key, value_json),
                )
        return True
    except Exception:
        return False


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
