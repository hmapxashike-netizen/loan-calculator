"""Insert posted teller receipts into ``loan_repayments`` and optional batch ingest."""

from __future__ import annotations

from datetime import date, datetime

from decimal_utils import as_10dp

from .cash_gl import validate_source_cash_gl_account_id_for_new_posting
from .db import _connection
from .loan_approval_gl_guard import require_loan_approval_gl_before_repayment
from .repayment_waterfall import allocate_repayment_waterfall
from .serialization import _date_conv


def record_repayment(
    loan_id: int,
    amount: float,
    payment_date: date | str,
    source_cash_gl_account_id: str,
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
    Reversals must use reverse_repayment(); negative amounts are rejected.
    source_cash_gl_account_id: posting leaf UUID **or** GL **code** for cash on this receipt; must
    appear in the configured source-cash cache (under A100000 tree rules).
    """
    if amount <= 0:
        raise ValueError(
            "Negative or zero amounts are not allowed. Use reverse_repayment() for reversals."
        )
    pdate = _date_conv(payment_date) if payment_date else None
    if not pdate:
        raise ValueError("payment_date is required")
    vdate = _date_conv(value_date) if value_date else pdate
    sdate = system_date
    if sdate is None:
        try:
            from eod.system_business_date import get_effective_date

            sdate = datetime.combine(get_effective_date(), datetime.now().time())
        except ImportError:
            sdate = datetime.now()
    elif isinstance(sdate, str):
        sdate = datetime.fromisoformat(sdate.replace("Z", "+00:00"))
    ref = customer_reference or reference
    require_loan_approval_gl_before_repayment(loan_id)
    src_cash = validate_source_cash_gl_account_id_for_new_posting(
        source_cash_gl_account_id,
        field_label="source_cash_gl_account_id",
    )
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO loan_repayments (
                    loan_id, schedule_line_id, period_number, amount, payment_date,
                    reference, customer_reference, company_reference, value_date, system_date, status,
                    source_cash_gl_account_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (
                    loan_id,
                    schedule_line_id,
                    period_number,
                    float(as_10dp(amount)),
                    pdate,
                    ref,
                    customer_reference,
                    company_reference,
                    vdate,
                    sdate,
                    status,
                    src_cash,
                ),
            )
            return cur.fetchone()[0]


def record_repayments_batch(rows: list[dict]) -> tuple[int, int, list[str]]:
    """
    Record multiple repayments. Each row: loan_id, amount, payment_date, customer_reference,
    company_reference, value_date (optional), system_date (optional),
    source_cash_gl_account_id (required: UUID or GL **code** — must be in the configured source-cash cache).
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
                source_cash_gl_account_id=row["source_cash_gl_account_id"],
                customer_reference=row.get("customer_reference"),
                company_reference=row.get("company_reference"),
                value_date=row.get("value_date"),
                system_date=row.get("system_date"),
            )
            allocate_repayment_waterfall(repayment_id)
            success += 1
        except Exception as e:
            fail += 1
            errors.append(f"Row {i + 1}: {e}")
    return success, fail, errors
