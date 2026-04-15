"""Insert posted teller receipts into ``loan_repayments`` and optional batch ingest."""

from __future__ import annotations

import os
from datetime import date, datetime

from decimal_utils import as_10dp

from .cash_gl import validate_source_cash_gl_account_id_for_new_posting
from .db import _connection, connect_loan_management
from .loan_approval_gl_guard import require_loan_approval_gl_before_repayment
from .product_catalog import load_system_config_from_db
from .repayment_waterfall import allocate_repayment_waterfall
from .serialization import _date_conv

# Default rows per DB connection when not set in system config or env (see System configurations → EOD).
_DEFAULT_REPAYMENT_BATCH_SLICE_SIZE = 100


def _repayment_batch_slice_size_from_system_config(system_config: dict | None) -> int | None:
    """
    Read ``eod_settings.tasks.repayment_batch_slice_size`` from DB-backed system config.

    Returns a positive int (clamped to 500) when set and valid; otherwise None so callers
    fall back to env / default.
    """
    if not system_config or not isinstance(system_config, dict):
        return None
    eod = system_config.get("eod_settings")
    if not isinstance(eod, dict):
        return None
    tasks = eod.get("tasks")
    if not isinstance(tasks, dict):
        return None
    raw = tasks.get("repayment_batch_slice_size")
    if raw is None or raw == "":
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    if n < 1:
        return None
    return min(500, n)


def _repayment_batch_slice_size(system_config: dict | None = None) -> int:
    """
    Rows per DB connection for ``record_repayments_batch`` (then reconnect next slice).

    Resolution order:
    1. ``system_config["eod_settings"]["tasks"]["repayment_batch_slice_size"]`` (when valid)
    2. Env ``FARNDACRED_REPAYMENT_BATCH_SLICE_SIZE``
    3. Default **100** (clamped 1..500)
    """
    from_cfg = _repayment_batch_slice_size_from_system_config(system_config)
    if from_cfg is not None:
        return max(1, from_cfg)
    raw = os.environ.get("FARNDACRED_REPAYMENT_BATCH_SLICE_SIZE", "").strip()
    if raw:
        try:
            n = int(raw)
        except ValueError:
            n = _DEFAULT_REPAYMENT_BATCH_SLICE_SIZE
        return max(1, min(500, n))
    return _DEFAULT_REPAYMENT_BATCH_SLICE_SIZE


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
    *,
    conn=None,
    system_config: dict | None = None,
    skip_loan_approval_guard: bool = False,
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
    if not skip_loan_approval_guard:
        require_loan_approval_gl_before_repayment(loan_id, conn=conn)
    src_cash = validate_source_cash_gl_account_id_for_new_posting(
        source_cash_gl_account_id,
        field_label="source_cash_gl_account_id",
        system_config=system_config,
    )
    def _insert(_conn) -> int:
        with _conn.cursor() as cur:
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

    if conn is not None:
        return _insert(conn)
    with _connection() as _conn:
        return _insert(_conn)


def record_repayments_batch(rows: list[dict]) -> tuple[int, int, list[str]]:
    """
    Record multiple repayments. Each row: loan_id, amount, payment_date, customer_reference,
    company_reference, value_date (optional), system_date (optional),
    source_cash_gl_account_id (required: UUID or GL **code** — must be in the configured source-cash cache).
    Returns (success_count, fail_count, list of error messages).

    Rows are processed **in slices** (see ``_repayment_batch_slice_size``): one PostgreSQL
    connection is reused for up to N consecutive rows. Each row still gets its own
    ``commit()`` after insert+allocation so a failure does not roll back prior successful rows.
    """
    success = 0
    fail = 0
    errors: list[str] = []
    # Load once per batch to avoid per-row config queries.
    cfg = load_system_config_from_db() or {}
    batch_loans_guarded: set[int] = set()
    slice_n = _repayment_batch_slice_size(cfg)
    nrows = len(rows)
    base = 0
    while base < nrows:
        conn = connect_loan_management()
        try:
            for offset in range(slice_n):
                i = base + offset
                if i >= nrows:
                    break
                row = rows[i]
                try:
                    lid = int(row["loan_id"])
                    if lid not in batch_loans_guarded:
                        require_loan_approval_gl_before_repayment(lid, conn=conn)
                        batch_loans_guarded.add(lid)
                    repayment_id = record_repayment(
                        loan_id=lid,
                        amount=float(row["amount"]),
                        payment_date=row["payment_date"],
                        source_cash_gl_account_id=row["source_cash_gl_account_id"],
                        customer_reference=row.get("customer_reference"),
                        company_reference=row.get("company_reference"),
                        value_date=row.get("value_date"),
                        system_date=row.get("system_date"),
                        conn=conn,
                        system_config=cfg,
                        skip_loan_approval_guard=True,
                    )
                    allocate_repayment_waterfall(
                        repayment_id,
                        system_config=cfg,
                        conn=conn,
                        skip_loan_approval_guard=True,
                    )
                    conn.commit()
                    success += 1
                except Exception as e:
                    conn.rollback()
                    fail += 1
                    errors.append(f"Row {i + 1}: {e}")
        finally:
            conn.close()
        base += slice_n
    return success, fail, errors
