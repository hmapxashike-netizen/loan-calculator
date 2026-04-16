"""
Teller / repayment workflows: parsing, DB-backed operations, accounting posts.

UI (Streamlit) collects inputs, calls these functions, renders outcomes.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from typing import Any

import pandas as pd

from loan_management import (
    allocate_repayment_waterfall,
    cancel_scheduled_repayment,
    get_repayments_with_allocations,
    get_teller_amount_due_today,
    list_scheduled_receipts_for_loan,
    load_system_config_from_db,
    record_repayment,
    record_repayments_batch,
    record_scheduled_repayments_batch,
    reverse_repayment,
)

_logger = logging.getLogger(__name__)


def build_batch_upload_template_excel_bytes(*, sample_system_date_iso: str) -> bytes:
    """Excel template for batch repayment upload (same columns as Teller UI)."""
    template_df = pd.DataFrame(
        columns=[
            "loan_id",
            "amount",
            "payment_date",
            "value_date",
            "customer_reference",
            "company_reference",
            "source_cash_gl_account_id",
        ]
    )
    template_df.loc[0] = [
        1,
        100.00,
        sample_system_date_iso,
        sample_system_date_iso,
        "Receipt-001",
        "GL-001",
        "",
    ]
    buf = BytesIO()
    template_df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf.getvalue()


def parse_batch_repayment_rows_from_dataframe(
    df: pd.DataFrame,
    *,
    fallback_payment_date_iso: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Validate and normalize rows from an uploaded batch spreadsheet.

    Returns (valid_rows for record_repayments_batch, parse_error_messages).
    """
    valid_rows: list[dict[str, Any]] = []
    parse_errors: list[str] = []
    for i, r in df.iterrows():
        try:
            lid = int(r["loan_id"])
            amt = float(r["amount"])
            if lid <= 0 or amt <= 0:
                parse_errors.append(f"Row {i + 2}: loan_id and amount must be positive")
                continue
            pdate = r.get("payment_date")
            if pd.isna(pdate):
                pdate = fallback_payment_date_iso
            elif hasattr(pdate, "date"):
                pdate = pdate.date().isoformat()
            else:
                pdate = str(pdate)[:10]
            vdate = r.get("value_date")
            if pd.notna(vdate) and hasattr(vdate, "date"):
                vdate = vdate.date().isoformat()
            elif pd.notna(vdate):
                vdate = str(vdate)[:10]
            else:
                vdate = pdate
            raw_gl = r.get("source_cash_gl_account_id")
            if raw_gl is None or (isinstance(raw_gl, float) and pd.isna(raw_gl)):
                src_gl = None
            else:
                src_gl = str(raw_gl).strip() or None
            if not src_gl:
                parse_errors.append(
                    f"Row {i + 2}: source_cash_gl_account_id is required (posting account UUID or GL **code**, e.g. from chart)"
                )
                continue
            valid_rows.append(
                {
                    "loan_id": lid,
                    "amount": amt,
                    "payment_date": pdate,
                    "value_date": vdate,
                    "customer_reference": str(r.get("customer_reference", "")).strip() or None,
                    "company_reference": str(r.get("company_reference", "")).strip() or None,
                    "source_cash_gl_account_id": src_gl,
                }
            )
        except (ValueError, TypeError) as e:
            parse_errors.append(f"Row {i + 2}: {e}")
    return valid_rows, parse_errors


def run_batch_repayments(valid_rows: list[dict[str, Any]]) -> tuple[int, int, list[str]]:
    """Persist batch repayments (allocation handled inside loan_management batch)."""
    return record_repayments_batch(valid_rows)


def build_scheduled_batch_upload_template_excel_bytes(*, sample_future_value_date_iso: str) -> bytes:
    """Excel template for scheduled (future value date) batch upload — same columns as normal batch."""
    template_df = pd.DataFrame(
        columns=[
            "loan_id",
            "amount",
            "payment_date",
            "value_date",
            "customer_reference",
            "company_reference",
            "source_cash_gl_account_id",
        ]
    )
    template_df.loc[0] = [
        1,
        100.00,
        sample_future_value_date_iso,
        sample_future_value_date_iso,
        "Scheduled-001",
        "GL-001",
        "",
    ]
    buf = BytesIO()
    template_df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf.getvalue()


def run_batch_scheduled_repayments(valid_rows: list[dict[str, Any]]) -> tuple[int, int, list[str]]:
    """Persist scheduled receipts (no allocation until EOD on each row's value date)."""
    return record_scheduled_repayments_batch(valid_rows)


def list_scheduled_rows_for_loan(loan_id: int, *, limit: int = 200) -> list[dict[str, Any]]:
    return list_scheduled_receipts_for_loan(loan_id, limit=limit)


def execute_cancel_scheduled_repayment(
    repayment_id: int,
    *,
    reason: str,
    cancelled_by: str,
) -> None:
    cancel_scheduled_repayment(repayment_id, reason=reason, cancelled_by=cancelled_by)


def record_repayment_with_allocation(
    *,
    loan_id: int,
    amount: float,
    payment_date: date,
    source_cash_gl_account_id: str,
    customer_reference: str | None,
    company_reference: str | None,
    value_date: date,
    system_date: datetime,
) -> int:
    """Insert repayment row and run waterfall allocation with current system config."""
    t0 = datetime.now().timestamp()
    rid = record_repayment(
        loan_id=loan_id,
        amount=amount,
        payment_date=payment_date,
        source_cash_gl_account_id=source_cash_gl_account_id,
        customer_reference=customer_reference,
        company_reference=company_reference,
        value_date=value_date,
        system_date=system_date,
    )
    cfg = load_system_config_from_db() or {}
    allocate_repayment_waterfall(rid, system_config=cfg)
    # Optional high-level timing trace (fine-grained trace is inside allocate_repayment_waterfall).
    # Enabled via env var to avoid noisy logs by default.
    try:
        import os

        if os.environ.get("FARNDACRED_TRACE_TELLER", "").strip().lower() in ("1", "true", "yes", "on"):
            dt_s = max(0.0, datetime.now().timestamp() - t0)
            _logger.info(
                "TRACE teller.record_repayment_with_allocation repayment_id=%s loan_id=%s amount=%s value_date=%s wall_s=%.3f",
                rid,
                loan_id,
                amount,
                getattr(value_date, "isoformat", lambda: str(value_date))(),
                dt_s,
            )
    except Exception:
        pass
    return rid


def fetch_teller_amount_due_summary(loan_id: int) -> dict[str, Any] | None:
    """Amount-due breakdown for Teller preview; None if unavailable."""
    try:
        return get_teller_amount_due_today(loan_id)
    except Exception:
        return None


def list_recent_receipts_for_loan(
    loan_id: int,
    *,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Posted receipts with allocations in [start_date, end_date] (empty on error)."""
    try:
        return get_repayments_with_allocations(loan_id, start_date, end_date)
    except Exception:
        return []


def execute_reverse_repayment(repayment_id: int):
    """Reverse a receipt and replay loan state; propagates exceptions to the UI."""
    return reverse_repayment(repayment_id)


def post_borrowing_repayment_journal(
    accounting_service: Any,
    *,
    value_date: date,
    principal: Decimal,
    interest: Decimal,
    reference: str | None,
    description: str,
    created_by: str,
    account_overrides: dict | None = None,
) -> None:
    """
    Post BORROWING_REPAYMENT with explicit principal / interest lines (balanced to cash).

    ``account_overrides`` should include ``cash_operating`` = leaf UUID when the chart
    requires a leaf for cash (same as Teller source-cash list).
    """
    from decimal_utils import as_10dp

    p = as_10dp(Decimal(str(principal)))
    i = as_10dp(Decimal(str(interest)))
    if p < 0 or i < 0:
        raise ValueError("principal and interest must be non-negative.")
    if p + i <= 0:
        raise ValueError("At least one of principal or interest must be positive.")
    cash = as_10dp(p + i)
    payload: dict[str, Any] = {
        "borrowings_loan_principal": p,
        "interest_payable": i,
        "cash_operating": cash,
    }
    if account_overrides:
        payload["account_overrides"] = dict(account_overrides)
    accounting_service.post_event(
        event_type="BORROWING_REPAYMENT",
        reference=(reference or "").strip() or None,
        description=description.strip() or "Payment of borrowings",
        event_id=f"BORROWING-MANUAL-{uuid.uuid4().hex}",
        created_by=created_by,
        entry_date=value_date,
        amount=None,
        payload=payload,
        is_reversal=False,
    )


def post_writeoff_recovery_journal(
    accounting_service: Any,
    *,
    loan_id: int,
    value_date: date,
    amount: Decimal,
    customer_reference: str | None,
    company_reference: str | None,
    created_by: str,
) -> None:
    cref = (customer_reference or "").strip()
    comp = (company_reference or "").strip()
    accounting_service.post_event(
        event_type="WRITEOFF_RECOVERY",
        reference=comp or cref or None,
        description=(
            f"Recovery on written-off loan #{loan_id}"
            if not comp and not cref
            else (comp or cref)
        ),
        event_id=str(loan_id),
        created_by=created_by,
        entry_date=value_date,
        amount=amount,
        payload=None,
        is_reversal=False,
        loan_id=int(loan_id),
    )
