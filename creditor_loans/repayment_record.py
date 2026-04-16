"""Record creditor repayment and post BORROWING_REPAYMENT with explicit per-tag amounts."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from loan_management.cash_gl import validate_source_cash_gl_account_id_for_new_posting

from .daily_state import get_creditor_daily_state_balances
from .db import _connection
from .loan_config import loan_config_from_behavior
from .persistence import get_creditor_loan
from .repayment_waterfall import allocate_creditor_repayment_waterfall


def record_creditor_repayment(
    creditor_loan_id: int,
    amount: float,
    payment_date: date | str,
    source_cash_gl_account_id: str,
    *,
    value_date: date | str | None = None,
    reference: str | None = None,
    company_reference: str | None = None,
    system_config: dict | None = None,
) -> int:
    """Insert repayment, allocate, post GL. Returns repayment id."""
    if amount <= 0:
        raise ValueError("amount must be positive")
    pdate = payment_date if isinstance(payment_date, date) else date.fromisoformat(str(payment_date))
    vdate = value_date if value_date is None or isinstance(value_date, date) else date.fromisoformat(str(value_date))
    if vdate is None:
        vdate = pdate

    try:
        from eod.system_business_date import get_effective_date

        biz = get_effective_date()
    except Exception:
        biz = vdate
    if vdate > biz:
        raise ValueError("Value date cannot be after the system business date.")

    src = validate_source_cash_gl_account_id_for_new_posting(
        source_cash_gl_account_id,
        field_label="source_cash_gl_account_id",
        system_config=system_config,
    )

    row = get_creditor_loan(creditor_loan_id)
    if not row or row.get("status") != "active":
        raise ValueError("Creditor facility not found or not active.")

    behavior = row.get("type_behavior_json") or {}
    if hasattr(behavior, "copy"):
        behavior = dict(behavior)
    cfg = loan_config_from_behavior(behavior)
    wf = cfg.waterfall_bucket_order or []

    balances = get_creditor_daily_state_balances(int(creditor_loan_id), vdate)
    if balances is None:
        balances = {k: 0.0 for k in (
            "principal_not_due", "principal_arrears", "interest_accrued_balance",
            "interest_arrears_balance", "default_interest_balance", "penalty_interest_balance",
            "fees_charges_balance",
        )}

    amt_dec = Decimal(str(as_10dp(amount)))

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO creditor_repayments (
                    creditor_drawdown_id, amount, payment_date, value_date, reference,
                    company_reference, status, source_cash_gl_account_id, system_date
                ) VALUES (%s, %s, %s, %s, %s, %s, 'posted', %s, %s)
                RETURNING id
                """,
                (
                    int(creditor_loan_id),
                    float(as_10dp(amount)),
                    pdate,
                    vdate,
                    (reference or "").strip() or None,
                    (company_reference or "").strip() or None,
                    src,
                    datetime.now(),
                ),
            )
            rid = int(cur.fetchone()[0])

        allocate_creditor_repayment_waterfall(
            int(creditor_loan_id),
            rid,
            amt_dec,
            balances=balances,
            waterfall_bucket_order=list(wf),
            value_date=vdate,
            conn=conn,
        )

        prin = Decimal("0")
        intr = Decimal("0")
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT alloc_principal_total, alloc_interest_total, alloc_fees_total
                FROM creditor_repayment_allocation WHERE repayment_id = %s
                """,
                (rid,),
            )
            a = cur.fetchone()
            if a:
                prin = as_10dp(Decimal(str(a[0] or 0)))
                intr = as_10dp(Decimal(str(a[1] or 0)) + Decimal(str(a[2] or 0)))

        cash_out = as_10dp(prin + intr)
        payload = {
            "borrowings_loan_principal": prin,
            "interest_payable": intr,
            "cash_operating": cash_out,
        }
        try:
            from accounting.service import AccountingService

            AccountingService().post_event(
                event_type="BORROWING_REPAYMENT",
                reference=(company_reference or reference or f"CL-{creditor_loan_id}").strip() or None,
                description=f"Creditor repayment CL-{creditor_loan_id} repayment_id={rid}",
                event_id=f"CL-REPAY-{creditor_loan_id}-{rid}",
                created_by="creditor_repayment",
                entry_date=vdate,
                amount=None,
                payload=payload,
                loan_id=None,
                creditor_drawdown_id=int(creditor_loan_id),
                creditor_facility_id=int(row["creditor_facility_id"]),
                repayment_id=rid,
            )
        except Exception:
            conn.rollback()
            raise
        conn.commit()
    return rid
