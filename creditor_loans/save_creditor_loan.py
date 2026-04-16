"""Capture creditor drawdown + schedule; post BORROWING_DRAWDOWN when disbursement date is not deferred."""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any

import pandas as pd

from decimal_utils import as_10dp

from loan_management.cash_gl import validate_source_cash_gl_account_id_for_new_posting

from .db import _connection
from .persistence import get_facility, insert_creditor_schedule_from_dataframe

_logger = logging.getLogger(__name__)


def save_creditor_loan(
    *,
    creditor_facility_id: int,
    creditor_loan_type_code: str,
    details: dict[str, Any],
    schedule_df: pd.DataFrame,
    post_drawdown_gl: bool = True,
) -> int:
    """
    Insert creditor_drawdowns + schedule v1 + lines.

    ``details``: principal, disbursement_date, cash_gl_account_id (required when cache configured),
    drawdown_fee_amount, arrangement_fee_amount, facility, term, annual_rate, monthly_rate, end_date,
    accrual_mode (daily_mirror | periodic_schedule), penalty_rate_pct (optional).
    """
    if not get_facility(int(creditor_facility_id)):
        raise ValueError("creditor facility not found")

    principal = float(as_10dp(details.get("principal") or 0))
    if principal <= 0:
        raise ValueError("principal must be positive")

    disb = details.get("disbursement_date") or details.get("start_date")
    if not disb:
        raise ValueError("disbursement_date is required")
    if hasattr(disb, "isoformat"):
        disb_date: date = disb
    else:
        from creditor_loans.serialization import _date_conv

        d2 = _date_conv(disb)
        if not d2:
            raise ValueError("Invalid disbursement_date")
        disb_date = d2

    cash_raw = details.get("cash_gl_account_id")
    cash_s = None if cash_raw is None else str(cash_raw).strip()
    cash_gl_account_id = None
    if cash_s:
        cash_gl_account_id = validate_source_cash_gl_account_id_for_new_posting(
            cash_s, field_label="cash_gl_account_id"
        )

    facility = float(as_10dp(details.get("facility") or principal))
    term = int(details.get("term") or 0) if details.get("term") is not None else None
    annual_rate = details.get("annual_rate")
    monthly_rate = details.get("monthly_rate")
    ar = float(as_10dp(annual_rate)) if annual_rate is not None else None
    mr = float(as_10dp(monthly_rate)) if monthly_rate is not None else None
    ddf = float(as_10dp(details.get("drawdown_fee_amount") or 0))
    aaf = float(as_10dp(details.get("arrangement_fee_amount") or 0))
    end_date = details.get("end_date")
    end_d = end_date if hasattr(end_date, "isoformat") else None
    accrual_mode = str(details.get("accrual_mode") or "periodic_schedule").strip()
    if accrual_mode not in ("daily_mirror", "periodic_schedule"):
        raise ValueError("accrual_mode must be daily_mirror or periodic_schedule")
    pr_pen = details.get("penalty_rate_pct")
    penalty_rate = float(as_10dp(pr_pen)) if pr_pen is not None else None

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM creditor_drawdowns")
            dd_id = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO creditor_drawdowns (
                    id, creditor_facility_id, creditor_loan_type_code,
                    facility, principal, term, annual_rate, monthly_rate,
                    disbursement_date, start_date, end_date, status,
                    cash_gl_account_id, drawdown_fee_amount, arrangement_fee_amount,
                    accrual_mode, penalty_rate_pct, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s, %s, %s, %s, %s)
                """,
                (
                    dd_id,
                    int(creditor_facility_id),
                    str(creditor_loan_type_code).strip(),
                    facility,
                    principal,
                    term,
                    ar,
                    mr,
                    disb_date,
                    disb_date,
                    end_d,
                    cash_gl_account_id,
                    ddf,
                    aaf,
                    accrual_mode,
                    penalty_rate,
                    None,
                ),
            )
            cur.execute(
                """
                INSERT INTO creditor_loan_schedules (creditor_drawdown_id, version)
                VALUES (%s, 1) RETURNING id
                """,
                (dd_id,),
            )
            sch_id = int(cur.fetchone()[0])
        insert_creditor_schedule_from_dataframe(conn, sch_id, schedule_df)
        conn.commit()

    fid = int(creditor_facility_id)
    if post_drawdown_gl:
        try:
            from eod.system_business_date import get_effective_date

            biz = get_effective_date()
        except Exception:
            biz = disb_date
        if disb_date <= biz:
            try:
                from accounting.service import AccountingService

                svc = AccountingService()
                fee_total = float(as_10dp(ddf + aaf))
                net_cash = max(0.0, float(as_10dp(principal - fee_total)))
                principal_dec = Decimal(str(as_10dp(principal)))
                fee_asset = Decimal(str(as_10dp(fee_total)))
                cash_amt = Decimal(str(as_10dp(net_cash)))
                payload = {
                    "cash_operating": cash_amt,
                    "deferred_fee_asset_borrowings": fee_asset,
                    "borrowings_loan_principal": principal_dec,
                }
                svc.post_event(
                    event_type="BORROWING_DRAWDOWN",
                    reference=f"CL-{dd_id}",
                    description=f"Creditor drawdown CL-{dd_id}",
                    event_id=f"CL-DRAWDOWN-{dd_id}",
                    created_by="creditor_loan_capture",
                    entry_date=disb_date,
                    amount=None,
                    payload=payload,
                    loan_id=None,
                    creditor_drawdown_id=dd_id,
                    creditor_facility_id=fid,
                )
            except Exception as e:
                _logger.warning("BORROWING_DRAWDOWN not posted for creditor_drawdown_id=%s: %s", dd_id, e)

    return dd_id
