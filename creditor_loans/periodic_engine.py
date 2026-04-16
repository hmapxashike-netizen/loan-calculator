"""Periodic (schedule-based) creditor drawdown state: bill principal/interest to arrears on due dates."""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from eod.core import ARREARS_ZERO_TOLERANCE

from .daily_state import save_creditor_loan_daily_state
from .serialization import _date_conv

_logger = logging.getLogger(__name__)


def _parse_line_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    s = str(raw).strip()
    if not s:
        return None
    d = _date_conv(s)
    return d


def run_periodic_creditor_drawdown_for_date(
    conn,
    drawdown_row: dict[str, Any],
    schedule_rows: list[dict[str, Any]],
    as_of_date: date,
    *,
    yesterday: date,
    alloc: dict[str, float],
    yesterday_saved: dict[str, Any] | None,
    block_accruals: bool,
) -> None:
    """
    Sparse accrual: on each calendar day carry balances; on a schedule line's due date,
    move that line's principal and interest into arrears (installment billing).
    """
    cid = int(drawdown_row["id"])
    disb = drawdown_row.get("disbursement_date") or drawdown_row.get("start_date")
    if not isinstance(disb, date):
        return
    if disb > as_of_date:
        return

    principal = float(as_10dp(drawdown_row.get("principal") or 0))

    if yesterday_saved is None:
        pnd = principal
        pa = 0.0
        iacc = 0.0
        iarr = 0.0
        dibal = 0.0
        pibal = 0.0
        fees = 0.0
        ddays = 0
    else:
        pnd = float(yesterday_saved.get("principal_not_due", 0) or 0)
        pa = float(yesterday_saved.get("principal_arrears", 0) or 0)
        iacc = float(yesterday_saved.get("interest_accrued_balance", 0) or 0)
        iarr = float(yesterday_saved.get("interest_arrears_balance", 0) or 0)
        dibal = float(yesterday_saved.get("default_interest_balance", 0) or 0)
        pibal = float(yesterday_saved.get("penalty_interest_balance", 0) or 0)
        fees = float(yesterday_saved.get("fees_charges_balance", 0) or 0)
        ddays = int(yesterday_saved.get("days_overdue", 0) or 0)

    # Repayments on value date reduce buckets (same net effect as daily engine _tb for principal)
    pnd = max(0.0, pnd - float(alloc.get("alloc_principal_not_due", 0.0) or 0.0))
    pa = max(0.0, pa - float(alloc.get("alloc_principal_arrears", 0.0) or 0.0))
    iacc = max(0.0, iacc - float(alloc.get("alloc_interest_accrued", 0.0) or 0.0))
    iarr = max(0.0, iarr - float(alloc.get("alloc_interest_arrears", 0.0) or 0.0))
    dibal = max(0.0, dibal - float(alloc.get("alloc_default_interest", 0.0) or 0.0))
    pibal = max(0.0, pibal - float(alloc.get("alloc_penalty_interest", 0.0) or 0.0))
    fees = max(0.0, fees - float(alloc.get("alloc_fees_charges", 0.0) or 0.0))

    due_today = False
    if not block_accruals:
        for r in schedule_rows:
            dline = _parse_line_date(r.get("Date"))
            if dline == as_of_date:
                due_today = True
                pr = float(as_10dp(r.get("principal") or 0))
                intr = float(as_10dp(r.get("interest") or 0))
                take_p = min(pnd, pr)
                pnd = max(0.0, pnd - take_p)
                pa = max(0.0, pa + take_p)
                iarr = max(0.0, iarr + intr)

    no_arrears = pa <= ARREARS_ZERO_TOLERANCE and iarr <= ARREARS_ZERO_TOLERANCE
    days_overdue_save = 0 if no_arrears else (ddays + 1 if yesterday_saved else 1)

    save_creditor_loan_daily_state(
        cid,
        as_of_date,
        regular_interest_daily=Decimal("0"),
        principal_not_due=pnd,
        principal_arrears=pa,
        interest_accrued_balance=iacc,
        interest_arrears_balance=iarr,
        default_interest_daily=0.0,
        default_interest_balance=dibal,
        penalty_interest_daily=0.0,
        penalty_interest_balance=pibal,
        fees_charges_balance=fees,
        days_overdue=days_overdue_save,
        regular_interest_period_to_date=Decimal("0"),
        penalty_interest_period_to_date=0.0,
        default_interest_period_to_date=0.0,
        net_allocation=0.0,
        unallocated=0.0,
        conn=conn,
    )
    if due_today:
        _logger.debug("periodic creditor drawdown_id=%s billed schedule on %s", cid, as_of_date.isoformat())
