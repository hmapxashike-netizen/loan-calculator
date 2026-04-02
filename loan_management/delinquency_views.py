"""Delinquency summaries and teller “amount due” previews from ``loan_daily_state``."""

from __future__ import annotations

from datetime import date, timedelta

from .daily_state import get_loan_daily_state_balances
from .db import _connection


def get_total_delinquency_arrears_summary(loan_id: int, as_of: date | None = None) -> dict:
    """
    Teller-style "amount due today" = total delinquency arrears as-of the system business date.

    Uses the persisted `loan_daily_state.total_delinquency_arrears` when available.
    If the exact daily-state row doesn't exist yet (e.g. system date was advanced but EOD not run),
    it will backfill the single-loan daily state for that date (includes allocations on that date)
    so we never show a misleading 0 just because the row is missing.
    """
    if as_of is None:
        from eod.system_business_date import get_effective_date

        as_of = get_effective_date()

    # Prefer exact-date state; if missing, compute single-loan state for that date.
    ds = get_loan_daily_state_balances(loan_id, as_of)
    if not ds:
        try:
            from eod.core import run_single_loan_eod

            run_single_loan_eod(loan_id, as_of)
        except Exception:
            pass
        ds = get_loan_daily_state_balances(loan_id, as_of)

    if ds and ds.get("total_delinquency_arrears") is not None:
        try:
            amt = float(ds.get("total_delinquency_arrears") or 0)
        except (TypeError, ValueError):
            amt = 0.0
    else:
        # Fallback for legacy rows/DBs: compute from buckets.
        amt = (
            float((ds or {}).get("principal_arrears") or 0)
            + float((ds or {}).get("interest_arrears_balance") or 0)
            + float((ds or {}).get("default_interest_balance") or 0)
            + float((ds or {}).get("penalty_interest_balance") or 0)
            + float((ds or {}).get("fees_charges_balance") or 0)
        )

    return {"total_delinquency_arrears": max(amt, 0.0), "as_of_date": as_of}


def get_teller_amount_due_today(loan_id: int, as_of: date | None = None) -> dict:
    """
    Fast teller preview that avoids running EOD on every receipt.

    Policy:
    - Use persisted EOD delinquency (`loan_daily_state.total_delinquency_arrears`) as the base.
      If the exact as_of row exists, use it directly.
      Otherwise use the latest <= (as_of - 1) as the base (typically yesterday close).
    - Then factor in *today's* posted allocations that reduce/increase delinquency buckets:
      principal_arrears, interest_arrears, default, penalty, fees.

    This gives an intraday "amount due today" that reflects receipts/reversals already posted
    for the day, while keeping persistence to the normal EOD run.
    """
    if as_of is None:
        from eod.system_business_date import get_effective_date

        as_of = get_effective_date()

    # 1) If today's state already exists, it's authoritative for teller preview.
    ds_today = get_loan_daily_state_balances(loan_id, as_of)
    if ds_today and ds_today.get("total_delinquency_arrears") is not None:
        try:
            amt_today = float(ds_today.get("total_delinquency_arrears") or 0)
        except (TypeError, ValueError):
            amt_today = 0.0
        return {
            "amount_due_today": max(amt_today, 0.0),
            "as_of_date": as_of,
            "base_as_of_date": as_of,
            "base_total_delinquency_arrears": max(amt_today, 0.0),
            "today_allocations_to_delinquency": 0.0,
            "method": "daily_state_exact",
        }

    base_date = as_of - timedelta(days=1)
    base_ds = get_loan_daily_state_balances(loan_id, base_date)
    base_amt = 0.0
    if base_ds:
        try:
            base_amt = float(
                base_ds.get("total_delinquency_arrears")
                if base_ds.get("total_delinquency_arrears") is not None
                else (
                    float(base_ds.get("principal_arrears") or 0)
                    + float(base_ds.get("interest_arrears_balance") or 0)
                    + float(base_ds.get("default_interest_balance") or 0)
                    + float(base_ds.get("penalty_interest_balance") or 0)
                    + float(base_ds.get("fees_charges_balance") or 0)
                )
            )
        except (TypeError, ValueError):
            base_amt = 0.0

    # 2) Today's allocation deltas (posted + reversed, including internal movements).
    today_alloc = 0.0
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(
                        COALESCE(lra.alloc_principal_arrears, 0)
                      + COALESCE(lra.alloc_interest_arrears, 0)
                      + COALESCE(lra.alloc_default_interest, 0)
                      + COALESCE(lra.alloc_penalty_interest, 0)
                      + COALESCE(lra.alloc_fees_charges, 0)
                    ), 0) AS alloc_to_delinquency
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lr.loan_id = %s
                  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
                  AND lr.status IN ('posted', 'reversed')
                """,
                (loan_id, as_of),
            )
            r = cur.fetchone()
            today_alloc = float(r[0] or 0) if r else 0.0

    amount_due = max(base_amt - today_alloc, 0.0)
    return {
        "amount_due_today": amount_due,
        "as_of_date": as_of,
        "base_as_of_date": base_date,
        "base_total_delinquency_arrears": max(base_amt, 0.0),
        "today_allocations_to_delinquency": today_alloc,
        "method": "base_minus_today_allocations",
    }
