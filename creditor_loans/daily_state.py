"""Persist and read ``creditor_loan_daily_state`` (mirror of debtor daily state)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from .db import RealDictCursor, _connection
from .serialization import _date_conv


def get_creditor_daily_state_balances(creditor_loan_id: int, as_of_date: date) -> dict[str, float] | None:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance, penalty_interest_balance,
                       fees_charges_balance, days_overdue,
                       COALESCE(total_exposure, 0) AS total_exposure,
                       COALESCE(regular_interest_daily, 0) AS regular_interest_daily
                FROM creditor_loan_daily_state
                WHERE creditor_drawdown_id = %s AND as_of_date <= %s
                ORDER BY as_of_date DESC LIMIT 1
                """,
                (creditor_loan_id, as_of_date),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {k: float(row[k] or 0) for k in row.keys()}


def save_creditor_loan_daily_state(
    creditor_loan_id: int,
    as_of_date: date,
    *,
    regular_interest_daily: Decimal | float = 0.0,
    principal_not_due: float = 0.0,
    principal_arrears: float = 0.0,
    interest_accrued_balance: float = 0.0,
    interest_arrears_balance: float = 0.0,
    default_interest_daily: Decimal | float = 0.0,
    default_interest_balance: float = 0.0,
    penalty_interest_daily: Decimal | float = 0.0,
    penalty_interest_balance: float = 0.0,
    fees_charges_balance: float = 0.0,
    days_overdue: int = 0,
    regular_interest_period_to_date: Decimal | float = 0.0,
    penalty_interest_period_to_date: Decimal | float = 0.0,
    default_interest_period_to_date: Decimal | float = 0.0,
    net_allocation: float = 0.0,
    unallocated: float = 0.0,
    conn: Any = None,
) -> None:
    as_of_date = _date_conv(as_of_date) or (
        as_of_date.date() if isinstance(as_of_date, datetime) else as_of_date
    )

    def _n(v): return float(as_10dp(v)) if v is not None else 0.0

    principal_not_due = max(0.0, _n(principal_not_due))
    principal_arrears = max(0.0, _n(principal_arrears))
    interest_accrued_balance = max(0.0, _n(interest_accrued_balance))
    interest_arrears_balance = max(0.0, _n(interest_arrears_balance))
    default_interest_balance = max(0.0, _n(default_interest_balance))
    penalty_interest_balance = max(0.0, _n(penalty_interest_balance))
    fees_charges_balance = max(0.0, _n(fees_charges_balance))
    regular_interest_daily = as_10dp(regular_interest_daily)
    default_interest_daily = as_10dp(default_interest_daily)
    penalty_interest_daily = as_10dp(penalty_interest_daily)
    regular_interest_period_to_date = as_10dp(regular_interest_period_to_date)
    penalty_interest_period_to_date = as_10dp(penalty_interest_period_to_date)
    default_interest_period_to_date = as_10dp(default_interest_period_to_date)
    net_allocation = float(as_10dp(net_allocation))
    unallocated = float(as_10dp(unallocated))

    arrears_total = (
        principal_arrears + interest_arrears_balance + default_interest_balance + penalty_interest_balance
    )
    if arrears_total <= 0:
        days_overdue = 0

    total_delinquency_arrears = float(
        as_10dp(
            principal_arrears
            + interest_arrears_balance
            + default_interest_balance
            + penalty_interest_balance
            + fees_charges_balance
        )
    )
    total_exposure = float(
        as_10dp(
            principal_not_due
            + principal_arrears
            + interest_accrued_balance
            + interest_arrears_balance
            + default_interest_balance
            + penalty_interest_balance
            + fees_charges_balance
        )
    )

    sql = """
        INSERT INTO creditor_loan_daily_state (
            creditor_drawdown_id, as_of_date,
            regular_interest_daily, principal_not_due, principal_arrears,
            interest_accrued_balance, interest_arrears_balance,
            default_interest_daily, default_interest_balance,
            penalty_interest_daily, penalty_interest_balance,
            fees_charges_balance, days_overdue,
            total_delinquency_arrears, total_exposure,
            regular_interest_period_to_date, penalty_interest_period_to_date, default_interest_period_to_date,
            net_allocation, unallocated
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (creditor_drawdown_id, as_of_date) DO UPDATE SET
            regular_interest_daily = EXCLUDED.regular_interest_daily,
            principal_not_due = EXCLUDED.principal_not_due,
            principal_arrears = EXCLUDED.principal_arrears,
            interest_accrued_balance = EXCLUDED.interest_accrued_balance,
            interest_arrears_balance = EXCLUDED.interest_arrears_balance,
            default_interest_daily = EXCLUDED.default_interest_daily,
            default_interest_balance = EXCLUDED.default_interest_balance,
            penalty_interest_daily = EXCLUDED.penalty_interest_daily,
            penalty_interest_balance = EXCLUDED.penalty_interest_balance,
            fees_charges_balance = EXCLUDED.fees_charges_balance,
            days_overdue = EXCLUDED.days_overdue,
            total_delinquency_arrears = EXCLUDED.total_delinquency_arrears,
            total_exposure = EXCLUDED.total_exposure,
            regular_interest_period_to_date = EXCLUDED.regular_interest_period_to_date,
            penalty_interest_period_to_date = EXCLUDED.penalty_interest_period_to_date,
            default_interest_period_to_date = EXCLUDED.default_interest_period_to_date,
            net_allocation = EXCLUDED.net_allocation,
            unallocated = EXCLUDED.unallocated
    """
    params = (
        int(creditor_loan_id),
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
        total_delinquency_arrears,
        total_exposure,
        regular_interest_period_to_date,
        penalty_interest_period_to_date,
        default_interest_period_to_date,
        net_allocation,
        unallocated,
    )

    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        return

    with _connection() as c:
        with c.cursor() as cur:
            cur.execute(sql, params)
