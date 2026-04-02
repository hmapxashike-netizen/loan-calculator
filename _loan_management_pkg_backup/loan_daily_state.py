"""Persisted loan_daily_state: read balances/range and upsert daily bucket snapshot."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from .db import RealDictCursor, _connection
from .serialization import _date_conv


def get_loan_daily_state_balances(loan_id: int, as_of_date: date) -> dict[str, float] | None:
    """Get bucket balances for a loan as of a date (latest row on or before as_of_date)."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance,
                       penalty_interest_balance, fees_charges_balance, days_overdue,
                       COALESCE(total_exposure, 0)                    AS total_exposure,
                       COALESCE(total_delinquency_arrears, 0)         AS total_delinquency_arrears,
                       COALESCE(regular_interest_daily, 0)            AS regular_interest_daily,
                       COALESCE(penalty_interest_daily, 0)            AS penalty_interest_daily,
                       COALESCE(default_interest_daily, 0)            AS default_interest_daily,
                       COALESCE(regular_interest_period_to_date, 0)   AS regular_interest_period_to_date,
                       COALESCE(penalty_interest_period_to_date, 0)   AS penalty_interest_period_to_date,
                       COALESCE(default_interest_period_to_date, 0)   AS default_interest_period_to_date,
                       COALESCE(unallocated, 0)                       AS unallocated,
                       COALESCE(regular_interest_in_suspense_balance, 0) AS regular_interest_in_suspense_balance,
                       COALESCE(penalty_interest_in_suspense_balance, 0) AS penalty_interest_in_suspense_balance,
                       COALESCE(default_interest_in_suspense_balance, 0) AS default_interest_in_suspense_balance,
                       COALESCE(total_interest_in_suspense_balance, 0) AS total_interest_in_suspense_balance
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
                "total_exposure": float(row.get("total_exposure") or 0),
                "total_delinquency_arrears": float(row.get("total_delinquency_arrears") or 0),
                "regular_interest_daily": float(row["regular_interest_daily"] or 0),
                "penalty_interest_daily": float(row["penalty_interest_daily"] or 0),
                "default_interest_daily": float(row["default_interest_daily"] or 0),
                "regular_interest_period_to_date": float(row["regular_interest_period_to_date"] or 0),
                "penalty_interest_period_to_date": float(row["penalty_interest_period_to_date"] or 0),
                "default_interest_period_to_date": float(row["default_interest_period_to_date"] or 0),
                "unallocated": float(row["unallocated"] or 0),
                "regular_interest_in_suspense_balance": float(
                    row["regular_interest_in_suspense_balance"] or 0
                ),
                "penalty_interest_in_suspense_balance": float(
                    row["penalty_interest_in_suspense_balance"] or 0
                ),
                "default_interest_in_suspense_balance": float(
                    row["default_interest_in_suspense_balance"] or 0
                ),
                "total_interest_in_suspense_balance": float(
                    row["total_interest_in_suspense_balance"] or 0
                ),
            }


def get_loan_daily_state_range(loan_id: int, start_date: date, end_date: date) -> list[dict]:
    """All loan_daily_state rows for a loan in [start_date, end_date] ordered by as_of_date."""
    try:
        from system_business_date import get_effective_date

        eff = get_effective_date()
        if end_date > eff:
            end_date = eff
    except Exception:
        pass
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT as_of_date, regular_interest_daily, principal_not_due, principal_arrears,
                       interest_accrued_balance, interest_arrears_balance,
                       default_interest_daily, default_interest_balance,
                       penalty_interest_daily, penalty_interest_balance,
                       fees_charges_balance, total_exposure,
                       COALESCE(regular_interest_period_to_date, 0) AS regular_interest_period_to_date,
                       COALESCE(penalty_interest_period_to_date, 0)  AS penalty_interest_period_to_date,
                       COALESCE(default_interest_period_to_date, 0)  AS default_interest_period_to_date,
                       COALESCE(regular_interest_in_suspense_balance, 0) AS regular_interest_in_suspense_balance,
                       COALESCE(penalty_interest_in_suspense_balance, 0) AS penalty_interest_in_suspense_balance,
                       COALESCE(default_interest_in_suspense_balance, 0) AS default_interest_in_suspense_balance,
                       COALESCE(total_interest_in_suspense_balance, 0) AS total_interest_in_suspense_balance
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date >= %s AND as_of_date <= %s
                ORDER BY as_of_date
                """,
                (loan_id, start_date, end_date),
            )
            return [dict(r) for r in cur.fetchall()]


def save_loan_daily_state(
    loan_id: int,
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
    credits: float | None = None,
    net_allocation: float | None = None,
    unallocated: float | None = None,
    regular_interest_in_suspense_balance: float = 0.0,
    penalty_interest_in_suspense_balance: float = 0.0,
    default_interest_in_suspense_balance: float = 0.0,
    conn: Any = None,
) -> None:
    """
    Upsert daily loan bucket balances into loan_daily_state.
    total_exposure is computed as the sum of all principal + interest + fees buckets.
    Period-to-date columns are for efficient statement generation (no summing over days).
    If conn is provided, use it and do not commit (caller commits). Otherwise use a new connection and commit on exit.
    """
    as_of_date = _date_conv(as_of_date) or (as_of_date.date() if isinstance(as_of_date, datetime) else as_of_date)
    # Quantize all numerics to 10dp for storage

    def _n(v):
        return float(as_10dp(v)) if v is not None else 0.0

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
    net_allocation = as_10dp(net_allocation) if net_allocation is not None else None
    unallocated = as_10dp(unallocated) if unallocated is not None else None
    credits = as_10dp(credits) if credits is not None else None
    # Control: if all arrears/penalty/default balances are zero, days_overdue must be 0
    arrears_total = principal_arrears + interest_arrears_balance + default_interest_balance + penalty_interest_balance
    if arrears_total <= 0:
        days_overdue = 0

    total_delinquency_arrears = (
        principal_arrears
        + interest_arrears_balance
        + default_interest_balance
        + penalty_interest_balance
        + fees_charges_balance
    )
    # Quantize for stable storage precision (NUMERIC(22,10)).
    total_delinquency_arrears = float(as_10dp(total_delinquency_arrears))
    total_exposure = (
        principal_not_due
        + principal_arrears
        + interest_accrued_balance
        + interest_arrears_balance
        + default_interest_balance
        + penalty_interest_balance
        + fees_charges_balance
    )

    reg_susp = max(0.0, _n(regular_interest_in_suspense_balance))
    pen_susp = max(0.0, _n(penalty_interest_in_suspense_balance))
    def_susp = max(0.0, _n(default_interest_in_suspense_balance))
    total_int_susp = float(
        as_10dp(
            Decimal(str(reg_susp)) + Decimal(str(pen_susp)) + Decimal(str(def_susp))
        )
    )

    def _do_upsert(c: Any) -> None:
        with c.cursor() as cur:
            if net_allocation is not None and unallocated is not None:
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
                        total_delinquency_arrears,
                        total_exposure,
                        regular_interest_period_to_date,
                        penalty_interest_period_to_date,
                        default_interest_period_to_date,
                        net_allocation,
                        unallocated,
                        regular_interest_in_suspense_balance,
                        penalty_interest_in_suspense_balance,
                        default_interest_in_suspense_balance,
                        total_interest_in_suspense_balance
                    )
                    VALUES (
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s
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
                        total_delinquency_arrears = EXCLUDED.total_delinquency_arrears,
                        total_exposure           = EXCLUDED.total_exposure,
                        regular_interest_period_to_date = EXCLUDED.regular_interest_period_to_date,
                        penalty_interest_period_to_date  = EXCLUDED.penalty_interest_period_to_date,
                        default_interest_period_to_date = EXCLUDED.default_interest_period_to_date,
                        net_allocation           = EXCLUDED.net_allocation,
                        unallocated              = EXCLUDED.unallocated,
                        regular_interest_in_suspense_balance = EXCLUDED.regular_interest_in_suspense_balance,
                        penalty_interest_in_suspense_balance = EXCLUDED.penalty_interest_in_suspense_balance,
                        default_interest_in_suspense_balance = EXCLUDED.default_interest_in_suspense_balance,
                        total_interest_in_suspense_balance = EXCLUDED.total_interest_in_suspense_balance
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
                        total_delinquency_arrears,
                        total_exposure,
                        regular_interest_period_to_date,
                        penalty_interest_period_to_date,
                        default_interest_period_to_date,
                        net_allocation,
                        unallocated,
                        reg_susp,
                        pen_susp,
                        def_susp,
                        total_int_susp,
                    ),
                )
            elif credits is not None:
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
                        total_delinquency_arrears,
                        total_exposure,
                        regular_interest_period_to_date,
                        penalty_interest_period_to_date,
                        default_interest_period_to_date,
                        credits,
                        regular_interest_in_suspense_balance,
                        penalty_interest_in_suspense_balance,
                        default_interest_in_suspense_balance,
                        total_interest_in_suspense_balance
                    )
                    VALUES (
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s, %s, %s
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
                        total_delinquency_arrears = EXCLUDED.total_delinquency_arrears,
                        total_exposure           = EXCLUDED.total_exposure,
                        regular_interest_period_to_date = EXCLUDED.regular_interest_period_to_date,
                        penalty_interest_period_to_date  = EXCLUDED.penalty_interest_period_to_date,
                        default_interest_period_to_date = EXCLUDED.default_interest_period_to_date,
                        credits                  = EXCLUDED.credits,
                        regular_interest_in_suspense_balance = EXCLUDED.regular_interest_in_suspense_balance,
                        penalty_interest_in_suspense_balance = EXCLUDED.penalty_interest_in_suspense_balance,
                        default_interest_in_suspense_balance = EXCLUDED.default_interest_in_suspense_balance,
                        total_interest_in_suspense_balance = EXCLUDED.total_interest_in_suspense_balance
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
                        total_delinquency_arrears,
                        total_exposure,
                        regular_interest_period_to_date,
                        penalty_interest_period_to_date,
                        default_interest_period_to_date,
                        credits,
                        reg_susp,
                        pen_susp,
                        def_susp,
                        total_int_susp,
                    ),
                )
            else:
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
                        total_delinquency_arrears,
                        total_exposure,
                        regular_interest_period_to_date,
                        penalty_interest_period_to_date,
                        default_interest_period_to_date,
                        regular_interest_in_suspense_balance,
                        penalty_interest_in_suspense_balance,
                        default_interest_in_suspense_balance,
                        total_interest_in_suspense_balance
                    )
                    VALUES (
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s
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
                        total_delinquency_arrears = EXCLUDED.total_delinquency_arrears,
                        total_exposure           = EXCLUDED.total_exposure,
                        regular_interest_period_to_date = EXCLUDED.regular_interest_period_to_date,
                        penalty_interest_period_to_date  = EXCLUDED.penalty_interest_period_to_date,
                        default_interest_period_to_date = EXCLUDED.default_interest_period_to_date,
                        regular_interest_in_suspense_balance = EXCLUDED.regular_interest_in_suspense_balance,
                        penalty_interest_in_suspense_balance = EXCLUDED.penalty_interest_in_suspense_balance,
                        default_interest_in_suspense_balance = EXCLUDED.default_interest_in_suspense_balance,
                        total_interest_in_suspense_balance = EXCLUDED.total_interest_in_suspense_balance
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
                        total_delinquency_arrears,
                        total_exposure,
                        regular_interest_period_to_date,
                        penalty_interest_period_to_date,
                        default_interest_period_to_date,
                        reg_susp,
                        pen_susp,
                        def_susp,
                        total_int_susp,
                    ),
                )

    if conn is not None:
        _do_upsert(conn)
    else:
        with _connection() as new_conn:
            _do_upsert(new_conn)
