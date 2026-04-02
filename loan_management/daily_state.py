"""Read access to persisted ``loan_daily_state`` rows."""

from __future__ import annotations

from datetime import date

from .db import RealDictCursor, _connection


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
