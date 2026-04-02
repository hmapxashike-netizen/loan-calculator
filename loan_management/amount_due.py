"""Schedule vs repayments summary for simple “amount due” views."""

from __future__ import annotations

from datetime import date

from .db import _connection


def get_amount_due_summary(loan_id: int, as_of: date | None = None) -> dict:
    """
    Compute a simple 'amount due today' view for a loan, based on:
      - total scheduled payments up to as_of (from schedule_lines)
      - total repayments up to as_of (from loan_repayments)
    Returns a dict with:
      {
        "amount_due": ...,
        "scheduled_total": ...,
        "repaid_total": ...,
      }
    """
    if as_of is None:
        from system_business_date import get_effective_date

        as_of = get_effective_date()

    with _connection() as conn:
        with conn.cursor() as cur:
            # Sum scheduled payments up to as_of
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(sl.payment), 0) AS scheduled_total
                FROM schedule_lines sl
                JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
                WHERE ls.loan_id = %s
                  AND (
                    sl."Date" IS NULL
                    OR TO_DATE(sl."Date", 'DD-Mon-YYYY') <= %s
                  )
                """,
                (loan_id, as_of),
            )
            row = cur.fetchone()
            scheduled_total = float(row[0]) if row and row[0] is not None else 0.0

            # Sum repayments up to as_of, using value_date when available
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM loan_repayments
                WHERE loan_id = %s
                  AND COALESCE(value_date, payment_date) <= %s
                """,
                (loan_id, as_of),
            )
            row = cur.fetchone()
            repaid_total = float(row[0]) if row and row[0] is not None else 0.0

    amount_due = max(scheduled_total - repaid_total, 0.0)
    return {
        "amount_due": amount_due,
        "scheduled_total": scheduled_total,
        "repaid_total": repaid_total,
    }
