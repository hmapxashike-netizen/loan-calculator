"""
Print EXPLAIN plans for heavy EOD batch SQL (yesterday-state LATERAL + allocation rollup).

Usage (from project root):

  python scripts/explain_eod_batch_queries.py
  python scripts/explain_eod_batch_queries.py --analyze

``--analyze`` runs EXPLAIN (ANALYZE, BUFFERS) and executes the queries (use on a copy DB
or off-peak). Without ``--analyze``, uses EXPLAIN alone (no execution).

Requires database connectivity (same as the app: ``config.get_database_url()``).
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(description="EXPLAIN EOD batch queries.")
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Use EXPLAIN (ANALYZE, BUFFERS) — executes queries.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=500,
        metavar="N",
        help="Number of active loan ids to sample (default 500).",
    )
    args = parser.parse_args()

    import psycopg2
    from psycopg2.extras import RealDictCursor

    from config import get_database_url

    yesterday = date.today() - timedelta(days=1)
    mode = "EXPLAIN (ANALYZE, BUFFERS)" if args.analyze else "EXPLAIN"

    with psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM loans
                WHERE status = 'active'
                ORDER BY id
                LIMIT %s
                """,
                (max(1, min(50_000, args.sample_size)),),
            )
            loan_ids = [int(r["id"]) for r in cur.fetchall()]

        if not loan_ids:
            print("No active loans found; cannot run EXPLAIN.")
            sys.exit(1)

        as_of = yesterday

        lateral_sql = f"""
{mode}
SELECT
    x.loan_id AS loan_id,
    lds.principal_not_due,
    lds.principal_arrears
FROM unnest(%s::int[]) AS x(loan_id)
LEFT JOIN LATERAL (
    SELECT
        principal_not_due, principal_arrears,
        interest_accrued_balance, interest_arrears_balance,
        default_interest_balance, penalty_interest_balance,
        fees_charges_balance, days_overdue, total_exposure,
        regular_interest_daily, penalty_interest_daily, default_interest_daily,
        regular_interest_period_to_date, penalty_interest_period_to_date,
        default_interest_period_to_date,
        regular_interest_in_suspense_balance, penalty_interest_in_suspense_balance,
        default_interest_in_suspense_balance, total_interest_in_suspense_balance
    FROM loan_daily_state
    WHERE loan_daily_state.loan_id = x.loan_id
      AND loan_daily_state.as_of_date <= %s
    ORDER BY loan_daily_state.as_of_date DESC
    LIMIT 1
) lds ON TRUE
"""

        alloc_sql = f"""
{mode}
SELECT lr.loan_id,
    COALESCE(SUM(lra.alloc_principal_not_due), 0) AS alloc_principal_not_due
FROM loan_repayments lr
JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
WHERE lr.loan_id = ANY(%s)
  AND lr.status IN ('posted', 'reversed')
  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
GROUP BY lr.loan_id
"""

        for label, sql, params in (
            ("yesterday_state_lateral", lateral_sql, (loan_ids, yesterday)),
            ("allocation_totals_by_loan_date", alloc_sql, (loan_ids, as_of)),
        ):
            print(f"\n{'=' * 72}\n{label}\n{'=' * 72}\n")
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    vals = list(row.values())
                    if len(vals) == 1:
                        print(vals[0])
                    else:
                        print(row)


if __name__ == "__main__":
    main()
