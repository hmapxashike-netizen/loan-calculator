#!/usr/bin/env python3
"""
Backfill credits column in loan_daily_state.
Run after schema/23_loan_daily_state_credits.sql.
Credits = cumulative allocation affecting balances (payment +, reversal -).

Usage: python scripts/populate_credits.py [--dry-run]
"""

import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    ap = argparse.ArgumentParser(description="Populate credits in loan_daily_state")
    ap.add_argument("--dry-run", action="store_true", help="Preview without updating")
    args = ap.parse_args()

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from loan_management import get_credits_for_loan_date

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT loan_id, as_of_date FROM loan_daily_state
                ORDER BY loan_id, as_of_date
                """
            )
            rows = cur.fetchall()
        updated = 0
        for r in rows:
            loan_id = r["loan_id"]
            as_of_date = r["as_of_date"]
            if hasattr(as_of_date, "date"):
                as_of_date = as_of_date.date()
            credits = get_credits_for_loan_date(loan_id, as_of_date)
            if not args.dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE loan_daily_state SET credits = %s WHERE loan_id = %s AND as_of_date = %s",
                        (credits, loan_id, as_of_date),
                    )
                    if cur.rowcount:
                        updated += 1
            else:
                updated += 1
        conn.commit()
        print(f"Populated credits for {updated} row(s).")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
