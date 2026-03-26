"""
List loan_repayments + loan_repayment_allocation rows for a loan that relate to
unapplied / liquidation / reversal (for debugging balance vs EOD).

Uses config / DB URL from env (same as the app). Run from project root:

  set FARNDACRED_DB_* (or LMS_DB_*) then:
  python scripts/list_loan_unapplied_allocations.py 9

Optional second arg filters value_date >= date:

  python scripts/list_loan_unapplied_allocations.py 9 2025-06-01
"""
from __future__ import annotations

import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import get_database_url

import psycopg2
from psycopg2.extras import RealDictCursor


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("loan_id", type=int)
    parser.add_argument("from_date", nargs="?", default=None, help="YYYY-MM-DD optional lower bound on value_date")
    args = parser.parse_args()

    sql = """
        SELECT lr.id AS repayment_id, lr.status, lr.reference,
               (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
               lr.amount AS repayment_amount, lr.original_repayment_id,
               lra.event_type,
               lra.alloc_interest_arrears, lra.alloc_total, lra.source_repayment_id
        FROM loan_repayments lr
        JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
        WHERE lr.loan_id = %s
          AND (
            COALESCE(lr.reference, '') ILIKE '%%unapplied%%'
            OR COALESCE(lr.reference, '') ILIKE '%%Reversal of unapplied%%'
            OR lra.event_type IN ('unapplied_funds_allocation', 'unallocation_parent_reversed')
          )
    """
    params: list = [args.loan_id]
    if args.from_date:
        sql += " AND (COALESCE(lr.value_date, lr.payment_date))::date >= %s::date"
        params.append(args.from_date)
    sql += " ORDER BY COALESCE(lr.value_date, lr.payment_date), lr.id"

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print("No matching rows (check loan_id, DB name via FARNDACRED_DB_NAME, and filters).")
        return 0

    keys = list(rows[0].keys())
    print(" | ".join(keys))
    for r in rows:
        print(" | ".join(str(r.get(k)) for k in keys))
    print(f"({len(rows)} row(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
