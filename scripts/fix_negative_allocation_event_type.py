#!/usr/bin/env python3
"""
Fix allocation rows for negative repayments that incorrectly have event_type='new_allocation'.
They should be 'unallocation_parent_reversed' (reversals/unallocations, not new allocations).

Usage: python scripts/fix_negative_allocation_event_type.py [--dry-run]
"""

import argparse
import sys

import os
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    ap = argparse.ArgumentParser(description="Fix event_type for negative-repayment allocations")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be updated")
    args = ap.parse_args()

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT lra.id, lra.repayment_id, lr.amount, lra.event_type
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lr.amount < 0 AND lra.event_type = 'new_allocation'
                """
            )
            rows = cur.fetchall()
        if not rows:
            print("No allocation rows with negative repayment and new_allocation found.")
            return 0
        print(f"Found {len(rows)} allocation row(s) to fix:")
        for r in rows:
            print(f"  allocation_id={r['id']} repayment_id={r['repayment_id']} amount={r['amount']}")
        if args.dry_run:
            print("Dry run: no changes made.")
            return 0
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_repayment_allocation lra
                SET event_type = 'unallocation_parent_reversed'
                FROM loan_repayments lr
                WHERE lr.id = lra.repayment_id
                  AND lr.amount < 0
                  AND lra.event_type = 'new_allocation'
                """
            )
            updated = cur.rowcount
        conn.commit()
        print(f"Updated {updated} row(s) to event_type='unallocation_parent_reversed'.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
