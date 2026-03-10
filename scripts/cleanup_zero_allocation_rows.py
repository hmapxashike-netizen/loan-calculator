#!/usr/bin/env python3
"""
Delete zero-allocation rows from loan_repayment_allocation.

These rows (unallocation_waterfall_correction and reallocation_waterfall_correction
with all alloc_* = 0) were created by the EOD recursion bug before the fix.
They net to nothing and bloat the table. Safe to delete.

Usage:
  python scripts/cleanup_zero_allocation_rows.py [--dry-run]
"""

import argparse
import sys

# Add project root for imports
sys.path.insert(0, ".")

from config import get_database_url
import psycopg2


def main():
    ap = argparse.ArgumentParser(description="Delete zero-allocation rows from loan_repayment_allocation")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    args = ap.parse_args()

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            # Count zero rows (all alloc columns = 0)
            cur.execute(
                """
                SELECT COUNT(*) FROM loan_repayment_allocation
                WHERE alloc_principal_not_due = 0 AND alloc_principal_arrears = 0
                  AND alloc_interest_accrued = 0 AND alloc_interest_arrears = 0
                  AND alloc_default_interest = 0 AND alloc_penalty_interest = 0
                  AND alloc_fees_charges = 0
                """
            )
            count = cur.fetchone()[0]

            if count == 0:
                print("No zero-allocation rows found.")
                return 0

            print(f"Found {count} zero-allocation row(s).")

            if args.dry_run:
                print("Dry run: no rows deleted. Run without --dry-run to delete.")
                return 0

            cur.execute(
                """
                DELETE FROM loan_repayment_allocation
                WHERE alloc_principal_not_due = 0 AND alloc_principal_arrears = 0
                  AND alloc_interest_accrued = 0 AND alloc_interest_arrears = 0
                  AND alloc_default_interest = 0 AND alloc_penalty_interest = 0
                  AND alloc_fees_charges = 0
                """
            )
            deleted = cur.rowcount
            conn.commit()
            print(f"Deleted {deleted} zero-allocation row(s).")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
