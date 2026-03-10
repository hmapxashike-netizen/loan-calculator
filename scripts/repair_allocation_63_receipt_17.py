#!/usr/bin/env python3
"""
Restore allocation 63 for receipt 17, which was incorrectly deleted by
cleanup_duplicate_allocation_receipt_17.py (it grouped 61 and 63 as duplicates
by totals only, but they had different bucket breakdowns).

Allocation 63 was: reallocation_waterfall_correction with 788.33 all to
interest arrears (0 principal, 788.33 interest).

Usage: python scripts/repair_allocation_63_receipt_17.py [--dry-run]
"""

import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    ap = argparse.ArgumentParser(description="Restore incorrectly deleted allocation 63")
    ap.add_argument("--dry-run", action="store_true", help="Preview without inserting")
    args = ap.parse_args()

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)

    # Allocation 63 values (from export before deletion)
    REPAYMENT_ID = 17
    VALUES = (
        0, 0, 0, 788.33, 0, 0, 0,  # buckets: prin_not_due, prin_arrears, int_accrued, int_arrears, default, penalty, fees
        0, 788.33, 0,  # totals: prin, int, fees
    )

    try:
        # Check if we already have a reallocation with this exact breakdown (63's values)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayment_allocation
                WHERE repayment_id = 17 AND event_type = 'reallocation_waterfall_correction'
                  AND alloc_principal_total = 0 AND alloc_interest_total = 788.33
                  AND alloc_interest_arrears = 788.33 AND alloc_default_interest = 0 AND alloc_penalty_interest = 0
                """
            )
            if cur.fetchone():
                print("Allocation with 63's values already exists. No repair needed.")
                return 0

        print("Restoring allocation 63: reallocation_waterfall_correction 0 principal, 788.33 interest (all to interest arrears)")

        if args.dry_run:
            print("Dry run: no changes made.")
            return 0

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total,
                    event_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (REPAYMENT_ID,) + VALUES + ("reallocation_waterfall_correction",),
            )
        conn.commit()
        print("Restored allocation 63.")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
