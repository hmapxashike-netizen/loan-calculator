#!/usr/bin/env python3
"""
Remove duplicate reallocation_waterfall_correction rows for receipt 17 and fix
the corresponding unallocation that undoes them.

Receipt 17 has 4 identical reallocations (ids 855, 856, 857 duplicate 854) and
unallocation 954 that undoes 4x the amount. This script:
1. Deletes allocation ids 855, 856, 857 (keeps 854)
2. Updates allocation 954 to undo just one copy (-531.13 principal, -5.34 penalty)

Usage: python scripts/cleanup_duplicate_allocation_receipt_17.py [--dry-run]
"""

import argparse
import os
import sys
from collections import defaultdict

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    ap = argparse.ArgumentParser(description="Remove duplicate allocation rows for receipt 17")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    args = ap.parse_args()

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, repayment_id, event_type,
                       alloc_principal_not_due, alloc_principal_arrears,
                       alloc_interest_accrued, alloc_interest_arrears,
                       alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                       alloc_principal_total, alloc_interest_total, alloc_fees_total
                FROM loan_repayment_allocation
                WHERE repayment_id = 17
                ORDER BY id
                """,
            )
            rows = list(cur.fetchall())

        reallocs = [r for r in rows if r["event_type"] == "reallocation_waterfall_correction"]
        unallocs = [r for r in rows if r["event_type"] == "unallocation_waterfall_correction"]

        def sig(r):
            """Full bucket signature - only identical rows are duplicates."""
            return tuple(
                round(float(r.get(k) or 0), 2)
                for k in (
                    "alloc_principal_not_due", "alloc_principal_arrears",
                    "alloc_interest_accrued", "alloc_interest_arrears",
                    "alloc_default_interest", "alloc_penalty_interest", "alloc_fees_charges",
                )
            )

        by_sig = defaultdict(list)
        for r in reallocs:
            by_sig[sig(r)].append(r["id"])

        dupes = [ids for ids in by_sig.values() if len(ids) > 1]
        if not dupes:
            print("No duplicate reallocation rows found for receipt 17.")
            return 0

        to_delete = []
        single_prin = single_int = 0
        for ids in dupes:
            to_delete.extend(ids[1:])
            r0 = next(r for r in reallocs if r["id"] == ids[0])
            single_prin = float(r0.get("alloc_principal_total") or 0)
            single_int = float(r0.get("alloc_interest_total") or 0)

        unalloc_to_fix = None
        for u in unallocs:
            up = float(u.get("alloc_principal_total") or 0)
            if up < -1e-6 and single_prin > 1e-6:
                n = round(abs(up) / single_prin)
                if n > 1 and abs(abs(up) - n * single_prin) < 0.02:
                    unalloc_to_fix = (u["id"], -single_prin, -single_int)
                    break

        if not unalloc_to_fix:
            print("No matching unallocation found to fix.")
            return 1

        u_id, new_prin, new_int = unalloc_to_fix
        print("Planned changes:")
        print(f"  Delete allocation ids {to_delete} (duplicates)")
        print(f"  Update allocation id {u_id}: -> principal={new_prin}, interest={new_int}")

        if args.dry_run:
            print("Dry run: no changes made.")
            return 0

        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM loan_repayment_allocation WHERE id IN %s",
                (tuple(to_delete),),
            )
            deleted = cur.rowcount

            cur.execute(
                """
                UPDATE loan_repayment_allocation SET
                    alloc_principal_not_due = 0,
                    alloc_principal_arrears = %s,
                    alloc_interest_accrued = 0,
                    alloc_interest_arrears = 0,
                    alloc_default_interest = 0,
                    alloc_penalty_interest = %s,
                    alloc_fees_charges = 0,
                    alloc_principal_total = %s,
                    alloc_interest_total = %s,
                    alloc_fees_total = 0
                WHERE id = %s
                """,
                (new_prin, new_int, new_prin, new_int, u_id),
            )
            updated = cur.rowcount

        conn.commit()
        print(f"Deleted {deleted} duplicate row(s). Updated {updated} unallocation row.")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
