#!/usr/bin/env python3
"""
Clear loan_repayment_allocation and unapplied_funds, then run EOD from loan
start date to repopulate allocations. Use to verify no duplicates occur after
the recursion fix.

Flow:
1. TRUNCATE loan_repayment_allocation
2. DELETE from unapplied_funds (to avoid double credits on reallocate)
3. Run EOD for each date from min(disbursement_date) to max(value_date)
   with reallocate (creates allocations for posted receipts)
4. For reversed receipts with no allocation, run fix_reversal_allocation logic

Usage: python scripts/clear_allocation_and_rerun_eod.py [--dry-run] [--start YYYY-MM-DD] [--end YYYY-MM-DD]
"""

import argparse
import os
import sys
from datetime import date, timedelta

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    ap = argparse.ArgumentParser(description="Clear allocation table and re-run EOD")
    ap.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    ap.add_argument("--start", help="Start date (YYYY-MM-DD). Default: min disbursement_date")
    ap.add_argument("--end", help="End date (YYYY-MM-DD). Default: max value_date or today")
    args = ap.parse_args()

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MIN((COALESCE(disbursement_date, start_date))::date) AS start_d FROM loans WHERE status = 'active'"
            )
            start_row = cur.fetchone()
            cur.execute(
                "SELECT MAX((COALESCE(value_date, payment_date))::date) AS end_d FROM loan_repayments WHERE status IN ('posted', 'reversed')"
            )
            end_row = cur.fetchone()
        row = {"start_d": (start_row or {}).get("start_d"), "end_d": (end_row or {}).get("end_d")}
        start_d = (args.start and date.fromisoformat(args.start)) or (row and row.get("start_d"))
        end_d = (args.end and date.fromisoformat(args.end)) or (row and row.get("end_d")) or date.today()
        if not start_d:
            start_d = date.today()
        if not end_d or end_d < start_d:
            end_d = start_d

        print(f"Date range: {start_d} to {end_d}")
        print("Planned: TRUNCATE loan_repayment_allocation, DELETE FROM unapplied_funds, then EOD+reallocate per day")

        if args.dry_run:
            print("Dry run: no changes made.")
            return 0

        with conn.cursor() as cur:
            cur.execute("TRUNCATE loan_repayment_allocation RESTART IDENTITY CASCADE")
            cur.execute("DELETE FROM unapplied_funds")
            conn.commit()
        print("Cleared loan_repayment_allocation and unapplied_funds.")

        from eod import run_eod_for_date
        from loan_management import get_repayment_ids_for_value_date, reallocate_repayment

        current = start_d
        total_reallocated = 0
        errors = []
        total_days = (end_d - start_d).days + 1
        day_num = 0
        while current <= end_d:
            day_num += 1
            print(f"  EOD {current} ({day_num}/{total_days})...", flush=True)
            try:
                # We reallocate all receipts ourselves; skip EOD's reallocate to avoid duplicates.
                result = run_eod_for_date(current, skip_reallocate_after_reversals=True)
                rids = get_repayment_ids_for_value_date(current)
                for rid in rids:
                    try:
                        reallocate_repayment(rid)
                        total_reallocated += 1
                    except Exception as e:
                        errors.append((current, f"realloc {rid}: {e}"))
            except Exception as e:
                errors.append((current, str(e)))
                print(f"    ERROR: {e}", flush=True)
            current += timedelta(days=1)

        # Fix reversed receipts: ensure originals have allocation, then insert unallocation_parent_reversed.
        # Originals have status='reversed' so they're skipped by the main loop; we must allocate them first.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, original_repayment_id FROM loan_repayments
                WHERE status = 'reversed' AND amount < 0 AND original_repayment_id IS NOT NULL
                ORDER BY (COALESCE(value_date, payment_date))::date, id
                """
            )
            reversed_rows = [dict(r) for r in cur.fetchall()]

        from loan_management import (
            _get_allocation_sum_for_repayment,
            allocate_repayment_waterfall,
            load_system_config_from_db,
        )

        affected_reversal_dates = set()
        for rev_row in reversed_rows:
            rev_id = rev_row["id"]
            orig_id = rev_row["original_repayment_id"]
            if not orig_id:
                continue
            try:
                alloc = _get_allocation_sum_for_repayment(orig_id, conn)
                if not alloc:
                    # Original has status=reversed so was skipped by main loop; allocate it first.
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "SELECT COALESCE(value_date, payment_date) AS vd FROM loan_repayments WHERE id = %s",
                            (orig_id,),
                        )
                        r = cur.fetchone()
                    if r and r.get("vd"):
                        vd = r["vd"]
                        if hasattr(vd, "date"):
                            vd = vd.date()
                        run_eod_for_date(vd, skip_reallocate_after_reversals=True)
                    cfg = load_system_config_from_db() or {}
                    allocate_repayment_waterfall(orig_id, system_config=cfg)
                    alloc = _get_allocation_sum_for_repayment(orig_id, conn)
                if not alloc:
                    errors.append((None, f"reversal {rev_id}: original {orig_id} has no allocation"))
                    continue
                def _f(v):
                    return float(v or 0)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(value_date, payment_date)::date AS rev_date FROM loan_repayments WHERE id = %s",
                        (rev_id,),
                    )
                    rev_date_row = cur.fetchone()
                    rev_date = rev_date_row["rev_date"] if rev_date_row else None
                    cur.execute(
                        """
                        INSERT INTO loan_repayment_allocation (
                            repayment_id, alloc_principal_not_due, alloc_principal_arrears,
                            alloc_interest_accrued, alloc_interest_arrears,
                            alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                            alloc_principal_total, alloc_interest_total, alloc_fees_total,
                            event_type
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            rev_id,
                            -_f(alloc["alloc_principal_not_due"]),
                            -_f(alloc["alloc_principal_arrears"]),
                            -_f(alloc["alloc_interest_accrued"]),
                            -_f(alloc["alloc_interest_arrears"]),
                            -_f(alloc["alloc_default_interest"]),
                            -_f(alloc["alloc_penalty_interest"]),
                            -_f(alloc["alloc_fees_charges"]),
                            -(_f(alloc["alloc_principal_not_due"]) + _f(alloc["alloc_principal_arrears"])),
                            -(_f(alloc["alloc_interest_accrued"]) + _f(alloc["alloc_interest_arrears"])
                              + _f(alloc["alloc_default_interest"]) + _f(alloc["alloc_penalty_interest"])),
                            -_f(alloc["alloc_fees_charges"]),
                            "unallocation_parent_reversed",
                        ),
                    )
                if rev_date:
                    affected_reversal_dates.add(rev_date)
                conn.commit()
            except Exception as e:
                errors.append((None, f"reversal {rev_id}: {e}"))

        # Recompute daily state on dates affected by reversal unallocations so
        # loan_daily_state aligns with net allocation rows (including negatives).
        from eod import run_eod_for_date
        for d in sorted(affected_reversal_dates):
            run_eod_for_date(d, skip_reallocate_after_reversals=True)

        # Rebuild loan_daily_state from the final allocation table so reversal unallocations
        # are reflected in daily balances (avoids carrying original-only allocation effect).
        print("Refreshing loan_daily_state from final allocations...", flush=True)
        current = start_d
        while current <= end_d:
            try:
                run_eod_for_date(current, skip_reallocate_after_reversals=True)
            except Exception as e:
                errors.append((current, f"post-reversal eod refresh: {e}"))
            current += timedelta(days=1)

        print(f"Done. Reallocated {total_reallocated} receipts, fixed {len(reversed_rows)} reversals.")
        if errors:
            print("Errors:", file=sys.stderr)
            for d, msg in errors:
                print(f"  {d}: {msg}", file=sys.stderr)
            return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
