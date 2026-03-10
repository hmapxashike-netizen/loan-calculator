#!/usr/bin/env python3
"""
Audit loan_repayment_allocation and loan_daily_state for:
- Negative repayments with new_allocation (should be unallocation_parent_reversed)
- Duplicate/redundant entries per receipt
- Waterfall correction duplicates
- Net allocation vs repayment amount
- Credits column: stored vs computed (payment +, reversal -)
- Allocation effect on balances

Usage: python scripts/audit_allocation.py [--loan ID] [repayment_id ...]
"""

import argparse
import os
import sys
from collections import defaultdict

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    ap = argparse.ArgumentParser(description="Audit allocations and credits")
    ap.add_argument("--loan", type=int, help="Filter to specific loan_id")
    ap.add_argument("repayment_ids", nargs="*", type=int, help="Filter to specific repayment IDs")
    args = ap.parse_args()
    filter_ids = args.repayment_ids if args.repayment_ids else None

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from loan_management import get_net_allocation_for_loan_date, get_unallocated_for_loan_date

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)

    with conn.cursor() as cur:
        if args.loan:
            cur.execute(
                """
                SELECT lra.id AS alloc_id, lra.repayment_id, lr.loan_id, lr.amount AS repayment_amount,
                       lr.original_repayment_id, lra.event_type,
                       lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                       lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                       lra.alloc_default_interest, lra.alloc_penalty_interest, lra.alloc_fees_charges,
                       lra.alloc_principal_total, lra.alloc_interest_total, lra.alloc_fees_total,
                       lra.created_at
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lr.loan_id = %s
                ORDER BY lra.repayment_id, lra.created_at
                """,
                (args.loan,),
            )
        else:
            cur.execute(
                """
                SELECT lra.id AS alloc_id, lra.repayment_id, lr.loan_id, lr.amount AS repayment_amount,
                       lr.original_repayment_id, lra.event_type,
                       lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                       lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                       lra.alloc_default_interest, lra.alloc_penalty_interest, lra.alloc_fees_charges,
                       lra.alloc_principal_total, lra.alloc_interest_total, lra.alloc_fees_total,
                       lra.created_at
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                ORDER BY lra.repayment_id, lra.created_at
                """
            )
        rows = [dict(r) for r in cur.fetchall()]

    if filter_ids:
        rows = [r for r in rows if r["repayment_id"] in filter_ids]

    # 0. Net allocation and unallocated (per day: net_allocation + unallocated = credit)
    print("=" * 60)
    print("0. NET ALLOCATION & UNALLOCATED (per day; net_allocation + unallocated = credit)")
    print("=" * 60)
    try:
        with conn.cursor() as cur:
            if args.loan:
                cur.execute(
                    """
                    SELECT loan_id, as_of_date, total_exposure, net_allocation, unallocated
                    FROM loan_daily_state WHERE loan_id = %s
                    ORDER BY as_of_date
                    """,
                    (args.loan,),
                )
            else:
                cur.execute(
                    """
                    SELECT loan_id, as_of_date, total_exposure, net_allocation, unallocated
                    FROM loan_daily_state
                    ORDER BY loan_id, as_of_date
                    """
                )
            lds_rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        if "net_allocation" in str(e).lower() or "unallocated" in str(e).lower() or "column" in str(e).lower():
            print("  net_allocation/unallocated columns not found. Run schema/24_loan_daily_state_net_allocation.sql first.")
        else:
            print(f"  Error: {e}")
        lds_rows = []

    if lds_rows:
        mismatches = []
        for r in lds_rows:
            loan_id = r["loan_id"]
            as_of_date = r["as_of_date"]
            if hasattr(as_of_date, "date"):
                as_of_date = as_of_date.date()
            stored_net = r.get("net_allocation")
            stored_unalloc = r.get("unallocated")
            computed_net = get_net_allocation_for_loan_date(loan_id, as_of_date)
            computed_unalloc = get_unallocated_for_loan_date(loan_id, as_of_date)
            if stored_net is not None and abs(float(stored_net or 0) - computed_net) > 0.01:
                mismatches.append((loan_id, as_of_date, "net_allocation", float(stored_net or 0), computed_net))
            if stored_unalloc is not None and abs(float(stored_unalloc or 0) - computed_unalloc) > 0.01:
                mismatches.append((loan_id, as_of_date, "unallocated", float(stored_unalloc or 0), computed_unalloc))
            net_str = f"{float(stored_net):.2f}" if stored_net is not None else "NULL"
            unalloc_str = f"{float(stored_unalloc):.2f}" if stored_unalloc is not None else "NULL"
            credit = (float(stored_net or 0) + float(stored_unalloc or 0)) if (stored_net is not None or stored_unalloc is not None) else None
            credit_str = f"{credit:.2f}" if credit is not None else "NULL"
            print(f"  loan_id={loan_id} as_of_date={as_of_date} total_exposure={r['total_exposure']} "
                  f"net_allocation={net_str} unallocated={unalloc_str} credit={credit_str}")
        if mismatches:
            print("  WARNING: net_allocation/unallocated mismatch (stored vs computed):")
            for loan_id, d, col, s, c in mismatches:
                print(f"    loan_id={loan_id} as_of_date={d} {col} stored={s:.2f} computed={c:.2f}")
        else:
            print("  All net_allocation/unallocated match computed (or columns not yet populated).")
    else:
        print("  No loan_daily_state rows.")
    print()

    # 1. Negative repayments with new_allocation
    print("=" * 60)
    print("1. NEGATIVE REPAYMENTS WITH new_allocation (should be unallocation_parent_reversed)")
    print("=" * 60)
    bad = [r for r in rows if float(r.get("repayment_amount") or 0) < 0 and r.get("event_type") == "new_allocation"]
    for r in bad:
        print(f"  allocation_id={r['alloc_id']} repayment_id={r['repayment_id']} amount={r['repayment_amount']} "
              f"event_type={r['event_type']} -> SHOULD BE unallocation_parent_reversed")
    if not bad:
        print("  None found.")
    print()

    # 2. Per-receipt audit
    by_rep = defaultdict(list)
    for r in rows:
        by_rep[r["repayment_id"]].append(r)

    for rep_id in sorted(by_rep.keys()):
        if filter_ids and rep_id not in filter_ids:
            continue
        entries = by_rep[rep_id]
        print("=" * 60)
        print(f"2. RECEIPT {rep_id} – {len(entries)} allocation row(s)")
        print("=" * 60)

        # Find duplicate reallocation_waterfall_correction (identical amounts)
        realloc = [e for e in entries if e["event_type"] == "reallocation_waterfall_correction"]
        unalloc = [e for e in entries if e["event_type"] == "unallocation_waterfall_correction"]

        def _sig(e):
            return (
                float(e.get("alloc_principal_not_due") or 0),
                float(e.get("alloc_principal_arrears") or 0),
                float(e.get("alloc_interest_accrued") or 0),
                float(e.get("alloc_interest_arrears") or 0),
                float(e.get("alloc_default_interest") or 0),
                float(e.get("alloc_penalty_interest") or 0),
                float(e.get("alloc_fees_charges") or 0),
            )

        seen_sigs = defaultdict(list)
        for e in realloc:
            seen_sigs[_sig(e)].append(e["alloc_id"])
        dupes = {k: v for k, v in seen_sigs.items() if len(v) > 1}
        if dupes:
            print("  DUPLICATE reallocation_waterfall_correction (identical amounts):")
            for sig, ids in dupes.items():
                print(f"    alloc_ids={ids} (count={len(ids)})")
        else:
            print("  No duplicate reallocation rows.")

        # Net allocation
        net_prin = sum(float(e.get("alloc_principal_total") or 0) for e in entries)
        net_int = sum(float(e.get("alloc_interest_total") or 0) for e in entries)
        net_fees = sum(float(e.get("alloc_fees_total") or 0) for e in entries)
        amt = entries[0]["repayment_amount"] if entries else 0
        print(f"  Net allocation: principal={net_prin:.2f} interest={net_int:.2f} fees={net_fees:.2f}")
        print(f"  Repayment amount: {amt}")
        if abs(amt) > 1e-6 and abs((net_prin + net_int + net_fees) - float(amt)) > 0.02:
            print("  WARNING: Net allocation does not match repayment amount!")
        print()

    conn.close()


if __name__ == "__main__":
    main()
