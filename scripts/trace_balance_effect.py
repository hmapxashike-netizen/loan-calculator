#!/usr/bin/env python3
"""
Trace how principal_arrears and interest_arrears_balance in loan_daily_state
were affected by payments (net allocation) for a given loan and date.

Usage: python scripts/trace_balance_effect.py LOAN_ID DATE
  e.g. python scripts/trace_balance_effect.py 31 2025-10-31
"""

import os
import sys
from datetime import timedelta

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/trace_balance_effect.py LOAN_ID DATE (e.g. 31 2025-10-31)")
        return 1
    loan_id = int(sys.argv[1])
    as_of = sys.argv[2]
    if "-" in as_of:
        as_of_date = __import__("datetime").date.fromisoformat(as_of)
    else:
        parts = as_of.split(".")
        if len(parts) == 3:
            as_of_date = __import__("datetime").date(int(parts[2]), int(parts[1]), int(parts[0]))
        else:
            print("Date format: YYYY-MM-DD or DD.MM.YYYY")
            return 1

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)

    # 1. loan_daily_state for target date and yesterday
    yesterday = as_of_date - timedelta(days=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT as_of_date, principal_not_due, principal_arrears, interest_accrued_balance,
                   interest_arrears_balance, default_interest_balance, penalty_interest_balance,
                   fees_charges_balance, total_exposure, net_allocation, unallocated
            FROM loan_daily_state
            WHERE loan_id = %s AND as_of_date IN (%s, %s)
            ORDER BY as_of_date
            """,
            (loan_id, yesterday, as_of_date),
        )
        state_rows = [dict(r) for r in cur.fetchall()]

    # 2. Allocations for repayments with value_date = as_of_date (affecting that day's state)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT lr.id AS repayment_id, lr.amount, lr.status, lra.event_type,
                   lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                   lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                   lra.alloc_default_interest, lra.alloc_penalty_interest, lra.alloc_fees_charges,
                   lra.alloc_principal_total, lra.alloc_interest_total, lra.alloc_fees_total
            FROM loan_repayments lr
            JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
            WHERE lr.loan_id = %s
              AND lr.status IN ('posted', 'reversed')
              AND (COALESCE(lr.value_date, lr.payment_date))::date = %s
            ORDER BY lr.id, lra.created_at
            """,
            (loan_id, as_of_date),
        )
        alloc_rows = [dict(r) for r in cur.fetchall()]

    conn.close()

    # Report
    print("=" * 70)
    print(f"LOAN {loan_id} – Balance effect for {as_of_date}")
    print("=" * 70)

    state_today = next((r for r in state_rows if r["as_of_date"] == as_of_date), None)
    state_yest = next((r for r in state_rows if r["as_of_date"] == yesterday), None)

    if not state_today:
        print(f"No loan_daily_state for {as_of_date}")
        return 1

    pa_today = float(state_today.get("principal_arrears") or 0)
    ia_today = float(state_today.get("interest_arrears_balance") or 0)
    net_alloc = float(state_today.get("net_allocation") or 0)
    unalloc = float(state_today.get("unallocated") or 0)

    print(f"\nloan_daily_state on {as_of_date}:")
    print(f"  principal_arrears         = {pa_today:.2f}")
    print(f"  interest_arrears_balance  = {ia_today:.2f}")
    print(f"  net_allocation            = {net_alloc:.2f}")
    print(f"  unallocated               = {unalloc:.2f}")

    if state_yest:
        pa_yest = float(state_yest.get("principal_arrears") or 0)
        ia_yest = float(state_yest.get("interest_arrears_balance") or 0)
        print(f"\nloan_daily_state on {yesterday} (yesterday):")
        print(f"  principal_arrears         = {pa_yest:.2f}")
        print(f"  interest_arrears_balance  = {ia_yest:.2f}")

    # Net allocation to principal_arrears and interest_arrears from allocations that day
    tot_alloc_pa = sum(float(r.get("alloc_principal_arrears") or 0) for r in alloc_rows)
    tot_alloc_ia = sum(float(r.get("alloc_interest_arrears") or 0) for r in alloc_rows)

    print(f"\n--- Net allocation affecting balances on {as_of_date} ---")
    print(f"  Total alloc_principal_arrears  (reduces principal_arrears)  = {tot_alloc_pa:.2f}")
    print(f"  Total alloc_interest_arrears   (reduces interest_arrears)   = {tot_alloc_ia:.2f}")

    if alloc_rows:
        print(f"\n  Per repayment (value_date = {as_of_date}):")
        by_rep = {}
        for r in alloc_rows:
            rid = r["repayment_id"]
            if rid not in by_rep:
                by_rep[rid] = []
            by_rep[rid].append(r)

        for rid in sorted(by_rep.keys()):
            entries = by_rep[rid]
            amt = entries[0]["amount"]
            status = entries[0]["status"]
            net_pa = sum(float(e.get("alloc_principal_arrears") or 0) for e in entries)
            net_ia = sum(float(e.get("alloc_interest_arrears") or 0) for e in entries)
            net_prin = sum(float(e.get("alloc_principal_total") or 0) for e in entries)
            net_int = sum(float(e.get("alloc_interest_total") or 0) for e in entries)
            net_fees = sum(float(e.get("alloc_fees_total") or 0) for e in entries)
            print(f"    repayment_id={rid} amount={amt} status={status}")
            print(f"      alloc_principal_arrears={net_pa:.2f} alloc_interest_arrears={net_ia:.2f}")
            print(f"      alloc_principal_total={net_prin:.2f} alloc_interest_total={net_int:.2f} alloc_fees_total={net_fees:.2f}")
            for e in entries:
                print(f"        event_type={e.get('event_type')}")
    else:
        print(f"  No allocation rows for repayments with value_date = {as_of_date}")

    # Effect: yesterday + accruals - allocations = today (conceptually)
    if state_yest:
        pa_yest = float(state_yest.get("principal_arrears") or 0)
        ia_yest = float(state_yest.get("interest_arrears_balance") or 0)
        print(f"\n--- Effect summary ---")
        print(f"  principal_arrears:  yesterday={pa_yest:.2f} - alloc_principal_arrears={tot_alloc_pa:.2f} -> today={pa_today:.2f}")
        print(f"  interest_arrears:   yesterday={ia_yest:.2f} - alloc_interest_arrears={tot_alloc_ia:.2f} -> today={ia_today:.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
