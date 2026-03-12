"""
Drill into each non-zero residual day and determine the root cause.
Identity: delta_exposure + allocation = daily_accrual
Residual means one of those three is wrong in the table.
"""
from datetime import date, timedelta
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import psycopg2
from psycopg2.extras import RealDictCursor
from config import get_database_url
from loan_management import get_loan_daily_state_balances, get_loan_daily_state_range, get_allocation_totals_for_loan_date

loan_id = 10
start = date(2025, 6, 30)
end = date(2025, 10, 31)

def db():
    return psycopg2.connect(get_database_url())

BALANCE_KEYS = [
    "principal_not_due", "principal_arrears", "interest_accrued_balance",
    "interest_arrears_balance", "default_interest_balance",
    "penalty_interest_balance", "fees_charges_balance"
]
ALLOC_KEYS = [
    "alloc_principal_not_due", "alloc_principal_arrears",
    "alloc_interest_accrued", "alloc_interest_arrears",
    "alloc_default_interest", "alloc_penalty_interest", "alloc_fees_charges"
]

def tot(s):
    if not s: return 0.0
    return sum(float(s.get(k) or 0) for k in BALANCE_KEYS)

# ---------------------------------------------------------------
# 1. Find ALL days with non-zero per-day identity residual
# ---------------------------------------------------------------
print("=== all days with identity residual > 0.005 ===")
print(f"{'date':<12} {'delta_exp':>10} {'alloc':>10} {'daily_acc':>10} {'residual':>10}  note")
rng = get_loan_daily_state_range(loan_id, start, end)
by = {}
for r in rng:
    k = r.get("as_of_date")
    if k:
        by[k if isinstance(k, date) else k] = r

non_zero = []
d = start
while d <= end:
    curr = by.get(d)
    if curr is None:
        curr = get_loan_daily_state_balances(loan_id, d)
    prev = by.get(d - timedelta(days=1))
    if prev is None:
        prev = get_loan_daily_state_balances(loan_id, d - timedelta(days=1))
    if curr and prev:
        a = get_allocation_totals_for_loan_date(loan_id, d)
        at = sum(float(a.get(k) or 0) for k in ALLOC_KEYS)
        de = tot(curr) - tot(prev)
        daily_acc = (
            float(curr.get("regular_interest_daily") or 0)
            + float(curr.get("penalty_interest_daily") or 0)
            + float(curr.get("default_interest_daily") or 0)
        )
        resid = de + at - daily_acc
        if abs(resid) > 0.005:
            non_zero.append((d, de, at, daily_acc, resid, curr, prev))
            print(f"  {d}  {de:>10.4f}  {at:>10.4f}  {daily_acc:>10.4f}  {resid:>10.4f}")
    d += timedelta(days=1)

print(f"\nTotal non-zero days: {len(non_zero)}")
print(f"Sum of residuals: {round(sum(x[4] for x in non_zero), 4)}")

# ---------------------------------------------------------------
# 2. For each non-zero day: show bucket-level breakdown
# ---------------------------------------------------------------
print("\n=== bucket-level breakdown for each non-zero day ===")
for d, de, at, daily_acc, resid, curr, prev in non_zero:
    print(f"\n--- {d}  (resid={round(resid,4)}) ---")
    print(f"  {'bucket':<30} {'prev':>10} {'curr':>10} {'delta':>10}")
    for k in BALANCE_KEYS:
        pv = float(prev.get(k) or 0)
        cv = float(curr.get(k) or 0)
        dv = cv - pv
        if abs(dv) > 0.001:
            print(f"  {k:<30} {pv:>10.4f} {cv:>10.4f} {dv:>10.4f}")
    print(f"  regular_interest_daily = {float(curr.get('regular_interest_daily') or 0):.4f}")
    print(f"  penalty_interest_daily = {float(curr.get('penalty_interest_daily') or 0):.4f}")
    print(f"  default_interest_daily = {float(curr.get('default_interest_daily') or 0):.4f}")

    # Get allocation detail for this day
    with db() as c:
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT lra.id, lra.repayment_id, lra.event_type,
                       lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                       lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                       lra.alloc_penalty_interest, lra.alloc_default_interest,
                       lra.alloc_fees_charges,
                       lr.amount, lr.reference, lr.customer_reference
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lr.loan_id = %s
                  AND COALESCE(lr.value_date, lr.payment_date) = %s
                ORDER BY lra.id
            """, (loan_id, d))
            alloc_rows = cur.fetchall()
    if alloc_rows:
        print(f"  allocation rows ({len(alloc_rows)}):")
        for r in alloc_rows:
            r = dict(r)
            non_zero_alloc = {k: v for k, v in r.items() if k.startswith("alloc_") and float(v or 0) != 0}
            print(f"    id={r['id']} rep_id={r['repayment_id']} event={r.get('event_type')} amt={r['amount']} ref={r['reference']} allocs={non_zero_alloc}")
    else:
        print(f"  no allocation rows")

# ---------------------------------------------------------------
# 3. Interest_accrued_balance growth vs regular_interest_daily
#    Find days where balance grew but regular_daily=0
# ---------------------------------------------------------------
print("\n=== days where interest_accrued grew but regular_interest_daily=0 ===")
d = start
while d <= end:
    curr = by.get(d) or get_loan_daily_state_balances(loan_id, d)
    prev = by.get(d - timedelta(days=1)) or get_loan_daily_state_balances(loan_id, d - timedelta(days=1))
    if curr and prev:
        ia_delta = float(curr.get("interest_accrued_balance") or 0) - float(prev.get("interest_accrued_balance") or 0)
        rdaily = float(curr.get("regular_interest_daily") or 0)
        if ia_delta > 0.005 and abs(rdaily) < 0.001:
            print(f"  {d}: interest_accrued grew by {ia_delta:.4f} but regular_interest_daily=0")
    d += timedelta(days=1)
