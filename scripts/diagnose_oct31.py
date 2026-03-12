"""
Diagnose table-level identity residual on 2025-10-31 for loan 10.
Checks: daily_state balances, allocation rows, unapplied ledger, and per-day
exposure + allocation identity to find what causes the 4.21 non-cash movement.
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

def conn():
    return psycopg2.connect(get_database_url())

def tot(s):
    if not s:
        return 0.0
    return sum(float(s.get(k) or 0) for k in [
        "principal_not_due", "principal_arrears", "interest_accrued_balance",
        "interest_arrears_balance", "default_interest_balance",
        "penalty_interest_balance", "fees_charges_balance"
    ])

# -------------------------------------------------------------------
print("=== loan_daily_state: Oct 28 - Oct 31 ===")
for day_n in range(28, 32):
    d = date(2025, 10, day_n)
    s = get_loan_daily_state_balances(loan_id, d)
    if s:
        ia   = float(s.get("interest_accrued_balance") or 0)
        iarr = float(s.get("interest_arrears_balance") or 0)
        pnd  = float(s.get("principal_not_due") or 0)
        pa   = float(s.get("principal_arrears") or 0)
        rd   = float(s.get("regular_interest_daily") or 0)
        na   = float(s.get("net_allocation") or 0)
        un   = float(s.get("unallocated") or 0)
        print(f"  {d}: pnd={pnd:.4f} pa={pa:.4f} int_acc={ia:.4f} int_arr={iarr:.4f}  regular_daily={rd:.4f}  net_alloc={na:.4f}  unallocated={un:.4f}  TOTAL={tot(s):.4f}")
    else:
        print(f"  {d}: NO STATE")

# -------------------------------------------------------------------
print("\n=== loan_repayment_allocation rows on 2025-10-31 ===")
with conn() as c:
    with c.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT lra.id,
                   lra.repayment_id,
                   lra.alloc_interest_accrued,
                   lra.alloc_interest_arrears,
                   lra.alloc_principal_not_due,
                   lra.alloc_principal_arrears,
                   lra.alloc_penalty_interest,
                   lra.alloc_default_interest,
                   lra.alloc_fees_charges,
                   lr.amount,
                   lr.value_date,
                   lr.reference,
                   lr.customer_reference
            FROM loan_repayment_allocation lra
            JOIN loan_repayments lr ON lr.id = lra.repayment_id
            WHERE lr.loan_id = %s
              AND COALESCE(lr.value_date, lr.payment_date) = %s
            ORDER BY lra.id
        """, (loan_id, date(2025, 10, 31)))
        alloc_rows = cur.fetchall()
        for r in alloc_rows:
            print(f"  {dict(r)}")

# -------------------------------------------------------------------
print("\n=== unapplied_funds rows on 2025-10-31 ===")
with conn() as c:
    with c.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, loan_id, repayment_id, source_repayment_id,
                   amount, value_date, entry_type, notes
            FROM unapplied_funds
            WHERE loan_id = %s AND value_date = %s
            ORDER BY id
        """, (loan_id, date(2025, 10, 31)))
        urows = cur.fetchall()
        for r in urows:
            print(f"  {dict(r)}")

# -------------------------------------------------------------------
print("\n=== per-day exposure+alloc identity Oct 28-31 ===")
print(f"  {'date':<12} {'delta_exp':>10} {'alloc_total':>12} {'delta+alloc':>12} {'daily_accr':>12} {'residual':>10}")
for day_n in range(28, 32):
    d = date(2025, 10, day_n)
    curr = get_loan_daily_state_balances(loan_id, d)
    prev = get_loan_daily_state_balances(loan_id, d - timedelta(days=1))
    a = get_allocation_totals_for_loan_date(loan_id, d)
    at = sum(float(a.get(k) or 0) for k in [
        "alloc_principal_not_due", "alloc_principal_arrears",
        "alloc_interest_accrued", "alloc_interest_arrears",
        "alloc_default_interest", "alloc_penalty_interest", "alloc_fees_charges"
    ])
    if curr and prev:
        de = tot(curr) - tot(prev)
        daily_acc = (
            float(curr.get("regular_interest_daily") or 0)
            + float(curr.get("penalty_interest_daily") or 0)
            + float(curr.get("default_interest_daily") or 0)
        )
        resid = de + at - daily_acc
        print(f"  {d}  {de:>10.4f}  {at:>12.4f}  {de+at:>12.4f}  {daily_acc:>12.4f}  {resid:>10.4f}")
    else:
        print(f"  {d}: missing state")

# -------------------------------------------------------------------
# Cumulative per-day identity over full period
print("\n=== cumulative per-day identity residual (full range) ===")
rng = get_loan_daily_state_range(loan_id, start, end)
by = {r["as_of_date"]: r for r in rng if r.get("as_of_date")}

cum_residual = 0.0
non_zero_days = []
d = start
while d <= end:
    curr = by.get(d) or get_loan_daily_state_balances(loan_id, d)
    prev = by.get(d - timedelta(days=1)) or get_loan_daily_state_balances(loan_id, d - timedelta(days=1))
    if curr and prev:
        a = get_allocation_totals_for_loan_date(loan_id, d)
        at = sum(float(a.get(k) or 0) for k in [
            "alloc_principal_not_due", "alloc_principal_arrears",
            "alloc_interest_accrued", "alloc_interest_arrears",
            "alloc_default_interest", "alloc_penalty_interest", "alloc_fees_charges"
        ])
        de = tot(curr) - tot(prev)
        daily_acc = (
            float(curr.get("regular_interest_daily") or 0)
            + float(curr.get("penalty_interest_daily") or 0)
            + float(curr.get("default_interest_daily") or 0)
        )
        resid = de + at - daily_acc
        cum_residual += resid
        if abs(resid) > 0.005:
            non_zero_days.append((d, round(de, 4), round(at, 4), round(daily_acc, 4), round(resid, 4)))
    d += timedelta(days=1)

print(f"  Total cumulative residual: {round(cum_residual, 4)}")
print(f"  Days with non-trivial residual ({len(non_zero_days)}):")
for entry in non_zero_days:
    print(f"    {entry[0]}  delta_exp={entry[1]}  alloc={entry[2]}  daily_acc={entry[3]}  residual={entry[4]}")
