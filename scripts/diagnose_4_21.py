"""
Now we know the actual daily columns. Trace the 4.21 precisely.
Sum of daily accruals = 3802.65 + 63.26 + 107.40 = 3973.31
Required (from formula) = 3969.10
Difference = 4.21

Trace: sum of penalty_daily by period, and find where the 4.21 comes from.
"""
from datetime import date, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import psycopg2
from psycopg2.extras import RealDictCursor
from config import get_database_url
from loan_management import get_schedule_lines

loan_id = 10

conn = psycopg2.connect(get_database_url())
with conn.cursor(cursor_factory=RealDictCursor) as cur:
    # Get all daily states for the period
    cur.execute("""
        SELECT as_of_date, regular_interest_daily, penalty_interest_daily, default_interest_daily,
               principal_arrears, interest_arrears_balance, penalty_interest_balance, default_interest_balance,
               interest_accrued_balance, principal_not_due
        FROM loan_daily_state
        WHERE loan_id = %s AND as_of_date BETWEEN '2025-06-30' AND '2025-10-31'
        ORDER BY as_of_date
    """, (loan_id,))
    rows = cur.fetchall()

by_date = {r["as_of_date"]: r for r in rows}

# Get schedule to see due dates and principal installments
schedule = get_schedule_lines(loan_id)
due_dates = {s["due_date"]: s for s in schedule if s.get("due_date")}

print("=== Per-due-period accrual sums ===")
periods = sorted(due_dates.keys())
prev_due = date(2025, 6, 30)  # disbursement

total_reg = total_pen = total_def = 0.0
for due_d in periods:
    sched = due_dates[due_d]
    period_reg = period_pen = period_def = 0.0
    d = prev_due + timedelta(days=1)
    while d <= due_d:
        row = by_date.get(d)
        if row:
            period_reg += float(row.get("regular_interest_daily") or 0)
            period_pen += float(row.get("penalty_interest_daily") or 0)
            period_def += float(row.get("default_interest_daily") or 0)
        d += timedelta(days=1)
    total_reg += period_reg
    total_pen += period_pen
    total_def += period_def
    sched_int = float(sched.get("interest_component") or sched.get("interest") or 0)
    print(f"  Period {prev_due+timedelta(days=1)} → {due_d}: "
          f"reg={period_reg:.4f}  pen={period_pen:.4f}  def={period_def:.4f}  "
          f"sched_int={sched_int:.4f}  diff_reg={period_reg-sched_int:.4f}")
    prev_due = due_d

print(f"\nTOTAL: reg={total_reg:.4f}  pen={total_pen:.4f}  def={total_def:.4f}  sum={total_reg+total_pen+total_def:.4f}")
print(f"Required charges (from formula) = 3969.10")
print(f"Diff from formula = {total_reg+total_pen+total_def - 3969.10:.4f}")

# Specifically: where do penalty charges come from?
# Penalty started from Sep 30 (first arrears after grace).
# Penalty daily by key dates:
print("\n=== Penalty daily by date (Sep 30 - Oct 31) ===")
d = date(2025, 9, 30)
while d <= date(2025, 10, 31):
    row = by_date.get(d)
    if row:
        pen = float(row.get("penalty_interest_daily") or 0)
        def_ = float(row.get("default_interest_daily") or 0)
        pen_bal = float(row.get("penalty_interest_balance") or 0)
        def_bal = float(row.get("default_interest_balance") or 0)
        prin_arr = float(row.get("principal_arrears") or 0)
        if pen > 0 or def_ > 0 or pen_bal > 0 or def_bal > 0 or prin_arr > 0:
            print(f"  {d}: pen_daily={pen:.4f}  def_daily={def_:.4f}  pen_bal={pen_bal:.4f}  "
                  f"def_bal={def_bal:.4f}  prin_arr={prin_arr:.4f}")
    d += timedelta(days=1)

# Net allocation to penalty and default across all dates
with conn.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute("""
        SELECT 
            COALESCE(SUM(lra.alloc_penalty_interest), 0) AS total_alloc_penalty,
            COALESCE(SUM(lra.alloc_default_interest), 0) AS total_alloc_default
        FROM loan_repayment_allocation lra
        JOIN loan_repayments lr ON lr.id = lra.repayment_id
        WHERE lr.loan_id = %s
    """, (loan_id,))
    alloc_totals = dict(cur.fetchone())
    print(f"\n=== Net allocation totals (all time) ===")
    print(f"  total_alloc_penalty = {alloc_totals['total_alloc_penalty']:.4f}  (sum_daily = 63.26)")
    print(f"  total_alloc_default = {alloc_totals['total_alloc_default']:.4f}  (sum_daily = 107.40)")
    print(f"  If alloc > daily: closing balance was over-reduced (balance went negative temporarily)")
    print(f"  penalty: closing_bal=0, daily_sum=63.26, alloc={float(alloc_totals['total_alloc_penalty']):.4f}")
    print(f"  identity penalty: 0+63.26-{float(alloc_totals['total_alloc_penalty']):.4f} = {63.26-float(alloc_totals['total_alloc_penalty']):.4f} (should=0)")
    print(f"  identity default: 0+107.40-{float(alloc_totals['total_alloc_default']):.4f} = {107.40-float(alloc_totals['total_alloc_default']):.4f} (should=0)")

conn.close()
