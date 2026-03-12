"""
Check every allocation bucket: does alloc amount in loan_repayment_allocation
match the actual balance drop in loan_daily_state?
"""
from datetime import date
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import psycopg2
from psycopg2.extras import RealDictCursor
from config import get_database_url
from loan_management import get_loan_daily_state_balances

loan_id = 10

def db():
    return psycopg2.connect(get_database_url())

# ----------------------------------------------------------
# Step 1: For each non-trivial day, compare alloc amounts to
#         actual balance drops bucket by bucket
# ----------------------------------------------------------
BUCKET_ALLOC_STATE_MAP = {
    "alloc_principal_not_due":  "principal_not_due",
    "alloc_principal_arrears":  "principal_arrears",
    "alloc_interest_accrued":   "interest_accrued_balance",
    "alloc_interest_arrears":   "interest_arrears_balance",
    "alloc_default_interest":   "default_interest_balance",
    "alloc_penalty_interest":   "penalty_interest_balance",
    "alloc_fees_charges":       "fees_charges_balance",
}

FOCUS_DATES = [
    date(2025, 10, 21),
    date(2025, 10, 22),
    date(2025, 10, 28),
    date(2025, 10, 31),
]

print(f"{'date':<12} {'bucket':<30} {'alloc_table':>12} {'balance_drop':>13} {'over_alloc':>11}")
print("-" * 82)

total_over = {}
for d in FOCUS_DATES:
    s_today = get_loan_daily_state_balances(loan_id, d)
    s_prev  = get_loan_daily_state_balances(loan_id, date(d.year, d.month, d.day - 1) if d.day > 1 else date(d.year, d.month - 1, 28))
    # Use the actual previous day from the db
    from datetime import timedelta
    s_prev = get_loan_daily_state_balances(loan_id, d - timedelta(days=1))

    # NET alloc from table for this date
    with db() as c:
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(lra.alloc_principal_not_due),0)  AS alloc_principal_not_due,
                    COALESCE(SUM(lra.alloc_principal_arrears),0)   AS alloc_principal_arrears,
                    COALESCE(SUM(lra.alloc_interest_accrued),0)    AS alloc_interest_accrued,
                    COALESCE(SUM(lra.alloc_interest_arrears),0)    AS alloc_interest_arrears,
                    COALESCE(SUM(lra.alloc_default_interest),0)    AS alloc_default_interest,
                    COALESCE(SUM(lra.alloc_penalty_interest),0)    AS alloc_penalty_interest,
                    COALESCE(SUM(lra.alloc_fees_charges),0)        AS alloc_fees_charges
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lr.loan_id = %s
                  AND COALESCE(lr.value_date, lr.payment_date) = %s
            """, (loan_id, d))
            alloc_net = dict(cur.fetchone())

    print(f"\n  === {d} ===")
    daily_acc = (
        float(s_today.get("regular_interest_daily") or 0)
        + float(s_today.get("penalty_interest_daily") or 0)
        + float(s_today.get("default_interest_daily") or 0)
    ) if s_today else 0
    print(f"  daily_accruals: {daily_acc:.4f}")

    for alloc_col, state_col in BUCKET_ALLOC_STATE_MAP.items():
        alloc_amt = float(alloc_net.get(alloc_col) or 0)
        if abs(alloc_amt) < 0.001:
            continue
        prev_bal = float(s_prev.get(state_col) or 0) if s_prev else 0
        curr_bal = float(s_today.get(state_col) or 0) if s_today else 0
        # Add daily accrual to the bucket that accrues (for comparison)
        if alloc_col == "alloc_interest_accrued":
            prev_bal_plus_accrual = prev_bal + float(s_today.get("regular_interest_daily") or 0) if s_today else prev_bal
        elif alloc_col == "alloc_penalty_interest":
            prev_bal_plus_accrual = prev_bal + float(s_today.get("penalty_interest_daily") or 0) if s_today else prev_bal
        elif alloc_col == "alloc_default_interest":
            prev_bal_plus_accrual = prev_bal + float(s_today.get("default_interest_daily") or 0) if s_today else prev_bal
        else:
            prev_bal_plus_accrual = prev_bal  # no daily accrual for principal/fees

        # balance drop = prev(+accrual) - curr  (how much the bucket actually fell)
        actual_drop = prev_bal_plus_accrual - curr_bal
        over = alloc_amt - actual_drop
        print(f"  {alloc_col:<30} alloc={alloc_amt:>9.4f}  prev+acc={prev_bal_plus_accrual:>9.4f}  curr={curr_bal:>9.4f}  drop={actual_drop:>9.4f}  over={over:>+9.4f}")
        if abs(over) > 0.001:
            total_over[alloc_col] = total_over.get(alloc_col, 0) + over

print("\n" + "=" * 82)
print("TOTAL OVER-ALLOCATION PER BUCKET:")
for k, v in total_over.items():
    print(f"  {k:<30} {v:>+12.4f}")
print(f"\n  GRAND TOTAL over-allocation = {sum(total_over.values()):>+12.4f}")
print(f"\n  Statement diff = -4.21 (charges in daily_state > formula requirement by 4.21)")
print(f"  These over-allocations in allocation table reflect stale reads by the waterfall engine.")
