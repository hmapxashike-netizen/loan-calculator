"""
Show raw loan_daily_state + alloc detail for the flagged dates on loan 12.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config, psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal

LOAN_ID = 12
DATES   = ["2025-07-31", "2025-08-31", "2025-09-30", "2025-10-21", "2025-10-31"]

conn = psycopg2.connect(config.get_database_url())
cur  = conn.cursor(cursor_factory=RealDictCursor)

for d in DATES:
    print(f"\n=== {d} ===")
    # Daily state row
    cur.execute("""
        SELECT principal_not_due, principal_arrears,
               interest_accrued_balance, interest_arrears_balance,
               default_interest_balance, penalty_interest_balance,
               regular_interest_daily, penalty_interest_daily, default_interest_daily,
               total_exposure, days_overdue
        FROM loan_daily_state
        WHERE loan_id=%s AND as_of_date=%s
    """, (LOAN_ID, d))
    row = cur.fetchone()
    if row:
        te = sum([
            float(row["principal_not_due"]     or 0),
            float(row["principal_arrears"]     or 0),
            float(row["interest_accrued_balance"] or 0),
            float(row["interest_arrears_balance"] or 0),
            float(row["default_interest_balance"] or 0),
            float(row["penalty_interest_balance"] or 0),
        ])
        print(f"  prin_not_due={row['principal_not_due']}  prin_arr={row['principal_arrears']}")
        print(f"  int_accrued={row['interest_accrued_balance']}  int_arr={row['interest_arrears_balance']}")
        print(f"  def_bal={row['default_interest_balance']}  pen_bal={row['penalty_interest_balance']}")
        print(f"  reg_d={row['regular_interest_daily']}  pen_d={row['penalty_interest_daily']}  def_d={row['default_interest_daily']}")
        print(f"  total_exposure(saved)={row['total_exposure']}  total_exposure(summed)={te:.4f}")
        print(f"  days_overdue={row['days_overdue']}")

    # Allocation detail
    cur.execute("""
        SELECT lr.id AS rep_id, lr.amount, lr.reference, lra.event_type,
               lra.alloc_principal_not_due, lra.alloc_principal_arrears,
               lra.alloc_interest_accrued, lra.alloc_interest_arrears,
               lra.alloc_default_interest, lra.alloc_penalty_interest,
               lra.alloc_principal_total, lra.alloc_interest_total
        FROM loan_repayment_allocation lra
        JOIN loan_repayments lr ON lr.id=lra.repayment_id
        WHERE lr.loan_id=%s AND lr.value_date=%s
        ORDER BY lra.id
    """, (LOAN_ID, d))
    allocs = cur.fetchall()
    if allocs:
        for a in allocs:
            print(f"  alloc rep#{a['rep_id']} ({a['amount']} {a['reference']}) [{a['event_type']}]: "
                  f"prin_arr={a['alloc_principal_arrears']} int_arr={a['alloc_interest_arrears']} "
                  f"def={a['alloc_default_interest']} pen={a['alloc_penalty_interest']} "
                  f"total_prin={a['alloc_principal_total']} total_int={a['alloc_interest_total']}")
    else:
        print("  (no allocation rows)")

cur.close()
conn.close()
