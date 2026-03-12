"""Direct SQL check of the Oct 21 daily state row to see what's actually stored."""
from datetime import date
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import psycopg2
from psycopg2.extras import RealDictCursor
from config import get_database_url

conn = psycopg2.connect(get_database_url())
with conn.cursor(cursor_factory=RealDictCursor) as cur:
    for d in [date(2025, 10, 21), date(2025, 10, 22), date(2025, 10, 28), date(2025, 10, 31)]:
        cur.execute("""
            SELECT loan_id, as_of_date,
                   regular_interest_daily, penalty_interest_daily, default_interest_daily,
                   principal_arrears, interest_arrears_balance, penalty_interest_balance,
                   default_interest_balance, interest_accrued_balance, principal_not_due,
                   days_overdue, net_allocation, unallocated
            FROM loan_daily_state
            WHERE loan_id = 10 AND as_of_date = %s
        """, (d,))
        row = cur.fetchone()
        if row:
            print(f"\n=== {d} raw ===")
            for k, v in row.items():
                print(f"  {k}: {v!r}")
        else:
            print(f"\n=== {d}: NO ROW ===")

# Also check Oct 20 for comparison
with conn.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute("""
        SELECT loan_id, as_of_date,
               regular_interest_daily, penalty_interest_daily, default_interest_daily,
               days_overdue
        FROM loan_daily_state
        WHERE loan_id = 10 AND as_of_date = '2025-10-20'
    """)
    row = cur.fetchone()
    if row:
        print(f"\n=== 2025-10-20 raw ===")
        for k, v in row.items():
            print(f"  {k}: {v!r}")

# Show count of non-null penalty_interest_daily per period
with conn.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute("""
        SELECT COUNT(*) as total_rows, 
               COUNT(penalty_interest_daily) as non_null_penalty,
               COUNT(default_interest_daily) as non_null_default,
               COUNT(regular_interest_daily) as non_null_regular,
               SUM(COALESCE(penalty_interest_daily, 0)) as sum_penalty,
               SUM(COALESCE(default_interest_daily, 0)) as sum_default,
               SUM(COALESCE(regular_interest_daily, 0)) as sum_regular
        FROM loan_daily_state
        WHERE loan_id = 10
          AND as_of_date BETWEEN '2025-06-30' AND '2025-10-31'
    """)
    row = cur.fetchone()
    if row:
        print(f"\n=== Period summary ===")
        for k, v in row.items():
            print(f"  {k}: {v!r}")
conn.close()
