"""
Quick check: print loan_daily_state.regular_interest_daily for a loan/date or date range.

Run from project root:
  python scripts/check_loan_daily_accrual.py 9 2025-11-30
  python scripts/check_loan_daily_accrual.py 9 2025-10-08 2025-12-01
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/check_loan_daily_accrual.py <loan_id> <start_date> [end_date]")
        print("  e.g. python scripts/check_loan_daily_accrual.py 9 2025-11-30")
        print("  e.g. python scripts/check_loan_daily_accrual.py 9 2025-10-08 2025-12-01")
        sys.exit(1)

    from datetime import datetime

    loan_id = int(sys.argv[1])
    start = datetime.strptime(sys.argv[2].strip(), "%Y-%m-%d").date()
    end = datetime.strptime(sys.argv[3].strip(), "%Y-%m-%d").date() if len(sys.argv) > 3 else start

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT as_of_date, regular_interest_daily,
               interest_accrued_balance, interest_arrears_balance,
               principal_not_due, principal_arrears,
               default_interest_balance, penalty_interest_balance, fees_charges_balance
        FROM loan_daily_state
        WHERE loan_id = %s AND as_of_date >= %s AND as_of_date <= %s
        ORDER BY as_of_date
        """,
        (loan_id, start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        print(f"No rows for loan_id={loan_id} between {start} and {end}.")
        return

    print(f"Loan {loan_id} | {start} to {end}")
    print("-" * 72)
    for r in rows:
        reg = r.get("regular_interest_daily")
        acc = r.get("interest_accrued_balance")
        arr = r.get("interest_arrears_balance")
        pnd = r.get("principal_not_due")
        pa = r.get("principal_arrears")
        print(f"  {r['as_of_date']}  regular_interest_daily={reg}  interest_accrued_balance={acc}  interest_arrears_balance={arr}  principal_not_due={pnd}  principal_arrears={pa}")
    print("-" * 72)
    print(f"{len(rows)} row(s). Values are from loan_daily_state (regular_interest_daily = schedule interest / days in period).")


if __name__ == "__main__":
    main()
