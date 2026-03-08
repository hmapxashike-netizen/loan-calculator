"""
Delete a loan and all related data (repayments, allocations, daily state, schedules, unapplied, etc.).
Uses ON DELETE CASCADE so one DELETE from loans is enough.

Run from project root:
  python scripts/delete_loan.py 8
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/delete_loan.py <loan_id>")
        print("Example: python scripts/delete_loan.py 8")
        sys.exit(1)
    try:
        loan_id = int(sys.argv[1])
    except ValueError:
        print("Error: loan_id must be an integer.")
        sys.exit(1)

    from config import get_database_url
    import psycopg2

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM loans WHERE id = %s", (loan_id,))
            if cur.fetchone() is None:
                print(f"Loan {loan_id} does not exist.")
                sys.exit(0)
            cur.execute("DELETE FROM loans WHERE id = %s", (loan_id,))
            deleted = cur.rowcount
        conn.commit()
        print(f"Deleted loan {loan_id} and all related data (repayments, allocations, loan_daily_state, schedules, unapplied_funds, etc.).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
