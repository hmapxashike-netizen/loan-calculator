"""
Remove loan_daily_state rows where as_of_date is before the loan existed
(disbursement_date or start_date). Run from project root: python cleanup_loan_daily_state.py
"""

import sys

def main():
    try:
        from config import get_database_url
        import psycopg2
    except ImportError:
        print("Need config and psycopg2. Run from project root: python cleanup_loan_daily_state.py", file=sys.stderr)
        sys.exit(1)

    sql = """
    DELETE FROM loan_daily_state lds
    USING loans l
    WHERE lds.loan_id = l.id
      AND lds.as_of_date < COALESCE(l.disbursement_date, l.start_date);
    """
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            deleted = cur.rowcount
        conn.commit()
        print(f"Deleted {deleted} row(s) from loan_daily_state (dates before loan disbursement/start).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
