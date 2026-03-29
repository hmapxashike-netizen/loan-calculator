"""
Run schema/52_loan_daily_state_interest_in_suspense_balances.sql
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_database_url
import psycopg2


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "52_loan_daily_state_interest_in_suspense_balances.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 52 complete: loan_daily_state interest-in-suspense balance columns.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
