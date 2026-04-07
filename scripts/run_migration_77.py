"""
Apply schema/77_eom_regular_interest_templates.sql
Replaces CLEAR_DAILY_ACCRUAL + BILLING_REGULAR_INTEREST (EOD) with
REGULAR_INTEREST_BILLING_RECEIVABLE (EOD) and EOM_REGULAR_INTEREST_INCOME_RECOGNITION (EOM).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "77_eom_regular_interest_templates.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 77 complete: EOM regular interest transaction templates applied.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
