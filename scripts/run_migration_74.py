"""
Apply schema/74_loan_modifications_restructure_fee_amount.sql
Adds restructure_fee_amount to loan_modifications for fee posting/amortisation.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "74_loan_modifications_restructure_fee_amount.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 74 complete: loan_modifications.restructure_fee_amount added.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
