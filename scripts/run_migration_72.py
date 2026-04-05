"""
Apply schema/72_unapplied_funds_ledger_fix_system_receipt_double_count.sql
Fixes phantom unapplied deltas for recast/EOD liquidation repayment rows.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "72_unapplied_funds_ledger_fix_system_receipt_double_count.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 72 complete: unapplied_funds_ledger excludes system liquidation from credits CTE.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
