"""
Migration 56: journal_items debit/credit -> NUMERIC(28,10)

Ensures EOD and other postings preserve the same precision as loan_daily_state.
Run: python scripts/run_migration_56.py
"""
import os
import sys

# Allow running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def run() -> None:
    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "schema",
        "56_journal_items_debit_credit_10dp.sql",
    )
    with open(schema_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 56 complete: journal_items.debit/credit -> NUMERIC(28,10).")
    except Exception as e:
        conn.rollback()
        print("Error:", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
