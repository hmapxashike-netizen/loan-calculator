"""Apply schema/51_manual_cash_gl_accounts.sql — manual cash GL on loans and receipts."""

import os
import sys

import psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config


def run_migration():
    conn = psycopg2.connect(config.get_database_url())
    path = os.path.join(os.path.dirname(__file__), "..", "schema", "51_manual_cash_gl_accounts.sql")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    conn.close()
    print("Migration 51 complete: loans.cash_gl_account_id, loan_repayments.source_cash_gl_account_id.")


if __name__ == "__main__":
    run_migration()
