"""Apply schema/81_journal_entries_loan_id.sql (indexed loan_id on journal_entries)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "81_journal_entries_loan_id.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print(
            "Migration 81 complete: journal_entries.loan_id column (+ FK).\n"
            "Next (large DBs): python scripts/backfill_journal_entries_loan_id.py\n"
            "Then: python scripts/run_migration_82.py  # CONCURRENT index"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
