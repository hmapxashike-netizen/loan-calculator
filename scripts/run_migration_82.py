"""Create idx_journal_entries_loan_id CONCURRENTLY (must use autocommit; not inside a multi-statement txn)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "82_journal_entries_loan_id_index.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        print("Migration 82 complete: CREATE INDEX CONCURRENTLY idx_journal_entries_loan_id.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
