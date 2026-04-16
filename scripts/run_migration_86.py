"""Apply schema/86_creditor_nav_admin_officer.sql (Creditor nav for ADMIN / LOAN_OFFICER)."""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config import get_database_url


def main() -> None:
    import psycopg2

    sql_path = os.path.join(ROOT, "schema", "86_creditor_nav_admin_officer.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        print("Migration 86 applied OK.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
