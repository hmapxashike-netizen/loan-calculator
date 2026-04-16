"""Apply schema 89: RBAC permission loan_management.batch_capture (+ SUPERADMIN grant)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "89_loan_management_batch_capture_permission.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 89 complete: loan_management.batch_capture RBAC.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
