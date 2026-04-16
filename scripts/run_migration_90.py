"""Apply schema/90_creditor_facilities_drawdowns.sql (facilities, drawdowns, journal columns)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 AS x FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'creditor_drawdowns'
                """
            )
            if cur.fetchone():
                print("Migration 90 skipped: creditor_drawdowns already exists.")
                return
            cur.execute(
                """
                SELECT 1 AS x FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'creditor_loans'
                """
            )
            if not cur.fetchone():
                print("Migration 90 skipped: creditor_loans not present (creditor module not installed?).")
                return
        sql_path = os.path.join(root, "schema", "90_creditor_facilities_drawdowns.sql")
        with open(sql_path, encoding="utf-8") as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 90 complete: creditor facilities, drawdowns, journal creditor columns.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
