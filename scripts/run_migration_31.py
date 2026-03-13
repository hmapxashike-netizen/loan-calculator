"""
Run migration 31: enforce one allocation row per repayment_id.
Usage: python scripts/run_migration_31.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_database_url
import psycopg2

MIGRATION_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "schema",
    "31_one_allocation_per_repayment.sql",
)


def main():
    sql = open(MIGRATION_FILE, encoding="utf-8").read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 31 applied successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Migration 31 FAILED: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
