"""
Run schema migration 25_loan_daily_state_highprec_daily.sql using project config.
Upgrades daily and period-to-date columns to NUMERIC(22,10) so sum of daily = schedule total.

From project root:
  python scripts/run_migration_25.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_database_url


def main():
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 not installed. Install with: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    schema_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema")
    sql_path = os.path.join(schema_dir, "25_loan_daily_state_highprec_daily.sql")
    if not os.path.isfile(sql_path):
        print(f"Migration file not found: {sql_path}", file=sys.stderr)
        sys.exit(1)

    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    url = get_database_url()
    if not url or "/" not in url:
        print("Database URL not configured. Set LMS_DB_PASSWORD (and LMS_DB_USER if needed).", file=sys.stderr)
        sys.exit(1)

    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
        print("Migration 25 applied successfully. Re-run EOD for affected dates to repopulate at 10dp.")
    except Exception as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
