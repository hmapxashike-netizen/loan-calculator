"""
Run schema migration 18_allocation_audit_log.sql using project config.
Creates allocation_audit_log table for reversal add-back and system reallocation audit trail.

From project root:
  python scripts/run_migration_18.py

Prompts for database password if FARNDACRED_DB_PASSWORD / LMS_DB_PASSWORD is not set.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_database_url
from urllib.parse import quote_plus


def main():
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 not installed. Install with: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    schema_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema")
    sql_path = os.path.join(schema_dir, "18_allocation_audit_log.sql")
    if not os.path.isfile(sql_path):
        print(f"Migration file not found: {sql_path}", file=sys.stderr)
        sys.exit(1)

    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    url = get_database_url()
    if not url or "/" not in url or not (os.environ.get("FARNDACRED_DB_PASSWORD") or os.environ.get("LMS_DB_PASSWORD")):
        try:
            import getpass
            from config import DB_USER, DB_HOST, DB_PORT, DB_NAME
            password = getpass.getpass("Database password: ")
            if password:
                safe_password = quote_plus(password)
                auth = f"{DB_USER}:{safe_password}"
                url = f"postgresql://{auth}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        except Exception:
            pass
    if not url or "/" not in url:
        print("Database URL not configured. Set FARNDACRED_DB_PASSWORD (or LMS_DB_PASSWORD) or enter password when prompted.", file=sys.stderr)
        sys.exit(1)

    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
        print("Migration 18 (allocation_audit_log) applied successfully.")
    except Exception as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
