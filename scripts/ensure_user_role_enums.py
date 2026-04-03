"""
Add missing PostgreSQL user_role enum labels VENDOR and SUPERADMIN (idempotent).

Run once per database if you see:
  invalid input value for enum user_role: "VENDOR"
  invalid input value for enum user_role: "SUPERADMIN"

  python scripts/ensure_user_role_enums.py

Uses config.get_database_url() (same as the app). Requires autocommit (script sets it).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2

from config import get_database_url

_SQL = """
DO $body$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum e
    JOIN pg_type t ON e.enumtypid = t.oid
    WHERE t.typname = 'user_role' AND e.enumlabel = 'VENDOR'
  ) THEN
    ALTER TYPE user_role ADD VALUE 'VENDOR';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_enum e
    JOIN pg_type t ON e.enumtypid = t.oid
    WHERE t.typname = 'user_role' AND e.enumlabel = 'SUPERADMIN'
  ) THEN
    ALTER TYPE user_role ADD VALUE 'SUPERADMIN';
  END IF;
END
$body$;
"""


def main() -> int:
    try:
        conn = psycopg2.connect(get_database_url())
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        return 1
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(_SQL)
        cur.close()
    except Exception as e:
        print(f"Failed to extend user_role enum: {e}", file=sys.stderr)
        print("You can also run: psql \"<url>\" -f schema/69_user_role_vendor.sql", file=sys.stderr)
        return 2
    finally:
        conn.close()
    print("OK: user_role enum includes VENDOR and SUPERADMIN (any missing values were added).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
