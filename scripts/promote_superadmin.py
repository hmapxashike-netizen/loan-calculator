"""One-off: promote a user to SUPERADMIN by email. Usage:
    python scripts/promote_superadmin.py hmapxashike@gmail.com
Ensures enum label SUPERADMIN exists, then updates the user. For backup-code tables also apply
schema/70_totp_superadmin_backup_codes.sql (or rely on this script for enum + role only).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2

from config import get_database_url


def _ensure_superadmin_enum(cur) -> None:
    """Add user_role enum value SUPERADMIN if missing (idempotent)."""
    cur.execute(
        """
        DO $body$
        BEGIN
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
    )


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/promote_superadmin.py <email>", file=sys.stderr)
        return 1
    email = sys.argv[1].strip()
    conn = psycopg2.connect(get_database_url())
    try:
        conn.autocommit = True
        cur = conn.cursor()
        try:
            _ensure_superadmin_enum(cur)
        except Exception as e:
            print(
                "Could not ensure SUPERADMIN enum (run schema/70_totp_superadmin_backup_codes.sql):",
                e,
                file=sys.stderr,
            )
            return 3
        cur.execute(
            """
            UPDATE users
            SET role = 'SUPERADMIN'
            WHERE lower(email::text) = lower(%s)
            """,
            (email,),
        )
        n = cur.rowcount
        cur.close()
        if n:
            print(f"Updated {n} row(s): {email!r} is now SUPERADMIN.")
            return 0
        cur2 = conn.cursor()
        cur2.execute(
            "SELECT id, email, role FROM users WHERE lower(email::text) = lower(%s)",
            (email,),
        )
        row = cur2.fetchone()
        cur2.close()
        if not row:
            print(f"No user found with email {email!r}.")
        else:
            print(f"User exists but UPDATE matched 0 rows (unexpected): {row!r}")
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
