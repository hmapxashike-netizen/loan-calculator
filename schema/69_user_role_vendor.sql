-- Add platform vendor role (subscription vendor tools, not tenant organisation admin).
-- Apply with autocommit (e.g. psql -f schema/69_user_role_vendor.sql "$DATABASE_URL").
-- Or: python scripts/ensure_user_role_enums.py  (adds VENDOR + SUPERADMIN if missing)
-- PostgreSQL: ALTER TYPE ... ADD VALUE cannot run inside a transaction block on some versions.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON e.enumtypid = t.oid
    WHERE t.typname = 'user_role'
      AND e.enumlabel = 'VENDOR'
  ) THEN
    ALTER TYPE user_role ADD VALUE 'VENDOR';
  END IF;
END
$$;
