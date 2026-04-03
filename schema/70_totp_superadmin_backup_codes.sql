-- TOTP backup codes for SUPERADMIN / VENDOR recovery (hashed at rest).
-- SUPERADMIN role: full platform admin (separate from tenant ADMIN).
--
-- Do NOT run this file with Python (python schema/70_...sql) — that will SyntaxError.
-- Apply with psql, for example:
--   psql "postgresql://USER:PASS@HOST:5432/DBNAME" -v ON_ERROR_STOP=1 -f schema/70_totp_superadmin_backup_codes.sql
-- Or from repo root (uses config.get_database_url):
--   python scripts/apply_schema_70_totp.py
--
-- Note: the app also creates public.user_totp_backup_codes on first TOTP use if missing.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON e.enumtypid = t.oid
    WHERE t.typname = 'user_role'
      AND e.enumlabel = 'SUPERADMIN'
  ) THEN
    ALTER TYPE user_role ADD VALUE 'SUPERADMIN';
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS user_totp_backup_codes (
  id           BIGSERIAL PRIMARY KEY,
  user_id      UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  code_hash    TEXT NOT NULL,
  used_at      TIMESTAMPTZ NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_totp_backup_user_unused
  ON user_totp_backup_codes (user_id)
  WHERE used_at IS NULL;

COMMENT ON TABLE user_totp_backup_codes IS 'One-time TOTP recovery codes (bcrypt hashes); SUPERADMIN/VENDOR password reset.';

-- First SUPERADMIN (UI cannot assign VENDOR/SUPERADMIN without one): e.g.
--   UPDATE users SET role = 'SUPERADMIN'::user_role WHERE lower(email) = lower('you@example.com');
