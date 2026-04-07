-- Stable external key for migration / batch linking (independent of SERIAL id).

ALTER TABLE customers
    ADD COLUMN IF NOT EXISTS migration_ref TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_migration_ref_unique
    ON customers (migration_ref)
    WHERE migration_ref IS NOT NULL AND btrim(migration_ref) <> '';
