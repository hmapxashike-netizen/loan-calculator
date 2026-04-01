-- Add updated_at to loan_purposes if the table was created before that column existed.
-- Idempotent. App also runs this via _ensure_loan_purposes_schema.

ALTER TABLE loan_purposes
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
