-- Loan purposes: configurable list for loan capture; optional FK on loans.
-- Optional idempotent defaults: edit loan_purpose_seed.py then run scripts/seed_loan_purposes.py
-- (System config UI reads this table only — no second copy in JSON config.)

CREATE TABLE IF NOT EXISTS loan_purposes (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_loan_purposes_name_lower
    ON loan_purposes (LOWER(TRIM(name)));

CREATE INDEX IF NOT EXISTS idx_loan_purposes_active_sort
    ON loan_purposes (is_active, sort_order, id);

ALTER TABLE loans
    ADD COLUMN IF NOT EXISTS loan_purpose_id INTEGER
    REFERENCES loan_purposes (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_loans_loan_purpose ON loans (loan_purpose_id);

COMMENT ON TABLE loan_purposes IS 'User-defined loan purpose labels for capture and reporting.';
COMMENT ON COLUMN loans.loan_purpose_id IS 'Optional FK to loan_purposes; set at loan capture / approval.';
