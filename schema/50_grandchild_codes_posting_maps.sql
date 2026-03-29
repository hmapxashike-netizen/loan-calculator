-- Grandchild account codes (e.g. A100001-01), subaccount resolution modes,
-- disbursement bank options, and product → GL leaf maps.

ALTER TABLE accounts
    ALTER COLUMN code TYPE VARCHAR(32);

ALTER TABLE account_template
    ALTER COLUMN code TYPE VARCHAR(32),
    ALTER COLUMN parent_code TYPE VARCHAR(32);

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS subaccount_resolution VARCHAR(32) NULL;

COMMENT ON COLUMN accounts.subaccount_resolution IS
    'When this tagged account has active children: PRODUCT = resolve leaf via product_gl_subaccount_map + loan product_code; '
    'LOAN_CAPTURE = resolve cash_operating via loan disbursement_bank_option_id; '
    'JOURNAL = automated posting requires payload account_overrides for this tag. NULL = legacy (parent with children is an error).';

CREATE TABLE IF NOT EXISTS disbursement_bank_options (
    id SERIAL PRIMARY KEY,
    label VARCHAR(255) NOT NULL,
    gl_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_disbursement_bank_options_active
    ON disbursement_bank_options (is_active, sort_order);

CREATE TABLE IF NOT EXISTS product_gl_subaccount_map (
    id SERIAL PRIMARY KEY,
    product_code VARCHAR(64) NOT NULL,
    system_tag VARCHAR(100) NOT NULL,
    gl_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (product_code, system_tag)
);

CREATE INDEX IF NOT EXISTS idx_product_gl_map_product ON product_gl_subaccount_map (product_code);

ALTER TABLE loans
    ADD COLUMN IF NOT EXISTS disbursement_bank_option_id INTEGER
        REFERENCES disbursement_bank_options(id);

CREATE INDEX IF NOT EXISTS idx_loans_disbursement_bank_option
    ON loans (disbursement_bank_option_id);
