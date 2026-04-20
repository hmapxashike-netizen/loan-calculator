-- Loan applications (prospect / pipeline) and optional commission accrual ledger.
-- Run connected to tenant DB after base loans + agents + loan_approval_drafts exist.
-- Idempotent helpers also live in loan_management.schema_ddl._ensure_loan_applications_schema.

CREATE TABLE IF NOT EXISTS loan_application_ref_sequences (
    prefix      VARCHAR(8) PRIMARY KEY,
    next_num    INTEGER NOT NULL DEFAULT 1
);

COMMENT ON TABLE loan_application_ref_sequences IS 'Per-prefix monotonic sequence for loan application reference_number (e.g. MPA, NON).';

CREATE TABLE IF NOT EXISTS loan_applications (
    id                      BIGSERIAL PRIMARY KEY,
    reference_number        VARCHAR(32) NOT NULL UNIQUE,
    customer_id             INTEGER REFERENCES customers(id) ON DELETE SET NULL,
    agent_id                INTEGER REFERENCES agents(id) ON DELETE SET NULL,
    national_id             TEXT,
    requested_principal     NUMERIC(22, 10),
    product_code            VARCHAR(64),
    status                  VARCHAR(64) NOT NULL DEFAULT 'PROSPECT',
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    loan_id                 INTEGER UNIQUE REFERENCES loans(id) ON DELETE SET NULL,
    superseded_at           TIMESTAMPTZ,
    superseded_by_id        BIGINT REFERENCES loan_applications(id) ON DELETE SET NULL,
    deleted_at              TIMESTAMPTZ,
    deleted_by              VARCHAR(128),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by              VARCHAR(128)
);

CREATE INDEX IF NOT EXISTS idx_loan_applications_customer ON loan_applications(customer_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_loan_applications_agent ON loan_applications(agent_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_loan_applications_status ON loan_applications(status, updated_at) WHERE deleted_at IS NULL;

COMMENT ON TABLE loan_applications IS 'Prospect / loan application before booking; supersede and soft-delete supported.';

ALTER TABLE loans
    ADD COLUMN IF NOT EXISTS source_application_id BIGINT REFERENCES loan_applications(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_loans_source_application_id ON loans(source_application_id);

COMMENT ON COLUMN loans.source_application_id IS 'Originating loan_applications.id when booked via link_loan_to_application.';

ALTER TABLE loan_approval_drafts
    ADD COLUMN IF NOT EXISTS application_id BIGINT REFERENCES loan_applications(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS ix_loan_approval_drafts_application_id ON loan_approval_drafts(application_id);

COMMENT ON COLUMN loan_approval_drafts.application_id IS 'loan_applications row when this draft was created from send-for-approval.';

CREATE TABLE IF NOT EXISTS agent_commission_accruals (
    id                          BIGSERIAL PRIMARY KEY,
    loan_id                     INTEGER NOT NULL UNIQUE REFERENCES loans(id) ON DELETE CASCADE,
    application_id              BIGINT REFERENCES loan_applications(id) ON DELETE SET NULL,
    agent_id                    INTEGER REFERENCES agents(id) ON DELETE SET NULL,
    principal_at_booking        NUMERIC(22, 10) NOT NULL,
    commission_rate_pct_snapshot NUMERIC(22, 10),
    commission_amount           NUMERIC(22, 10) NOT NULL,
    accrual_status              VARCHAR(32) NOT NULL DEFAULT 'PENDING_POST',
    journal_entry_id            UUID,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_commission_accruals_application ON agent_commission_accruals(application_id);

COMMENT ON TABLE agent_commission_accruals IS 'Idempotent per-loan commission accrual stub (GL wiring optional).';
