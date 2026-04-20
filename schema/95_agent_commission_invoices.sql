-- Agent commission invoicing + settlement tracking.

CREATE TABLE IF NOT EXISTS agent_commission_invoices (
    id                      BIGSERIAL PRIMARY KEY,
    invoice_number          VARCHAR(64) NOT NULL UNIQUE,
    agent_id                INTEGER NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    period_start            DATE NOT NULL,
    period_end              DATE NOT NULL,
    invoice_date            DATE NOT NULL DEFAULT CURRENT_DATE,
    total_commission        NUMERIC(22, 10) NOT NULL,
    status                  VARCHAR(32) NOT NULL DEFAULT 'ISSUED',
    paid_at                 TIMESTAMPTZ,
    created_by              VARCHAR(128),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payment_journal_entry_id UUID
);

CREATE INDEX IF NOT EXISTS idx_agent_commission_invoices_agent_period
    ON agent_commission_invoices(agent_id, period_start, period_end, status);

ALTER TABLE agent_commission_accruals
    ADD COLUMN IF NOT EXISTS invoice_id BIGINT REFERENCES agent_commission_invoices(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recognised_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recognised_months INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recognised_amount NUMERIC(22, 10) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS payment_journal_entry_id UUID,
    ADD COLUMN IF NOT EXISTS recognition_journal_entry_id UUID;

CREATE INDEX IF NOT EXISTS idx_agent_commission_accruals_invoice
    ON agent_commission_accruals(invoice_id);

CREATE TABLE IF NOT EXISTS agent_commission_invoice_lines (
    id                  BIGSERIAL PRIMARY KEY,
    invoice_id          BIGINT NOT NULL REFERENCES agent_commission_invoices(id) ON DELETE CASCADE,
    accrual_id          BIGINT NOT NULL UNIQUE REFERENCES agent_commission_accruals(id) ON DELETE RESTRICT,
    loan_id             INTEGER NOT NULL REFERENCES loans(id) ON DELETE RESTRICT,
    application_id      BIGINT REFERENCES loan_applications(id) ON DELETE SET NULL,
    commission_amount   NUMERIC(22, 10) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
