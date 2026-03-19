-- Immutable financial statement snapshots captured on accounting period close.
-- One header row per statement snapshot, plus many lines.

CREATE TABLE IF NOT EXISTS financial_statement_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    statement_type VARCHAR(40) NOT NULL,
    period_type VARCHAR(10) NOT NULL,
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    source_ledger_cutoff_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'FINAL',
    is_final BOOLEAN NOT NULL DEFAULT TRUE,
    supersedes_snapshot_id UUID NULL REFERENCES financial_statement_snapshots(id),
    calculation_version VARCHAR(100) NOT NULL DEFAULT 'v1',
    generated_by VARCHAR(100) NOT NULL DEFAULT 'system',
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_statement_type
        CHECK (statement_type IN ('TRIAL_BALANCE', 'PROFIT_AND_LOSS', 'BALANCE_SHEET', 'CASH_FLOW', 'CHANGES_IN_EQUITY')),
    CONSTRAINT chk_period_type
        CHECK (period_type IN ('MONTH', 'YEAR')),
    CONSTRAINT chk_snapshot_dates
        CHECK (period_start_date <= period_end_date)
);

CREATE INDEX IF NOT EXISTS ix_stmt_snapshots_lookup
    ON financial_statement_snapshots (statement_type, period_type, period_end_date, status, generated_at DESC);

CREATE TABLE IF NOT EXISTS financial_statement_snapshot_lines (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id UUID NOT NULL REFERENCES financial_statement_snapshots(id) ON DELETE CASCADE,
    line_order INTEGER NOT NULL,
    line_code VARCHAR(40) NULL,
    line_name VARCHAR(255) NOT NULL,
    line_category VARCHAR(50) NULL,
    debit NUMERIC(28, 10) NOT NULL DEFAULT 0,
    credit NUMERIC(28, 10) NOT NULL DEFAULT 0,
    amount NUMERIC(28, 10) NOT NULL DEFAULT 0,
    currency_code VARCHAR(8) NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_snapshot_line_order
    ON financial_statement_snapshot_lines (snapshot_id, line_order);

CREATE INDEX IF NOT EXISTS ix_snapshot_lines_snapshot
    ON financial_statement_snapshot_lines (snapshot_id);
