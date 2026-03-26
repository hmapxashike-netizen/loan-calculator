CREATE TABLE IF NOT EXISTS account_template (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(7) NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL,
    system_tag VARCHAR(100),
    parent_code VARCHAR(7),
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(code)
);

CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(7) NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL,
    system_tag VARCHAR(100),
    parent_id UUID REFERENCES accounts(id),
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(code)
);

CREATE TABLE IF NOT EXISTS transaction_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    system_tag VARCHAR(100) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS journal_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_date DATE NOT NULL,
    reference VARCHAR(255),
    description TEXT,
    event_id VARCHAR(255),
    event_tag VARCHAR(100),
    entry_type VARCHAR(50) DEFAULT 'EVENT',
    status VARCHAR(50) DEFAULT 'POSTED',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    superseded_at TIMESTAMP WITH TIME ZONE,
    superseded_by_id UUID REFERENCES journal_entries(id),
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Ensure deterministic journals are not duplicated.
-- EOD posts use stable (event_id, event_tag) values.
-- We use a partial unique index so rows with NULL event_id/event_tag are not blocked.
CREATE UNIQUE INDEX IF NOT EXISTS uq_journal_entries_event_id_event_tag
    ON journal_entries(event_id, event_tag)
    WHERE event_id IS NOT NULL AND event_tag IS NOT NULL AND is_active = TRUE;

CREATE TABLE IF NOT EXISTS financial_periods (
    period_key VARCHAR(7) PRIMARY KEY, -- YYYY-MM
    is_closed BOOLEAN NOT NULL DEFAULT FALSE,
    closed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS journal_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_id UUID NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id),
    debit NUMERIC(28, 10) DEFAULT 0.0,
    credit NUMERIC(28, 10) DEFAULT 0.0,
    memo TEXT
);
