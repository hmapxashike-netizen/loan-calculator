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
    status VARCHAR(50) DEFAULT 'POSTED',
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS journal_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entry_id UUID NOT NULL REFERENCES journal_entries(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id),
    debit NUMERIC(28, 10) DEFAULT 0.0,
    credit NUMERIC(28, 10) DEFAULT 0.0,
    memo TEXT
);
