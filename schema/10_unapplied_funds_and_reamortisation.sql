-- Unapplied funds (suspense) and reamortisation support.
-- Run after 09_repayment_allocation.sql.

-- Unapplied funds: overpayments held for later application or recast.
CREATE TABLE IF NOT EXISTS unapplied_funds (
    id              SERIAL PRIMARY KEY,
    loan_id         INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
    repayment_id    INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL,
    amount          NUMERIC(18, 2) NOT NULL,
    currency        VARCHAR(8) NOT NULL DEFAULT 'USD',
    value_date      DATE NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_at      TIMESTAMPTZ,
    status          VARCHAR(32) NOT NULL DEFAULT 'pending',  -- pending, applied, reversed
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_unapplied_funds_loan_id ON unapplied_funds(loan_id);
CREATE INDEX IF NOT EXISTS idx_unapplied_funds_status ON unapplied_funds(status);
CREATE INDEX IF NOT EXISTS idx_unapplied_funds_value_date ON unapplied_funds(value_date);

COMMENT ON TABLE unapplied_funds IS 'Overpayments credited to suspense; applied on next due date per waterfall or via loan recast.';

-- Loan modifications: audit of restructures (new terms / new agreement).
CREATE TABLE IF NOT EXISTS loan_modifications (
    id                  SERIAL PRIMARY KEY,
    loan_id             INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
    modification_date   DATE NOT NULL,
    previous_schedule_version INTEGER NOT NULL,
    new_schedule_version INTEGER NOT NULL,
    outstanding_interest_treatment VARCHAR(32) NOT NULL,  -- capitalise, write_off
    new_loan_type       VARCHAR(64),
    new_term            INTEGER,
    new_annual_rate     NUMERIC(12, 6),
    new_principal       NUMERIC(18, 2),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_loan_modifications_loan_id ON loan_modifications(loan_id);

COMMENT ON TABLE loan_modifications IS 'Audit of loan modifications (new terms/agreement).';

-- Loan recasts: audit of re-amortisation (same terms, new instalment).
CREATE TABLE IF NOT EXISTS loan_recasts (
    id                  SERIAL PRIMARY KEY,
    loan_id             INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
    recast_date         DATE NOT NULL,
    previous_schedule_version INTEGER NOT NULL,
    new_schedule_version INTEGER NOT NULL,
    new_installment     NUMERIC(18, 2) NOT NULL,
    trigger_repayment_id INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_loan_recasts_loan_id ON loan_recasts(loan_id);

COMMENT ON TABLE loan_recasts IS 'Audit of loan recasts (prepayment → new instalment to original maturity).';
