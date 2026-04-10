-- Run this script connected to database: farndacred_db
-- One customer can have many loans; one loan can have many schedules (e.g. rescheduling).

-- Extensions (optional, for UUID if you prefer uuid primary keys later)
-- CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Customers
CREATE TABLE IF NOT EXISTS customers (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    email           VARCHAR(255),
    phone           VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE customers IS 'Borrowers; one customer can have many loans';

-- Loans (each belongs to one customer). Field names aligned with app.py loan_record.
CREATE TABLE IF NOT EXISTS loans (
    id                  SERIAL PRIMARY KEY,
    customer_id         INTEGER REFERENCES customers(id) ON DELETE SET NULL,
    loan_type           VARCHAR(64) NOT NULL,  -- consumer_loan, term_loan, bullet_loan, customised_repayments
    facility            NUMERIC(18, 2) NOT NULL,
    principal           NUMERIC(18, 2) NOT NULL,
    term                INTEGER NOT NULL,
    annual_rate         NUMERIC(12, 6),
    monthly_rate        NUMERIC(12, 6),        -- consumer_loan
    drawdown_fee        NUMERIC(8, 6),        -- decimal e.g. 0.025
    arrangement_fee     NUMERIC(8, 6),
    admin_fee           NUMERIC(8, 6),        -- consumer / scheme-based
    disbursement_date   DATE,
    start_date          DATE,
    end_date            DATE,
    maturity_date       DATE,                 -- bullet_loan
    status              VARCHAR(32) NOT NULL DEFAULT 'active',
    installment         NUMERIC(18, 2),
    total_payment       NUMERIC(18, 2),        -- bullet_loan
    grace_type          VARCHAR(64),
    moratorium_months   INTEGER,
    bullet_type         VARCHAR(32),
    scheme              VARCHAR(128),         -- consumer_loan scheme name
    metadata            JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE loans IS 'Loan contracts; app.py will feed data here. One customer can have many loans.';
CREATE INDEX IF NOT EXISTS idx_loans_customer_id ON loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_loans_loan_type ON loans(loan_type);
CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(status);

-- Loan schedules (one loan can have many schedules, e.g. original + reschedules)
CREATE TABLE IF NOT EXISTS loan_schedules (
    id          SERIAL PRIMARY KEY,
    loan_id     INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
    version     INTEGER NOT NULL DEFAULT 1,     -- 1 = original, 2+ = reschedule
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (loan_id, version)
);

COMMENT ON TABLE loan_schedules IS 'Each row is one schedule version for a loan (original or rescheduled).';
CREATE INDEX IF NOT EXISTS idx_loan_schedules_loan_id ON loan_schedules(loan_id);

-- Schedule lines (one row per period). Column names aligned with app.py schedule rows (Period, Date, Payment/Monthly Installment, Principal, Interest, Principal Balance, Total Outstanding).
CREATE TABLE IF NOT EXISTS schedule_lines (
    id                  SERIAL PRIMARY KEY,
    loan_schedule_id     INTEGER NOT NULL REFERENCES loan_schedules(id) ON DELETE CASCADE,
    "Period"             INTEGER NOT NULL,
    "Date"               VARCHAR(32),         -- e.g. 28-Feb-2026
    payment             NUMERIC(18, 2) NOT NULL DEFAULT 0,  -- Payment or Monthly Installment from app
    principal           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    interest            NUMERIC(18, 2) NOT NULL DEFAULT 0,
    principal_balance    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_outstanding   NUMERIC(18, 2) NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE schedule_lines IS 'One row per period in a loan schedule (amortization/reschedule).';
CREATE INDEX IF NOT EXISTS idx_schedule_lines_loan_schedule_id ON schedule_lines(loan_schedule_id);

-- Optional: configuration table for key-value app/config stored in DB later
CREATE TABLE IF NOT EXISTS config (
    key         VARCHAR(128) PRIMARY KEY,
    value       TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE config IS 'Optional key-value configuration (can move major config here later).';
