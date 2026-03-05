-- Run this script connected to database: lms_db
-- Stores daily loan bucket balances for value-dated allocation and reporting.

CREATE TABLE IF NOT EXISTS loan_daily_state (
    id                          SERIAL PRIMARY KEY,
    loan_id                     INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
    as_of_date                  DATE NOT NULL,

    regular_interest_daily      NUMERIC(18, 2) NOT NULL DEFAULT 0,
    principal_not_due           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    principal_arrears           NUMERIC(18, 2) NOT NULL DEFAULT 0,
    interest_accrued_balance    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    interest_arrears_balance    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    default_interest_daily      NUMERIC(18, 2) NOT NULL DEFAULT 0,
    default_interest_balance    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    penalty_interest_daily      NUMERIC(18, 2) NOT NULL DEFAULT 0,
    penalty_interest_balance    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    fees_charges_balance        NUMERIC(18, 2) NOT NULL DEFAULT 0,
    days_overdue                INTEGER NOT NULL DEFAULT 0,

    total_exposure              NUMERIC(18, 2) NOT NULL DEFAULT 0,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_loan_daily_state_loan_date
    ON loan_daily_state(loan_id, as_of_date);

CREATE INDEX IF NOT EXISTS idx_loan_daily_state_date
    ON loan_daily_state(as_of_date);

