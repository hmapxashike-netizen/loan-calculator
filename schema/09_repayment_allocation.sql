-- Stores waterfall-driven allocation of each receipt across loan buckets.
-- Run on farndacred_db after 04_loan_repayments.sql, 05_teller_repayments.sql, and 07_loan_daily_state.sql.

CREATE TABLE IF NOT EXISTS loan_repayment_allocation (
    id                          SERIAL PRIMARY KEY,
    repayment_id                INTEGER NOT NULL REFERENCES loan_repayments(id) ON DELETE CASCADE,

    -- Per-bucket allocations (waterfall items)
    alloc_principal_not_due     NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alloc_principal_arrears     NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alloc_interest_accrued      NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alloc_interest_arrears      NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alloc_default_interest      NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alloc_penalty_interest      NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alloc_fees_charges          NUMERIC(18, 2) NOT NULL DEFAULT 0,

    -- Aggregated allocations
    alloc_principal_total       NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alloc_interest_total        NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alloc_fees_total            NUMERIC(18, 2) NOT NULL DEFAULT 0,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_repayment_allocation_unique
    ON loan_repayment_allocation(repayment_id);

