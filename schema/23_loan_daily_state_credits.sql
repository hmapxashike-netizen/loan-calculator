-- Run on farndacred_db after 07_loan_daily_state.sql
-- Credits = cumulative allocation affecting balances (payment +, reversal -).
-- Only tracks amounts that reduce buckets (excludes unapplied).

ALTER TABLE loan_daily_state
    ADD COLUMN IF NOT EXISTS credits NUMERIC(18, 2);

COMMENT ON COLUMN loan_daily_state.credits IS 'Cumulative allocation affecting balances: SUM(alloc_principal_total+alloc_interest_total+alloc_fees_total) for repayments with value_date<=as_of_date. Payment=+, Reversal=-. Excludes unapplied.';
