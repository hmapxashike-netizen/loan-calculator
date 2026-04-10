-- Run on farndacred_db after 23_loan_daily_state_credits.sql
-- Per-day net allocation (used to reduce a balance) and unallocated (unapplied).
-- For each day: net_allocation + unallocated = credit (total payment flow that day).

ALTER TABLE loan_daily_state
    ADD COLUMN IF NOT EXISTS net_allocation NUMERIC(18, 2),
    ADD COLUMN IF NOT EXISTS unallocated NUMERIC(18, 2);

COMMENT ON COLUMN loan_daily_state.net_allocation IS 'Per-day allocation that reduced balances: SUM(alloc_principal_total+alloc_interest_total+alloc_fees_total) for repayments with value_date=as_of_date. Payment=+, Reversal=-.';
COMMENT ON COLUMN loan_daily_state.unallocated IS 'Per-day amount credited to unapplied (overpayment). net_allocation + unallocated = credit for the day.';
