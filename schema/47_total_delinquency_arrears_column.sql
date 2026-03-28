-- Add derived arrears total to loan_daily_state for reporting/export.
--
-- total_delinquency_arrears is defined as:
--   principal_arrears
-- + interest_arrears_balance
-- + default_interest_balance
-- + penalty_interest_balance
-- + fees_charges_balance
--
-- Stored at NUMERIC(22,10) precision for consistency with other
-- loan_daily_state monetary columns.

ALTER TABLE loan_daily_state
  ADD COLUMN IF NOT EXISTS total_delinquency_arrears NUMERIC(22, 10) NOT NULL DEFAULT 0;

-- Backfill existing rows (idempotent).
UPDATE loan_daily_state
SET total_delinquency_arrears =
    COALESCE(principal_arrears, 0)
  + COALESCE(interest_arrears_balance, 0)
  + COALESCE(default_interest_balance, 0)
  + COALESCE(penalty_interest_balance, 0)
  + COALESCE(fees_charges_balance, 0);

