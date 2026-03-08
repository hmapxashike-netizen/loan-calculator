-- Run this script connected to database: lms_db
-- Adds period-to-date columns so statements can read pre-aggregated penalty/default/regular
-- interest for the current schedule period (no summing over days).

ALTER TABLE loan_daily_state
    ADD COLUMN IF NOT EXISTS regular_interest_period_to_date   NUMERIC(18, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS penalty_interest_period_to_date    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS default_interest_period_to_date    NUMERIC(18, 2) NOT NULL DEFAULT 0;

COMMENT ON COLUMN loan_daily_state.regular_interest_period_to_date IS 'Sum of regular_interest_daily from current period start to as_of_date (reset on due date).';
COMMENT ON COLUMN loan_daily_state.penalty_interest_period_to_date  IS 'Sum of penalty_interest_daily from current period start to as_of_date (reset on due date).';
COMMENT ON COLUMN loan_daily_state.default_interest_period_to_date IS 'Sum of default_interest_daily from current period start to as_of_date (reset on due date).';
