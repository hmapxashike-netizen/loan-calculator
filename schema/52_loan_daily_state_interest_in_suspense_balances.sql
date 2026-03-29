-- Persisted interest-in-suspense balances for provision reporting (EOD roll-forward).
-- Regular: accrues to suspense only when loans.interest_in_suspense; reduced by alloc_interest_accrued.
-- Penalty / default: mirror economic balances (same accrual − allocation as penalty_interest_balance / default_interest_balance).
-- total_interest_in_suspense_balance = sum of the three (stored for efficient reads).

ALTER TABLE loan_daily_state
    ADD COLUMN IF NOT EXISTS regular_interest_in_suspense_balance NUMERIC(22, 10) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS penalty_interest_in_suspense_balance NUMERIC(22, 10) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS default_interest_in_suspense_balance NUMERIC(22, 10) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_interest_in_suspense_balance NUMERIC(22, 10) NOT NULL DEFAULT 0;

COMMENT ON COLUMN loan_daily_state.regular_interest_in_suspense_balance IS
    'When interest_in_suspense: prior + regular_interest_daily − alloc_interest_accrued (non-negative).';
COMMENT ON COLUMN loan_daily_state.penalty_interest_in_suspense_balance IS
    'Tracks penalty interest in suspense lane; aligned with penalty_interest_balance roll-forward.';
COMMENT ON COLUMN loan_daily_state.default_interest_in_suspense_balance IS
    'Tracks default interest in suspense lane; aligned with default_interest_balance roll-forward.';
COMMENT ON COLUMN loan_daily_state.total_interest_in_suspense_balance IS
    'Sum of regular + penalty + default interest in suspense balances (provision numerator input).';

UPDATE loan_daily_state
SET
    penalty_interest_in_suspense_balance = COALESCE(penalty_interest_balance, 0),
    default_interest_in_suspense_balance = COALESCE(default_interest_balance, 0),
    regular_interest_in_suspense_balance = COALESCE(regular_interest_in_suspense_balance, 0),
    total_interest_in_suspense_balance =
        COALESCE(regular_interest_in_suspense_balance, 0)
        + COALESCE(penalty_interest_balance, 0)
        + COALESCE(default_interest_balance, 0)
WHERE total_interest_in_suspense_balance = 0
   OR total_interest_in_suspense_balance IS NULL;
