-- Engine-only snapshot for incremental EOD (accrual Loan state at end of as_of_date).
-- Merged loan_daily_state bucket columns are not sufficient to resume the engine.

ALTER TABLE loan_daily_state
    ADD COLUMN IF NOT EXISTS engine_resume JSONB NULL;

-- Table comment (replaces any prior COMMENT ON TABLE for this relation).
COMMENT ON TABLE loan_daily_state IS
    'Daily per-loan bucket balances and EOD metrics per as_of_date. Column engine_resume stores a versioned accrual-engine snapshot for incremental EOD.';

COMMENT ON COLUMN loan_daily_state.engine_resume IS
    'Versioned JSON snapshot of eod.loan_daily_engine.Loan accrual state at end of as_of_date; used to skip replay from disbursement when valid.';
