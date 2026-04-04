-- Optional: run if app DDL has not yet created these columns (save_loan / portfolio also ensures them).
-- Boolean flags on loans for cheap portfolio filters (partial indexes); maintained by modification / split / top-up flows.

ALTER TABLE loans ADD COLUMN IF NOT EXISTS remodified_in_place BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE loans ADD COLUMN IF NOT EXISTS originated_from_split BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE loans ADD COLUMN IF NOT EXISTS modification_topup_applied BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_loans_remodified_in_place ON loans (id) WHERE remodified_in_place = TRUE;
CREATE INDEX IF NOT EXISTS idx_loans_originated_from_split ON loans (id) WHERE originated_from_split = TRUE;
CREATE INDEX IF NOT EXISTS idx_loans_modification_topup ON loans (id) WHERE modification_topup_applied = TRUE;

COMMENT ON COLUMN loans.remodified_in_place IS 'True after in-place loan modification (same loan id; see loan_modifications).';
COMMENT ON COLUMN loans.originated_from_split IS 'True for loans created from an approved split modification (replacement legs).';
COMMENT ON COLUMN loans.modification_topup_applied IS 'True when this loan record received a modification top-up drawdown GL as part of approval.';

-- Optional backfill for legacy in-place modifications (not split legs):
-- UPDATE loans l SET remodified_in_place = TRUE
-- WHERE EXISTS (SELECT 1 FROM loan_modifications lm WHERE lm.loan_id = l.id);
