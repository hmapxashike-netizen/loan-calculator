-- Partial index for GL loan filter (run with CREATE INDEX CONCURRENTLY — see run_migration_82.py).
-- Cannot be combined in one transaction with other DDL on the same table in some workflows.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_journal_entries_loan_id
    ON journal_entries (loan_id)
    WHERE loan_id IS NOT NULL;
