-- Optional loan linkage on journal headers (posting policy unchanged).
-- Heavy index + backfill are split: see schema/82_journal_entries_loan_id_index.sql
-- and scripts/backfill_journal_entries_loan_id.py for large tables (~1M+ rows).

ALTER TABLE journal_entries
    ADD COLUMN IF NOT EXISTS loan_id INTEGER REFERENCES loans(id) ON DELETE SET NULL;

COMMENT ON COLUMN journal_entries.loan_id IS 'Loan this posting relates to, when applicable; NULL for non-loan journals.';
