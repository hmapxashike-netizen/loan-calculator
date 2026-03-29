-- Manual cash/bank GL selection (aligned with journal-style account pick):
-- loans.cash_gl_account_id     — chosen at loan capture (default cash for loan-level postings).
-- loan_repayments.source_cash_gl_account_id — chosen per receipt; reversals reuse the original receipt's value.

ALTER TABLE loans
    ADD COLUMN IF NOT EXISTS cash_gl_account_id UUID REFERENCES accounts(id);

CREATE INDEX IF NOT EXISTS idx_loans_cash_gl_account ON loans (cash_gl_account_id);

COMMENT ON COLUMN loans.cash_gl_account_id IS
    'Posting (leaf) GL account for operating cash for this loan (loan capture). Used when cash_operating is not overridden per receipt.';

ALTER TABLE loan_repayments
    ADD COLUMN IF NOT EXISTS source_cash_gl_account_id UUID REFERENCES accounts(id);

CREATE INDEX IF NOT EXISTS idx_loan_repayments_source_cash_gl
    ON loan_repayments (source_cash_gl_account_id);

COMMENT ON COLUMN loan_repayments.source_cash_gl_account_id IS
    'GL account that received this receipt (teller/batch). Reversal journals reuse the original receipt id to read this column.';

-- Backfill loan cash from legacy disbursement_bank_options when present.
UPDATE loans l
SET cash_gl_account_id = d.gl_account_id
FROM disbursement_bank_options d
WHERE l.disbursement_bank_option_id = d.id
  AND l.cash_gl_account_id IS NULL;
