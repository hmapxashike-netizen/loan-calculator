-- Single unapplied_funds table (ledger-style) and lineage for reversal cascade.
-- Run after 21_unapplied_funds_ledger.sql.
-- Merges unapplied_funds + unapplied_funds_ledger into one table. Balance = SUM(amount) per loan.

-- Add ledger columns to unapplied_funds
ALTER TABLE unapplied_funds ADD COLUMN IF NOT EXISTS entry_type VARCHAR(16) DEFAULT 'credit';
ALTER TABLE unapplied_funds ADD COLUMN IF NOT EXISTS reference VARCHAR(255);
ALTER TABLE unapplied_funds ADD COLUMN IF NOT EXISTS allocation_repayment_id INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL;
ALTER TABLE unapplied_funds ADD COLUMN IF NOT EXISTS source_repayment_id INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL;
ALTER TABLE unapplied_funds ADD COLUMN IF NOT EXISTS source_unapplied_id INTEGER REFERENCES unapplied_funds(id) ON DELETE SET NULL;

-- Migrate existing unapplied_funds rows to credit entries
UPDATE unapplied_funds
SET entry_type = 'credit', reference = 'Overpayment'
WHERE entry_type IS NULL OR entry_type = '';

-- Migrate ledger debits into unapplied_funds (if ledger exists)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'unapplied_funds_ledger') THEN
    INSERT INTO unapplied_funds (loan_id, amount, value_date, entry_type, reference, allocation_repayment_id, source_repayment_id, currency, created_at)
    SELECT ufl.loan_id, ufl.amount, ufl.value_date, ufl.entry_type, ufl.reference,
           CASE WHEN ufl.reference LIKE '%arrears%' OR ufl.reference LIKE '%EOD%' THEN ufl.repayment_id ELSE NULL END,
           CASE WHEN ufl.reference LIKE 'Reversal%' OR ufl.reference LIKE 'Reallocate%' THEN ufl.repayment_id
                WHEN ufl.unapplied_id IS NOT NULL THEN (SELECT repayment_id FROM unapplied_funds uf WHERE uf.id = ufl.unapplied_id LIMIT 1)
                ELSE NULL END,
           'USD', ufl.created_at
    FROM unapplied_funds_ledger ufl
    WHERE ufl.entry_type = 'debit' AND ufl.amount < 0;
    DROP TABLE unapplied_funds_ledger;
  END IF;
END $$;

-- Add source_repayment_id to allocation for lineage (unapplied_funds_allocation events)
ALTER TABLE loan_repayment_allocation ADD COLUMN IF NOT EXISTS source_repayment_id INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL;

COMMENT ON COLUMN unapplied_funds.entry_type IS 'credit=overpayment; debit=consumption (apply to arrears, recast, reversal)';
COMMENT ON COLUMN unapplied_funds.source_repayment_id IS 'For debits: receipt whose overpayment was consumed. Enables reversal cascade.';
COMMENT ON COLUMN unapplied_funds.source_unapplied_id IS 'For debits: credit row consumed (recast). Prevents double-apply.';
COMMENT ON COLUMN unapplied_funds.allocation_repayment_id IS 'For debits: system repayment (EOD apply-to-arrears) that allocated the funds.';
COMMENT ON COLUMN loan_repayment_allocation.source_repayment_id IS 'For unapplied_funds_allocation: receipt whose overpayment was applied. Enables reversal cascade.';
