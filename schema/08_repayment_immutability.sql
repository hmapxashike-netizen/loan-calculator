-- Strengthen immutability and value-date semantics for loan_repayments.
-- Run on farndacred_db after 04_loan_repayments.sql and 05_teller_repayments.sql.

-- 1) Add optional link to original repayment (for reversals)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'loan_repayments' AND column_name = 'original_repayment_id'
  ) THEN
    ALTER TABLE loan_repayments
      ADD COLUMN original_repayment_id INTEGER REFERENCES loan_repayments(id) ON DELETE RESTRICT;
  END IF;
END $$;

-- 2) Optional: index on value_date for reporting and allocation
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'public' AND tablename = 'loan_repayments' AND indexname = 'idx_loan_repayments_value_date'
  ) THEN
    CREATE INDEX idx_loan_repayments_value_date
      ON loan_repayments(value_date);
  END IF;
END $$;

