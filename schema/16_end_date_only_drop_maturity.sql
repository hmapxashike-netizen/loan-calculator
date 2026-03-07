-- Standardise: all loans use end_date. Copy maturity_date into end_date where missing, then drop maturity_date.
-- Run after 02_schema.sql (and any migrations that add maturity_date).

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'maturity_date') THEN
    UPDATE loans SET end_date = maturity_date WHERE end_date IS NULL AND maturity_date IS NOT NULL;
    ALTER TABLE loans DROP COLUMN maturity_date;
  END IF;
END $$;

COMMENT ON COLUMN loans.end_date IS 'Loan end / maturity date (last scheduled date).';
