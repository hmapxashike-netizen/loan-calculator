-- Rename loan amount columns to match business terms:
--   facility (total loan amount) -> principal
--   principal (net proceeds / amount required) -> disbursed_amount
-- Run after 02_schema.sql (and any migrations that reference loans.facility/principal).

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'principal')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'disbursed_amount') THEN
    ALTER TABLE loans RENAME COLUMN principal TO disbursed_amount;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'facility') THEN
    ALTER TABLE loans RENAME COLUMN facility TO principal;
  END IF;
END $$;

COMMENT ON COLUMN loans.principal IS 'Total loan amount (facility).';
COMMENT ON COLUMN loans.disbursed_amount IS 'Net proceeds / amount disbursed to borrower.';
