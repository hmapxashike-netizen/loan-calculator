-- Teller: add columns for customer/company references and value/system dates.
-- customer_reference: appears on customer loan statement
-- company_reference: appears in company general ledger
-- value_date: effective date of payment (default = payment_date)
-- system_date: when payment was captured (default = now)

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loan_repayments' AND column_name = 'customer_reference') THEN
    ALTER TABLE loan_repayments ADD COLUMN customer_reference VARCHAR(255);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loan_repayments' AND column_name = 'company_reference') THEN
    ALTER TABLE loan_repayments ADD COLUMN company_reference VARCHAR(255);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loan_repayments' AND column_name = 'value_date') THEN
    ALTER TABLE loan_repayments ADD COLUMN value_date DATE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loan_repayments' AND column_name = 'system_date') THEN
    ALTER TABLE loan_repayments ADD COLUMN system_date TIMESTAMPTZ DEFAULT NOW();
  END IF;
END $$;

COMMENT ON COLUMN loan_repayments.customer_reference IS 'Reference shown on customer loan statement';
COMMENT ON COLUMN loan_repayments.company_reference IS 'Reference for company general ledger';
