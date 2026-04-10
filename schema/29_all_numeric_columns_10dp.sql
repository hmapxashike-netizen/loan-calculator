-- Run on farndacred_db after 28_loan_fee_amounts.sql
--
-- Align ALL numeric columns (except dates) to NUMERIC(22,10).
-- Ensures consistent precision across storage and computation.
--
-- Tables already partially migrated: loan_daily_state (25) has daily/period_to_date at 10dp.
-- This migration extends 10dp to all remaining numeric columns.

-- loan_daily_state: upgrade balance and exposure columns (25 already did daily/period_to_date)
ALTER TABLE loan_daily_state
    ALTER COLUMN principal_not_due           TYPE NUMERIC(22, 10),
    ALTER COLUMN principal_arrears           TYPE NUMERIC(22, 10),
    ALTER COLUMN interest_accrued_balance    TYPE NUMERIC(22, 10),
    ALTER COLUMN interest_arrears_balance    TYPE NUMERIC(22, 10),
    ALTER COLUMN default_interest_balance    TYPE NUMERIC(22, 10),
    ALTER COLUMN penalty_interest_balance    TYPE NUMERIC(22, 10),
    ALTER COLUMN fees_charges_balance        TYPE NUMERIC(22, 10),
    ALTER COLUMN total_exposure              TYPE NUMERIC(22, 10);

ALTER TABLE loan_daily_state
    ALTER COLUMN credits         TYPE NUMERIC(22, 10),
    ALTER COLUMN net_allocation  TYPE NUMERIC(22, 10),
    ALTER COLUMN unallocated     TYPE NUMERIC(22, 10);

-- loans (principal/disbursed_amount from migration 15; facility may not exist)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'facility') THEN
    ALTER TABLE loans ALTER COLUMN facility TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'principal') THEN
    ALTER TABLE loans ALTER COLUMN principal TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'disbursed_amount') THEN
    ALTER TABLE loans ALTER COLUMN disbursed_amount TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'annual_rate') THEN
    ALTER TABLE loans ALTER COLUMN annual_rate TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'monthly_rate') THEN
    ALTER TABLE loans ALTER COLUMN monthly_rate TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'drawdown_fee') THEN
    ALTER TABLE loans ALTER COLUMN drawdown_fee TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'arrangement_fee') THEN
    ALTER TABLE loans ALTER COLUMN arrangement_fee TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'admin_fee') THEN
    ALTER TABLE loans ALTER COLUMN admin_fee TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'installment') THEN
    ALTER TABLE loans ALTER COLUMN installment TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'total_payment') THEN
    ALTER TABLE loans ALTER COLUMN total_payment TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'admin_fee_amount') THEN
    ALTER TABLE loans ALTER COLUMN admin_fee_amount TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'drawdown_fee_amount') THEN
    ALTER TABLE loans ALTER COLUMN drawdown_fee_amount TYPE NUMERIC(22, 10);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'arrangement_fee_amount') THEN
    ALTER TABLE loans ALTER COLUMN arrangement_fee_amount TYPE NUMERIC(22, 10);
  END IF;
END $$;

-- schedule_lines
ALTER TABLE schedule_lines
    ALTER COLUMN payment             TYPE NUMERIC(22, 10),
    ALTER COLUMN principal           TYPE NUMERIC(22, 10),
    ALTER COLUMN interest            TYPE NUMERIC(22, 10),
    ALTER COLUMN principal_balance   TYPE NUMERIC(22, 10),
    ALTER COLUMN total_outstanding   TYPE NUMERIC(22, 10);

-- loan_repayments
ALTER TABLE loan_repayments
    ALTER COLUMN amount TYPE NUMERIC(22, 10);

-- loan_repayment_allocation
ALTER TABLE loan_repayment_allocation
    ALTER COLUMN alloc_principal_not_due   TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_principal_arrears  TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_interest_accrued   TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_interest_arrears   TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_default_interest   TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_penalty_interest   TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_fees_charges       TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_principal_total    TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_interest_total     TYPE NUMERIC(22, 10),
    ALTER COLUMN alloc_fees_total         TYPE NUMERIC(22, 10);

-- unapplied_funds
ALTER TABLE unapplied_funds
    ALTER COLUMN amount TYPE NUMERIC(22, 10);

-- loan_modifications
ALTER TABLE loan_modifications
    ALTER COLUMN new_annual_rate TYPE NUMERIC(22, 10),
    ALTER COLUMN new_principal   TYPE NUMERIC(22, 10);

-- loan_recasts
ALTER TABLE loan_recasts
    ALTER COLUMN new_installment TYPE NUMERIC(22, 10);

-- agents (if table exists)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'agents') THEN
    ALTER TABLE agents ALTER COLUMN commission_rate_pct TYPE NUMERIC(22, 10);
  END IF;
END $$;

-- customers (if shareholding_pct exists)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'customers' AND column_name = 'shareholding_pct') THEN
    ALTER TABLE customers ALTER COLUMN shareholding_pct TYPE NUMERIC(22, 10);
  END IF;
END $$;
