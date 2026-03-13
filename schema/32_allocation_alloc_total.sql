-- Migration 32: Add alloc_total to loan_repayment_allocation.
--
-- alloc_total = alloc_principal_total + alloc_interest_total + alloc_fees_total
-- Stored for consistency and to avoid recomputation in statements.

ALTER TABLE loan_repayment_allocation
    ADD COLUMN IF NOT EXISTS alloc_total NUMERIC(22, 10) NOT NULL DEFAULT 0;

COMMENT ON COLUMN loan_repayment_allocation.alloc_total IS
    'Sum of alloc_principal_total + alloc_interest_total + alloc_fees_total. Amount applied to loan balances.';

-- Backfill all rows
UPDATE loan_repayment_allocation
SET alloc_total = COALESCE(alloc_principal_total, 0) + COALESCE(alloc_interest_total, 0) + COALESCE(alloc_fees_total, 0);
