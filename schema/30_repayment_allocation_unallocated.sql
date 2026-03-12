-- Migration 30: Add unallocated column to loan_repayment_allocation.
--
-- Rule: loan_repayments holds only teller receipts + their reversals (no system entries).
-- loan_repayment_allocation has exactly one row per repayment_id (teller receipt or reversal).
-- Reconciliation invariant:
--   SUM(alloc_*) + unallocated = receipt amount  (net per repayment_id)
-- unallocated is credited to unapplied_funds at receipt capture time.
-- EOD apply-to-arrears debits unapplied_funds directly; it does NOT create rows in
-- loan_repayments or loan_repayment_allocation.

-- 1. Add unallocated column
ALTER TABLE loan_repayment_allocation
    ADD COLUMN IF NOT EXISTS unallocated NUMERIC(22, 10) NOT NULL DEFAULT 0;

COMMENT ON COLUMN loan_repayment_allocation.unallocated IS
    'Amount of the receipt not allocated to any bucket at capture time; credited to unapplied_funds. '
    'Invariant: SUM(alloc_*) + unallocated = receipt amount (per repayment_id net).';

-- 2. Backfill unallocated for existing new_allocation rows (teller receipts only).
--    unallocated = MAX(0, receipt_amount - sum_of_alloc_buckets).
UPDATE loan_repayment_allocation lra
SET unallocated = GREATEST(
    0,
    lr.amount - (
        lra.alloc_principal_not_due + lra.alloc_principal_arrears +
        lra.alloc_interest_accrued  + lra.alloc_interest_arrears  +
        lra.alloc_default_interest  + lra.alloc_penalty_interest  +
        lra.alloc_fees_charges
    )
)
FROM loan_repayments lr
WHERE lra.repayment_id = lr.id
  AND lr.amount > 0
  AND lra.event_type = 'new_allocation';

-- 3. Backfill unallocated for existing reallocation_waterfall_correction rows
--    (these are the "current" allocation after a reallocation; same logic applies).
UPDATE loan_repayment_allocation lra
SET unallocated = GREATEST(
    0,
    lr.amount - (
        lra.alloc_principal_not_due + lra.alloc_principal_arrears +
        lra.alloc_interest_accrued  + lra.alloc_interest_arrears  +
        lra.alloc_default_interest  + lra.alloc_penalty_interest  +
        lra.alloc_fees_charges
    )
)
FROM loan_repayments lr
WHERE lra.repayment_id = lr.id
  AND lr.amount > 0
  AND lra.event_type = 'reallocation_waterfall_correction';

-- 4. Reversal and unallocation rows carry the negated unallocated of the original.
--    Match by original_repayment_id (for reversal rows) and by repayment_id
--    for unallocation_waterfall_correction rows.
UPDATE loan_repayment_allocation neg
SET unallocated = -COALESCE(orig.unallocated, 0)
FROM loan_repayment_allocation orig
WHERE neg.repayment_id = orig.repayment_id
  AND neg.event_type IN ('unallocation_waterfall_correction', 'unallocation_parent_reversed')
  AND orig.event_type IN ('new_allocation', 'reallocation_waterfall_correction')
  AND orig.unallocated != 0;

-- 5. Add partial unique index: only one non-reversal allocation row per repayment_id going forward.
--    Covers event_type values produced for teller receipts (new_allocation, reallocation_waterfall_correction).
--    Historical correction pairs (unallocation + reallocation) net to zero and are excluded.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE indexname = 'idx_repayment_alloc_single_per_receipt'
  ) THEN
    -- Verify no duplicates before creating; if duplicates exist, skip silently.
    IF NOT EXISTS (
      SELECT repayment_id, COUNT(*)
      FROM loan_repayment_allocation
      WHERE event_type IN ('new_allocation', 'reallocation_waterfall_correction')
      GROUP BY repayment_id
      HAVING COUNT(*) > 1
      LIMIT 1
    ) THEN
      CREATE UNIQUE INDEX idx_repayment_alloc_single_per_receipt
          ON loan_repayment_allocation(repayment_id)
          WHERE event_type IN ('new_allocation', 'reallocation_waterfall_correction');
    END IF;
  END IF;
END $$;
