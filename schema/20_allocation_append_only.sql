-- Append-only allocation events. Each event is a new row; no updates.
-- Event types: new_allocation, unallocation_parent_reversed, unallocation_waterfall_correction, reallocation_waterfall_correction
-- Run after 19_allocation_event_type.sql.

-- Drop unique constraint so multiple rows per repayment_id are allowed
DROP INDEX IF EXISTS idx_repayment_allocation_unique;

-- Normalize event_type to new names
UPDATE loan_repayment_allocation SET event_type = 'new_allocation'
  WHERE event_type IS NULL OR event_type IN ('initial_allocation', 'reallocation_upon_reversal', 'reallocation_after_reversal');
UPDATE loan_repayment_allocation SET event_type = 'unallocation_parent_reversed'
  WHERE event_type IN ('unallocating_upon_reversal', 'reversal_of_receipt');

COMMENT ON COLUMN loan_repayment_allocation.event_type IS 'new_allocation=receipt captured; unallocation_parent_reversed=receipt reversed (negative); unallocation_waterfall_correction=first leg undo; reallocation_waterfall_correction=second leg; unapplied_funds_allocation=EOD apply unapplied to arrears';
