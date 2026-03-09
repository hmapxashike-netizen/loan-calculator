-- Add event_type to loan_repayment_allocation for allocation audit trail.
-- Run after 09_repayment_allocation.sql.
-- Values: initial_allocation (immutable first allocation), reversal_of_receipt (negative allocation when receipt reversed),
--         reallocation_after_reversal (separate entry when receipt reallocated after another reversed)

ALTER TABLE loan_repayment_allocation
ADD COLUMN IF NOT EXISTS event_type VARCHAR(64) DEFAULT 'new_allocation';

COMMENT ON COLUMN loan_repayment_allocation.event_type IS 'new_allocation=receipt captured; unallocation_parent_reversed=receipt reversed; unallocation_waterfall_correction=first leg undo; reallocation_waterfall_correction=second leg';
