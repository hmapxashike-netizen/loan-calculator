-- Migration 31: Enforce one allocation row per repayment_id.
--
-- Policy: loan_repayment_allocation has exactly one row per repayment_id.
-- Reallocate overrides the existing row instead of appending correction pairs.
--
-- loan_repayments: id is already unique (PRIMARY KEY). Each row = one receipt.
-- unapplied_funds: ledger-style; multiple rows per repayment_id are expected (credits, debits).

-- 1. Consolidate duplicate allocation rows into one per repayment_id.
--    Keep the row with the highest id (most recent) and net its values with all others.
--    For repayment_ids with only one row, no change.
DO $$
DECLARE
    r RECORD;
    alloc_id INT;
    net_prin_not_due NUMERIC := 0;
    net_prin_arrears NUMERIC := 0;
    net_int_accrued NUMERIC := 0;
    net_int_arrears NUMERIC := 0;
    net_def_int NUMERIC := 0;
    net_pen_int NUMERIC := 0;
    net_fees NUMERIC := 0;
    net_unallocated NUMERIC := 0;
BEGIN
    FOR r IN (
        SELECT repayment_id, COUNT(*) AS cnt
        FROM loan_repayment_allocation
        GROUP BY repayment_id
        HAVING COUNT(*) > 1
    )
    LOOP
        -- Sum all allocation rows for this repayment_id
        SELECT
            COALESCE(SUM(alloc_principal_not_due), 0),
            COALESCE(SUM(alloc_principal_arrears), 0),
            COALESCE(SUM(alloc_interest_accrued), 0),
            COALESCE(SUM(alloc_interest_arrears), 0),
            COALESCE(SUM(alloc_default_interest), 0),
            COALESCE(SUM(alloc_penalty_interest), 0),
            COALESCE(SUM(alloc_fees_charges), 0),
            COALESCE(SUM(unallocated), 0)
        INTO net_prin_not_due, net_prin_arrears, net_int_accrued, net_int_arrears,
             net_def_int, net_pen_int, net_fees, net_unallocated
        FROM loan_repayment_allocation
        WHERE repayment_id = r.repayment_id;

        -- Pick the row with the highest id to keep
        SELECT id INTO alloc_id
        FROM loan_repayment_allocation
        WHERE repayment_id = r.repayment_id
        ORDER BY id DESC
        LIMIT 1;

        -- Update the kept row with net values
        UPDATE loan_repayment_allocation
        SET
            alloc_principal_not_due = net_prin_not_due,
            alloc_principal_arrears = net_prin_arrears,
            alloc_interest_accrued = net_int_accrued,
            alloc_interest_arrears = net_int_arrears,
            alloc_default_interest = net_def_int,
            alloc_penalty_interest = net_pen_int,
            alloc_fees_charges = net_fees,
            alloc_principal_total = net_prin_not_due + net_prin_arrears,
            alloc_interest_total = net_int_accrued + net_int_arrears + net_def_int + net_pen_int,
            alloc_fees_total = net_fees,
            unallocated = net_unallocated
        WHERE id = alloc_id;

        -- Delete all other rows for this repayment_id
        DELETE FROM loan_repayment_allocation
        WHERE repayment_id = r.repayment_id AND id != alloc_id;
    END LOOP;
END $$;

-- 2. Re-add unique constraint: one row per repayment_id
DROP INDEX IF EXISTS idx_repayment_alloc_single_per_receipt;
CREATE UNIQUE INDEX idx_repayment_alloc_unique_repayment_id
    ON loan_repayment_allocation(repayment_id);

COMMENT ON INDEX idx_repayment_alloc_unique_repayment_id IS
    'Policy: exactly one allocation row per repayment_id. Reallocate overrides in place.';
