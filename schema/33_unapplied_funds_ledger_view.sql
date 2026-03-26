-- View: unapplied_funds_ledger
--
-- Purpose:
--   Reconstruct the unapplied funds ledger (credits, reversals, liquidations)
--   from persisted tables, with a running unapplied balance per loan.
--
--   This mirrors the logic in scripts/export_loan_tables.py
--   (_export_unapplied_funds_ledger), so that statements and exports see
--   exactly the same unapplied_running_balance definition.
--
-- Columns:
--   repayment_id              -- source receipt ID (or source_repayment_id for liquidations)
--   repayment_key             -- display key (handles reversals like REV-123)
--   loan_id                   -- loan
--   value_date                -- value date of the movement
--   entry_kind                -- 'credit' | 'reversal' | 'liquidation'
--   liquidation_repayment_id  -- repayment_id of the system allocation (for liquidations)
--   unapplied_delta           -- signed movement (credits +, liquidations/reversals -)
--   alloc_prin_arrears        -- liquidation bucket breakdown: principal arrears
--   alloc_int_arrears         -- liquidation bucket breakdown: interest arrears
--   alloc_penalty_int         -- liquidation bucket breakdown: penalty interest
--   alloc_default_int         -- liquidation bucket breakdown: default interest
--   alloc_fees_charges        -- liquidation bucket breakdown: fees/charges
--   unapplied_running_balance -- SUM(unapplied_delta) over loan/date/order
--
CREATE OR REPLACE VIEW unapplied_funds_ledger AS
WITH alloc_receipts AS (
    SELECT
        lr.id AS repayment_id,
        lr.loan_id,
        (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
        COALESCE(SUM(lra.alloc_principal_total), 0) AS alloc_prin_total,
        COALESCE(SUM(lra.alloc_interest_total), 0) AS alloc_int_total,
        COALESCE(SUM(lra.alloc_fees_total), 0) AS alloc_fees_total
    FROM loan_repayments lr
    LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
    WHERE NOT (
        COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
        OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
        OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
    )
    GROUP BY lr.id, lr.loan_id, lr.value_date, lr.payment_date, lr.amount, lr.status, lr.original_repayment_id
),
credits_and_reversals AS (
    -- Single source for credits/reversals: unapplied derived from receipt allocation view.
    SELECT
        ar.repayment_id,
        CASE
            WHEN lr.status = 'reversed' AND lr.original_repayment_id IS NOT NULL
                THEN 'REV-' || lr.original_repayment_id::text
            ELSE ar.repayment_id::text
        END AS repayment_key,
        ar.loan_id,
        ar.value_date,
        CASE
            WHEN (lr.amount - (ar.alloc_prin_total + ar.alloc_int_total + ar.alloc_fees_total)) >= 0
                THEN 'credit' ELSE 'reversal'
        END AS entry_kind,
        NULL::integer AS liquidation_repayment_id,
        (lr.amount - (ar.alloc_prin_total + ar.alloc_int_total + ar.alloc_fees_total)) AS unapplied_delta,
        0::numeric AS alloc_prin_arrears,
        0::numeric AS alloc_int_arrears,
        0::numeric AS alloc_penalty_int,
        0::numeric AS alloc_default_int,
        0::numeric AS alloc_fees_charges
    FROM alloc_receipts ar
    JOIN loan_repayments lr ON lr.id = ar.repayment_id
    WHERE ABS(lr.amount - (ar.alloc_prin_total + ar.alloc_int_total + ar.alloc_fees_total)) > 1e-9
),
liquidations AS (
    -- Liquidations come only from unapplied_funds_allocation (always negative unapplied delta).
    -- unallocation_parent_reversed undoes a prior liquidation when the parent receipt is reversed.
    -- Omit these from the view when a reversal repayment row exists for source_repayment_id:
    -- the reversal receipt already appears in credits_and_reversals with mirrored unallocated,
    -- and unioning both double-counts +unapplied_delta (often dated on the liquidation's value_date).
    SELECT
        lr.id AS repayment_id,
        CASE
            WHEN lra.event_type = 'unapplied_funds_allocation' THEN lr.id::text
            ELSE 'REV-' || lr.original_repayment_id::text
        END AS repayment_key,
        lr.loan_id AS loan_id,
        (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
        CASE
            WHEN lra.event_type = 'unapplied_funds_allocation' THEN 'liquidation'
            ELSE 'reversal'
        END AS entry_kind,
        NULL::integer AS liquidation_repayment_id,
        -SUM(
            COALESCE(lra.alloc_principal_total, 0)
            + COALESCE(lra.alloc_interest_total, 0)
            + COALESCE(lra.alloc_fees_total, 0)
        ) AS unapplied_delta,
        SUM(COALESCE(lra.alloc_principal_arrears, 0)) AS alloc_prin_arrears,
        SUM(COALESCE(lra.alloc_interest_arrears, 0)) AS alloc_int_arrears,
        SUM(COALESCE(lra.alloc_penalty_interest, 0)) AS alloc_penalty_int,
        SUM(COALESCE(lra.alloc_default_interest, 0)) AS alloc_default_int,
        SUM(COALESCE(lra.alloc_fees_charges, 0)) AS alloc_fees_charges
    FROM loan_repayment_allocation lra
    JOIN loan_repayments lr ON lr.id = lra.repayment_id
    WHERE lra.event_type IN ('unapplied_funds_allocation', 'unallocation_parent_reversed')
      AND lra.source_repayment_id IS NOT NULL
    GROUP BY
        lr.id,
        lr.original_repayment_id,
        lr.loan_id,
        (COALESCE(lr.value_date, lr.payment_date))::date,
        lra.event_type
),
ledger AS (
    SELECT * FROM credits_and_reversals
    UNION ALL
    SELECT * FROM liquidations
)
SELECT
    l.repayment_id,
    l.repayment_key,
    l.loan_id,
    l.value_date,
    l.entry_kind,
    l.liquidation_repayment_id,
    l.unapplied_delta,
    l.alloc_prin_arrears,
    l.alloc_int_arrears,
    l.alloc_penalty_int,
    l.alloc_default_int,
    l.alloc_fees_charges,
    SUM(l.unapplied_delta) OVER (
        PARTITION BY l.loan_id
        ORDER BY l.value_date, l.repayment_id, l.entry_kind
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS unapplied_running_balance
FROM ledger l
ORDER BY l.value_date, l.repayment_id, l.entry_kind;

