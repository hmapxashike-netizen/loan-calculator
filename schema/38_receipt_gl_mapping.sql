-- 38_receipt_gl_mapping.sql
--
-- Configuration table that tells the system how to translate
-- repayment allocations into accounting events (GL postings).
-- This is intentionally data‑driven so operational users can
-- adjust receipt → GL behaviour without code changes.

CREATE TABLE IF NOT EXISTS receipt_gl_mapping (
    id              SERIAL PRIMARY KEY,

    -- Source of the trigger in the application
    -- e.g. SAVE_RECEIPT, SAVE_REVERSAL, APPLY_UNAPPLIED
    trigger_source  VARCHAR(50) NOT NULL,

    -- Logical allocation bucket / key used by the loan engine
    -- e.g. alloc_principal_arrears, alloc_interest_accrued, etc.
    allocation_key  VARCHAR(100) NOT NULL,

    -- Which accounting event to fire for this allocation bucket
    -- e.g. PAYMENT_PRINCIPAL, PAYMENT_REGULAR_INTEREST, WRITEOFF_RECOVERY
    event_type      VARCHAR(100) NOT NULL,

    -- Which field on the allocation / payload to use as the amount
    -- Usually the same as allocation_key, but can be "amount" or another
    -- computed field where needed.
    amount_source   VARCHAR(100) NOT NULL,

    -- 1 = use amount as‑is; -1 = post as reversal (negated amount)
    amount_sign     SMALLINT NOT NULL DEFAULT 1,

    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    priority        INTEGER NOT NULL DEFAULT 100,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_receipt_gl_mapping_key
    ON receipt_gl_mapping (trigger_source, allocation_key, event_type);

COMMENT ON TABLE receipt_gl_mapping IS
'Maps repayment allocation buckets to accounting events for GL posting.';

