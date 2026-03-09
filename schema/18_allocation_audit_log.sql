-- Audit trail for allocation events: reversal add-back and system reallocation after reversals.
-- Run after 09_repayment_allocation.sql.

CREATE TABLE IF NOT EXISTS allocation_audit_log (
    id                      SERIAL PRIMARY KEY,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type              VARCHAR(64) NOT NULL,  -- 'reversal_add_back', 'reallocate_after_reversal'
    loan_id                 INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
    as_of_date              DATE NOT NULL,
    repayment_id            INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL,
    original_repayment_id   INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL,
    narration              VARCHAR(255),  -- e.g. 'system auto rev'
    details                 JSONB
);

CREATE INDEX IF NOT EXISTS idx_allocation_audit_loan_date ON allocation_audit_log(loan_id, as_of_date);
CREATE INDEX IF NOT EXISTS idx_allocation_audit_event ON allocation_audit_log(event_type);
