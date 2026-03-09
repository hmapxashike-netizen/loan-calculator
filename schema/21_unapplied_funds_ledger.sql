-- Unapplied funds ledger: append-only audit of credits (+) and debits (-).
-- Run after 10_unapplied_funds_and_reamortisation.sql.

CREATE TABLE IF NOT EXISTS unapplied_funds_ledger (
    id              SERIAL PRIMARY KEY,
    loan_id         INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
    amount          NUMERIC(18, 2) NOT NULL,  -- + for credit, - for debit
    value_date      DATE NOT NULL,
    entry_type      VARCHAR(16) NOT NULL,      -- 'credit' | 'debit'
    reference       VARCHAR(255),
    repayment_id    INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL,
    unapplied_id    INTEGER REFERENCES unapplied_funds(id) ON DELETE SET NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_uf_ledger_loan_id ON unapplied_funds_ledger(loan_id);
CREATE INDEX IF NOT EXISTS idx_uf_ledger_value_date ON unapplied_funds_ledger(value_date);
CREATE INDEX IF NOT EXISTS idx_uf_ledger_created ON unapplied_funds_ledger(created_at);

COMMENT ON TABLE unapplied_funds_ledger IS 'Audit of unapplied funds: credits (+), debits (-). Balance = SUM(amount) per loan.';

-- Backfill: credits from pending unapplied_funds (existing rows not yet in ledger)
INSERT INTO unapplied_funds_ledger (loan_id, amount, value_date, entry_type, reference, repayment_id, unapplied_id)
SELECT uf.loan_id, uf.amount, uf.value_date, 'credit', 'Overpayment (backfill)', uf.repayment_id, uf.id
FROM unapplied_funds uf
WHERE uf.status = 'pending' AND uf.amount > 0
  AND NOT EXISTS (SELECT 1 FROM unapplied_funds_ledger ufl WHERE ufl.unapplied_id = uf.id);
