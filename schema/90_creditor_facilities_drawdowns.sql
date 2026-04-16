-- Creditor: facilities + drawdowns (split from monolithic creditor_loans).
-- Run once after schema/84. Idempotent via scripts/run_migration_90.py guard.

-- Counterparty status (active / inactive / deleted)
ALTER TABLE creditor_counterparties
    ADD COLUMN IF NOT EXISTS status VARCHAR(16) NOT NULL DEFAULT 'active';

DO $cp_mig$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'creditor_counterparties' AND column_name = 'is_active'
  ) THEN
    UPDATE creditor_counterparties
    SET status = CASE WHEN is_active THEN 'active' ELSE 'inactive' END;
    ALTER TABLE creditor_counterparties DROP COLUMN is_active;
  END IF;
END $cp_mig$;

DO $cp_chk$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public' AND r.relname = 'creditor_counterparties'
      AND c.conname = 'creditor_counterparties_status_chk'
  ) THEN
    ALTER TABLE creditor_counterparties
        ADD CONSTRAINT creditor_counterparties_status_chk
        CHECK (status IN ('active', 'inactive', 'deleted'));
  END IF;
END $cp_chk$;

-- Facilities (one per legacy creditor_loan for migration)
CREATE TABLE IF NOT EXISTS creditor_facilities (
    id                      SERIAL PRIMARY KEY,
    creditor_counterparty_id INTEGER NOT NULL REFERENCES creditor_counterparties(id),
    facility_limit          NUMERIC(22, 10) NOT NULL DEFAULT 0,
    facility_expiry_date    DATE,
    facility_fee_amount     NUMERIC(22, 10) NOT NULL DEFAULT 0,
    status                  VARCHAR(32) NOT NULL DEFAULT 'active',
    metadata                JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    legacy_creditor_loan_id INTEGER UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_creditor_facilities_counterparty ON creditor_facilities(creditor_counterparty_id);

INSERT INTO creditor_facilities (
    creditor_counterparty_id,
    facility_limit,
    facility_expiry_date,
    facility_fee_amount,
    status,
    legacy_creditor_loan_id
)
SELECT
    cl.creditor_counterparty_id,
    COALESCE(NULLIF(cl.facility, 0), cl.principal),
    COALESCE(cl.maturity_date, cl.end_date),
    0::NUMERIC(22, 10),
    'active',
    cl.id
FROM creditor_loans cl
ORDER BY cl.id;

-- Drawdowns (preserve ids from creditor_loans)
CREATE TABLE creditor_drawdowns (
    id                      INTEGER PRIMARY KEY,
    creditor_facility_id    INTEGER NOT NULL REFERENCES creditor_facilities(id),
    creditor_loan_type_code VARCHAR(64) NOT NULL REFERENCES creditor_loan_types(code),
    facility                NUMERIC(22, 10) NOT NULL DEFAULT 0,
    principal               NUMERIC(22, 10) NOT NULL,
    term                    INTEGER,
    annual_rate             NUMERIC(22, 10),
    monthly_rate            NUMERIC(22, 10),
    disbursement_date       DATE,
    start_date              DATE,
    end_date                DATE,
    maturity_date           DATE,
    status                  VARCHAR(32) NOT NULL DEFAULT 'active',
    cash_gl_account_id      UUID,
    drawdown_fee_amount     NUMERIC(22, 10) NOT NULL DEFAULT 0,
    arrangement_fee_amount  NUMERIC(22, 10) NOT NULL DEFAULT 0,
    accrual_mode            VARCHAR(32) NOT NULL DEFAULT 'periodic_schedule',
    penalty_rate_pct        NUMERIC(22, 10),
    metadata                JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT creditor_drawdowns_accrual_mode_chk
        CHECK (accrual_mode IN ('daily_mirror', 'periodic_schedule'))
);

CREATE INDEX idx_creditor_drawdowns_facility ON creditor_drawdowns(creditor_facility_id);
CREATE INDEX idx_creditor_drawdowns_status ON creditor_drawdowns(status);

INSERT INTO creditor_drawdowns (
    id, creditor_facility_id, creditor_loan_type_code, facility, principal, term,
    annual_rate, monthly_rate, disbursement_date, start_date, end_date, maturity_date,
    status, cash_gl_account_id, drawdown_fee_amount, arrangement_fee_amount,
    accrual_mode, penalty_rate_pct, metadata, created_at, updated_at
)
SELECT
    cl.id,
    cf.id,
    cl.creditor_loan_type_code,
    cl.facility,
    cl.principal,
    cl.term,
    cl.annual_rate,
    cl.monthly_rate,
    cl.disbursement_date,
    cl.start_date,
    cl.end_date,
    cl.maturity_date,
    cl.status,
    cl.cash_gl_account_id,
    cl.drawdown_fee_amount,
    cl.arrangement_fee_amount,
    'daily_mirror',
    NULL,
    cl.metadata,
    cl.created_at,
    cl.updated_at
FROM creditor_loans cl
JOIN creditor_facilities cf ON cf.legacy_creditor_loan_id = cl.id;

SELECT setval(
    pg_get_serial_sequence('creditor_facilities', 'id'),
    COALESCE((SELECT MAX(id) FROM creditor_facilities), 1)
);

-- Schedules: repoint FK (unique on drawdown_id+version follows column rename)
ALTER TABLE creditor_loan_schedules DROP CONSTRAINT IF EXISTS creditor_loan_schedules_creditor_loan_id_fkey;
ALTER TABLE creditor_loan_schedules RENAME COLUMN creditor_loan_id TO creditor_drawdown_id;
ALTER TABLE creditor_loan_schedules
    ADD CONSTRAINT creditor_loan_schedules_creditor_drawdown_id_fkey
    FOREIGN KEY (creditor_drawdown_id) REFERENCES creditor_drawdowns(id) ON DELETE CASCADE;

-- Daily state
ALTER TABLE creditor_loan_daily_state DROP CONSTRAINT IF EXISTS creditor_loan_daily_state_creditor_loan_id_fkey;
DROP INDEX IF EXISTS idx_creditor_loan_daily_state_loan_date;
ALTER TABLE creditor_loan_daily_state RENAME COLUMN creditor_loan_id TO creditor_drawdown_id;
ALTER TABLE creditor_loan_daily_state
    ADD CONSTRAINT creditor_loan_daily_state_creditor_drawdown_id_fkey
    FOREIGN KEY (creditor_drawdown_id) REFERENCES creditor_drawdowns(id) ON DELETE CASCADE;
CREATE UNIQUE INDEX idx_creditor_loan_daily_state_drawdown_date
    ON creditor_loan_daily_state(creditor_drawdown_id, as_of_date);

-- Repayments
ALTER TABLE creditor_repayments DROP CONSTRAINT IF EXISTS creditor_repayments_creditor_loan_id_fkey;
ALTER TABLE creditor_repayments RENAME COLUMN creditor_loan_id TO creditor_drawdown_id;
ALTER TABLE creditor_repayments
    ADD CONSTRAINT creditor_repayments_creditor_drawdown_id_fkey
    FOREIGN KEY (creditor_drawdown_id) REFERENCES creditor_drawdowns(id) ON DELETE CASCADE;
DROP INDEX IF EXISTS idx_creditor_repayments_loan;
CREATE INDEX idx_creditor_repayments_drawdown ON creditor_repayments(creditor_drawdown_id);

-- Unapplied
ALTER TABLE creditor_unapplied_funds DROP CONSTRAINT IF EXISTS creditor_unapplied_funds_creditor_loan_id_fkey;
ALTER TABLE creditor_unapplied_funds RENAME COLUMN creditor_loan_id TO creditor_drawdown_id;
ALTER TABLE creditor_unapplied_funds
    ADD CONSTRAINT creditor_unapplied_funds_creditor_drawdown_id_fkey
    FOREIGN KEY (creditor_drawdown_id) REFERENCES creditor_drawdowns(id) ON DELETE CASCADE;
DROP INDEX IF EXISTS idx_creditor_unapplied_loan;
CREATE INDEX idx_creditor_unapplied_drawdown ON creditor_unapplied_funds(creditor_drawdown_id);

-- Journals: new columns, backfill, drop old
ALTER TABLE journal_entries ADD COLUMN IF NOT EXISTS creditor_facility_id INTEGER;
ALTER TABLE journal_entries ADD COLUMN IF NOT EXISTS creditor_drawdown_id INTEGER;

DO $je_fk$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public' AND r.relname = 'journal_entries'
      AND c.conname = 'journal_entries_creditor_facility_id_fkey'
  ) THEN
    ALTER TABLE journal_entries
        ADD CONSTRAINT journal_entries_creditor_facility_id_fkey
        FOREIGN KEY (creditor_facility_id) REFERENCES creditor_facilities(id) ON DELETE SET NULL;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public' AND r.relname = 'journal_entries'
      AND c.conname = 'journal_entries_creditor_drawdown_id_fkey'
  ) THEN
    ALTER TABLE journal_entries
        ADD CONSTRAINT journal_entries_creditor_drawdown_id_fkey
        FOREIGN KEY (creditor_drawdown_id) REFERENCES creditor_drawdowns(id) ON DELETE SET NULL;
  END IF;
END $je_fk$;

UPDATE journal_entries je
SET creditor_drawdown_id = je.creditor_loan_id,
    creditor_facility_id = cf.id
FROM creditor_facilities cf
WHERE je.creditor_loan_id IS NOT NULL
  AND cf.legacy_creditor_loan_id = je.creditor_loan_id;

ALTER TABLE journal_entries DROP CONSTRAINT IF EXISTS journal_entries_loan_or_creditor_chk;

DO $je_chk$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public' AND r.relname = 'journal_entries'
      AND c.conname = 'journal_entries_loan_or_creditor_chk'
  ) THEN
    ALTER TABLE journal_entries
        ADD CONSTRAINT journal_entries_loan_or_creditor_chk
        CHECK (
            NOT (
                loan_id IS NOT NULL
                AND (creditor_drawdown_id IS NOT NULL OR creditor_facility_id IS NOT NULL)
            )
        );
  END IF;
END $je_chk$;

ALTER TABLE journal_entries DROP COLUMN IF EXISTS creditor_loan_id;

ALTER TABLE creditor_facilities DROP COLUMN IF EXISTS legacy_creditor_loan_id;

DROP TABLE creditor_loans;

COMMENT ON TABLE creditor_facilities IS 'Borrowing facility (limit, expiry, facility-level deferred fee) under one counterparty.';
COMMENT ON TABLE creditor_drawdowns IS 'Drawdown / tranche under a facility; schedules and daily state attach here.';
COMMENT ON COLUMN creditor_drawdowns.accrual_mode IS 'daily_mirror: prior EOD engine; periodic_schedule: bill principal/interest from schedule on due dates.';

-- Align creditor loan type labels with product naming
UPDATE creditor_loan_types SET label = 'Term Loan (Actual/30)' WHERE code = 'term_standard';
UPDATE creditor_loan_types SET label = 'Term Loan (30/30)' WHERE code = 'consumer_30_30';
UPDATE creditor_loan_types SET label = 'Bullet' WHERE code = 'bullet_actual_360';
UPDATE creditor_loan_types SET label = 'Customised' WHERE code = 'customised_actual_360';
