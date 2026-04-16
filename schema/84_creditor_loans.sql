-- Creditor (borrowing / liability) mirror facilities — separate from debtor loans.
-- Run after loans, journal_entries, RBAC base. Safe to re-run (IF NOT EXISTS).

-- External lenders / banks (not customers table)
CREATE TABLE IF NOT EXISTS creditor_counterparties (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    reference_code  VARCHAR(128),
    tax_id          VARCHAR(128),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE creditor_counterparties IS 'Lenders/financiers for creditor (borrowing) facilities; kept separate from customers (borrowers).';

-- Behaviour + waterfall (JSON); engine keys mirror LoanConfig / waterfall_profiles.
CREATE TABLE IF NOT EXISTS creditor_loan_types (
    code            VARCHAR(64) PRIMARY KEY,
    label           VARCHAR(255) NOT NULL,
    behavior_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN creditor_loan_types.behavior_json IS
    'Keys: regular_rate_per_month, default_interest_absolute_rate_per_month, penalty_interest_absolute_rate_per_month, '
    'grace_period_days, penalty_on_principal_arrears_only, flat_interest, waterfall_bucket_order (array of bucket names).';

INSERT INTO creditor_loan_types (code, label, behavior_json)
VALUES (
    'term_standard',
    'Term facility (standard mirror)',
    jsonb_build_object(
        'regular_rate_per_month', 0,
        'default_interest_absolute_rate_per_month', 0,
        'penalty_interest_absolute_rate_per_month', 0,
        'grace_period_days', 0,
        'penalty_on_principal_arrears_only', true,
        'flat_interest', false,
        'waterfall_bucket_order',
        jsonb_build_array(
            'interest_arrears_balance',
            'interest_accrued_balance',
            'principal_arrears',
            'principal_not_due',
            'default_interest_balance',
            'penalty_interest_balance',
            'fees_charges_balance'
        )
    )
)
ON CONFLICT (code) DO NOTHING;

CREATE TABLE IF NOT EXISTS creditor_loans (
    id                      SERIAL PRIMARY KEY,
    creditor_counterparty_id INTEGER NOT NULL REFERENCES creditor_counterparties(id),
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
    metadata                JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_creditor_loans_counterparty ON creditor_loans(creditor_counterparty_id);
CREATE INDEX IF NOT EXISTS idx_creditor_loans_status ON creditor_loans(status);

CREATE TABLE IF NOT EXISTS creditor_loan_schedules (
    id                  SERIAL PRIMARY KEY,
    creditor_loan_id    INTEGER NOT NULL REFERENCES creditor_loans(id) ON DELETE CASCADE,
    version             INTEGER NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (creditor_loan_id, version)
);

CREATE TABLE IF NOT EXISTS creditor_schedule_lines (
    id                      SERIAL PRIMARY KEY,
    creditor_loan_schedule_id INTEGER NOT NULL REFERENCES creditor_loan_schedules(id) ON DELETE CASCADE,
    "Period"                INTEGER NOT NULL,
    "Date"                  VARCHAR(32),
    payment                 NUMERIC(22, 10) NOT NULL DEFAULT 0,
    principal               NUMERIC(22, 10) NOT NULL DEFAULT 0,
    interest                NUMERIC(22, 10) NOT NULL DEFAULT 0,
    principal_balance       NUMERIC(22, 10) NOT NULL DEFAULT 0,
    total_outstanding       NUMERIC(22, 10) NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_creditor_schedule_lines_schedule_id
    ON creditor_schedule_lines(creditor_loan_schedule_id);

CREATE TABLE IF NOT EXISTS creditor_loan_daily_state (
    id                          SERIAL PRIMARY KEY,
    creditor_loan_id           INTEGER NOT NULL REFERENCES creditor_loans(id) ON DELETE CASCADE,
    as_of_date                  DATE NOT NULL,
    regular_interest_daily      NUMERIC(22, 10) NOT NULL DEFAULT 0,
    principal_not_due           NUMERIC(22, 10) NOT NULL DEFAULT 0,
    principal_arrears           NUMERIC(22, 10) NOT NULL DEFAULT 0,
    interest_accrued_balance    NUMERIC(22, 10) NOT NULL DEFAULT 0,
    interest_arrears_balance    NUMERIC(22, 10) NOT NULL DEFAULT 0,
    default_interest_daily      NUMERIC(22, 10) NOT NULL DEFAULT 0,
    default_interest_balance    NUMERIC(22, 10) NOT NULL DEFAULT 0,
    penalty_interest_daily      NUMERIC(22, 10) NOT NULL DEFAULT 0,
    penalty_interest_balance    NUMERIC(22, 10) NOT NULL DEFAULT 0,
    fees_charges_balance         NUMERIC(22, 10) NOT NULL DEFAULT 0,
    days_overdue                 INTEGER NOT NULL DEFAULT 0,
    total_exposure               NUMERIC(22, 10) NOT NULL DEFAULT 0,
    total_delinquency_arrears    NUMERIC(22, 10) NOT NULL DEFAULT 0,
    regular_interest_period_to_date NUMERIC(22, 10) NOT NULL DEFAULT 0,
    penalty_interest_period_to_date NUMERIC(22, 10) NOT NULL DEFAULT 0,
    default_interest_period_to_date NUMERIC(22, 10) NOT NULL DEFAULT 0,
    net_allocation               NUMERIC(22, 10) NOT NULL DEFAULT 0,
    unallocated                  NUMERIC(22, 10) NOT NULL DEFAULT 0,
    regular_interest_in_suspense_balance NUMERIC(22, 10) NOT NULL DEFAULT 0,
    penalty_interest_in_suspense_balance NUMERIC(22, 10) NOT NULL DEFAULT 0,
    default_interest_in_suspense_balance NUMERIC(22, 10) NOT NULL DEFAULT 0,
    total_interest_in_suspense_balance NUMERIC(22, 10) NOT NULL DEFAULT 0,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_creditor_loan_daily_state_loan_date
    ON creditor_loan_daily_state(creditor_loan_id, as_of_date);

CREATE INDEX IF NOT EXISTS idx_creditor_loan_daily_state_date ON creditor_loan_daily_state(as_of_date);

CREATE TABLE IF NOT EXISTS creditor_repayments (
    id                      SERIAL PRIMARY KEY,
    creditor_loan_id        INTEGER NOT NULL REFERENCES creditor_loans(id) ON DELETE CASCADE,
    amount                  NUMERIC(22, 10) NOT NULL,
    payment_date            DATE NOT NULL,
    value_date              DATE,
    reference               VARCHAR(255),
    company_reference       VARCHAR(255),
    status                  VARCHAR(32) NOT NULL DEFAULT 'posted',
    source_cash_gl_account_id UUID,
    system_date             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT creditor_repayments_status_chk
        CHECK (status IN ('posted', 'reversed'))
);

CREATE INDEX IF NOT EXISTS idx_creditor_repayments_loan ON creditor_repayments(creditor_loan_id);
CREATE INDEX IF NOT EXISTS idx_creditor_repayments_value_date ON creditor_repayments((COALESCE(value_date, payment_date)));

CREATE TABLE IF NOT EXISTS creditor_repayment_allocation (
    id                          SERIAL PRIMARY KEY,
    repayment_id               INTEGER NOT NULL REFERENCES creditor_repayments(id) ON DELETE CASCADE,
    alloc_principal_not_due     NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_principal_arrears     NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_interest_accrued      NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_interest_arrears      NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_default_interest      NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_penalty_interest      NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_fees_charges          NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_principal_total       NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_interest_total        NUMERIC(22, 10) NOT NULL DEFAULT 0,
    alloc_fees_total            NUMERIC(22, 10) NOT NULL DEFAULT 0,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_creditor_repayment_allocation_unique
    ON creditor_repayment_allocation(repayment_id);

CREATE TABLE IF NOT EXISTS creditor_unapplied_funds (
    id                      SERIAL PRIMARY KEY,
    creditor_loan_id        INTEGER NOT NULL REFERENCES creditor_loans(id) ON DELETE CASCADE,
    creditor_repayment_id   INTEGER REFERENCES creditor_repayments(id) ON DELETE SET NULL,
    amount                  NUMERIC(22, 10) NOT NULL,
    value_date              DATE NOT NULL,
    entry_type              VARCHAR(16) NOT NULL DEFAULT 'credit',
    reference               VARCHAR(255),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_creditor_unapplied_loan ON creditor_unapplied_funds(creditor_loan_id);

-- Journal header: link to creditor facility (mutually exclusive with loan_id debtor link)
ALTER TABLE journal_entries
    ADD COLUMN IF NOT EXISTS creditor_loan_id INTEGER REFERENCES creditor_loans(id) ON DELETE SET NULL;

COMMENT ON COLUMN journal_entries.creditor_loan_id IS 'Creditor (borrowing) facility for liability-side postings; never mix with debtor loan_id.';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'public' AND r.relname = 'journal_entries'
          AND c.conname = 'journal_entries_loan_or_creditor_chk'
    ) THEN
        ALTER TABLE journal_entries
            ADD CONSTRAINT journal_entries_loan_or_creditor_chk
            CHECK (NOT (loan_id IS NOT NULL AND creditor_loan_id IS NOT NULL));
    END IF;
END $$;

-- RBAC (skip if tables missing)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'rbac_permissions'
    ) THEN
        INSERT INTO rbac_permissions (
            permission_key, label, category, summary, grants_md, risk_tag,
            grant_restricted_to_superadmin, nav_section
        ) VALUES
        (
            'nav.creditor_loans',
            'Creditor loans',
            'Navigation',
            'Open the Creditor loans area (borrowings / mirror liabilities).',
            '- Access creditor loan capture, counterparties, receipts, and write-offs per sub-permissions.',
            'financial',
            FALSE,
            'Creditor loans'
        ),
        (
            'creditor_loans.view',
            'Creditor loans — view',
            'Creditor loans',
            'View facilities, schedules, and daily mirror state.',
            '- View creditor facilities and schedules.',
            'financial',
            FALSE,
            NULL
        ),
        (
            'creditor_loans.capture',
            'Creditor loans — capture',
            'Creditor loans',
            'Create creditor facilities and schedules.',
            '- Capture new creditor (borrowing) facilities.',
            'financial',
            FALSE,
            NULL
        ),
        (
            'creditor_loans.receipts',
            'Creditor loans — receipts',
            'Creditor loans',
            'Record payments to lenders and run allocation.',
            '- Post creditor repayments with GL.',
            'financial',
            FALSE,
            NULL
        ),
        (
            'creditor_loans.writeoff',
            'Creditor loans — write-off',
            'Creditor loans',
            'Post creditor-specific write-off journals.',
            '- Write off principal or interest on creditor facilities.',
            'financial',
            TRUE,
            NULL
        ),
        (
            'creditor_loans.counterparties',
            'Creditor loans — counterparties',
            'Creditor loans',
            'Maintain lender / financier master records.',
            '- Add or edit creditor counterparties.',
            'standard',
            FALSE,
            NULL
        )
        ON CONFLICT (permission_key) DO UPDATE SET
            label = EXCLUDED.label,
            category = EXCLUDED.category,
            summary = EXCLUDED.summary,
            grants_md = EXCLUDED.grants_md,
            risk_tag = EXCLUDED.risk_tag,
            grant_restricted_to_superadmin = EXCLUDED.grant_restricted_to_superadmin,
            nav_section = EXCLUDED.nav_section,
            updated_at = NOW();

        IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'rbac_role_permissions'
        ) THEN
            INSERT INTO rbac_role_permissions (role_id, permission_key)
            SELECT r.id, p.permission_key
            FROM rbac_roles r
            CROSS JOIN (VALUES
                ('nav.creditor_loans'),
                ('creditor_loans.view'),
                ('creditor_loans.capture'),
                ('creditor_loans.receipts'),
                ('creditor_loans.writeoff'),
                ('creditor_loans.counterparties')
            ) AS p(permission_key)
            WHERE UPPER(r.role_key) = 'SUPERADMIN'
            ON CONFLICT (role_id, permission_key) DO NOTHING;
        END IF;
    END IF;
END $$;
