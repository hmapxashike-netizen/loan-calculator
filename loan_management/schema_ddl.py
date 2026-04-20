"""Idempotent DDL for loan capture and related tables (when migrations lag)."""

from __future__ import annotations

from typing import Any


def _ensure_loan_purposes_schema(conn: Any) -> None:
    """Idempotent DDL for loan_purposes and loans.loan_purpose_id (see schema 62)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS loan_purposes (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        # Older DBs may have loan_purposes without updated_at (CREATE TABLE IF NOT EXISTS does not add columns).
        cur.execute(
            """
            ALTER TABLE loan_purposes
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_loan_purposes_name_lower
            ON loan_purposes (LOWER(TRIM(name)));
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loan_purposes_active_sort
            ON loan_purposes (is_active, sort_order, id);
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS loan_purpose_id INTEGER
            REFERENCES loan_purposes(id) ON DELETE SET NULL;
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_loans_loan_purpose ON loans (loan_purpose_id);"
        )


def _ensure_loans_schema_for_save_loan(conn: Any) -> None:
    """
    Idempotent DDL so save_loan()'s INSERT matches the database when formal
    migrations have not been applied yet (same intent as schema 50, 51, and
    collateral columns from 53).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS disbursement_bank_options (
                id SERIAL PRIMARY KEY,
                label VARCHAR(255) NOT NULL,
                gl_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_disbursement_bank_options_active
            ON disbursement_bank_options (is_active, sort_order);
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS disbursement_bank_option_id INTEGER
            REFERENCES disbursement_bank_options(id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loans_disbursement_bank_option
            ON loans (disbursement_bank_option_id);
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS cash_gl_account_id UUID REFERENCES accounts(id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loans_cash_gl_account
            ON loans (cash_gl_account_id);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS provision_security_subtypes (
                id SERIAL PRIMARY KEY,
                security_type VARCHAR(128) NOT NULL,
                subtype_name VARCHAR(255) NOT NULL,
                typical_haircut_pct NUMERIC(22, 10) NOT NULL DEFAULT 0,
                system_notes TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (security_type, subtype_name)
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS collateral_security_subtype_id INTEGER
                REFERENCES provision_security_subtypes(id) ON DELETE SET NULL;
            """
        )
        cur.execute(
            "ALTER TABLE loans ADD COLUMN IF NOT EXISTS collateral_charge_amount NUMERIC(22, 10);"
        )
        cur.execute(
            "ALTER TABLE loans ADD COLUMN IF NOT EXISTS collateral_valuation_amount NUMERIC(22, 10);"
        )
        # Restructure reporting flags (indexed for cheap OR filters in portfolio SQL).
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS remodified_in_place BOOLEAN NOT NULL DEFAULT FALSE;
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS originated_from_split BOOLEAN NOT NULL DEFAULT FALSE;
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS modification_topup_applied BOOLEAN NOT NULL DEFAULT FALSE;
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loans_remodified_in_place
            ON loans (id) WHERE remodified_in_place = TRUE;
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loans_originated_from_split
            ON loans (id) WHERE originated_from_split = TRUE;
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loans_modification_topup
            ON loans (id) WHERE modification_topup_applied = TRUE;
            """
        )
    _ensure_loan_purposes_schema(conn)


def _ensure_loan_approval_drafts_table(conn: Any) -> None:
    """Create approval draft queue table if it does not yet exist."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS loan_approval_drafts (
                id BIGSERIAL PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                loan_type VARCHAR(64) NOT NULL,
                product_code VARCHAR(64),
                details_json JSONB NOT NULL,
                schedule_json JSONB NOT NULL,
                assigned_approver_id VARCHAR(128),
                status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                created_by VARCHAR(128),
                approved_by VARCHAR(128),
                rework_note TEXT,
                dismissed_note TEXT,
                loan_id INTEGER,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                approved_at TIMESTAMPTZ,
                dismissed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_loan_approval_drafts_status ON loan_approval_drafts(status)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_loan_approval_drafts_assignee ON loan_approval_drafts(assigned_approver_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_loan_approval_drafts_customer ON loan_approval_drafts(customer_id)"
        )
        cur.execute(
            """
            ALTER TABLE loan_approval_drafts
            ADD COLUMN IF NOT EXISTS schedule_json_secondary JSONB NOT NULL DEFAULT '[]'::jsonb
            """
        )
        # Older runs may have created this as INTEGER; allow UUID/text user ids.
        cur.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'loan_approval_drafts'
                      AND column_name = 'assigned_approver_id'
                      AND data_type IN ('integer', 'bigint', 'smallint')
                ) THEN
                    ALTER TABLE loan_approval_drafts
                    ALTER COLUMN assigned_approver_id TYPE VARCHAR(128)
                    USING assigned_approver_id::text;
                END IF;
            END $$;
            """
        )


def _ensure_loan_applications_schema(conn: Any) -> None:
    """Idempotent DDL for loan_applications, ref sequences, commission accrual stub, and FK columns."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS loan_application_ref_sequences (
                prefix VARCHAR(8) PRIMARY KEY,
                next_num INTEGER NOT NULL DEFAULT 1
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS loan_applications (
                id BIGSERIAL PRIMARY KEY,
                reference_number VARCHAR(32) NOT NULL UNIQUE,
                customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL,
                agent_id INTEGER REFERENCES agents(id) ON DELETE SET NULL,
                national_id TEXT,
                requested_principal NUMERIC(22, 10),
                product_code VARCHAR(64),
                status VARCHAR(64) NOT NULL DEFAULT 'PROSPECT',
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                loan_id INTEGER UNIQUE REFERENCES loans(id) ON DELETE SET NULL,
                superseded_at TIMESTAMPTZ,
                superseded_by_id BIGINT REFERENCES loan_applications(id) ON DELETE SET NULL,
                deleted_at TIMESTAMPTZ,
                deleted_by VARCHAR(128),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_by VARCHAR(128)
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_loan_applications_customer ON loan_applications(customer_id) "
            "WHERE deleted_at IS NULL;"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_loan_applications_agent ON loan_applications(agent_id) "
            "WHERE deleted_at IS NULL;"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_loan_applications_status ON loan_applications(status, updated_at) "
            "WHERE deleted_at IS NULL;"
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS source_application_id BIGINT
            REFERENCES loan_applications(id) ON DELETE SET NULL;
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_loans_source_application_id ON loans(source_application_id);"
        )
        cur.execute(
            """
            ALTER TABLE loan_approval_drafts
            ADD COLUMN IF NOT EXISTS application_id BIGINT
            REFERENCES loan_applications(id) ON DELETE SET NULL;
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_loan_approval_drafts_application_id "
            "ON loan_approval_drafts(application_id);"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_commission_accruals (
                id BIGSERIAL PRIMARY KEY,
                loan_id INTEGER NOT NULL UNIQUE REFERENCES loans(id) ON DELETE CASCADE,
                application_id BIGINT REFERENCES loan_applications(id) ON DELETE SET NULL,
                agent_id INTEGER REFERENCES agents(id) ON DELETE SET NULL,
                principal_at_booking NUMERIC(22, 10) NOT NULL,
                commission_rate_pct_snapshot NUMERIC(22, 10),
                commission_amount NUMERIC(22, 10) NOT NULL,
                accrual_status VARCHAR(32) NOT NULL DEFAULT 'PENDING_POST',
                journal_entry_id UUID,
                invoice_id BIGINT,
                paid_at TIMESTAMPTZ,
                recognised_at TIMESTAMPTZ,
                payment_journal_entry_id UUID,
                recognition_journal_entry_id UUID,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_commission_invoices (
                id BIGSERIAL PRIMARY KEY,
                invoice_number VARCHAR(64) NOT NULL UNIQUE,
                agent_id INTEGER NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                invoice_date DATE NOT NULL DEFAULT CURRENT_DATE,
                total_commission NUMERIC(22, 10) NOT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'ISSUED',
                paid_at TIMESTAMPTZ,
                created_by VARCHAR(128),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                payment_journal_entry_id UUID
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_commission_invoice_lines (
                id BIGSERIAL PRIMARY KEY,
                invoice_id BIGINT NOT NULL REFERENCES agent_commission_invoices(id) ON DELETE CASCADE,
                accrual_id BIGINT NOT NULL UNIQUE REFERENCES agent_commission_accruals(id) ON DELETE RESTRICT,
                loan_id INTEGER NOT NULL REFERENCES loans(id) ON DELETE RESTRICT,
                application_id BIGINT REFERENCES loan_applications(id) ON DELETE SET NULL,
                commission_amount NUMERIC(22, 10) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_commission_accruals_application "
            "ON agent_commission_accruals(application_id);"
        )
        cur.execute(
            """
            ALTER TABLE agent_commission_accruals
            ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS recognised_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS recognised_months INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS recognised_amount NUMERIC(22, 10) NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS payment_journal_entry_id UUID,
            ADD COLUMN IF NOT EXISTS recognition_journal_entry_id UUID;
            """
        )
        cur.execute(
            """
            DO $ac_inv$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'agent_commission_accruals'
                      AND column_name = 'invoice_id'
                ) THEN
                    ALTER TABLE agent_commission_accruals
                    ADD COLUMN invoice_id BIGINT;
                END IF;
            END $ac_inv$;
            """
        )
        cur.execute(
            """
            DO $ac_fk$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE table_schema = current_schema()
                      AND table_name = 'agent_commission_accruals'
                      AND constraint_name = 'fk_agent_commission_accruals_invoice'
                ) THEN
                    ALTER TABLE agent_commission_accruals
                    ADD CONSTRAINT fk_agent_commission_accruals_invoice
                    FOREIGN KEY (invoice_id) REFERENCES agent_commission_invoices(id) ON DELETE SET NULL;
                END IF;
            END $ac_fk$;
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_commission_accruals_invoice "
            "ON agent_commission_accruals(invoice_id);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_commission_invoices_agent_period "
            "ON agent_commission_invoices(agent_id, period_start, period_end, status);"
        )
        cur.execute(
            """
            DO $ldl$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'loan_applications'
                      AND column_name = 'status'
                      AND character_maximum_length IS NOT NULL
                      AND character_maximum_length < 64
                ) THEN
                    ALTER TABLE loan_applications ALTER COLUMN status TYPE VARCHAR(64);
                END IF;
            END $ldl$;
            """
        )
