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
