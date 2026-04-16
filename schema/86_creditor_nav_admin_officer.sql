-- Grant Creditor loans navigation + standard feature permissions to ADMIN and LOAN_OFFICER.
-- Migration 84 only seeded creditor permissions on SUPERADMIN, so the sidebar (main.py
-- permission-built menu) omitted "Creditor loans" for tenant admins.
-- Write-off remains SUPERADMIN-gated via creditor_loans.writeoff (not granted here).

DO $$
BEGIN
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
            ('creditor_loans.counterparties')
        ) AS p(permission_key)
        WHERE UPPER(r.role_key) IN ('ADMIN', 'LOAN_OFFICER')
        ON CONFLICT (role_id, permission_key) DO NOTHING;
    END IF;
END $$;
