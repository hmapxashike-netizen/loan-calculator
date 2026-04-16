-- System role ACCOUNTS_OFFICER (Accounts Officer): Accounting + Creditor loans (no write-off).
-- Idempotent grants for ADMIN and SUPERADMIN (may already hold these via full nav seed).

INSERT INTO rbac_roles (role_key, display_name, is_system)
VALUES ('ACCOUNTS_OFFICER', 'Accounts Officer', TRUE)
ON CONFLICT (role_key) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    is_system = EXCLUDED.is_system;

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
            ('nav.accounting'),
            ('nav.creditor_loans'),
            ('creditor_loans.view'),
            ('creditor_loans.capture'),
            ('creditor_loans.receipts'),
            ('creditor_loans.counterparties')
        ) AS p(permission_key)
        WHERE UPPER(r.role_key) IN ('ACCOUNTS_OFFICER', 'ADMIN', 'SUPERADMIN')
        ON CONFLICT (role_id, permission_key) DO NOTHING;

        -- SUPERADMIN: ensure creditor write-off remains assignable (catalog default).
        INSERT INTO rbac_role_permissions (role_id, permission_key)
        SELECT r.id, 'creditor_loans.writeoff'
        FROM rbac_roles r
        WHERE UPPER(r.role_key) = 'SUPERADMIN'
        ON CONFLICT (role_id, permission_key) DO NOTHING;
    END IF;
END $$;
