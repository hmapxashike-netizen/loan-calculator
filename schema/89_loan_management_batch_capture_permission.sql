-- Granular RBAC: batch loan capture (migration) under Loan management.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'rbac_permissions'
  ) THEN
    INSERT INTO rbac_permissions (
        permission_key,
        label,
        category,
        summary,
        grants_md,
        risk_tag,
        grant_restricted_to_superadmin,
        nav_section
    ) VALUES (
        'loan_management.batch_capture',
        'Loan management — batch loan capture (migration)',
        'Loan management',
        'CSV batch import of loans (migration / data take-on) without the approval queue.',
        '- Shows the **Batch Capture** horizontal tab under Loan management.\n- High risk: commits loans directly; assign only to trusted migration operators.',
        'financial',
        TRUE,
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
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'rbac_role_permissions'
  ) THEN
    INSERT INTO rbac_role_permissions (role_id, permission_key)
    SELECT r.id, 'loan_management.batch_capture'
    FROM rbac_roles r
    WHERE UPPER(r.role_key) = 'SUPERADMIN'
    ON CONFLICT (role_id, permission_key) DO NOTHING;
  END IF;
END $$;
