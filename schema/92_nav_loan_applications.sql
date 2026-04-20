-- Standalone sidebar: nav.loan_applications (section title "Loan pipeline").
-- Inserts the permission row if missing (schema 92 previously skipped grants when the row did not exist).
-- Then grants the nav key to every role that already has nav.loan_management.
-- Safe to re-run.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'rbac_permissions'
  ) THEN
    RETURN;
  END IF;

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
      'nav.loan_applications',
      'Loan pipeline',
      'Navigation',
      'Loan prospect pipeline: find/create customers, file applications, update status, link booked loans.',
      '- Open **Loan pipeline** from the sidebar (separate from Loan Capture).' || E'\n'
      || '- Pipeline status buttons are configured under **System configurations → Loan pipeline**.',
      'standard',
      FALSE,
      'Loan pipeline'
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
    SELECT DISTINCT rp.role_id, 'nav.loan_applications'
    FROM rbac_role_permissions rp
    WHERE rp.permission_key = 'nav.loan_management'
    ON CONFLICT DO NOTHING;
  END IF;
END $$;
