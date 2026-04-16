-- Loan supervisor, Accounts supervisor, Viewer roles + feature permissions.
-- Safe to re-run.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'rbac_permissions'
  ) THEN
    INSERT INTO rbac_permissions (
        permission_key, label, category, summary, grants_md, risk_tag,
        grant_restricted_to_superadmin, nav_section
    ) VALUES (
        'loan_management.approve_loans',
        'Loan management — approve loans',
        'Loan management',
        'Approve loan approval drafts in Loan management.',
        '- Shows the **Approve Loans** tab under Loan management.',
        'sensitive',
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

    INSERT INTO rbac_permissions (
        permission_key, label, category, summary, grants_md, risk_tag,
        grant_restricted_to_superadmin, nav_section
    ) VALUES (
        'accounting.supervise',
        'Accounting — supervise',
        'Accounting',
        'Senior accounting oversight (use with policy-specific UI gates).',
        '- Seeded on accounts supervisor roles alongside Accounting and Journals.',
        'financial',
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

    -- Granular Journals keys (required FK before rbac_role_permissions below).
    INSERT INTO rbac_permissions (
        permission_key, label, category, summary, grants_md, risk_tag,
        grant_restricted_to_superadmin, nav_section
    ) VALUES (
        'journals.manual',
        'Journals — manual journal',
        'Journals',
        'Template-based manual journal posting under Journals.',
        '- **Manual Journals** horizontal tab.',
        'financial',
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

    INSERT INTO rbac_permissions (
        permission_key, label, category, summary, grants_md, risk_tag,
        grant_restricted_to_superadmin, nav_section
    ) VALUES (
        'journals.balance_adjustment',
        'Journals — balance adjustment',
        'Journals',
        'One-off debit/credit balance adjustment between posting accounts.',
        '- **Balance Adjustments** horizontal tab under Journals.',
        'financial',
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

    INSERT INTO rbac_permissions (
        permission_key, label, category, summary, grants_md, risk_tag,
        grant_restricted_to_superadmin, nav_section
    ) VALUES (
        'journals.approvals',
        'Journals — journal approvals',
        'Journals',
        'Review and approve journal workflows (reserved for future queues).',
        '- **Journal approvals** tab under Journals.',
        'financial',
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
  END IF;
END $$;

INSERT INTO rbac_roles (role_key, display_name, is_system)
VALUES
    ('LOAN_SUPERVISOR', 'Loan Supervisor', TRUE),
    ('ACCOUNTS_SUPERVISOR', 'Accounts Supervisor', TRUE),
    ('VIEWER', 'Viewer', TRUE)
ON CONFLICT (role_key) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    is_system = EXCLUDED.is_system;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'rbac_role_permissions'
  ) THEN
    RETURN;
  END IF;

  -- Loan supervisor: officer nav (no System configurations) + creditor actions + approve loans.
  INSERT INTO rbac_role_permissions (role_id, permission_key)
  SELECT r.id, t.k
  FROM rbac_roles r
  CROSS JOIN LATERAL unnest(ARRAY[
    'nav.customers',
    'nav.loan_management',
    'nav.creditor_loans',
    'nav.portfolio_reports',
    'nav.teller',
    'nav.reamortisation',
    'nav.statements',
    'nav.accounting',
    'nav.journals',
    'nav.notifications',
    'nav.document_management',
    'nav.end_of_day',
    'nav.subscription',
    'creditor_loans.view',
    'creditor_loans.capture',
    'creditor_loans.receipts',
    'creditor_loans.counterparties',
    'loan_management.approve_loans',
    'journals.manual',
    'journals.balance_adjustment',
    'journals.approvals'
  ]) AS t(k)
  WHERE UPPER(r.role_key) = 'LOAN_SUPERVISOR'
  ON CONFLICT (role_id, permission_key) DO NOTHING;

  -- Accounts supervisor: Accounting + Creditor + Journals + supervise.
  INSERT INTO rbac_role_permissions (role_id, permission_key)
  SELECT r.id, t.k
  FROM rbac_roles r
  CROSS JOIN LATERAL unnest(ARRAY[
    'nav.accounting',
    'nav.creditor_loans',
    'nav.journals',
    'creditor_loans.view',
    'creditor_loans.capture',
    'creditor_loans.receipts',
    'creditor_loans.counterparties',
    'accounting.supervise',
    'journals.manual',
    'journals.balance_adjustment',
    'journals.approvals'
  ]) AS t(k)
  WHERE UPPER(r.role_key) = 'ACCOUNTS_SUPERVISOR'
  ON CONFLICT (role_id, permission_key) DO NOTHING;

  -- Viewer: statements + portfolio reports only.
  INSERT INTO rbac_role_permissions (role_id, permission_key)
  SELECT r.id, t.k
  FROM rbac_roles r
  CROSS JOIN LATERAL unnest(ARRAY[
    'nav.statements',
    'nav.portfolio_reports'
  ]) AS t(k)
  WHERE UPPER(r.role_key) = 'VIEWER'
  ON CONFLICT (role_id, permission_key) DO NOTHING;

  -- Ensure admins hold approve + supervise keys (idempotent).
  INSERT INTO rbac_role_permissions (role_id, permission_key)
  SELECT r.id, p.k
  FROM rbac_roles r
  CROSS JOIN (VALUES ('loan_management.approve_loans'), ('accounting.supervise')) AS p(k)
  WHERE UPPER(r.role_key) IN ('ADMIN', 'SUPERADMIN')
  ON CONFLICT (role_id, permission_key) DO NOTHING;
END $$;
