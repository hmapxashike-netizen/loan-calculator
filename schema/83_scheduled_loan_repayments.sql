-- Scheduled / future-dated receipts (data take-on): status values and indexes.
-- Safe to run multiple times. Skips loan_repayments changes if that table does not exist yet
-- (run 04_loan_repayments.sql / base schema first).

-- Status semantics:
--   posted     — normal teller receipt; allocation + GL at capture (or at EOD activation for former scheduled).
--   reversed   — reversal rows / reversed originals (existing behaviour).
--   scheduled  — captured for a future value_date; no allocation row until EOD activates on value date.
--   cancelled  — scheduled receipt voided before value date; excluded from economics.

DO $$
DECLARE
  lr regclass := to_regclass('public.loan_repayments');
BEGIN
  IF lr IS NULL THEN
    RAISE NOTICE 'Migration 83: skipped loan_repayments DDL — table public.loan_repayments does not exist. Apply base schema (e.g. 04_loan_repayments.sql) first, then re-run this migration.';
    RETURN;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public'
      AND r.relname = 'loan_repayments'
      AND c.conname = 'loan_repayments_status_chk'
  ) THEN
    EXECUTE 'ALTER TABLE public.loan_repayments DROP CONSTRAINT loan_repayments_status_chk';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public'
      AND r.relname = 'loan_repayments'
      AND c.conname = 'loan_repayments_status_chk'
  ) THEN
    EXECUTE $c$
      ALTER TABLE public.loan_repayments
        ADD CONSTRAINT loan_repayments_status_chk
        CHECK (status IN ('posted', 'reversed', 'scheduled', 'cancelled'))
    $c$;
  END IF;

  EXECUTE $c$
    COMMENT ON CONSTRAINT loan_repayments_status_chk ON public.loan_repayments IS
      'posted=receipt with economics; reversed=reversal pair; scheduled=future value_date pending EOD; cancelled=voided scheduled.'
  $c$;

  EXECUTE $c$
    CREATE INDEX IF NOT EXISTS idx_loan_repayments_scheduled_eff_date
      ON public.loan_repayments (loan_id, (COALESCE(value_date, payment_date)))
      WHERE status = 'scheduled'
  $c$;

  EXECUTE $c$
    CREATE INDEX IF NOT EXISTS idx_loan_repayments_scheduled_value_date_global
      ON public.loan_repayments ((COALESCE(value_date, payment_date)))
      WHERE status = 'scheduled'
  $c$;
END $$;

-- Register RBAC permission when tables exist (fresh/minimal DBs skip).
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
        'teller.scheduled_receipts',
        'Scheduled receipts (data take-on)',
        'Teller',
        'Capture future value-dated receipts that activate on value date; cancel before value date.',
        '- Upload scheduled receipt batches (data take-on / migration).\n- Cancel scheduled receipts before their value date.\n- Does not grant normal Teller unless nav.teller is also assigned.',
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
    SELECT r.id, 'teller.scheduled_receipts'
    FROM rbac_roles r
    WHERE UPPER(r.role_key) = 'SUPERADMIN'
    ON CONFLICT (role_id, permission_key) DO NOTHING;
  END IF;
END $$;
