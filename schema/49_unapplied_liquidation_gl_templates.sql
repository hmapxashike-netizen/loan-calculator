-- Unapplied→arrears liquidation GL: debit unapplied_funds (L220004 / system_tag unapplied_funds), not bank.
-- Run after transaction_templates has trigger_type (see scripts/migration_37_trigger_type.py).
--
-- After this migration: run GL repost for affected loans (repost_gl_for_loan_date_range) so corrected
-- journals replace the soft-deactivated legacy rows below.

-- ---------------------------------------------------------------------------
-- 1) Transaction templates (idempotent per event_type + system_tag + direction)
-- ---------------------------------------------------------------------------
INSERT INTO transaction_templates (id, event_type, system_tag, direction, description, trigger_type)
SELECT gen_random_uuid(), v.event_type, v.system_tag, v.direction, v.description, 'EVENT'
FROM (VALUES
    ('UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS', 'unapplied_funds', 'DEBIT', 'Unapplied liquidation: principal arrears'),
    ('UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS', 'principal_arrears', 'CREDIT', 'Unapplied liquidation: principal arrears'),
    ('UNAPPLIED_LIQUIDATION_PRINCIPAL_NOT_YET_DUE', 'unapplied_funds', 'DEBIT', 'Unapplied liquidation: principal not yet due'),
    ('UNAPPLIED_LIQUIDATION_PRINCIPAL_NOT_YET_DUE', 'loan_principal', 'CREDIT', 'Unapplied liquidation: principal not yet due'),
    ('UNAPPLIED_LIQUIDATION_REGULAR_INTEREST', 'unapplied_funds', 'DEBIT', 'Unapplied liquidation: regular interest arrears'),
    ('UNAPPLIED_LIQUIDATION_REGULAR_INTEREST', 'regular_interest_arrears', 'CREDIT', 'Unapplied liquidation: regular interest arrears'),
    ('UNAPPLIED_LIQUIDATION_REGULAR_INTEREST_NOT_YET_DUE', 'unapplied_funds', 'DEBIT', 'Unapplied liquidation: regular interest accrued'),
    ('UNAPPLIED_LIQUIDATION_REGULAR_INTEREST_NOT_YET_DUE', 'regular_interest_accrued', 'CREDIT', 'Unapplied liquidation: regular interest accrued'),
    ('UNAPPLIED_LIQUIDATION_PENALTY_INTEREST', 'unapplied_funds', 'DEBIT', 'Unapplied liquidation: penalty interest (12)'),
    ('UNAPPLIED_LIQUIDATION_PENALTY_INTEREST', 'penalty_interest_asset', 'CREDIT', 'Unapplied liquidation: penalty interest (12)'),
    ('UNAPPLIED_LIQUIDATION_PENALTY_INTEREST', 'penalty_interest_suspense', 'DEBIT', 'Unapplied liquidation: recognise penalty income (12a)'),
    ('UNAPPLIED_LIQUIDATION_PENALTY_INTEREST', 'penalty_interest_income', 'CREDIT', 'Unapplied liquidation: recognise penalty income (12a)'),
    ('UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST', 'unapplied_funds', 'DEBIT', 'Unapplied liquidation: default interest (15)'),
    ('UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST', 'default_interest_asset', 'CREDIT', 'Unapplied liquidation: default interest (15)'),
    ('UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST', 'default_interest_suspense', 'DEBIT', 'Unapplied liquidation: recognise default income (15a)'),
    ('UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST', 'default_interest_income', 'CREDIT', 'Unapplied liquidation: recognise default income (15a)'),
    ('UNAPPLIED_LIQUIDATION_PASS_THROUGH_COST_RECOVERY', 'unapplied_funds', 'DEBIT', 'Unapplied liquidation: fees/charges (recovery)'),
    ('UNAPPLIED_LIQUIDATION_PASS_THROUGH_COST_RECOVERY', 'deferred_fee_commission_asset', 'CREDIT', 'Unapplied liquidation: fees/charges (recovery)')
) AS v(event_type, system_tag, direction, description)
WHERE NOT EXISTS (
    SELECT 1 FROM transaction_templates t
    WHERE t.event_type = v.event_type
      AND t.system_tag = v.system_tag
      AND t.direction = v.direction
);

-- ---------------------------------------------------------------------------
-- 2) Soft-deactivate legacy liquidation journals that used cash-receipt templates
--    (same event_id / reference family; event_tag was PAYMENT_* / PASS_THROUGH_*).
--    Repost will create UNAPPLIED_LIQUIDATION_* rows with the same deterministic event_id.
-- ---------------------------------------------------------------------------
UPDATE journal_entries je
SET is_active = FALSE,
    superseded_at = COALESCE(je.superseded_at, NOW())
WHERE COALESCE(je.is_active, TRUE) = TRUE
  AND je.event_id IS NOT NULL
  AND (
        je.event_id LIKE 'liquidation:%'
        OR je.event_id LIKE 'REV-liquidation:%'
      )
  AND je.event_tag IN (
        'PAYMENT_PRINCIPAL',
        'PAYMENT_PRINCIPAL_NOT_YET_DUE',
        'PAYMENT_REGULAR_INTEREST',
        'PAYMENT_REGULAR_INTEREST_NOT_YET_DUE',
        'PAYMENT_DEFAULT_INTEREST',
        'PAYMENT_PENALTY_INTEREST',
        'PASS_THROUGH_COST_RECOVERY'
      );
