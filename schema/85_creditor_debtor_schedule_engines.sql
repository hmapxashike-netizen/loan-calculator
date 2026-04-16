-- Creditor loan types: map to debtor schedule engines (loans.py / same math as loan calculators).
-- Safe to re-run.

UPDATE creditor_loan_types
SET
    label = 'Term loan (Actual/360)',
    behavior_json = COALESCE(behavior_json, '{}'::jsonb) || jsonb_build_object('debtor_schedule_engine', 'term_actual_360')
WHERE code = 'term_standard';

INSERT INTO creditor_loan_types (code, label, behavior_json)
SELECT
    'consumer_30_30',
    'Term loan (30/360)',
    jsonb_build_object(
        'debtor_schedule_engine', 'consumer_30_360',
        'regular_rate_per_month', 0,
        'default_interest_absolute_rate_per_month', 0,
        'penalty_interest_absolute_rate_per_month', 0,
        'grace_period_days', 0,
        'penalty_on_principal_arrears_only', true,
        'flat_interest', false,
        'waterfall_bucket_order', COALESCE(
            (SELECT behavior_json->'waterfall_bucket_order' FROM creditor_loan_types WHERE code = 'term_standard' LIMIT 1),
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
ON CONFLICT (code) DO UPDATE SET
    label = EXCLUDED.label,
    behavior_json = EXCLUDED.behavior_json;

INSERT INTO creditor_loan_types (code, label, behavior_json)
SELECT
    'bullet_actual_360',
    'Bullet (Actual/360)',
    jsonb_build_object(
        'debtor_schedule_engine', 'bullet_actual_360',
        'regular_rate_per_month', 0,
        'default_interest_absolute_rate_per_month', 0,
        'penalty_interest_absolute_rate_per_month', 0,
        'grace_period_days', 0,
        'penalty_on_principal_arrears_only', true,
        'flat_interest', false,
        'waterfall_bucket_order', COALESCE(
            (SELECT behavior_json->'waterfall_bucket_order' FROM creditor_loan_types WHERE code = 'term_standard' LIMIT 1),
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
ON CONFLICT (code) DO UPDATE SET
    label = EXCLUDED.label,
    behavior_json = EXCLUDED.behavior_json;

INSERT INTO creditor_loan_types (code, label, behavior_json)
SELECT
    'customised_actual_360',
    'Customised (Actual/360 on payments)',
    jsonb_build_object(
        'debtor_schedule_engine', 'customised_actual_360',
        'regular_rate_per_month', 0,
        'default_interest_absolute_rate_per_month', 0,
        'penalty_interest_absolute_rate_per_month', 0,
        'grace_period_days', 0,
        'penalty_on_principal_arrears_only', true,
        'flat_interest', false,
        'waterfall_bucket_order', COALESCE(
            (SELECT behavior_json->'waterfall_bucket_order' FROM creditor_loan_types WHERE code = 'term_standard' LIMIT 1),
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
ON CONFLICT (code) DO UPDATE SET
    label = EXCLUDED.label,
    behavior_json = EXCLUDED.behavior_json;

COMMENT ON COLUMN creditor_loan_types.behavior_json IS
    'debtor_schedule_engine: term_actual_360 | consumer_30_360 | bullet_actual_360 | customised_actual_360. '
    'Also: regular_rate_per_month, default_interest_absolute_rate_per_month, penalty_interest_absolute_rate_per_month, '
    'grace_period_days, penalty_on_principal_arrears_only, flat_interest, waterfall_bucket_order (array).';
