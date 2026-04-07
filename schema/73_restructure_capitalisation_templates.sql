-- Ensure LOAN_RESTRUCTURE_CAPITALISE has all required bucket templates.
-- Includes regular_interest_accrued so capitalisation can move unbilled interest into principal.

INSERT INTO transaction_templates (id, event_type, system_tag, direction, description, trigger_type)
SELECT gen_random_uuid(), v.event_type, v.system_tag, v.direction, v.description, 'EVENT'
FROM (VALUES
    ('LOAN_RESTRUCTURE_CAPITALISE', 'loan_principal', 'DEBIT', 'Capitalisation of interest and arrears (restructure)'),
    ('LOAN_RESTRUCTURE_CAPITALISE', 'principal_arrears', 'CREDIT', 'Capitalisation of principal arrears (restructure)'),
    ('LOAN_RESTRUCTURE_CAPITALISE', 'regular_interest_accrued', 'CREDIT', 'Capitalisation of regular interest accrued (restructure)'),
    ('LOAN_RESTRUCTURE_CAPITALISE', 'regular_interest_arrears', 'CREDIT', 'Capitalisation of regular interest arrears (restructure)'),
    ('LOAN_RESTRUCTURE_CAPITALISE', 'penalty_interest_asset', 'CREDIT', 'Capitalisation of penalty interest (restructure)'),
    ('LOAN_RESTRUCTURE_CAPITALISE', 'default_interest_asset', 'CREDIT', 'Capitalisation of default interest (restructure)'),
    ('LOAN_RESTRUCTURE_CAPITALISE', 'fees_charges_arrears', 'CREDIT', 'Capitalisation of fees and charges arrears (restructure)')
) AS v(event_type, system_tag, direction, description)
WHERE NOT EXISTS (
    SELECT 1
    FROM transaction_templates t
    WHERE t.event_type = v.event_type
      AND t.system_tag = v.system_tag
      AND t.direction = v.direction
);
