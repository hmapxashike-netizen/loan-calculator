-- Replace daily CLEAR/BILLING regular-interest GL with receivable move (EOD) + EOM P&L recognition.

DELETE FROM transaction_templates
WHERE event_type IN ('CLEAR_DAILY_ACCRUAL', 'BILLING_REGULAR_INTEREST')
  AND trigger_type = 'EOD';

INSERT INTO transaction_templates (id, event_type, system_tag, direction, description, trigger_type)
VALUES
    (gen_random_uuid(), 'REGULAR_INTEREST_BILLING_RECEIVABLE', 'regular_interest_arrears', 'DEBIT',
     'Regular interest billing: arrears from accrued (unbilled)', 'EOD'),
    (gen_random_uuid(), 'REGULAR_INTEREST_BILLING_RECEIVABLE', 'regular_interest_accrued', 'CREDIT',
     'Regular interest billing: arrears from accrued (unbilled)', 'EOD'),
    (gen_random_uuid(), 'EOM_REGULAR_INTEREST_INCOME_RECOGNITION', 'regular_interest_income_holding', 'DEBIT',
     'EOM recognition of regular interest income (MTD accrual)', 'EOM'),
    (gen_random_uuid(), 'EOM_REGULAR_INTEREST_INCOME_RECOGNITION', 'regular_interest_income', 'CREDIT',
     'EOM recognition of regular interest income (MTD accrual)', 'EOM');
