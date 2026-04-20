-- Per-tier product entitlements (vendor catalog). Idempotent.

ALTER TABLE public.vendor_subscription_tiers
    ADD COLUMN IF NOT EXISTS features JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.vendor_subscription_tiers.features IS
    'JSON: allowed_sidebar_sections (array of sidebar titles matching the loan app), bank_reconciliation (bool). '
    'Legacy excluded_sidebar_sections / loan_capture rows are migrated by subscription.repository.merge_vendor_tier_features.';

-- Seed defaults only where still empty (Python ensure_public_vendor_subscription_tiers matches).
UPDATE public.vendor_subscription_tiers
SET features = (
    '{"allowed_sidebar_sections": [
        "Customers", "Loan pipeline", "Loan management", "Creditor loans", "Teller",
        "Reamortisation", "Statements", "Accounting", "Journals",
        "End of day", "System configurations", "Subscription"
    ], "bank_reconciliation": false}'::jsonb
)
WHERE lower(trim(tier_name)) = 'basic' AND features = '{}'::jsonb;

UPDATE public.vendor_subscription_tiers
SET features = (
    '{"allowed_sidebar_sections": [
        "Customers", "Loan pipeline", "Loan management", "Creditor loans", "Portfolio reports",
        "Teller", "Reamortisation", "Statements", "Accounting", "Journals",
        "Notifications", "Document Management", "End of day", "System configurations", "Subscription"
    ], "bank_reconciliation": true}'::jsonb
)
WHERE lower(trim(tier_name)) = 'premium' AND features = '{}'::jsonb;
