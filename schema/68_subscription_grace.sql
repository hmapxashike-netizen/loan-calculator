-- Per-tenant subscription: grace access (temporary lift of delinquency enforcement).
-- Apply in each tenant schema (SET search_path), or rely on app idempotent DDL in subscription.repository.

ALTER TABLE tenant_subscription
    ADD COLUMN IF NOT EXISTS grace_access_until DATE;

COMMENT ON COLUMN tenant_subscription.grace_access_until IS
    'Inclusive end date: while today <= grace_access_until, subscription band is treated as current for access enforcement.';
