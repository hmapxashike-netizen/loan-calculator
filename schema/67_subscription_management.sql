-- Subscription: vendor tier catalog (public) + per-tenant subscription state and POP uploads.
-- Apply public section once on the cluster. Apply tenant section to each tenant schema (SET search_path
-- or qualify with schema name), or rely on app idempotent DDL (dal.subscription_repository).

-- ---------------------------------------------------------------------------
-- A) Public vendor catalog (platform-wide tier fees)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.vendor_subscription_tiers (
    tier_name      TEXT PRIMARY KEY,
    monthly_fee    NUMERIC(20, 10) NOT NULL DEFAULT 0,
    quarterly_fee  NUMERIC(20, 10) NOT NULL DEFAULT 0,
    is_active      BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT vendor_subscription_tiers_tier_name_nonempty CHECK (length(trim(tier_name)) > 0)
);

COMMENT ON TABLE public.vendor_subscription_tiers IS 'LendFlow Core vendor pricing: Basic / Premium; fees at 10 dp.';

INSERT INTO public.vendor_subscription_tiers (tier_name, monthly_fee, quarterly_fee, is_active)
VALUES
    ('Basic', 0, 0, TRUE),
    ('Premium', 0, 0, TRUE)
ON CONFLICT (tier_name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- B) Tenant schema: single-row subscription state (run per tenant schema)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_subscription (
    id                     SMALLINT PRIMARY KEY DEFAULT 1,
    tier_name              TEXT NOT NULL DEFAULT 'Basic',
    billing_cycle          TEXT NOT NULL DEFAULT 'Monthly',
    period_start           DATE,
    period_end             DATE,
    access_terminated_at   TIMESTAMPTZ,
    notes                  TEXT,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tenant_subscription_single_row CHECK (id = 1),
    CONSTRAINT tenant_subscription_billing_cycle_chk CHECK (billing_cycle IN ('Monthly', 'Quarterly')),
    CONSTRAINT tenant_subscription_tier_fk FOREIGN KEY (tier_name)
        REFERENCES public.vendor_subscription_tiers (tier_name)
);

COMMENT ON TABLE tenant_subscription IS 'One row (id=1): current tenant subscription term and tier.';

INSERT INTO tenant_subscription (id, tier_name, billing_cycle)
VALUES (1, 'Basic', 'Monthly')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- C) Tenant schema: POP (proof of payment) uploads
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subscription_pop_uploads (
    id                     BIGSERIAL PRIMARY KEY,
    uploaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    uploaded_by            TEXT NOT NULL DEFAULT '',
    file_name              TEXT NOT NULL,
    mime_type              TEXT NOT NULL DEFAULT '',
    file_size              BIGINT NOT NULL,
    file_content           BYTEA NOT NULL,
    period_end_applied_to  DATE,
    verified               BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_subscription_pop_uploads_uploaded_at
    ON subscription_pop_uploads (uploaded_at DESC);

COMMENT ON TABLE subscription_pop_uploads IS 'Proof-of-payment files for subscription billing.';
