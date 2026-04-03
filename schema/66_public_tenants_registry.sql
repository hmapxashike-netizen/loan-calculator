-- Master tenant registry (public): maps company display name → PostgreSQL schema (schema-per-tenant).
-- Apply after core database exists. Run as a user with CREATE privileges on public.

CREATE TABLE IF NOT EXISTS public.tenants (
    id              BIGSERIAL PRIMARY KEY,
    company_name    TEXT NOT NULL,
    schema_name     TEXT NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenants_schema_name_unique UNIQUE (schema_name),
    CONSTRAINT tenants_company_name_not_empty CHECK (length(trim(company_name)) > 0),
    CONSTRAINT tenants_schema_name_not_empty CHECK (length(trim(schema_name)) > 0)
);

-- One active logical company per case-insensitive name (prevents duplicate "Acme" / "acme").
CREATE UNIQUE INDEX IF NOT EXISTS tenants_company_name_lower_active_unique
    ON public.tenants (lower(trim(company_name)))
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS tenants_schema_name_idx ON public.tenants (schema_name);
CREATE INDEX IF NOT EXISTS tenants_is_active_idx ON public.tenants (is_active) WHERE is_active = TRUE;

COMMENT ON TABLE public.tenants IS 'Master lookup: company_name → schema_name for multi-tenant search_path routing.';
COMMENT ON COLUMN public.tenants.schema_name IS 'PostgreSQL schema identifier; must match app validation [a-zA-Z_][a-zA-Z0-9_]*';

-- Example seed (adjust schema_name to an existing tenant schema in your cluster):
-- INSERT INTO public.tenants (company_name, schema_name, is_active)
-- VALUES ('Farnda Demo', 'tenant_default', TRUE)
-- ON CONFLICT (schema_name) DO NOTHING;
