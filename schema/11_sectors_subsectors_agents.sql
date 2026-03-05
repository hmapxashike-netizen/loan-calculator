-- Sectors, subsectors (configurable; used on customers), and agents (used on loans).
-- Run after 02_schema.sql and 03_customers.sql.

-- 1. Sectors (user-configured in System configurations)
CREATE TABLE IF NOT EXISTS sectors (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(128) NOT NULL UNIQUE,
    sort_order  INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sectors_sort ON sectors(sort_order);

-- 2. Subsectors (per sector; user-configured)
CREATE TABLE IF NOT EXISTS subsectors (
    id          SERIAL PRIMARY KEY,
    sector_id   INTEGER NOT NULL REFERENCES sectors(id) ON DELETE CASCADE,
    name        VARCHAR(128) NOT NULL,
    sort_order  INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (sector_id, name)
);
CREATE INDEX IF NOT EXISTS idx_subsectors_sector ON subsectors(sector_id);

-- 3. Agents (brokers/referrers; commission, TIN, tax clearance)
CREATE TABLE IF NOT EXISTS agents (
    id                      SERIAL PRIMARY KEY,
    name                    VARCHAR(255) NOT NULL,
    id_number               VARCHAR(32),         -- e.g. 111111111x11
    address_line1           VARCHAR(255),
    address_line2           VARCHAR(255),
    city                    VARCHAR(128),
    country                 VARCHAR(64),
    phone1                  VARCHAR(64),
    phone2                  VARCHAR(64),
    email                   VARCHAR(255),
    commission_rate_pct     NUMERIC(6, 2),
    tin_number              VARCHAR(64),
    tax_clearance_expiry    DATE,
    status                  VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_agents_status ON agents(status);

-- 4. Customers: add sector and subsector
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'customers' AND column_name = 'sector_id') THEN
    ALTER TABLE customers ADD COLUMN sector_id INTEGER REFERENCES sectors(id) ON DELETE SET NULL;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'customers' AND column_name = 'subsector_id') THEN
    ALTER TABLE customers ADD COLUMN subsector_id INTEGER REFERENCES subsectors(id) ON DELETE SET NULL;
  END IF;
END $$;

-- 5. Loans: add agent (scheme already exists)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'agent_id') THEN
    ALTER TABLE loans ADD COLUMN agent_id INTEGER REFERENCES agents(id) ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_loans_agent_id ON loans(agent_id);
CREATE INDEX IF NOT EXISTS idx_customers_sector ON customers(sector_id);
CREATE INDEX IF NOT EXISTS idx_customers_subsector ON customers(subsector_id);
