-- Customer module: Individuals and Corporate entities. No delete; use status active/inactive.
-- Run after 02_schema.sql. If customers already exist with (name, email, phone), they are migrated to individuals.

-- 1. Extend customers: type (individual|corporate), status (active|inactive)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'customers' AND column_name = 'type') THEN
    ALTER TABLE customers ADD COLUMN type VARCHAR(32) NOT NULL DEFAULT 'individual';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'customers' AND column_name = 'status') THEN
    ALTER TABLE customers ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'active';
  END IF;
EXCEPTION WHEN undefined_table THEN
  -- customers table not created yet (run 02_schema first)
  RAISE NOTICE 'Run 02_schema.sql first to create customers table.';
END $$;

-- 2. Individuals (1:1 with customer when type=individual)
CREATE TABLE IF NOT EXISTS individuals (
    customer_id     INTEGER PRIMARY KEY REFERENCES customers(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    national_id     VARCHAR(64),
    employer_details TEXT,
    phone1          VARCHAR(64),
    phone2          VARCHAR(64),
    email1          VARCHAR(255),
    email2          VARCHAR(255),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE individuals IS 'Individual customer details; one row per customer when type=individual';

-- 3. Corporates (1:1 with customer when type=corporate)
CREATE TABLE IF NOT EXISTS corporates (
    customer_id     INTEGER PRIMARY KEY REFERENCES customers(id) ON DELETE CASCADE,
    legal_name      VARCHAR(255) NOT NULL,
    trading_name    VARCHAR(255),
    reg_number      VARCHAR(64),
    tin             VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE corporates IS 'Corporate customer details; one row per customer when type=corporate';

-- 4. Addresses (many per customer)
CREATE TABLE IF NOT EXISTS customer_addresses (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    address_type    VARCHAR(32),   -- e.g. physical, postal, registered
    line1           VARCHAR(255),
    line2           VARCHAR(255),
    city            VARCHAR(128),
    region          VARCHAR(128),
    postal_code     VARCHAR(32),
    country         VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_customer_addresses_customer_id ON customer_addresses(customer_id);

-- 5. Corporate: Contact Person (full name, national_id, designation, phone1, phone2, email, address)
CREATE TABLE IF NOT EXISTS corporate_contact_persons (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    full_name       VARCHAR(255) NOT NULL,
    national_id     VARCHAR(64),
    designation     VARCHAR(128),
    phone1          VARCHAR(64),
    phone2          VARCHAR(64),
    email           VARCHAR(255),
    address_line1   VARCHAR(255),
    address_line2   VARCHAR(255),
    city            VARCHAR(128),
    country         VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_corporate_contact_persons_customer_id ON corporate_contact_persons(customer_id);

-- 6. Corporate: Directors (same structure)
CREATE TABLE IF NOT EXISTS corporate_directors (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    full_name       VARCHAR(255) NOT NULL,
    national_id     VARCHAR(64),
    designation     VARCHAR(128),
    phone1          VARCHAR(64),
    phone2          VARCHAR(64),
    email           VARCHAR(255),
    address_line1   VARCHAR(255),
    address_line2   VARCHAR(255),
    city            VARCHAR(128),
    country         VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_corporate_directors_customer_id ON corporate_directors(customer_id);

-- 7. Corporate: Shareholders (same + shareholding_pct)
CREATE TABLE IF NOT EXISTS corporate_shareholders (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    full_name       VARCHAR(255) NOT NULL,
    national_id     VARCHAR(64),
    designation     VARCHAR(128),
    phone1          VARCHAR(64),
    phone2          VARCHAR(64),
    email           VARCHAR(255),
    address_line1   VARCHAR(255),
    address_line2   VARCHAR(255),
    city            VARCHAR(128),
    country         VARCHAR(64),
    shareholding_pct NUMERIC(6, 2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_corporate_shareholders_customer_id ON corporate_shareholders(customer_id);

-- 8. Migrate existing customers (name, email, phone) into individuals if columns still exist
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'customers' AND column_name = 'name') THEN
    INSERT INTO individuals (customer_id, name, phone1, email1)
    SELECT id, COALESCE(name, ''), phone, email FROM customers c
    ON CONFLICT (customer_id) DO NOTHING;
    ALTER TABLE customers DROP COLUMN IF EXISTS name;
    ALTER TABLE customers DROP COLUMN IF EXISTS email;
    ALTER TABLE customers DROP COLUMN IF EXISTS phone;
  END IF;
END $$;
