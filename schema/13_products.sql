-- Products: code (unique), name, loan_type, is_active. Config stored in config table as product_config:{code}.
-- Loans reference products by product_code.

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    code        VARCHAR(32) NOT NULL UNIQUE,
    name        VARCHAR(255) NOT NULL,
    loan_type   VARCHAR(64) NOT NULL,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_code ON products(code);
CREATE INDEX IF NOT EXISTS idx_products_is_active ON products(is_active);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'product_code') THEN
    ALTER TABLE loans ADD COLUMN product_code VARCHAR(32);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_loans_product_code ON loans(product_code);
