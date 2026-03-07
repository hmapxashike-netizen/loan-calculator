-- Ensure config table can store all product configurations.
-- Product configs are stored as key = 'product_config:' || code (code max 32), value = JSON.
-- key: VARCHAR(128) fits 'product_config:' + 32-char code. value: TEXT = no practical limit (PostgreSQL).

DO $$
BEGIN
  -- Ensure value column is TEXT (unbounded) so large product config JSON is always allowed
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'config' AND column_name = 'value') THEN
    IF (SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'config' AND column_name = 'value') IN ('character varying', 'varchar') THEN
      ALTER TABLE config ALTER COLUMN value TYPE TEXT;
    END IF;
  END IF;
END $$;

COMMENT ON TABLE config IS 'Key-value config: system_config, and product_config:{code} per product. value is JSON (TEXT = no size limit).';
