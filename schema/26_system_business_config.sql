-- System Business Date: decouples business date from physical calendar.
-- Used for EOD, accruals, and Amount Due logic.

CREATE TABLE IF NOT EXISTS system_business_config (
    id                      SERIAL PRIMARY KEY,
    current_system_date     DATE NOT NULL DEFAULT '2025-11-02',
    eod_auto_run_time      TIME NOT NULL DEFAULT '23:00:00',
    is_auto_eod_enabled    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = 1)
);

COMMENT ON TABLE system_business_config IS 'System business date and EOD automation. Only one row (id=1).';

-- Ensure we have exactly one row (id=1)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM system_business_config WHERE id = 1) THEN
    INSERT INTO system_business_config (id, current_system_date, eod_auto_run_time, is_auto_eod_enabled)
    VALUES (1, '2025-11-02'::date, '23:00:00'::time, FALSE);
  END IF;
END $$;
