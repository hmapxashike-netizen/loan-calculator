-- Align GL journal line amounts with loan engine / loan_daily_state (10 decimal places).
-- Older environments may have created journal_items.debit/credit as NUMERIC(18,2)
-- (e.g. via fix_accounting_schema.sql), which stores 3.5087719298 as 3.50.
--
-- Run on the application database (e.g. farndacred_db / lms_db), not on postgres.

ALTER TABLE journal_items
    ALTER COLUMN debit TYPE NUMERIC(28, 10),
    ALTER COLUMN credit TYPE NUMERIC(28, 10);
