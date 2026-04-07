-- schedule_lines."Date" must fit dd-Mon-yyyy (11 chars). Legacy VARCHAR(10) silently truncated years (e.g. 01-Jan-2024 -> 01-Jan-202).

ALTER TABLE schedule_lines
    ALTER COLUMN "Date" TYPE VARCHAR(32);

COMMENT ON COLUMN schedule_lines."Date" IS 'Instalment date as text: dd-Mon-yyyy or ISO; max 32 chars.';
