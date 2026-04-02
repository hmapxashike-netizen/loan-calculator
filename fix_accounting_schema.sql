-- 1) Ensure parent-account fields exist on accounts
ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS is_parent BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS transitioned_to_parent_at TIMESTAMP NULL;

-- 2) Ensure journal_entries has the columns expected by accounting.dal.save_journal_entry

-- Core columns used by the INSERT
ALTER TABLE journal_entries
    ADD COLUMN IF NOT EXISTS entry_date DATE NOT NULL DEFAULT CURRENT_DATE,
    ADD COLUMN IF NOT EXISTS reference TEXT,
    ADD COLUMN IF NOT EXISTS description TEXT,
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS event_tag TEXT,
    ADD COLUMN IF NOT EXISTS created_by TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();

-- If id is not already a primary key sequence, you may need:
-- (COMMENT THESE OUT if you already have a proper PK)
-- ALTER TABLE journal_entries
--     ADD COLUMN IF NOT EXISTS id SERIAL PRIMARY KEY;

-- 3) Ensure journal_items matches what save_journal_entry inserts

ALTER TABLE journal_items
    ADD COLUMN IF NOT EXISTS entry_id INTEGER,
    ADD COLUMN IF NOT EXISTS account_id INTEGER,
    ADD COLUMN IF NOT EXISTS debit NUMERIC(18,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS credit NUMERIC(18,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS memo TEXT;

-- 4) Prevent duplicate journal headers for the same deterministic (event_id, event_tag).
-- EOD uses stable event_id/event_tag; reruns should overwrite rather than duplicate.
CREATE UNIQUE INDEX IF NOT EXISTS uq_journal_entries_event_id_event_tag
    ON journal_entries(event_id, event_tag)
    WHERE event_id IS NOT NULL AND event_tag IS NOT NULL;

-- Optional but recommended: foreign keys (comment out if you already have them)
-- ALTER TABLE journal_items
--     ADD CONSTRAINT fk_journal_items_entry
--         FOREIGN KEY (entry_id) REFERENCES journal_entries(id)
--         ON DELETE CASCADE;
--
-- ALTER TABLE journal_items
--     ADD CONSTRAINT fk_journal_items_account
--         FOREIGN KEY (account_id) REFERENCES accounts(id);