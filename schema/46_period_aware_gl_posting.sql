-- Period-aware, auditable GL posting support.

ALTER TABLE journal_entries
  ADD COLUMN IF NOT EXISTS entry_type VARCHAR(50) DEFAULT 'EVENT';

ALTER TABLE journal_entries
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE journal_entries
  ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE journal_entries
  ADD COLUMN IF NOT EXISTS superseded_by_id UUID REFERENCES journal_entries(id);

CREATE TABLE IF NOT EXISTS financial_periods (
    period_key VARCHAR(7) PRIMARY KEY, -- YYYY-MM
    is_closed BOOLEAN NOT NULL DEFAULT FALSE,
    closed_at TIMESTAMP WITH TIME ZONE
);

DROP INDEX IF EXISTS uq_journal_entries_event_id_event_tag;

CREATE UNIQUE INDEX IF NOT EXISTS uq_journal_entries_event_id_event_tag
  ON journal_entries(event_id, event_tag)
  WHERE event_id IS NOT NULL
    AND event_tag IS NOT NULL
    AND is_active = TRUE;

-- Ensure at least one open period exists (current month).
INSERT INTO financial_periods (period_key, is_closed, closed_at)
VALUES (to_char(CURRENT_DATE, 'YYYY-MM'), FALSE, NULL)
ON CONFLICT (period_key) DO NOTHING;
