-- Soft-supersede journal entries for auditability.
-- Keeps full replacement history in-place:
--   - active row used by reports (is_active = TRUE)
--   - prior versions marked inactive with superseded_at/superseded_by_id

ALTER TABLE journal_entries
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE journal_entries
  ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE journal_entries
  ADD COLUMN IF NOT EXISTS superseded_by_id UUID REFERENCES journal_entries(id);

-- Ensure deterministic events can be replaced while preserving inactive history.
DROP INDEX IF EXISTS uq_journal_entries_event_id_event_tag;

CREATE UNIQUE INDEX IF NOT EXISTS uq_journal_entries_event_id_event_tag
  ON journal_entries(event_id, event_tag)
  WHERE event_id IS NOT NULL
    AND event_tag IS NOT NULL
    AND is_active = TRUE;
