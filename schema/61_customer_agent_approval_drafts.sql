-- Pending customer / agent field changes that require approver action (e.g. legal name changes).

CREATE TABLE IF NOT EXISTS customer_agent_approval_drafts (
    id                 SERIAL PRIMARY KEY,
    entity_type        VARCHAR(32)  NOT NULL,
    entity_id          INTEGER      NOT NULL,
    action_type        VARCHAR(64)  NOT NULL,
    status             VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
    old_details        JSONB        NOT NULL DEFAULT '{}',
    new_details        JSONB        NOT NULL DEFAULT '{}',
    supporting_document TEXT,
    requested_by       VARCHAR(255),
    reviewer_note      TEXT,
    reviewed_by        VARCHAR(255),
    submitted_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    reviewed_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ca_approval_drafts_status_submitted
    ON customer_agent_approval_drafts (status, submitted_at DESC);

COMMENT ON TABLE customer_agent_approval_drafts IS
    'Workflow drafts for customer/agent updates (e.g. NAME_CHANGE). APPROVED rows have been applied; REWORK/DISMISSED are terminal.';
