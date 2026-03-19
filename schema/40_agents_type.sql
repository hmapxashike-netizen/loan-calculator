-- 40_agents_type.sql
-- Add agent_type column so we can distinguish individual vs corporate agents.

ALTER TABLE agents
    ADD COLUMN IF NOT EXISTS agent_type VARCHAR(20) NOT NULL DEFAULT 'individual';

