-- 43_eod_audit_log.sql
-- Append-only EOD audit logging for run/stage visibility.

CREATE TABLE IF NOT EXISTS eod_runs (
    run_id UUID PRIMARY KEY,
    as_of_date DATE NOT NULL,
    run_status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    failed_stage VARCHAR(100) NULL,
    error_message TEXT NULL,
    policy_mode VARCHAR(20) NOT NULL DEFAULT 'hybrid',
    advance_on_degraded BOOLEAN NOT NULL DEFAULT FALSE,
    tasks_cfg JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_eod_runs_as_of_date
    ON eod_runs (as_of_date, started_at DESC);

CREATE INDEX IF NOT EXISTS ix_eod_runs_status
    ON eod_runs (run_status, started_at DESC);

CREATE TABLE IF NOT EXISTS eod_stage_events (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES eod_runs(run_id) ON DELETE CASCADE,
    stage_name VARCHAR(100) NOT NULL,
    is_blocking BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(20) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error_message TEXT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_eod_stage_events_run
    ON eod_stage_events (run_id, id);

