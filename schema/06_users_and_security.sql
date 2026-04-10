-- Run this script connected to database: farndacred_db
-- Creates users and security_audit_log tables for authentication & audit.

-- Enable extensions (idempotent)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "citext";

-- Role enum for application users
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
        CREATE TYPE user_role AS ENUM ('ADMIN', 'LOAN_OFFICER', 'BORROWER');
    END IF;
END$$;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id                   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email                CITEXT UNIQUE NOT NULL,
    password_hash        TEXT NOT NULL,
    full_name            TEXT NOT NULL,

    role                 user_role NOT NULL DEFAULT 'BORROWER',
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,

    failed_login_attempts INTEGER NOT NULL DEFAULT 0,
    locked_until         TIMESTAMPTZ NULL,
    last_login           TIMESTAMPTZ NULL,

    two_factor_enabled   BOOLEAN NOT NULL DEFAULT FALSE,
    two_factor_secret    TEXT NULL,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);

-- Security audit log for authentication-related events
CREATE TABLE IF NOT EXISTS security_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    user_id         UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    email_used      CITEXT NULL,
    success         BOOLEAN NOT NULL,
    ip_address      INET NULL,
    user_agent      TEXT NULL,
    event_type      TEXT NOT NULL DEFAULT 'LOGIN',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_user ON security_audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_email ON security_audit_log(email_used);