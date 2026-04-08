-- Dynamic roles: RBAC tables and users.role as VARCHAR (custom roles supported).
-- Run after 06_users_and_security.sql and user_role enum extensions.

CREATE TABLE IF NOT EXISTS rbac_roles (
    id                  SERIAL PRIMARY KEY,
    role_key            VARCHAR(64) NOT NULL UNIQUE,
    display_name        TEXT NOT NULL,
    is_system           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rbac_permissions (
    permission_key                      VARCHAR(128) PRIMARY KEY,
    label                               TEXT NOT NULL,
    category                            VARCHAR(64) NOT NULL,
    summary                             TEXT NOT NULL,
    grants_md                           TEXT NOT NULL,
    risk_tag                            VARCHAR(32) NOT NULL DEFAULT 'standard',
    grant_restricted_to_superadmin      BOOLEAN NOT NULL DEFAULT FALSE,
    nav_section                         VARCHAR(128) NULL,
    updated_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rbac_role_permissions (
    role_id             INT NOT NULL REFERENCES rbac_roles(id) ON DELETE CASCADE,
    permission_key      VARCHAR(128) NOT NULL REFERENCES rbac_permissions(permission_key) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_key)
);

CREATE INDEX IF NOT EXISTS idx_rbac_role_permissions_role ON rbac_role_permissions(role_id);

-- Allow arbitrary role keys on users (no longer bound to PostgreSQL enum).
ALTER TABLE users ALTER COLUMN role DROP DEFAULT;
ALTER TABLE users ALTER COLUMN role TYPE VARCHAR(64) USING role::text;
ALTER TABLE users ALTER COLUMN role SET DEFAULT 'BORROWER';
