-- Provisioning: security maintenance (haircuts), PD bands by DPD, loan collateral capture fields.
-- Run on farndacred_db after 52_loan_daily_state_interest_in_suspense_balances.sql

CREATE TABLE IF NOT EXISTS provision_security_subtypes (
    id                          SERIAL PRIMARY KEY,
    security_type               VARCHAR(128) NOT NULL,
    subtype_name                VARCHAR(255) NOT NULL,
    typical_haircut_pct         NUMERIC(22, 10) NOT NULL DEFAULT 0,
    system_notes                TEXT,
    is_active                   BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order                  INTEGER NOT NULL DEFAULT 0,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (security_type, subtype_name)
);

CREATE TABLE IF NOT EXISTS provision_pd_bands (
    id                          SERIAL PRIMARY KEY,
    status_label                VARCHAR(64) NOT NULL,
    dpd_min                     INTEGER NOT NULL,
    dpd_max                     INTEGER NULL,
    pd_rate_pct                 NUMERIC(22, 10) NOT NULL,
    is_active                   BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order                  INTEGER NOT NULL DEFAULT 0,
    UNIQUE (status_label)
);

COMMENT ON TABLE provision_security_subtypes IS
    'Collateral types for IFRS-style provision: haircut applied to min(charge, valuation).';
COMMENT ON TABLE provision_pd_bands IS
    'DPD → status/PD% for security-based provision (editable rates). dpd_max NULL = open-ended (e.g. 181+).';

ALTER TABLE loans
    ADD COLUMN IF NOT EXISTS collateral_security_subtype_id INTEGER REFERENCES provision_security_subtypes(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS collateral_charge_amount NUMERIC(22, 10),
    ADD COLUMN IF NOT EXISTS collateral_valuation_amount NUMERIC(22, 10);

COMMENT ON COLUMN loans.collateral_security_subtype_id IS 'FK to provision_security_subtypes; set at loan capture.';
COMMENT ON COLUMN loans.collateral_charge_amount IS 'Registered / agreed security value (10dp).';
COMMENT ON COLUMN loans.collateral_valuation_amount IS 'Market valuation (10dp).';

-- Seed security maintenance (starting table; editable in UI).
INSERT INTO provision_security_subtypes (security_type, subtype_name, typical_haircut_pct, system_notes, sort_order)
VALUES
    ('Cash/Financial', 'Cash Deposit / Fixed Deposit', 5, 'Highest quality. Near-zero risk.', 10),
    ('Cash/Financial', 'Listed Shares (ZSE/VFEX)', 70, 'High volatility; requires weekly revaluation.', 20),
    ('Cash/Financial', 'Treasury Bills (TBs)', 20, 'Government-backed but liquidity varies.', 30),
    ('Guarantees', 'Bank Guarantee (Local)', 10, 'Depends on the issuing bank''s tier.', 40),
    ('Guarantees', 'Insurance Guarantee', 30, 'Must be from an IPEC-registered insurer.', 50),
    ('Guarantees', 'Suretyship guarantee', 100, NULL, 60),
    ('Immovable', 'Residential Property', 10, 'Most stable. Requires Title Deeds.', 70),
    ('Immovable', 'Commercial / Industrial', 10, 'Harder to liquidate in a downturn.', 80),
    ('Immovable', 'Land / Serviced Stands', 20, 'High risk if not fully serviced/developed.', 90),
    ('Movable', 'Motor Vehicles', 90, 'Depreciates fast. Requires Blue Book value.', 100),
    ('Movable', 'Equipment / Machinery', 90, 'Highly specialized; hard to resell.', 110),
    ('Movable', 'Livestock (Cattle)', 90, 'High risk (disease/death); needs insurance.', 120),
    ('Movable', 'Household Effects', 100, 'Generally psychological collateral.', 130)
ON CONFLICT (security_type, subtype_name) DO NOTHING;

-- Seed PD key (editable % in UI).
INSERT INTO provision_pd_bands (status_label, dpd_min, dpd_max, pd_rate_pct, sort_order)
VALUES
    ('Standard', 0, 30, 1, 10),
    ('Watchlist', 31, 60, 10, 20),
    ('Substandard', 61, 90, 25, 30),
    ('Doubtful', 91, 180, 50, 40),
    ('Loss', 181, NULL, 100, 50)
ON CONFLICT (status_label) DO NOTHING;
