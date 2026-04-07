-- Add restructure fee amount for modification-fee charge + amortisation tracking.

ALTER TABLE loan_modifications
    ADD COLUMN IF NOT EXISTS restructure_fee_amount NUMERIC(22, 10) NOT NULL DEFAULT 0;
