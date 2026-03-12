-- Add absolute fee amount columns alongside the existing rate columns.
--
-- Existing (rate columns, kept):
--   admin_fee       NUMERIC(8,6)  e.g. 0.07   = 7%
--   drawdown_fee    NUMERIC(8,6)  e.g. 0.025  = 2.5%
--   arrangement_fee NUMERIC(8,6)  e.g. 0.025  = 2.5%
--
-- New (amount columns, currency value):
--   admin_fee_amount       NUMERIC(18,2)  absolute fee charged at disbursement
--   drawdown_fee_amount    NUMERIC(18,2)  absolute fee charged at disbursement
--   arrangement_fee_amount NUMERIC(18,2)  absolute fee charged at disbursement
--
-- Identity on disbursement date:
--   disbursed_amount + admin_fee_amount + drawdown_fee_amount + arrangement_fee_amount = principal

ALTER TABLE loans
    ADD COLUMN IF NOT EXISTS admin_fee_amount       NUMERIC(18, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS drawdown_fee_amount    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS arrangement_fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0;

COMMENT ON COLUMN loans.admin_fee_amount       IS 'Absolute admin fee amount at disbursement = principal * admin_fee rate.';
COMMENT ON COLUMN loans.drawdown_fee_amount    IS 'Absolute drawdown fee amount at disbursement = principal * drawdown_fee rate.';
COMMENT ON COLUMN loans.arrangement_fee_amount IS 'Absolute arrangement fee amount at disbursement = principal * arrangement_fee rate.';

-- Backfill existing rows: derive amount from rate * principal.
UPDATE loans
SET
    admin_fee_amount       = COALESCE(ROUND(principal * admin_fee::NUMERIC,       2), 0),
    drawdown_fee_amount    = COALESCE(ROUND(principal * drawdown_fee::NUMERIC,    2), 0),
    arrangement_fee_amount = COALESCE(ROUND(principal * arrangement_fee::NUMERIC, 2), 0)
WHERE admin_fee_amount = 0
  AND drawdown_fee_amount = 0
  AND arrangement_fee_amount = 0;