-- Loan details: add first_repayment_date and payment_timing for capture/reporting.
-- Repayments table: actual payments/receipts (distinct from schedule instalments).

-- Add columns to loans if not present (for first_repayment_date, payment_timing)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'first_repayment_date') THEN
    ALTER TABLE loans ADD COLUMN first_repayment_date DATE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'payment_timing') THEN
    ALTER TABLE loans ADD COLUMN payment_timing VARCHAR(64);  -- e.g. 'anniversary', 'last_day_of_month'
  END IF;
END $$;

-- Repayments: actual payment/receipt details (when money is received vs schedule which is the plan)
CREATE TABLE IF NOT EXISTS loan_repayments (
    id                  SERIAL PRIMARY KEY,
    loan_id             INTEGER NOT NULL REFERENCES loans(id) ON DELETE CASCADE,
    schedule_line_id    INTEGER REFERENCES schedule_lines(id) ON DELETE SET NULL,  -- optional link to schedule period
    period_number       INTEGER,          -- period this payment relates to (if known)
    amount              NUMERIC(18, 2) NOT NULL,
    payment_date        DATE NOT NULL,
    reference           VARCHAR(255),     -- receipt ref, transaction id, etc.
    status              VARCHAR(32) NOT NULL DEFAULT 'posted',  -- posted, reversed, etc.
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE loan_repayments IS 'Actual payments/receipts against a loan; schedule_lines hold the planned instalments.';
CREATE INDEX IF NOT EXISTS idx_loan_repayments_loan_id ON loan_repayments(loan_id);
CREATE INDEX IF NOT EXISTS idx_loan_repayments_payment_date ON loan_repayments(payment_date);
