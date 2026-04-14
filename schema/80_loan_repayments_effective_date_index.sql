-- Speed EOD batch allocation queries and statement repayment ranges that filter on
-- loan_id + COALESCE(value_date, payment_date) with posted/reversed status.
-- Safe to run multiple times (IF NOT EXISTS).

CREATE INDEX IF NOT EXISTS idx_loan_repayments_loan_eff_date_posted
    ON loan_repayments (loan_id, (COALESCE(value_date, payment_date)))
    WHERE status IN ('posted', 'reversed');
