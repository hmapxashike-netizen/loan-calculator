-- Extended audit columns for journal-aligned unapplied recast (maintain_term / maintain_instalment).
-- Run after 10_unapplied_funds_and_reamortisation.sql.

ALTER TABLE loan_recasts ADD COLUMN IF NOT EXISTS recast_mode VARCHAR(32);
ALTER TABLE loan_recasts ADD COLUMN IF NOT EXISTS previous_principal NUMERIC(18, 2);
ALTER TABLE loan_recasts ADD COLUMN IF NOT EXISTS previous_installment NUMERIC(18, 2);
ALTER TABLE loan_recasts ADD COLUMN IF NOT EXISTS previous_end_date DATE;
ALTER TABLE loan_recasts ADD COLUMN IF NOT EXISTS unapplied_credit_id INTEGER REFERENCES unapplied_funds(id) ON DELETE SET NULL;
ALTER TABLE loan_recasts ADD COLUMN IF NOT EXISTS liquidation_repayment_id INTEGER REFERENCES loan_repayments(id) ON DELETE SET NULL;

COMMENT ON COLUMN loan_recasts.recast_mode IS 'maintain_term | maintain_instalment';
COMMENT ON COLUMN loan_recasts.previous_principal IS 'Loan principal before recast (for reversal restore).';
COMMENT ON COLUMN loan_recasts.previous_installment IS 'Contract instalment before recast.';
COMMENT ON COLUMN loan_recasts.previous_end_date IS 'Maturity/end date before recast.';
COMMENT ON COLUMN loan_recasts.unapplied_credit_id IS 'Unapplied funds credit row consumed by this recast.';
COMMENT ON COLUMN loan_recasts.liquidation_repayment_id IS 'System repayment row for unapplied liquidation allocation.';
