-- Pipeline status codes can exceed 32 chars (e.g. PAYROLL_DEDUCTION_APPROVAL_GRANTED = 34).
ALTER TABLE loan_applications
    ALTER COLUMN status TYPE VARCHAR(64);
