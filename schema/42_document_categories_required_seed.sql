-- 42_document_categories_required_seed.sql
-- Ensure document_categories exist for the whitelisted KYC/loan document types.

INSERT INTO document_categories (name, description, is_active)
VALUES
  -- Individual / Director / Contact Person KYC
  ('National ID', 'National Identification Document', TRUE),
  ('Proof of Residence', 'Utility bill or similar proof of address', TRUE),
  ('Payslip', 'Recent payslip', TRUE),
  ('Confirmation of Employment', 'Employment confirmation / letter', TRUE),
  ('Tax Clearance', 'Tax clearance certificate', TRUE),
  -- Corporate KYC (also seeded in migration 41, but safe to re-run)
  ('CR5', 'Corporate CR5 document', TRUE),
  ('CR6', 'Corporate CR6 document', TRUE),
  ('Memorandum and Articles', 'Company memorandum and articles', TRUE),
  ('Certificate of Incorporation', 'Company certificate of incorporation', TRUE),
  ('CR2', 'Company CR2 document', TRUE),
  -- Loan capture documents
  ('Signed Loan Agreement', 'Signed agreement document', TRUE),
  ('Facility Letter', 'Facility letter / term letter', TRUE),
  ('Term Sheet', 'Loan term sheet', TRUE),
  ('Business Plan', 'Business plan document', TRUE),
  ('Application Form', 'Loan application form', TRUE),
  ('Application Letter', 'Loan application letter', TRUE),
  ('Purchase Orders', 'Purchase orders / PO documents', TRUE),
  ('Offtake Agreement', 'Offtake agreement', TRUE),
  ('Supply Agreement', 'Supply agreement', TRUE),
  -- Generic fallback
  ('Other', 'Other document (user will describe in notes)', TRUE)
ON CONFLICT (name) DO UPDATE
SET
  description = EXCLUDED.description,
  is_active = TRUE;

