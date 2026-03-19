-- 41_document_categories_corporate_kyc.sql
-- Seed/ensure core corporate KYC categories.

INSERT INTO document_categories (name, description)
VALUES
  ('CR5', 'Corporate CR5 document'),
  ('CR6', 'Corporate CR6 document'),
  ('Memorandum and Articles', 'Company memorandum and articles'),
  ('Certificate of Incorporation', 'Company certificate of incorporation'),
  ('CR2', 'Company CR2 document'),
  ('Other', 'Generic corporate document (user will describe in notes)')
ON CONFLICT (name) DO UPDATE
SET description = EXCLUDED.description,
    is_active = TRUE;

