CREATE TABLE IF NOT EXISTS document_classes (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE document_categories ADD COLUMN IF NOT EXISTS class_id INTEGER REFERENCES document_classes(id);

-- Insert default classes
INSERT INTO document_classes (name, description) VALUES
('Know Your Customer', 'KYC and identification documents'),
('Financial Information', 'Bank statements, management accounts, projections, etc.'),
('Agreements', 'Facility Letter, Term sheet, loan agreements, etc.'),
('Credit Proposal', 'Credit assessments and proposals'),
('Other', 'Other documents')
ON CONFLICT (name) DO NOTHING;

-- Map existing categories to classes where possible
UPDATE document_categories SET class_id = (SELECT id FROM document_classes WHERE name = 'Know Your Customer') WHERE name IN ('National ID', 'Proof of Residence');
UPDATE document_categories SET class_id = (SELECT id FROM document_classes WHERE name = 'Financial Information') WHERE name IN ('Bank Statement', 'Payslip');
UPDATE document_categories SET class_id = (SELECT id FROM document_classes WHERE name = 'Agreements') WHERE name IN ('Signed Loan Agreement', 'Offer Letter');

-- Set default class for any unmapped categories
UPDATE document_categories SET class_id = (SELECT id FROM document_classes WHERE name = 'Other') WHERE class_id IS NULL;
