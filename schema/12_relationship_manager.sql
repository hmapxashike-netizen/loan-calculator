-- Add relationship_manager_id to loans (internal staff from users table).
-- Agent = external broker; Relationship manager = internal user.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'loans' AND column_name = 'relationship_manager_id') THEN
    ALTER TABLE loans ADD COLUMN relationship_manager_id UUID REFERENCES users(id) ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_loans_relationship_manager_id ON loans(relationship_manager_id);
