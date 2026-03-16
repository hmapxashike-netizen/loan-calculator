import sys
import os
import psycopg2

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

def run_migration():
    conn = psycopg2.connect(config.get_database_url())
    cur = conn.cursor()
    
    try:
        # Add trigger_type column if it doesn't exist
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='transaction_templates' AND column_name='trigger_type') THEN
                    ALTER TABLE transaction_templates ADD COLUMN trigger_type VARCHAR(10) DEFAULT 'EVENT';
                END IF;
            END
            $$;
        """)
        conn.commit()
        print("Migration 37 applied successfully: Added trigger_type to transaction_templates.")
    except Exception as e:
        conn.rollback()
        print(f"Error applying migration: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    run_migration()
