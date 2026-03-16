import sys
import os
import psycopg2

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

def run_migration():
    conn = psycopg2.connect(config.get_database_url())
    cur = conn.cursor()
    
    try:
        # Add interest_in_suspense column to loans if it doesn't exist
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='loans' AND column_name='interest_in_suspense') THEN
                    ALTER TABLE loans ADD COLUMN interest_in_suspense BOOLEAN DEFAULT FALSE;
                END IF;
            END
            $$;
        """)
        
        conn.commit()
        print("Migration 38 applied successfully: Added interest_in_suspense to loans.")
    except Exception as e:
        conn.rollback()
        print(f"Error applying migration: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    run_migration()
