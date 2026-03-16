import os
import psycopg2
import sys

# Add parent directory to path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config

def run_migration():
    conn = psycopg2.connect(config.get_database_url())
    
    schema_path = os.path.join(os.path.dirname(__file__), "..", "schema", "36_accounting_module.sql")
    
    with open(schema_path, "r") as f:
        sql = f.read()
        
    with conn.cursor() as cur:
        cur.execute(sql)
        
    conn.commit()
    conn.close()
    print("Migration 36 complete.")

if __name__ == "__main__":
    run_migration()
