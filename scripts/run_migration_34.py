import os
import psycopg2

def run_migration():
    conn = psycopg2.connect(
        host=os.environ.get("LMS_DB_HOST", "localhost"),
        port=os.environ.get("LMS_DB_PORT", "5432"),
        dbname=os.environ.get("LMS_DB_NAME", "lms_db"),
        user=os.environ.get("LMS_DB_USER", "postgres"),
        password=os.environ.get("LMS_DB_PASSWORD", "postgres"),
    )
    
    schema_path = os.path.join(os.path.dirname(__file__), "..", "schema", "34_document_management.sql")
    
    with open(schema_path, "r") as f:
        sql = f.read()
        
    with conn.cursor() as cur:
        cur.execute(sql)
        
    conn.commit()
    conn.close()
    print("Migration 34 complete.")

if __name__ == "__main__":
    run_migration()
