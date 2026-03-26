import os
import psycopg2

def _env_with_legacy(new_key: str, legacy_key: str, default: str) -> str:
    return os.environ.get(new_key) or os.environ.get(legacy_key, default)

def run_migration():
    conn = psycopg2.connect(
        host=_env_with_legacy("FARNDACRED_DB_HOST", "LMS_DB_HOST", "localhost"),
        port=_env_with_legacy("FARNDACRED_DB_PORT", "LMS_DB_PORT", "5432"),
        dbname=_env_with_legacy("FARNDACRED_DB_NAME", "LMS_DB_NAME", "lms_db"),
        user=_env_with_legacy("FARNDACRED_DB_USER", "LMS_DB_USER", "postgres"),
        password=_env_with_legacy("FARNDACRED_DB_PASSWORD", "LMS_DB_PASSWORD", "postgres"),
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
