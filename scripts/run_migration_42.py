import os
import psycopg2
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config


def run_migration():
    conn = psycopg2.connect(config.get_database_url())

    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schema", "42_document_categories_required_seed.sql"
    )

    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()
    conn.close()
    print("Migration 42 complete: required document categories ensured.")


if __name__ == "__main__":
    run_migration()

