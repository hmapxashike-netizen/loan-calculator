import os
import psycopg2
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config


def run_migration():
    conn = psycopg2.connect(config.get_database_url())

    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schema", "47_total_delinquency_arrears_column.sql"
    )

    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()
    conn.close()
    print("Migration 47 complete: total_delinquency_arrears column added/backfilled.")


if __name__ == "__main__":
    run_migration()

