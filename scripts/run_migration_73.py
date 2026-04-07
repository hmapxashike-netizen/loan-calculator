"""
Apply schema/73_restructure_capitalisation_templates.sql
Ensures LOAN_RESTRUCTURE_CAPITALISE includes all required bucket templates.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "73_restructure_capitalisation_templates.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 73 complete: restructure capitalisation templates ensured.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
