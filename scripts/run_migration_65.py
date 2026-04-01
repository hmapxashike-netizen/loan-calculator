"""
Run schema/65_loan_grade_scale_provision_pct.sql
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_database_url
import psycopg2


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "65_loan_grade_scale_provision_pct.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Migration 65 complete: loan_grade_scale_rules provision % columns.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
