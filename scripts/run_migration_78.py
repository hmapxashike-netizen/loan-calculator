"""
Apply schema/78_rbac_dynamic_roles.sql and seed RBAC defaults.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url
from rbac.seed import seed_rbac_defaults


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(root, "schema", "78_rbac_dynamic_roles.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        seed_rbac_defaults(conn)
        print("Migration 78 complete: RBAC tables, users.role as VARCHAR, defaults seeded.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
