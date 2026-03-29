"""
Apply schema/50_grandchild_codes_posting_maps.sql

Widen account codes, add subaccount_resolution, disbursement_bank_options,
product_gl_subaccount_map, and loans.disbursement_bank_option_id.
"""

import os
import sys

import psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config


def run_migration():
    conn = psycopg2.connect(config.get_database_url())
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schema", "50_grandchild_codes_posting_maps.sql"
    )
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    conn.close()
    print(
        "Migration 50 complete: grandchild-friendly codes, subaccount_resolution, "
        "disbursement bank options, product GL map, loans.disbursement_bank_option_id."
    )


if __name__ == "__main__":
    run_migration()
