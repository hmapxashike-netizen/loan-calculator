"""
Apply schema/49_unapplied_liquidation_gl_templates.sql

Inserts UNAPPLIED_LIQUIDATION_* transaction templates and soft-deactivates legacy
liquidation journals that incorrectly used bank-side PAYMENT_* event types.

After running: repost GL for affected loans (repost_gl_for_loan_date_range).
"""

import os
import sys

import psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config


def run_migration():
    conn = psycopg2.connect(config.get_database_url())
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schema", "49_unapplied_liquidation_gl_templates.sql"
    )
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    conn.close()
    print(
        "Migration 49 complete: unapplied liquidation GL templates added; "
        "legacy bank-debit liquidation journals marked inactive. "
        "Run repost_gl_for_loan_date_range for affected loans."
    )


if __name__ == "__main__":
    run_migration()
