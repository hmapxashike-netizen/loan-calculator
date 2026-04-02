import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config

from accounting.defaults_loader import get_chart_account_template_tuples


def seed_db():
    import psycopg2

    conn = psycopg2.connect(config.get_database_url())
    with conn.cursor() as cur:
        for code, name, cat, tag, parent in get_chart_account_template_tuples():
            cur.execute(
                """
                INSERT INTO account_template (code, name, category, system_tag, parent_code)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (code) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    system_tag = EXCLUDED.system_tag,
                    parent_code = EXCLUDED.parent_code
                """,
                (code, name, cat, tag, parent),
            )

    conn.commit()
    conn.close()
    print("Seed complete (account_template from bundled defaults).")


if __name__ == "__main__":
    seed_db()
