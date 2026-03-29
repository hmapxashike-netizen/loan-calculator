"""
Export the current database chart, transaction templates, and receipt GL mappings
to `accounting_defaults/*.json` so Initialize / Restore in the app matches production.

Usage (from repo root):
  python scripts/export_accounting_defaults.py

Requires DATABASE_URL / config.get_database_url() pointing at the source database.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
from psycopg2.extras import RealDictCursor

import config
from accounting_defaults_loader import defaults_directory


def main() -> None:
    conn = psycopg2.connect(config.get_database_url(), cursor_factory=RealDictCursor)
    d = defaults_directory()
    d.mkdir(parents=True, exist_ok=True)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.code, a.name, a.category, a.system_tag, p.code AS parent_code
            FROM accounts a
            LEFT JOIN accounts p ON a.parent_id = p.id
            WHERE COALESCE(a.is_active, TRUE) = TRUE
            ORDER BY a.code
            """
        )
        acct_rows = cur.fetchall()

        cur.execute(
            """
            SELECT event_type, system_tag, direction, description,
                   COALESCE(trigger_type, 'EVENT') AS trigger_type
            FROM transaction_templates
            ORDER BY event_type, system_tag, direction
            """
        )
        tmpl_rows = cur.fetchall()

        cur.execute(
            """
            SELECT trigger_source, allocation_key, event_type, amount_source,
                   amount_sign, priority, is_active
            FROM receipt_gl_mapping
            ORDER BY trigger_source, priority, allocation_key, event_type
            """
        )
        map_rows = cur.fetchall()

    conn.close()

    chart = {
        "version": 1,
        "source": "exported_from_database",
        "accounts": [
            {
                "code": (r["code"] or "").strip(),
                "name": (r["name"] or "").strip(),
                "category": (r["category"] or "").strip(),
                "system_tag": (r["system_tag"] or "").strip() or None,
                "parent_code": (r["parent_code"] or "").strip() or None,
            }
            for r in acct_rows
        ],
    }
    templates = {
        "version": 1,
        "source": "exported_from_database",
        "templates": [
            {
                "event_type": r["event_type"],
                "system_tag": r["system_tag"],
                "direction": r["direction"],
                "description": r.get("description") or "",
                "trigger_type": r.get("trigger_type") or "EVENT",
            }
            for r in tmpl_rows
        ],
    }
    mappings = {
        "version": 1,
        "source": "exported_from_database",
        "mappings": [
            {
                "trigger_source": r["trigger_source"],
                "allocation_key": r["allocation_key"],
                "event_type": r["event_type"],
                "amount_source": r["amount_source"],
                "amount_sign": int(r["amount_sign"]),
                "priority": int(r["priority"]),
                "is_active": bool(r.get("is_active", True)),
            }
            for r in map_rows
        ],
    }

    (d / "chart_of_accounts.json").write_text(
        json.dumps(chart, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (d / "transaction_templates.json").write_text(
        json.dumps(templates, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (d / "receipt_gl_mapping.json").write_text(
        json.dumps(mappings, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(
        f"Exported {len(chart['accounts'])} accounts, "
        f"{len(templates['templates'])} template legs, "
        f"{len(mappings['mappings'])} receipt mappings → {d}"
    )


if __name__ == "__main__":
    main()
