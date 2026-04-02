"""
Write `accounting_defaults/*.json` from built-in Python fallbacks.

Run after clone or when you want JSON defaults to match `accounting.builtin_defaults`.
To capture your live perfected database instead, use `export_accounting_defaults.py`.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from accounting.builtin_defaults import (
    CHART_ACCOUNT_TUPLES,
    TRANSACTION_TEMPLATE_TUPLES,
    build_receipt_gl_mapping_tuples,
)
from accounting.defaults_loader import defaults_directory


def main() -> None:
    d = defaults_directory()
    d.mkdir(parents=True, exist_ok=True)

    chart = {
        "version": 1,
        "description": "Chart of accounts template rows (parent_code before children not required; loader topologically sorts).",
        "accounts": [
            {
                "code": code,
                "name": name,
                "category": cat,
                "system_tag": tag,
                "parent_code": parent,
            }
            for code, name, cat, tag, parent in CHART_ACCOUNT_TUPLES
        ],
    }
    templates = {
        "version": 1,
        "templates": [
            {
                "event_type": evt,
                "system_tag": tag,
                "direction": direction,
                "description": desc,
                "trigger_type": trig,
            }
            for evt, tag, direction, desc, trig in TRANSACTION_TEMPLATE_TUPLES
        ],
    }
    mappings = {
        "version": 1,
        "mappings": [
            {
                "trigger_source": r[0],
                "allocation_key": r[1],
                "event_type": r[2],
                "amount_source": r[3],
                "amount_sign": r[4],
                "priority": r[5],
                "is_active": True,
            }
            for r in build_receipt_gl_mapping_tuples()
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
    print(f"Wrote bundled defaults under {d}")


if __name__ == "__main__":
    main()
