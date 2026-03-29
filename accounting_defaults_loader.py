"""
Load bundled accounting defaults from `accounting_defaults/*.json` when present,
otherwise fall back to `accounting_builtin_defaults`.

Refresh JSON from your perfected database:
  python scripts/export_accounting_defaults.py

Initialise JSON files from built-in Python fallbacks (e.g. after clone):
  python scripts/bootstrap_accounting_defaults_from_builtin.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from accounting_builtin_defaults import (
    CHART_ACCOUNT_TUPLES,
    TRANSACTION_TEMPLATE_TUPLES,
    build_receipt_gl_mapping_tuples,
)

_DEFAULTS_DIR = Path(__file__).resolve().parent / "accounting_defaults"


def defaults_directory() -> Path:
    return _DEFAULTS_DIR


def _read_json(name: str) -> dict[str, Any] | None:
    p = _DEFAULTS_DIR / name
    if not p.is_file():
        return None
    with p.open(encoding="utf-8") as f:
        return json.load(f)


def _chart_rows_topological(accounts: list[dict[str, Any]]) -> list[tuple[str, str, str, str | None, str | None]]:
    """Parents before children (by parent_code link)."""
    by_code = {str(a["code"]).strip(): a for a in accounts if a.get("code")}
    remaining = set(by_code.keys())
    out: list[tuple[str, str, str, str | None, str | None]] = []
    while remaining:
        batch = []
        for c in remaining:
            raw_p = by_code[c].get("parent_code")
            pc = str(raw_p).strip() if raw_p else None
            if pc is None or pc not in remaining:
                batch.append(c)
        if not batch:
            batch = sorted(remaining)
        for c in sorted(batch):
            a = by_code[c]
            tag = a.get("system_tag")
            raw_p = a.get("parent_code")
            pco = str(raw_p).strip() if raw_p else None
            out.append(
                (
                    str(a["code"]).strip(),
                    str(a.get("name") or "").strip(),
                    str(a.get("category") or "").strip(),
                    str(tag).strip() if tag else None,
                    pco,
                )
            )
            remaining.discard(c)
    return out


def get_chart_account_template_tuples() -> list[tuple[str, str, str, str | None, str | None]]:
    data = _read_json("chart_of_accounts.json")
    if data and isinstance(data.get("accounts"), list):
        return _chart_rows_topological(data["accounts"])
    return list(CHART_ACCOUNT_TUPLES)


def get_default_transaction_template_tuples() -> list[tuple[str, str, str, str, str]]:
    data = _read_json("transaction_templates.json")
    if data and isinstance(data.get("templates"), list):
        rows: list[tuple[str, str, str, str, str]] = []
        for t in data["templates"]:
            rows.append(
                (
                    str(t["event_type"]),
                    str(t["system_tag"]),
                    str(t["direction"]),
                    str(t.get("description") or ""),
                    str(t.get("trigger_type") or "EVENT"),
                )
            )
        return rows
    return list(TRANSACTION_TEMPLATE_TUPLES)


def get_default_receipt_gl_mapping_tuples() -> list[tuple[str, str, str, str, int, int]]:
    data = _read_json("receipt_gl_mapping.json")
    if data and isinstance(data.get("mappings"), list):
        rows: list[tuple[str, str, str, str, int, int]] = []
        for m in data["mappings"]:
            if m.get("is_active") is False:
                continue
            rows.append(
                (
                    str(m["trigger_source"]),
                    str(m["allocation_key"]),
                    str(m["event_type"]),
                    str(m["amount_source"]),
                    int(m.get("amount_sign", 1)),
                    int(m.get("priority", 100)),
                )
            )
        return rows
    return build_receipt_gl_mapping_tuples()
