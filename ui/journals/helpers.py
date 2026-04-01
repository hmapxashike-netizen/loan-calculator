"""Shared helpers for manual journal widget keys and template tag ordering."""

from __future__ import annotations

import re

MANUAL_SUBACCOUNT_PLACEHOLDER = "— Select sub account —"


def widget_key_part(s) -> str:
    x = re.sub(r"[^a-zA-Z0-9]+", "_", str(s))
    x = x.strip("_") or "k"
    return x[:48]


def ordered_system_tags_for_direction(templates: list | None, direction: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    dir_u = (direction or "").strip().upper()
    for t in templates or []:
        if (t.get("direction") or "").strip().upper() != dir_u:
            continue
        tag = t.get("system_tag")
        if tag and tag not in seen:
            seen.add(tag)
            out.append(str(tag))
    return out
