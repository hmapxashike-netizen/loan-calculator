"""Granular Journals sub-area access (manual / balance adjustment / approvals)."""

from __future__ import annotations

from typing import FrozenSet, Literal

_JournalsFeature = Literal["manual", "balance_adjustment", "approvals"]

_KEY = {
    "manual": "journals.manual",
    "balance_adjustment": "journals.balance_adjustment",
    "approvals": "journals.approvals",
}


def journals_subfeature_allowed(keys: FrozenSet[str] | set[str], feature: _JournalsFeature) -> bool:
    """
    If the role has ``nav.journals`` but no ``journals.*`` rows (legacy), allow all sub-areas.
    If any ``journals.*`` is present, require the matching key for that sub-area.
    """
    keys = set(keys)
    if "nav.journals" not in keys:
        return False
    granular = [k for k in keys if k.startswith("journals.") and k != "nav.journals"]
    if not granular:
        return True
    return _KEY[feature] in keys
