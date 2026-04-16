"""Journals granular RBAC helpers."""

from __future__ import annotations

from rbac.journals_access import journals_subfeature_allowed


def test_journals_legacy_nav_only_allows_all_subfeatures():
    keys = frozenset({"nav.journals"})
    assert journals_subfeature_allowed(keys, "manual") is True
    assert journals_subfeature_allowed(keys, "balance_adjustment") is True
    assert journals_subfeature_allowed(keys, "approvals") is True


def test_journals_granular_requires_specific_key():
    keys = frozenset({"nav.journals", "journals.manual"})
    assert journals_subfeature_allowed(keys, "manual") is True
    assert journals_subfeature_allowed(keys, "balance_adjustment") is False


def test_journals_without_nav_denied():
    keys = frozenset({"journals.manual"})
    assert journals_subfeature_allowed(keys, "manual") is False
