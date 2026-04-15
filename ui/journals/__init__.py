"""Standalone Journals UI (manual journals + balance adjustments)."""

from __future__ import annotations

from typing import Any

__all__ = ["render_journals_ui", "clear_posting_leaf_accounts_cache"]


def render_journals_ui(*, get_system_date: Any) -> None:
    """Load the full journals UI only when this section is opened (avoids heavy imports on other pages)."""
    from ui.journals.journals_ui import render_journals_ui as _impl

    return _impl(get_system_date=get_system_date)


def clear_posting_leaf_accounts_cache() -> None:
    from ui.journals.posting_leaves import clear_posting_leaf_accounts_cache as _fn

    return _fn()
