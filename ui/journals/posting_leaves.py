"""Cached posting-leaf list for balance adjustment (shared with COA invalidation)."""

from __future__ import annotations

import streamlit as st


@st.cache_data(ttl=120, show_spinner=False)
def get_posting_leaf_accounts_for_balance_adjust() -> list:
    """
    Posting-leaf list for balance adjustment dropdowns.
    Cached ~2m to limit DB + tree walks on Streamlit reruns; TTL keeps COA edits visible quickly.
    """
    from accounting.service import AccountingService

    return AccountingService().list_posting_leaf_accounts()


def clear_posting_leaf_accounts_cache() -> None:
    """Call after COA changes that affect which accounts are posting leaves."""
    try:
        get_posting_leaf_accounts_for_balance_adjust.clear()
    except Exception:
        pass
