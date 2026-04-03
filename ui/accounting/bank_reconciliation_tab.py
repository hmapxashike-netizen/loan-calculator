"""Bank reconciliation (Premium). Placeholder for future bank vs GL matching workflows."""

from __future__ import annotations

import streamlit as st


def render_bank_reconciliation_tab() -> None:
    st.info(
        "Bank reconciliation is available on **Premium** subscriptions. "
        "This area will host bank statement import, matching rules, and exception queues."
    )
