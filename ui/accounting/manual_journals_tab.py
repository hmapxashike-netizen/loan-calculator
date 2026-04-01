"""Accounting: manual journals tab (pointer to Journals nav)."""

from __future__ import annotations

import streamlit as st


def render_manual_journals_tab() -> None:
    st.subheader("Manual Journals")
    st.info("Day-to-day manual postings should now be done via the standalone **Journals** menu in the left navigation.")
