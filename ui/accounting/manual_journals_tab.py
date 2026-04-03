"""Accounting: manual journals tab (pointer to Journals nav)."""

from __future__ import annotations

import streamlit as st



from style import render_main_header, render_sub_header, render_sub_sub_header

def render_manual_journals_tab() -> None:
    render_sub_sub_header("Manual Journals")
    st.info("Day-to-day manual postings should now be done via the standalone **Journals** menu in the left navigation.")
