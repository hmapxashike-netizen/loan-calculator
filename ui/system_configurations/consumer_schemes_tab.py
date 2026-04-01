"""Consumer schemes admin tab."""

from __future__ import annotations

import streamlit as st


def render_consumer_schemes_tab(*, consumer_schemes_admin_editor_ui) -> None:
    st.subheader("Consumer schemes (admin)")
    st.caption(
        "Used for consumer loan schedule calculation. Normally you set rates at the product level; "
        "this list is mainly for enabling/disabling scheme names (SSB/TPC/future)."
    )
    consumer_schemes_admin_editor_ui(key_prefix="syscfg_consumer_schemes")
