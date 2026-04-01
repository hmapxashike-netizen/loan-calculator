"""Small shared Streamlit UI fragments (deduplicated from the main app)."""

from __future__ import annotations

from html import escape

import streamlit as st


def render_green_page_title(title: str) -> None:
    """Primary section heading (green, 2rem) used across main navigation destinations."""
    safe = escape(str(title).strip() or "—")
    st.markdown(
        f"<div style='color:#16A34A; font-weight:700; font-size:2rem; margin:0.25rem 0 0.75rem 0;'>{safe}</div>",
        unsafe_allow_html=True,
    )
