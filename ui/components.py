"""Small shared Streamlit UI fragments (deduplicated from the main app)."""

from __future__ import annotations

from html import escape
from typing import Any

import pandas as pd
import streamlit as st

from style import inject_style_block

_HTML_TBL_RULES = """
.farnda-cust-tbl { width: 100%; border-collapse: collapse; font-size: 0.875rem; margin: 0.1rem 0 0.35rem 0; }
.farnda-cust-tbl th, .farnda-cust-tbl td { text-align: center; vertical-align: middle; padding: 0.35rem 0.45rem; border: 1px solid rgba(49, 51, 63, 0.2); }
.farnda-cust-tbl thead th { font-weight: 600; background-color: #f0f2f6; color: #31333F; }
"""


def _ensure_html_table_css() -> None:
    """One-shot CSS for HTML data tables (Agents / audit-style compact grids)."""
    if st.session_state.get("_farnda_cust_tbl_css"):
        return
    st.session_state["_farnda_cust_tbl_css"] = True
    inject_style_block(_HTML_TBL_RULES)


def inject_tertiary_hyperlink_css_once() -> None:
    """Style Streamlit ``type='tertiary'`` widgets as blue underlined links (``stBaseButton-tertiary``)."""
    if st.session_state.get("_farnda_tertiary_hyperlink_css"):
        return
    st.session_state["_farnda_tertiary_hyperlink_css"] = True
    inject_style_block(
        """
.stApp button[data-testid="stBaseButton-tertiary"] {
  color: #2563EB !important;
  text-decoration: underline !important;
  text-underline-offset: 2px !important;
  font-weight: 500 !important;
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  padding: 0.1rem 0.15rem !important;
  min-height: 0 !important;
  line-height: 1.25 !important;
}
.stApp button[data-testid="stBaseButton-tertiary"]:hover {
  color: #1D4ED8 !important;
}
.stApp button[data-testid="stBaseButton-tertiary"]:disabled {
  color: #94a3b8 !important;
  text-decoration: none !important;
}
"""
    )


def _html_table_cell(v: object) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (ValueError, TypeError):
        pass
    return escape(str(v))


def _unwrap_styler_to_dataframe(obj: Any) -> tuple[pd.DataFrame, bool]:
    """Return (DataFrame, was_styler). ``format_schedule_display`` returns a pandas Styler."""
    if isinstance(obj, pd.io.formats.style.Styler):
        return obj.data.copy(), True
    if isinstance(obj, pd.DataFrame):
        return obj, False
    return pd.DataFrame(obj), False


def _format_schedule_money_columns_for_html(df: pd.DataFrame) -> pd.DataFrame:
    """Match ``format_schedule_display`` grouping/decimals for HTML tables (no Styler)."""
    from loans import SCHEDULE_AMOUNT_COLUMNS
    from display_formatting import format_display_amount, get_display_format_settings

    s = get_display_format_settings(system_config=None)
    out = df.copy()
    for c in SCHEDULE_AMOUNT_COLUMNS:
        if c in out.columns:
            out[c] = out[c].map(lambda v, _s=s: format_display_amount(v, settings=_s))
    return out


def render_centered_html_table(df: pd.DataFrame | Any, headers: list[str]) -> None:
    """Full-width table with centered headers and cells (matches Agents list styling).

    Accepts a ``DataFrame`` or a pandas ``Styler`` (e.g. from ``format_schedule_display``).
    """
    raw, was_styler = _unwrap_styler_to_dataframe(df)
    if raw.empty or len(headers) != len(raw.columns):
        return
    display_df = _format_schedule_money_columns_for_html(raw) if was_styler else raw
    _ensure_html_table_css()
    cols = list(display_df.columns)
    thead = "".join(f"<th>{escape(h)}</th>" for h in headers)
    rows_html = []
    for _, row in display_df.iterrows():
        cells = "".join(f"<td>{_html_table_cell(row[c])}</td>" for c in cols)
        rows_html.append(f"<tr>{cells}</tr>")
    st.markdown(
        f'<table class="farnda-cust-tbl"><thead><tr>{thead}</tr></thead><tbody>{"".join(rows_html)}</tbody></table>',
        unsafe_allow_html=True,
    )


def render_green_page_title(title: str, *, compact: bool = False) -> None:
    """Primary section heading (green, 2rem) used across main navigation destinations."""
    safe = escape(str(title).strip() or "—")
    mt, mb = ("0.08rem", "0.3rem") if compact else ("0.25rem", "0.75rem")
    st.markdown(
        f"<div style='color:#16A34A; font-weight:700; font-size:2.5rem; margin:{mt} 0 {mb} 0;'>{safe}</div>",
        unsafe_allow_html=True,
    )
