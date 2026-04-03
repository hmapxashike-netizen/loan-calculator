"""Display & numbers tab — values merged into system_config on save."""

from __future__ import annotations

from typing import Any, NamedTuple

import streamlit as st


from style import render_main_header, render_sub_header, render_sub_sub_header

from display_formatting import resolve_display_format


class DisplayFormatSnapshot(NamedTuple):
    disp_amount_decimals: int
    disp_thousands_separator: str
    disp_currency_symbol: str
    disp_streamlit_fmt: str
    disp_auto_all_float: bool
    disp_money_subs: str
    disp_skip_subs: str


def render_display_tab(*, cfg: dict[str, Any]) -> DisplayFormatSnapshot:
    render_sub_sub_header("Display — amounts in tables and labels")
    st.caption(
        "Controls thousands grouping, decimals, and which dataframe columns are treated as money. "
        "New columns are picked up automatically when their names match the configured substrings."
    )
    d0 = resolve_display_format(cfg.get("display_format"))
    sep_ix = 0 if str(d0.get("thousands_separator", ",")) == "," else 1
    d_sep = st.selectbox(
        "Thousands separator",
        ["Comma (,)", "Space ( )"],
        index=sep_ix,
        key="syscfg_disp_sep",
    )
    disp_thousands_separator = "," if d_sep.startswith("Comma") else " "
    disp_amount_decimals = int(
        st.number_input(
            "Amount decimal places (labels & schedule styling)",
            min_value=0,
            max_value=10,
            value=int(d0.get("amount_decimals", 2)),
            key="syscfg_disp_decimals",
        )
    )
    disp_currency_symbol = st.text_input(
        "Currency symbol (for format_display_currency when used)",
        value=str(d0.get("currency_symbol", "$")),
        max_chars=8,
        key="syscfg_disp_curr_sym",
    )
    _fmt_opts = ["dollar", "accounting", "localized"]
    _cur_fmt = str(d0.get("streamlit_money_format", "dollar")).lower()
    _fmt_ix = _fmt_opts.index(_cur_fmt) if _cur_fmt in _fmt_opts else 0
    disp_streamlit_fmt = st.selectbox(
        "Streamlit table number style",
        _fmt_opts,
        index=_fmt_ix,
        format_func=lambda x: {
            "dollar": "Dollar ($ + grouping)",
            "accounting": "Accounting (grouping)",
            "localized": "Localized",
        }[x],
        key="syscfg_disp_st_fmt",
    )
    disp_auto_all_float = st.checkbox(
        "Treat every numeric column as money in dataframes",
        value=bool(d0.get("auto_format_all_float_columns", False)),
        key="syscfg_disp_auto_all",
        help="If off, only columns whose names contain a substring from the list below are formatted.",
    )
    disp_money_subs = st.text_area(
        "Money column name substrings (comma-separated)",
        value=", ".join(d0.get("money_column_name_substrings") or []),
        height=90,
        key="syscfg_disp_money_subs",
    )
    disp_skip_subs = st.text_area(
        "Skip substrings (never format as money if name contains)",
        value=", ".join(d0.get("skip_column_name_substrings") or []),
        height=80,
        key="syscfg_disp_skip_subs",
    )
    return DisplayFormatSnapshot(
        disp_amount_decimals=disp_amount_decimals,
        disp_thousands_separator=disp_thousands_separator,
        disp_currency_symbol=disp_currency_symbol,
        disp_streamlit_fmt=disp_streamlit_fmt,
        disp_auto_all_float=disp_auto_all_float,
        disp_money_subs=disp_money_subs,
        disp_skip_subs=disp_skip_subs,
    )
