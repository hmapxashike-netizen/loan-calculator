"""IFRS provision config tab (delegates to provisions.ui)."""

from __future__ import annotations

import streamlit as st



from style import render_main_header, render_sub_header, render_sub_sub_header

def render_ifrs_provision_tab() -> None:
    render_sub_sub_header("IFRS provision configuration")
    st.caption(
        "Collateral security subtypes, haircuts, and **PD bands by DPD** (fallback when no IFRS grade applies). "
        "Used by **Portfolio reports** (ECL / IFRS view and single-loan IFRS Provisions). "
        "**IFRS provision PD%** normally comes from **Loan grade scales → Standard provision %** per IFRS grade; "
        "if no grade matches, **PD %** is taken from the **PD band** table below."
    )
    from provisions.ui import render_provisions_config_tables

    render_provisions_config_tables()
    st.divider()
    st.markdown("**Regulatory (RBZ / non-IFRS)**")
    st.info(
        "Supervisory **grade**, **performing / non-performing**, and **regulatory provision %** by grade are under "
        "**Loan grade scales**. **Portfolio reports → Loan classification (regulatory)** shows exposure and "
        "grade-based supervisory provision using those percentages."
    )
