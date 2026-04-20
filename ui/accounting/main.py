"""Accounting module UI: tab shell and delegates to tab modules."""

from __future__ import annotations

import streamlit as st

from accounting.defaults_loader import defaults_directory


def _render_bundled_accounting_defaults_downloads() -> None:
    """Expose accounting_defaults/*.json as downloads (same source as Initialize / Reset actions)."""
    d = defaults_directory()
    with st.expander("Download bundled default templates", expanded=False):
        st.caption(
            "Same JSON files used when you initialize the chart, reset transaction templates, "
            "or reload receipt → GL defaults (`accounting_defaults/`). "
            "To capture your **live database** into these files for commit, run: "
            "`python scripts/export_accounting_defaults.py`. "
            "To regenerate from **Python** built-ins only: "
            "`python scripts/bootstrap_accounting_defaults_from_builtin.py`."
        )
        c1, c2, c3 = st.columns(3)
        files = (
            ("chart_of_accounts.json", "Chart of accounts"),
            ("transaction_templates.json", "Transaction templates"),
            ("receipt_gl_mapping.json", "Receipt → GL mapping"),
        )
        for col, (fname, label) in zip((c1, c2, c3), files):
            p = d / fname
            with col:
                if p.is_file():
                    st.download_button(
                        label=label,
                        data=p.read_bytes(),
                        file_name=fname,
                        mime="application/json",
                        key=f"acco_bundled_dl_{fname}",
                        use_container_width=True,
                    )
                else:
                    st.caption(f"{fname}: not on disk (app uses built-in fallbacks).")


def render_accounting_ui(
    *,
    loan_management_available: bool,
    list_products,
    get_system_config,
    get_system_date,
    money_df_column_config,
    show_bank_reconciliation_tab: bool = True,
) -> None:
    from services.accounting_ui import build_accounting_ui_bundle

    bundle = build_accounting_ui_bundle()

    _render_bundled_accounting_defaults_downloads()

    try:
        from rbac.subfeature_access import (
            accounting_can_bank_reconciliation,
            accounting_can_chart_templates_mapping,
            accounting_can_financial_reports,
        )
    except Exception:

        def accounting_can_chart_templates_mapping(user=None) -> bool:  # type: ignore[misc]
            return True

        def accounting_can_financial_reports(user=None) -> bool:  # type: ignore[misc]
            return True

        def accounting_can_bank_reconciliation(user=None) -> bool:  # type: ignore[misc]
            return True

    tab_labels: list[str] = []
    if accounting_can_chart_templates_mapping():
        tab_labels.extend(
            [
                "Chart of Accounts",
                "Transaction Templates",
                "Receipt → GL Mapping",
                "Manual Journals",
            ]
        )
    if accounting_can_financial_reports():
        tab_labels.append("Financial Reports")
    if show_bank_reconciliation_tab and accounting_can_bank_reconciliation():
        tab_labels.append("Bank reconciliation")
    if not tab_labels:
        st.warning("You do not have permission for any Accounting areas for this role.")
        return

    st.session_state.setdefault("accounting_subnav", tab_labels[0])
    if st.session_state["accounting_subnav"] not in tab_labels:
        st.session_state["accounting_subnav"] = tab_labels[0]
    st.markdown(
        '<p class="farnda-acco-section-nav" aria-hidden="true"></p>',
        unsafe_allow_html=True,
    )
    st.radio(
        "Accounting section",
        tab_labels,
        key="accounting_subnav",
        horizontal=True,
        label_visibility="collapsed",
    )
    _acco_active = st.session_state["accounting_subnav"]

    if _acco_active == "Chart of Accounts":
        from ui.accounting.coa_tab import render_accounting_coa_tab

        render_accounting_coa_tab(
            coa=bundle.coa,
            loan_management_available=loan_management_available,
            list_products=list_products,
        )
    elif _acco_active == "Transaction Templates":
        from ui.accounting.transaction_templates_tab import render_transaction_templates_tab

        render_transaction_templates_tab(templates_ui=bundle.templates)
    elif _acco_active == "Receipt → GL Mapping":
        from ui.accounting.receipt_gl_mapping_tab import render_receipt_gl_mapping_tab

        render_receipt_gl_mapping_tab(receipt_gl=bundle.receipt_gl)
    elif _acco_active == "Manual Journals":
        from ui.accounting.manual_journals_tab import render_manual_journals_tab

        render_manual_journals_tab()
    elif _acco_active == "Financial Reports":
        from ui.accounting.financial_reports_tab import render_financial_reports_tab

        render_financial_reports_tab(
            reports=bundle.reports,
            get_system_config=get_system_config,
            get_system_date=get_system_date,
            money_df_column_config=money_df_column_config,
        )
    elif _acco_active == "Bank reconciliation" and show_bank_reconciliation_tab:
        from ui.accounting.bank_reconciliation_tab import render_bank_reconciliation_tab

        render_bank_reconciliation_tab()
