"""Accounting module UI: tab shell and delegates to tab modules."""

from __future__ import annotations

import streamlit as st


def render_accounting_ui(
    *,
    loan_management_available: bool,
    list_products,
    get_system_config,
    get_system_date,
    money_df_column_config,
) -> None:
    from services.accounting_ui import build_accounting_ui_bundle

    bundle = build_accounting_ui_bundle()

    st.markdown(
        "<div style='background-color: #0F766E; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Accounting Module</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    tab_coa, tab_templates, tab_mapping, tab_manual, tab_reports = st.tabs(
        [
            "Chart of Accounts",
            "Transaction Templates",
            "Receipt → GL Mapping",
            "Manual Journals",
            "Financial Reports",
        ]
    )

    with tab_coa:
        from ui.accounting.coa_tab import render_accounting_coa_tab

        render_accounting_coa_tab(
            coa=bundle.coa,
            loan_management_available=loan_management_available,
            list_products=list_products,
        )

    with tab_templates:
        from ui.accounting.transaction_templates_tab import render_transaction_templates_tab

        render_transaction_templates_tab(templates_ui=bundle.templates)

    with tab_mapping:
        from ui.accounting.receipt_gl_mapping_tab import render_receipt_gl_mapping_tab

        render_receipt_gl_mapping_tab(receipt_gl=bundle.receipt_gl)

    with tab_manual:
        from ui.accounting.manual_journals_tab import render_manual_journals_tab

        render_manual_journals_tab()

    with tab_reports:
        from ui.accounting.financial_reports_tab import render_financial_reports_tab

        render_financial_reports_tab(
            reports=bundle.reports,
            get_system_config=get_system_config,
            get_system_date=get_system_date,
            money_df_column_config=money_df_column_config,
        )
