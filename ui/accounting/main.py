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
    show_bank_reconciliation_tab: bool = True,
) -> None:
    from services.accounting_ui import build_accounting_ui_bundle

    bundle = build_accounting_ui_bundle()

    tab_labels = [
        "Chart of Accounts",
        "Transaction Templates",
        "Receipt → GL Mapping",
        "Manual Journals",
        "Financial Reports",
    ]
    if show_bank_reconciliation_tab:
        tab_labels.append("Bank reconciliation")
    tabs = st.tabs(tab_labels)
    tab_coa = tabs[0]
    tab_templates = tabs[1]
    tab_mapping = tabs[2]
    tab_manual = tabs[3]
    tab_reports = tabs[4]
    tab_bank_recon = tabs[5] if show_bank_reconciliation_tab else None

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

    if tab_bank_recon is not None:
        with tab_bank_recon:
            from ui.accounting.bank_reconciliation_tab import render_bank_reconciliation_tab

            render_bank_reconciliation_tab()
