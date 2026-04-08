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
