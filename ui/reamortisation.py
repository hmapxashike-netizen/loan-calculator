"""Reamortisation: loan modification, recast, unapplied funds UI."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from style import render_sub_sub_header


def render_reamortisation_ui(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    customers_available: bool,
    list_customers,
    get_display_name,
    get_system_date,
    format_schedule_df,
    schedule_export_downloads,
    apply_unapplied_funds_recast,
    list_products,
    get_product_config_from_db,
    get_system_config,
    get_consumer_schemes,
    get_product_rate_basis,
    compute_consumer_schedule,
    compute_term_schedule,
    compute_bullet_schedule,
    pct_to_monthly,
    save_loan_approval_draft,
    update_loan_approval_draft_staged,
    resubmit_loan_approval_draft,
    documents_available: bool = False,
    list_document_categories=None,
    upload_document=None,
    provisions_config_ok: bool = False,
    list_provision_security_subtypes=None,
    source_cash_gl_cached_labels_and_ids=None,
    created_by: str | None = None,
) -> None:
    if not loan_management_available:
        st.error(loan_management_error or "Loan management not available.")
        return
    
    try:
        from loan_management import get_loans_by_customer
        from reamortisation import (
            get_loan_for_modification,
            preview_loan_recast,
            execute_loan_recast,
            list_unapplied_funds,
        )
    except ImportError as e:
        st.error(f"Reamortisation module not available: {e}")
        return
    
    tab_mod, tab_recast, tab_unapplied = st.tabs(
        ["Loan Modification", "Loan Recast", "Unapplied Funds"]
    )
    customers = list_customers() if customers_available else []
    
    with tab_mod:
        from ui.reamortisation_modification import render_loan_modification_tab

        _ldc = list_document_categories or (lambda **k: [])
        _lps = list_provision_security_subtypes or (lambda: [])
        render_loan_modification_tab(
            list_customers=list_customers,
            get_display_name=get_display_name,
            get_system_date=get_system_date,
            get_loan_for_modification=get_loan_for_modification,
            format_schedule_df=format_schedule_df,
            schedule_export_downloads=schedule_export_downloads,
            list_products=list_products,
            get_product_config_from_db=get_product_config_from_db,
            get_system_config=get_system_config,
            get_consumer_schemes=get_consumer_schemes,
            get_product_rate_basis=get_product_rate_basis,
            compute_consumer_schedule=compute_consumer_schedule,
            compute_term_schedule=compute_term_schedule,
            compute_bullet_schedule=compute_bullet_schedule,
            pct_to_monthly=pct_to_monthly,
            save_loan_approval_draft=save_loan_approval_draft,
            update_loan_approval_draft_staged=update_loan_approval_draft_staged,
            resubmit_loan_approval_draft=resubmit_loan_approval_draft,
            documents_available=bool(documents_available),
            list_document_categories=_ldc,
            upload_document=upload_document,
            provisions_config_ok=bool(provisions_config_ok),
            list_provision_security_subtypes=_lps,
            source_cash_gl_cached_labels_and_ids=source_cash_gl_cached_labels_and_ids,
            created_by=created_by or "reamortisation_ui",
        )

    with tab_recast:
        render_sub_sub_header("Loan Recast (Prepayment → New Instalment)")
        st.caption(
            "Re-amortise the loan from a given date to original maturity with a new principal balance. "
            "Same rate and type; only the instalment changes. Use when the borrower has made a lump-sum payment."
        )
        if customers_available and customers:
            cust_sel_r = st.selectbox("Customer", [get_display_name(c["id"]) for c in customers], key="recast_cust")
            cust_id_r = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel_r)
            loans_r = get_loans_by_customer(cust_id_r)
            loans_active_r = [l for l in loans_r if l.get("status") == "active"]
            if not loans_active_r:
                st.info("No active loans.")
            else:
                loan_opts = [(l["id"], f"Loan #{l['id']}") for l in loans_active_r]
                loan_labels_r = [t[1] for t in loan_opts]
                loan_sel_r = st.selectbox("Select loan", loan_labels_r, key="recast_loan")
                loan_id_r = loan_opts[loan_labels_r.index(loan_sel_r)][0] if loan_sel_r else None
                if loan_id_r:
                    recast_date = st.date_input("Recast effective date", value=get_system_date(), key="recast_date")
                    from loan_management import get_loan_daily_state_balances
                    bal = get_loan_daily_state_balances(loan_id_r, recast_date)
                    new_principal = st.number_input(
                        "New principal balance (after prepayment)",
                        min_value=0.01,
                        value=round((bal["principal_not_due"] + bal["principal_arrears"]) if bal else 0, 2) or 1000.0,
                        step=100.0,
                        key="recast_principal",
                    )
    
                    recast_preview_key = "recast_preview"
                    if st.button("Preview recast", type="secondary", key="recast_preview_btn"):
                        try:
                            preview = preview_loan_recast(loan_id_r, recast_date, new_principal)
                            st.session_state[recast_preview_key] = {
                                **preview,
                                "loan_id": loan_id_r,
                                "recast_date": recast_date,
                                "new_principal_balance": new_principal,
                            }
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
    
                    if st.session_state.get(recast_preview_key) and st.session_state[recast_preview_key].get("loan_id") == loan_id_r:
                        rp = st.session_state[recast_preview_key]
                        render_sub_sub_header("Proposed recast schedule (inspect before commit)")
                        st.caption(f"New instalment: **{rp['new_installment']:,.2f}**")
                        st.dataframe(
                            format_schedule_df(rp["schedule_df"]),
                            width="stretch",
                            hide_index=True,
                        )
                        schedule_export_downloads(
                            rp["schedule_df"],
                            file_stem=f"loan_{loan_id_r}_recast_preview_schedule",
                            key_prefix=f"dl_recast_sched_{loan_id_r}",
                        )
                        if st.button("Commit recast", type="primary", key="recast_commit"):
                            try:
                                inst = execute_loan_recast(
                                    rp["loan_id"],
                                    rp["recast_date"],
                                    rp["new_principal_balance"],
                                )
                                if recast_preview_key in st.session_state:
                                    del st.session_state[recast_preview_key]
                                st.success(f"Recast applied. New instalment: {inst:,.2f}.")
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
                        if st.button("Cancel preview", key="recast_cancel_preview"):
                            if recast_preview_key in st.session_state:
                                del st.session_state[recast_preview_key]
                            st.rerun()
    
    with tab_unapplied:
        render_sub_sub_header("Unapplied Funds (Suspense)")
        st.caption("Overpayments credited here. Apply to the loan via recast (reclassify accrued→arrears, principal not due→arrears, then apply). Recast is only available after funds are in Unapplied.")
        rows = list_unapplied_funds(status="pending")
        if not rows:
            st.info("No pending unapplied funds.")
        else:
            df_ua = pd.DataFrame(rows)
            cols = [c for c in ["id", "loan_id", "amount", "currency", "value_date", "status", "created_at"] if c in df_ua.columns]
            st.dataframe(df_ua[cols] if cols else df_ua, width="stretch", hide_index=True)
            st.markdown("**Apply via recast** (applies this entry to the loan: accrued interest→interest arrears, then principal not due→principal arrears).")
            for r in rows:
                uf_id = r.get("id")
                loan_id_ua = r.get("loan_id")
                amt = r.get("amount", 0)
                vd = r.get("value_date", "")
                with st.container():
                    c1, c2 = st.columns([3, 1])
                    with c1:
                        st.caption(f"Entry {uf_id}: Loan {loan_id_ua} · {amt:,.2f} · {vd}")
                    with c2:
                        if st.button("Apply via recast", key=f"unapplied_recast_{uf_id}"):
                            try:
                                apply_unapplied_funds_recast(uf_id)
                                st.success(f"Unapplied entry {uf_id} applied to loan {loan_id_ua}.")
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
