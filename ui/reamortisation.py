"""Reamortisation: loan modification, recast, unapplied funds UI."""

from __future__ import annotations

from datetime import datetime

import numpy_financial as npf
import pandas as pd
import streamlit as st


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
) -> None:
    st.markdown(
        "<div style='background-color: #1E40AF; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.375rem;'>Reamortisation</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)
    
    if not loan_management_available:
        st.error(loan_management_error or "Loan management not available.")
        return
    
    try:
        from loan_management import get_loans_by_customer
        from reamortisation import (
            get_loan_for_modification,
            preview_loan_modification,
            execute_loan_modification,
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
        st.subheader("Loan Modification (New Terms / Agreement)")
        st.caption(
            "Select an existing loan and apply new terms (rate, term, loan type). "
            "Restructure date cannot be in the future or before the last due date. "
            "Outstanding interest can be capitalised or written off."
        )
        if not customers:
            st.info("No customers. Create a customer first.")
        else:
            cust_sel = st.selectbox(
                "Customer",
                [get_display_name(c["id"]) for c in customers],
                key="reamod_cust",
            )
            cust_id = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel)
            loans = get_loans_by_customer(cust_id)
            loans_active = [l for l in loans if l.get("status") == "active"]
            if not loans_active:
                st.info("No active loans for this customer.")
            else:
                loan_options = [(l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}") for l in loans_active]
                loan_labels = [t[1] for t in loan_options]
                loan_sel = st.selectbox("Select loan", loan_labels, key="reamod_loan")
                loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None
                if loan_id:
                    info = get_loan_for_modification(loan_id)
                    if not info:
                        st.warning("Could not load loan details.")
                    else:
                        loan = info["loan"]
                        last_due = info.get("last_due_date")
                        st.caption(f"Current schedule version: {info['schedule_version']}. Last due date: {last_due}.")
                        restructure_date = st.date_input(
                            "Restructure date (not future, not before last due)",
                            value=datetime.now().date(),
                            max_value=datetime.now().date(),
                            key="reamod_date",
                        )
                        if last_due and restructure_date > last_due:
                            st.error("Restructure date cannot be after the last due date.")
                        elif last_due and restructure_date < get_system_date() and restructure_date < last_due:
                            pass
                        new_loan_type = st.selectbox(
                            "Modified loan type",
                            ["consumer_loan", "term_loan", "bullet_loan", "customised_repayments"],
                            key="reamod_type",
                        )
                        new_term = st.number_input("New term (months)", min_value=1, value=12, key="reamod_term")
                        new_annual_rate = st.number_input("New annual rate (%)", min_value=0.0, value=float(loan.get("annual_rate") or 0), step=0.1, key="reamod_rate")
                        outstanding_interest = st.selectbox(
                            "Outstanding interest",
                            ["capitalise", "write_off"],
                            key="reamod_interest",
                        )
    
                        def _reamod_params():
                            p = {"term": new_term, "annual_rate": new_annual_rate}
                            if new_loan_type == "consumer_loan":
                                p["monthly_rate"] = new_annual_rate / 12.0
                                p["installment"] = float(npf.pmt(new_annual_rate / 1200, new_term, -float(loan.get("principal") or loan.get("disbursed_amount") or 0)))
                            elif new_loan_type == "term_loan":
                                p["grace_type"] = loan.get("grace_type") or "none"
                                p["moratorium_months"] = loan.get("moratorium_months") or 0
                            elif new_loan_type == "bullet_loan":
                                from datetime import datetime as dt
                                p["end_date"] = dt.combine(restructure_date, dt.min.time())
                                p["bullet_type"] = loan.get("bullet_type") or "with_interest"
                            return p
    
                        preview_key = "reamod_preview"
                        if st.button("Preview schedule", type="secondary", key="reamod_preview_btn"):
                            try:
                                new_params = _reamod_params()
                                preview = preview_loan_modification(
                                    loan_id,
                                    restructure_date,
                                    new_loan_type,
                                    new_params,
                                    outstanding_interest,
                                )
                                st.session_state[preview_key] = {
                                    **preview,
                                    "loan_id": loan_id,
                                    "restructure_date": restructure_date,
                                    "new_params": new_params,
                                    "outstanding_interest": outstanding_interest,
                                }
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
    
                        if st.session_state.get(preview_key) and st.session_state[preview_key].get("loan_id") == loan_id:
                            pr = st.session_state[preview_key]
                            st.subheader("Proposed schedule (inspect before commit)")
                            cap = f"New principal: **{pr['new_principal']:,.2f}**"
                            if pr.get("new_installment") is not None:
                                cap += f" | New instalment: **{pr['new_installment']:,.2f}**"
                            st.caption(cap)
                            df_preview = pr["schedule_df"]
                            st.dataframe(
                                format_schedule_df(df_preview),
                                width="stretch",
                                hide_index=True,
                            )
                            schedule_export_downloads(
                                df_preview,
                                file_stem=f"loan_{loan_id}_modification_preview_schedule",
                                key_prefix=f"dl_reamod_sched_{loan_id}",
                            )
                            if st.button("Commit modification", type="primary", key="reamod_commit"):
                                try:
                                    v = execute_loan_modification(
                                        pr["loan_id"],
                                        pr["restructure_date"],
                                        pr["new_loan_type"],
                                        pr["new_params"],
                                        pr["outstanding_interest"],
                                    )
                                    if preview_key in st.session_state:
                                        del st.session_state[preview_key]
                                    st.success(f"Loan modification applied. New schedule version: {v}.")
                                    st.rerun()
                                except Exception as ex:
                                    st.error(str(ex))
                            if st.button("Cancel preview", key="reamod_cancel_preview"):
                                if preview_key in st.session_state:
                                    del st.session_state[preview_key]
                                st.rerun()
    
    with tab_recast:
        st.subheader("Loan Recast (Prepayment → New Instalment)")
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
                        st.subheader("Proposed recast schedule (inspect before commit)")
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
        st.subheader("Unapplied Funds (Suspense)")
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
