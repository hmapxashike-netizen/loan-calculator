"""Reamortisation: loan modification, recast, unapplied funds UI."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from decimal_utils import as_10dp
from services.modification_capture_bridge import EOD_SUMMARY_BUCKET_ROWS

def _section_heading(title: str) -> None:
    """Section title that renders reliably inside tab panels."""
    st.subheader(str(title).strip())


def render_reamortisation_ui(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    customers_available: bool,
    customers_error: str = "",
    list_customers,
    get_display_name,
    get_system_date,
    format_schedule_df,
    schedule_export_downloads,
    money_df_column_config=None,
    schedule_editor_disabled_amounts=None,
    first_repayment_from_customised_table=None,
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
    direct_principal_tab: bool = False,
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
    
    _ream_tab_labels = ["Loan Modification", "Loan Recast", "Unapplied Funds"]
    if direct_principal_tab:
        _ream_tab_labels.append("Direct principal (admin)")
    _ream_tabs = st.tabs(_ream_tab_labels)
    tab_mod = _ream_tabs[0]
    tab_recast = _ream_tabs[1]
    tab_unapplied = _ream_tabs[2]
    tab_direct = _ream_tabs[3] if direct_principal_tab else None
    customers = list_customers() if customers_available else []
    customers_ctx_ok = bool(customers_available and customers)

    # Streamlit 1.54+ ``st.stop()`` requests a script halt without raising; anything *after* it in this run
    # never executes. Render Recast and Unapplied *before* Loan Modification so ``st.stop()`` there cannot
    # blank the other tabs (tab order in the UI is unchanged).

    with tab_recast:
        _section_heading("Loan Recast (Unapplied → Liquidation → Re-amortise)")
        if not customers_ctx_ok:
            if not customers_available:
                st.warning(
                    (customers_error or "").strip()
                    or "Customer list is unavailable (customers module did not load). Check configuration and logs."
                )
            else:
                st.info(
                    "No customers were returned. Add or import customers before using loan recast."
                )
        if customers_ctx_ok:
            from loan_management import (
                execute_recast_from_unapplied,
                get_loan_daily_state_balances_for_recast_preview,
                list_unapplied_credit_rows_for_recast,
                preview_recast_from_unapplied,
            )

            rc1, rc2 = st.columns(2)
            with rc1:
                cust_sel_r = st.selectbox("Customer", [get_display_name(c["id"]) for c in customers], key="recast_cust")
            cust_id_r = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel_r)
            loans_r = get_loans_by_customer(cust_id_r)
            loans_active_r = [l for l in loans_r if l.get("status") == "active"]
            loan_opts = [(l["id"], f"Loan #{l['id']}") for l in loans_active_r]
            loan_labels_r = [t[1] for t in loan_opts]
            with rc2:
                if not loans_active_r:
                    st.caption("No active loans for this customer.")
                    loan_id_r = None
                else:
                    loan_sel_r = st.selectbox("Loan", loan_labels_r, key="recast_loan")
                    loan_id_r = loan_opts[loan_labels_r.index(loan_sel_r)][0] if loan_sel_r else None
            if not loans_active_r:
                st.info("No active loans.")
            elif loan_id_r:
                recast_preview_key = "recast_preview_v2"
                recast_date = st.date_input(
                    "Recast effective date",
                    value=get_system_date(),
                    key="recast_date",
                )
                bal_recast, as_of_bal = get_loan_daily_state_balances_for_recast_preview(
                    loan_id_r, recast_date
                )
                if bal_recast is None:
                    st.warning(
                        f"No **loan_daily_state** on or before **{recast_date.isoformat()}** "
                        f"(or the prior day). Run EOD through the recast effective date first."
                    )
                else:
                    _cap = (
                        " (same calendar day as recast — first persisted EOD row)"
                        if as_of_bal == recast_date
                        else ""
                    )
                    _section_heading(
                        f"Balance outstanding as of {as_of_bal.strftime('%d/%m/%Y')}{_cap}"
                    )
                    row_eod: dict[str, float] = {}
                    for lab, key in EOD_SUMMARY_BUCKET_ROWS:
                        row_eod[lab] = float(as_10dp(bal_recast.get(key) or 0))
                    row_eod["Total outstanding (total_exposure)"] = float(
                        as_10dp(bal_recast.get("total_exposure") or 0)
                    )
                    df_eod = pd.DataFrame([row_eod])
                    if money_df_column_config is not None:
                        st.dataframe(
                            df_eod,
                            column_config=money_df_column_config(
                                df_eod,
                                overrides={},
                                column_disabled={},
                                money_column_alignment="right",
                            ),
                            hide_index=True,
                            width="stretch",
                        )
                    else:
                        st.dataframe(df_eod, hide_index=True, width="stretch")

                credits = list_unapplied_credit_rows_for_recast(loan_id_r)
                if not credits:
                    st.info(
                        "No eligible unapplied credits for this loan (positive credit with repayment_id, "
                        "not yet consumed)."
                    )
                else:
                    _uf_by_id = {int(c["id"]): c for c in credits}
                    _uf_ids = list(_uf_by_id.keys())
                    ru1, ru2 = st.columns(2)
                    with ru1:
                        uf_id = int(
                            st.selectbox(
                                "Unapplied credit",
                                options=_uf_ids,
                                format_func=lambda i: (
                                    f"#{i}: {float(_uf_by_id[i].get('amount') or 0):,.2f} · "
                                    f"{_uf_by_id[i].get('value_date')}"
                                ),
                                key="recast_uf_row",
                            )
                        )
                    with ru2:
                        _mode_labels = {
                            "maintain_term": "Lower instalment (same term)",
                            "maintain_instalment": "Fixed instalment (prepay from last)",
                            "prepay_upcoming_installments": "Prepayment of upcoming instalments",
                        }
                        mode = st.selectbox(
                            "Mode",
                            options=list(_mode_labels.keys()),
                            format_func=lambda m: _mode_labels.get(m, m),
                            key="recast_mode",
                        )
                    balancing_position = "final_installment"
                    if mode == "maintain_instalment":
                        st.caption("Balancing installment: **Final instalment**")
                    rb1, rb2 = st.columns(2)
                    with rb1:
                        preview_clicked = st.button("Preview recast", type="secondary", key="recast_preview_btn")
                    with rb2:
                        st.caption("Preview: validations only, no DB writes.")
                    if preview_clicked:
                        try:
                            scfg = get_system_config() if callable(get_system_config) else {}
                            if not isinstance(scfg, dict):
                                scfg = {}
                            prev = preview_recast_from_unapplied(
                                loan_id_r,
                                recast_date,
                                uf_id,
                                mode,
                                balancing_position=balancing_position,
                                system_config=scfg,
                            )
                            st.session_state[recast_preview_key] = {
                                **prev,
                                "loan_id": loan_id_r,
                                "recast_date": recast_date,
                                "uf_id": uf_id,
                                "mode": mode,
                                "balancing_position": balancing_position,
                            }
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))

                if st.session_state.get(recast_preview_key) and st.session_state[recast_preview_key].get(
                    "loan_id"
                ) == loan_id_r:
                    rp = st.session_state[recast_preview_key]
                    _section_heading("Proposed recast schedule")
                    if str(rp.get("mode") or "") == "maintain_instalment":
                        st.caption("Balancing installment: **Final instalment**")
                    st.caption(
                        f"New instalment: **{rp['new_installment']:,.2f}** · "
                        f"New principal: **{rp['new_principal_balance']:,.2f}** · "
                        f"Unapplied applied: **{rp.get('unapplied_applied', 0):,.2f}**"
                    )
                    if rp.get("unapplied_unused_remainder", 0) and rp["unapplied_unused_remainder"] > 1e-6:
                        st.caption(
                            f"Excess over bucket balances (**{rp['unapplied_unused_remainder']:,.2f}**) "
                            "will be re-credited to unapplied on commit."
                        )
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
                    ra1, ra2 = st.columns(2)
                    with ra1:
                        if st.button("Commit recast", type="primary", key="recast_commit"):
                            try:
                                scfg = get_system_config() if callable(get_system_config) else {}
                                out = execute_recast_from_unapplied(
                                    rp["loan_id"],
                                    rp["recast_date"],
                                    int(rp["uf_id"]),
                                    str(rp.get("mode") or "maintain_term"),
                                    balancing_position=str(rp.get("balancing_position") or "final_installment"),
                                    system_config=scfg if isinstance(scfg, dict) else {},
                                )
                                if recast_preview_key in st.session_state:
                                    del st.session_state[recast_preview_key]
                                st.success(
                                    f"Recast applied. New instalment: {out['new_installment']:,.2f}. "
                                    f"Schedule version {out['new_schedule_version']}."
                                )
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
                    with ra2:
                        if st.button("Cancel preview", key="recast_cancel_preview"):
                            if recast_preview_key in st.session_state:
                                del st.session_state[recast_preview_key]
                            st.rerun()

    with tab_unapplied:
        try:
            from loan_management import apply_unapplied_funds_to_arrears_eod as _apply_ua_to_arrears_fn
        except Exception:
            _apply_ua_to_arrears_fn = None

        _section_heading("Unapplied Funds (Suspense)")
        st.caption(
            "Overpayments credited here. Use **Loan Recast** to apply unapplied with journal-backed liquidation, "
            "threshold checks, and a new amortisation schedule. EOD may still auto-apply unapplied to **arrears** "
            "when configured."
        )
        try:
            rows = list_unapplied_funds(status="pending")
        except Exception as ex:
            st.error(f"Could not load unapplied funds: {ex}")
            rows = []
        if not rows:
            st.info("No pending unapplied funds.")
        else:
            df_ua = pd.DataFrame(rows)
            cols = [c for c in ["id", "loan_id", "amount", "currency", "value_date", "status", "created_at"] if c in df_ua.columns]
            st.dataframe(df_ua[cols] if cols else df_ua, width="stretch", hide_index=True)

        st.divider()
        _section_heading("Apply unapplied to arrears")
        st.caption(
            "Runs the same routine as EOD **Apply unapplied to arrears**: uses pending unapplied (with "
            "source receipt) to pay **delinquency buckets** in waterfall order, up to the lesser of unapplied "
            "and total arrears (interest arrears, penalty, default interest, principal arrears, fees). "
            "Use this to clear arrears before **Loan Recast** if EOD has not run or that EOD task is disabled "
            "under System configurations → EOD."
        )
        _loan_ids_ua = sorted(
            {int(r["loan_id"]) for r in rows if r.get("loan_id") is not None},
        ) if rows else []
        if _apply_ua_to_arrears_fn is None:
            st.warning("This action is unavailable (loan management not loaded).")
        else:
            from loan_management import get_loan_daily_state_balances

            ua1, ua2, ua3 = st.columns(3)
            with ua1:
                if _loan_ids_ua:
                    loan_apply_id = int(
                        st.selectbox(
                            "Loan",
                            options=_loan_ids_ua,
                            format_func=lambda x: f"Loan #{x}",
                            key="ua_manual_arrears_loan",
                        )
                    )
                else:
                    loan_apply_id = int(
                        st.number_input("Loan ID", min_value=1, value=1, step=1, key="ua_manual_arrears_loan_num")
                    )
            with ua2:
                as_of_arrears = st.date_input(
                    "Effective date",
                    value=get_system_date(),
                    key="ua_manual_arrears_date",
                    help="Must match a date with **loan_daily_state** (usually the system business date).",
                )
            with ua3:
                run_arrears = st.button(
                    "Apply to arrears now",
                    type="secondary",
                    key="ua_manual_arrears_btn",
                )
            _st_arrears = get_loan_daily_state_balances(loan_apply_id, as_of_arrears)
            if _st_arrears:
                _arr = (
                    float(as_10dp(_st_arrears.get("interest_arrears_balance") or 0))
                    + float(as_10dp(_st_arrears.get("penalty_interest_balance") or 0))
                    + float(as_10dp(_st_arrears.get("default_interest_balance") or 0))
                    + float(as_10dp(_st_arrears.get("principal_arrears") or 0))
                    + float(as_10dp(_st_arrears.get("fees_charges_balance") or 0))
                )
                st.caption(f"Delinquency buckets total (approx.): **{_arr:,.2f}** as of {as_of_arrears.isoformat()}.")
            else:
                st.caption("No **loan_daily_state** for that loan/date — run EOD first or pick another date.")
            if run_arrears:
                try:
                    scfg = get_system_config() if callable(get_system_config) else {}
                    if not isinstance(scfg, dict):
                        scfg = {}
                    applied = float(
                        _apply_ua_to_arrears_fn(int(loan_apply_id), as_of_arrears, scfg)
                    )
                    if applied <= 1e-9:
                        st.info(
                            "Nothing applied: no consumable unapplied with receipt link, no arrears, or amount "
                            "below threshold."
                        )
                    else:
                        st.success(f"Applied **{applied:,.10f}** from unapplied to arrears for loan {loan_apply_id}.")
                        st.rerun()
                except Exception as ex:
                    st.error(str(ex))

    if direct_principal_tab and tab_direct is not None:
        with tab_direct:
            _direct_preview_key = "recast_direct_preview_v1"
            _section_heading("Direct principal & schedule (no unapplied)")
            st.caption(
                "**Admin only.** Skips unapplied suspense and liquidation journals. Use only for controlled "
                "**exceptions** (e.g. data correction). Standard recasts must use **Loan Recast** so subledger and GL "
                "stay aligned."
            )
            if not customers_ctx_ok:
                if not customers_available:
                    st.warning(
                        (customers_error or "").strip()
                        or "Customer list is unavailable (customers module did not load)."
                    )
                else:
                    st.info("No customers were returned.")
            if customers_ctx_ok:
                dm1, dm2 = st.columns(2)
                with dm1:
                    cust_sel_d = st.selectbox(
                        "Customer",
                        [get_display_name(c["id"]) for c in customers],
                        key="recast_direct_cust",
                    )
                cust_id_d = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel_d)
                loans_d = get_loans_by_customer(cust_id_d)
                loans_active_d = [l for l in loans_d if l.get("status") == "active"]
                if loans_active_d:
                    loan_opts_d = [(l["id"], f"Loan #{l['id']}") for l in loans_active_d]
                    loan_labels_d = [t[1] for t in loan_opts_d]
                    with dm2:
                        loan_sel_d = st.selectbox("Loan", loan_labels_d, key="recast_direct_loan")
                    loan_id_d = loan_opts_d[loan_labels_d.index(loan_sel_d)][0] if loan_sel_d else None
                else:
                    with dm2:
                        st.caption("No active loans.")
                    loan_id_d = None
                if not loans_active_d or not loan_id_d:
                    st.info("No active loans for this customer.")
                else:
                    from loan_management import get_loan_daily_state_balances

                    dm3, dm4, dm5 = st.columns(3)
                    with dm3:
                        recast_date_d = st.date_input(
                            "Effective date", value=get_system_date(), key="recast_direct_date"
                        )
                    bal_d = get_loan_daily_state_balances(loan_id_d, recast_date_d)
                    with dm4:
                        new_principal_d = st.number_input(
                            "New principal",
                            min_value=0.01,
                            value=round(
                                (bal_d["principal_not_due"] + bal_d["principal_arrears"]) if bal_d else 0, 2
                            )
                            or 1000.0,
                            step=100.0,
                            key="recast_direct_principal",
                        )
                    with dm5:
                        if st.button("Preview schedule", type="secondary", key="recast_direct_preview_btn"):
                            try:
                                preview_d = preview_loan_recast(loan_id_d, recast_date_d, new_principal_d)
                                st.session_state[_direct_preview_key] = {
                                    **preview_d,
                                    "loan_id": loan_id_d,
                                    "recast_date": recast_date_d,
                                    "new_principal_balance": new_principal_d,
                                }
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
                    if st.session_state.get(_direct_preview_key) and st.session_state[_direct_preview_key].get(
                        "loan_id"
                    ) == loan_id_d:
                        rpd = st.session_state[_direct_preview_key]
                        st.caption(f"New instalment: **{rpd['new_installment']:,.2f}**")
                        st.dataframe(
                            format_schedule_df(rpd["schedule_df"]),
                            width="stretch",
                            hide_index=True,
                        )
                        st.warning(
                            "Applying writes a new schedule and header instalment **without** unapplied liquidation "
                            "journals. Misuse can break subledger/GL alignment."
                        )
                        with st.expander("Apply to database (extra confirmations)", expanded=False):
                            st.caption("Execute is only available here, after explicit checks.")
                            ack_risk = st.checkbox(
                                "I understand this bypasses unapplied and journal-backed liquidation.",
                                key="recast_direct_ack_risk",
                            )
                            ack_prev = st.checkbox(
                                "I have verified the preview schedule and the new principal amount.",
                                key="recast_direct_ack_preview",
                            )
                            typed_apply = st.text_input(
                                "Type APPLY (capital letters) to enable execute",
                                key="recast_direct_type_apply",
                                placeholder="",
                            )
                            can_execute = ack_risk and ack_prev and typed_apply.strip() == "APPLY"
                            ex1, ex2 = st.columns(2)
                            with ex1:
                                if st.button(
                                    "Execute direct principal recast",
                                    type="secondary",
                                    disabled=not can_execute,
                                    key="recast_direct_execute_btn",
                                ):
                                    try:
                                        inst_d = execute_loan_recast(
                                            rpd["loan_id"],
                                            rpd["recast_date"],
                                            rpd["new_principal_balance"],
                                        )
                                        del st.session_state[_direct_preview_key]
                                        for _k in (
                                            "recast_direct_ack_risk",
                                            "recast_direct_ack_preview",
                                            "recast_direct_type_apply",
                                        ):
                                            st.session_state.pop(_k, None)
                                        st.success(f"Recast applied. New instalment: {inst_d:,.2f}.")
                                        st.rerun()
                                    except Exception as ex:
                                        st.error(str(ex))
                            with ex2:
                                st.caption("Execute stays disabled until both boxes are ticked and APPLY is entered.")

    with tab_mod:
        from ui.reamortisation_modification import render_loan_modification_tab

        _ldc = list_document_categories or (lambda **k: [])
        _lps = list_provision_security_subtypes or (lambda: [])
        try:
            render_loan_modification_tab(
                list_customers=list_customers,
                get_display_name=get_display_name,
                get_system_date=get_system_date,
                get_loan_for_modification=get_loan_for_modification,
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
                money_df_column_config=money_df_column_config,
                schedule_editor_disabled_amounts=schedule_editor_disabled_amounts,
                first_repayment_from_customised_table=first_repayment_from_customised_table,
            )
        except Exception as ex:
            st.error(f"Loan Modification failed to render: {ex}")
