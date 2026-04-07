"""Reamortisation: loan modification, recast, unapplied funds UI."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from decimal_utils import as_10dp
from loan_management.recast_orchestration import compute_recast_unapplied_allocation
from services.modification_capture_bridge import EOD_SUMMARY_BUCKET_ROWS
from ui.streamlit_feedback import run_with_spinner

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
    list_loan_approval_drafts,
    get_loan_approval_draft,
    approve_loan_approval_draft,
    send_back_loan_approval_draft,
    dismiss_loan_approval_draft,
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
    
    _ream_tab_labels = ["Loan Modification", "Loan Recast", "Approve Modifications", "Unapplied Funds"]
    if direct_principal_tab:
        _ream_tab_labels.append("Direct principal (admin)")
    _ream_tabs = st.tabs(_ream_tab_labels)
    tab_mod = _ream_tabs[0]
    tab_recast = _ream_tabs[1]
    tab_approve = _ream_tabs[2]
    tab_unapplied = _ream_tabs[3]
    tab_direct = _ream_tabs[4] if direct_principal_tab else None
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
                get_loan,
                get_loan_daily_state_balances_for_recast_preview,
                list_unapplied_credit_rows_for_recast,
                preview_recast_from_unapplied,
            )

            rc1, rc2, rc3 = st.columns([1.2, 1.0, 1.0])
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
            with rc3:
                recast_date = st.date_input(
                    "Recast effective date",
                    value=get_system_date(),
                    key="recast_date",
                )
            if not loans_active_r:
                st.info("No active loans.")
            elif loan_id_r:
                recast_preview_key = "recast_preview_v2"
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
                    alloc = dict(rp.get("allocation") or {})
                    journal_rows = []
                    unap_applied = float(as_10dp(rp.get("unapplied_applied") or 0.0))
                    if unap_applied > 0:
                        journal_rows.append(
                            {
                                "Entry": "DR",
                                "Account": "Unapplied funds suspense",
                                "Amount": unap_applied,
                            }
                        )
                    bucket_label = {
                        "alloc_fees_charges": "Fees & charges receivable",
                        "alloc_penalty_interest": "Penalty interest receivable",
                        "alloc_default_interest": "Default interest receivable",
                        "alloc_interest_arrears": "Interest arrears receivable",
                        "alloc_interest_accrued": "Interest accrued receivable",
                        "alloc_principal_arrears": "Principal arrears receivable",
                        "alloc_principal_not_due": "Principal not due receivable",
                    }
                    for k, lbl in bucket_label.items():
                        amt = float(as_10dp(alloc.get(k) or 0.0))
                        if amt > 0:
                            journal_rows.append({"Entry": "CR", "Account": lbl, "Amount": amt})
                    with st.expander("Preview journals to be posted (on approval)", expanded=False):
                        if journal_rows:
                            st.dataframe(pd.DataFrame(journal_rows), hide_index=True, width="stretch")
                        else:
                            st.caption("No projected journal lines.")
                    schedule_export_downloads(
                        rp["schedule_df"],
                        file_stem=f"loan_{loan_id_r}_recast_preview_schedule",
                        key_prefix=f"dl_recast_sched_{loan_id_r}",
                    )
                    ra1, ra2 = st.columns(2)
                    with ra1:
                        if st.button("Submit recast for approval", type="primary", key="recast_submit"):
                            try:
                                loan_src = get_loan(int(rp["loan_id"])) or {}
                                details = {
                                    "approval_action": "LOAN_RECAST",
                                    "source_loan_id": int(rp["loan_id"]),
                                    "recast_date": rp["recast_date"].isoformat(),
                                    "restructure_date": rp["recast_date"].isoformat(),
                                    "unapplied_funds_id": int(rp["uf_id"]),
                                    "recast_mode": str(rp.get("mode") or "maintain_term"),
                                    "balancing_position": str(
                                        rp.get("balancing_position") or "final_installment"
                                    ),
                                    "allocation": {k: float(as_10dp(v or 0.0)) for k, v in alloc.items()},
                                    "unapplied_applied": float(as_10dp(rp.get("unapplied_applied") or 0.0)),
                                    "unapplied_unused_remainder": float(
                                        as_10dp(rp.get("unapplied_unused_remainder") or 0.0)
                                    ),
                                    "new_principal_balance": float(
                                        as_10dp(rp.get("new_principal_balance") or 0.0)
                                    ),
                                    "new_installment": float(as_10dp(rp.get("new_installment") or 0.0)),
                                }
                                draft_id = save_loan_approval_draft(
                                    int(cust_id_r),
                                    str(loan_src.get("loan_type") or "term_loan"),
                                    details,
                                    rp["schedule_df"],
                                    product_code=loan_src.get("product_code"),
                                    created_by=created_by or "reamortisation_ui",
                                    status="PENDING",
                                    loan_id=int(rp["loan_id"]),
                                )
                                if recast_preview_key in st.session_state:
                                    del st.session_state[recast_preview_key]
                                st.success(
                                    f"Recast draft #{draft_id} submitted for approval. "
                                    "Open Approve Modifications tab to inspect and approve."
                                )
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
                    with ra2:
                        if st.button("Cancel preview", key="recast_cancel_preview"):
                            if recast_preview_key in st.session_state:
                                del st.session_state[recast_preview_key]
                            st.rerun()

    with tab_approve:
        _section_heading("Approve Modifications & Recasts")
        st.caption(
            "Review pending **Loan Modification / Split Modification / Loan Recast** drafts. "
            "Inspect schedule and projected journals before approval."
        )
        qa1, qa2 = st.columns([1.4, 1.0])
        with qa1:
            search_q = st.text_input(
                "Search pending approvals",
                value=str(st.session_state.get("reamod_approve_search", "") or ""),
                key="reamod_approve_search",
                placeholder="Draft ID / customer ID / product / loan type",
            )
        with qa2:
            show_non_pending = st.checkbox("Include non-pending", value=False, key="reamod_show_non_pending")
        statuses = None if show_non_pending else ["PENDING"]
        base_rows = list_loan_approval_drafts(
            statuses=statuses,
            search=search_q.strip() if search_q else None,
            limit=300,
        )
        allowed_actions = {"LOAN_MODIFICATION", "LOAN_MODIFICATION_SPLIT", "LOAN_RECAST"}
        filtered: list[dict] = []
        for r in base_rows:
            d = get_loan_approval_draft(int(r.get("id")))
            if not d:
                continue
            det = dict(d.get("details_json") or {})
            action = str(det.get("approval_action") or "").strip().upper()
            if action in allowed_actions:
                filtered.append(
                    {
                        "id": int(r.get("id")),
                        "action": action,
                        "customer_id": int(r.get("customer_id") or 0),
                        "loan_id": int(r.get("loan_id") or 0),
                        "loan_type": str(r.get("loan_type") or ""),
                        "status": str(r.get("status") or ""),
                        "submitted_at": str(r.get("submitted_at") or ""),
                        "created_by": str(r.get("created_by") or ""),
                    }
                )
        if not filtered:
            st.info("No modification/recast drafts found for the selected filters.")
        else:
            with st.expander("Dismiss batch (all modification / recast drafts in this list)", expanded=False):
                st.warning(
                    f"This will **dismiss all {len(filtered)} draft(s)** shown below (same search / pending filter). "
                    "Use this to clear a bad batch and start again."
                )
                st.caption(
                    "Note: the list is capped for performance. The batch dismiss will re-fetch up to 2,000 "
                    "matching drafts at click-time, then filter to modifications/recasts."
                )
                _rm_note = st.text_input(
                    "Optional note stored on each draft",
                    key="reamod_batch_dismiss_note",
                    placeholder="e.g. Clearing modification batch",
                )
                _rm_conf = st.text_input(
                    'Type **DISMISS BATCH** to confirm',
                    key="reamod_batch_dismiss_confirm",
                )
                if st.button("Dismiss entire batch", type="primary", key="reamod_batch_dismiss_go"):
                    if (_rm_conf or "").strip() != "DISMISS BATCH":
                        st.error('Confirmation must be exactly: DISMISS BATCH')
                    else:
                        # Re-fetch at click time for accurate batch coverage.
                        base_rows_now = list_loan_approval_drafts(
                            statuses=statuses,
                            search=search_q.strip() if search_q else None,
                            limit=2000,
                        )
                        filtered_now: list[int] = []
                        for r in base_rows_now:
                            d = get_loan_approval_draft(int(r.get("id")))
                            if not d:
                                continue
                            det = dict(d.get("details_json") or {})
                            action = str(det.get("approval_action") or "").strip().upper()
                            if action in allowed_actions:
                                filtered_now.append(int(r.get("id")))
                        errs_rm: list[str] = []
                        n_ok_rm = 0

                        def _run_rm_batch():
                            nonlocal n_ok_rm, errs_rm
                            for did in filtered_now:
                                try:
                                    dismiss_loan_approval_draft(
                                        did,
                                        note=(_rm_note or "").strip() or "Batch dismiss (Approve Modifications)",
                                        actor=created_by or "approver",
                                    )
                                    n_ok_rm += 1
                                except Exception as ex:
                                    errs_rm.append(f"Draft #{did}: {ex}")

                        run_with_spinner("Dismissing drafts…", _run_rm_batch)
                        st.session_state.pop("approve_selected_draft_id", None)
                        if errs_rm:
                            st.session_state["reamod_approve_flash"] = (
                                f"Dismissed {n_ok_rm} draft(s); {len(errs_rm)} error(s)."
                            )
                            st.session_state["reamod_batch_dismiss_errors"] = errs_rm[:50]
                        else:
                            st.session_state["reamod_approve_flash"] = f"Dismissed {n_ok_rm} draft(s)."
                            st.session_state.pop("reamod_batch_dismiss_errors", None)
                        st.rerun()
            _rm_errs = st.session_state.pop("reamod_batch_dismiss_errors", None)
            if _rm_errs:
                with st.expander("Batch dismiss errors (first 50)", expanded=False):
                    st.code("\n".join(_rm_errs))
            _rm_flash = st.session_state.pop("reamod_approve_flash", None)
            if _rm_flash:
                st.success(str(_rm_flash))
            st.caption("Click a **loan_id** to open the draft below.")
            hcols = st.columns([0.7, 1.8, 1.0, 0.9, 1.1, 0.9, 1.6, 1.6])
            for i, t in enumerate(("id", "action", "customer_id", "loan_id", "loan_type", "status", "submitted_at", "created_by")):
                with hcols[i]:
                    st.markdown(f"**{t}**")
            for row in filtered:
                did = int(row["id"])
                rcols = st.columns([0.7, 1.8, 1.0, 0.9, 1.1, 0.9, 1.6, 1.6])
                with rcols[0]:
                    st.write(did)
                with rcols[1]:
                    st.write(str(row["action"]))
                with rcols[2]:
                    st.write(int(row["customer_id"]))
                with rcols[3]:
                    if st.button(
                        str(int(row["loan_id"])),
                        key=f"reamod_open_from_loan_{did}",
                        type="tertiary",
                    ):
                        st.session_state["approve_selected_draft_id"] = did
                        st.rerun()
                with rcols[4]:
                    st.write(str(row["loan_type"]))
                with rcols[5]:
                    st.write(str(row["status"]))
                with rcols[6]:
                    st.write(str(row["submitted_at"]))
                with rcols[7]:
                    st.write(str(row["created_by"]))
            draft_ids = [int(x["id"]) for x in filtered]
            preselected = st.session_state.get("approve_selected_draft_id")
            idx = 0
            if preselected in draft_ids:
                idx = draft_ids.index(int(preselected))
            pick_id = int(
                st.selectbox(
                    "Open draft",
                    options=draft_ids,
                    index=idx,
                    format_func=lambda x: f"Draft #{x}",
                    key="reamod_approve_pick",
                )
            )
            draft = get_loan_approval_draft(pick_id)
            if draft:
                det = dict(draft.get("details_json") or {})
                action = str(det.get("approval_action") or "").strip().upper()
                st.markdown(
                    f"**Draft #{pick_id}** · **{action}** · status: **{draft.get('status')}** · "
                    f"loan: **{draft.get('loan_id')}** · customer: **{draft.get('customer_id')}**"
                )
                # Decision-support snapshot: balances and restructure/write-off context.
                snap = dict(det.get("bucket_snapshot") or {})
                if snap:
                    st.caption("Outstanding balance snapshot at submission")
                    row_bal: dict[str, float] = {}
                    for lab, key in EOD_SUMMARY_BUCKET_ROWS:
                        row_bal[lab] = float(as_10dp(snap.get(key) or 0.0))
                    out_snap = float(
                        as_10dp(
                            det.get("outstanding_snapshot")
                            or snap.get("total_exposure")
                            or 0.0
                        )
                    )
                    row_bal["Total outstanding (total_exposure)"] = out_snap
                    row_bal["Unapplied Funds Balance"] = float(
                        as_10dp(det.get("unapplied_balance_snapshot") or 0.0)
                    )
                    row_bal["Net (Balance outstanding - Unapplied Funds)"] = float(
                        as_10dp(
                            det.get("net_snapshot")
                            or max(0.0, row_bal["Total outstanding (total_exposure)"] - row_bal["Unapplied Funds Balance"])
                        )
                    )
                    df_bal = pd.DataFrame([row_bal])
                    if money_df_column_config is not None:
                        st.dataframe(
                            df_bal,
                            column_config=money_df_column_config(
                                df_bal,
                                overrides={},
                                column_disabled={},
                                money_column_alignment="right",
                            ),
                            hide_index=True,
                            width="stretch",
                        )
                    else:
                        st.dataframe(df_bal, hide_index=True, width="stretch")

                sum_row: dict[str, float] = {}
                if action == "LOAN_MODIFICATION_SPLIT":
                    split_net = det.get("split_net_by_leg")
                    if isinstance(split_net, list) and split_net:
                        for i, amt in enumerate(split_net, start=1):
                            sum_row[f"Restructure amount leg {i}"] = float(as_10dp(amt or 0.0))
                        sum_row["Restructure amount total"] = float(
                            as_10dp(sum(float(as_10dp(x or 0.0)) for x in split_net))
                        )
                else:
                    sum_row["Amount to restructure"] = float(as_10dp(det.get("carry_amount") or 0.0))
                sum_row["Top-up amount"] = float(as_10dp(det.get("topup_amount") or 0.0))
                sum_row["Write-off amount"] = float(as_10dp(det.get("writeoff_amount") or 0.0))
                sum_row["Total facility (submitted)"] = float(as_10dp(det.get("total_facility") or 0.0))
                st.caption("Restructure and write-off summary")
                df_sum = pd.DataFrame([sum_row])
                if money_df_column_config is not None:
                    st.dataframe(
                        df_sum,
                        column_config=money_df_column_config(
                            df_sum,
                            overrides={},
                            column_disabled={},
                            money_column_alignment="right",
                        ),
                        hide_index=True,
                        width="stretch",
                    )
                else:
                    st.dataframe(df_sum, hide_index=True, width="stretch")

                sc = pd.DataFrame(draft.get("schedule_json") or [])
                if not sc.empty:
                    st.caption("Proposed schedule")
                    st.dataframe(format_schedule_df(sc), hide_index=True, width="stretch")
                if action == "LOAN_MODIFICATION_SPLIT":
                    sc_b = pd.DataFrame(draft.get("schedule_json_secondary") or [])
                    if not sc_b.empty:
                        st.caption("Secondary schedule (split leg B)")
                        st.dataframe(format_schedule_df(sc_b), hide_index=True, width="stretch")
                    extras = det.get("split_schedules_extra")
                    if isinstance(extras, list):
                        for i, blk in enumerate(extras, start=3):
                            dfi = pd.DataFrame(blk or [])
                            if not dfi.empty:
                                st.caption(f"Split leg {i} schedule")
                                st.dataframe(format_schedule_df(dfi), hide_index=True, width="stretch")

                def _f10(raw: object) -> float:
                    try:
                        return float(as_10dp(float(raw or 0.0)))
                    except Exception:
                        return 0.0

                jr_rows: list[dict[str, object]] = []
                if action == "LOAN_RECAST":
                    alloc = dict(det.get("allocation") or {})
                    unap = _f10(det.get("unapplied_applied"))
                    if unap > 0:
                        jr_rows.append({"Entry": "DR", "Account": "Unapplied funds suspense", "Amount": unap})
                    for k, lbl in (
                        ("alloc_fees_charges", "Fees & charges receivable"),
                        ("alloc_penalty_interest", "Penalty interest receivable"),
                        ("alloc_default_interest", "Default interest receivable"),
                        ("alloc_interest_arrears", "Interest arrears receivable"),
                        ("alloc_interest_accrued", "Interest accrued receivable"),
                        ("alloc_principal_arrears", "Principal arrears receivable"),
                        ("alloc_principal_not_due", "Principal not due receivable"),
                    ):
                        amt = _f10(alloc.get(k))
                        if amt > 0:
                            jr_rows.append({"Entry": "CR", "Account": lbl, "Amount": amt})
                else:
                    # Modification approvals can include liquidation + optional write-off/top-up journals.
                    # Schedule rewrite / interest treatment is operational in current flow (no direct GL event).
                    liq_alloc_preview: dict[str, float] = {}
                    if bool((det.get("liquidation_intent") or {}).get("run_before_modification", True)):
                        unap = _f10(det.get("unapplied_balance_snapshot"))
                        buckets = dict(det.get("bucket_snapshot") or {})
                        bal_map = {k: _f10(v) for k, v in buckets.items()}
                        if unap > 0 and bal_map:
                            alloc, _unused = compute_recast_unapplied_allocation(unap, bal_map)
                            liq_alloc_preview = {k: _f10(v) for k, v in alloc.items()}
                            jr_rows.append(
                                {"Entry": "DR", "Account": "Unapplied funds suspense", "Amount": _f10(unap)}
                            )
                            for k, lbl in (
                                ("alloc_fees_charges", "Fees & charges receivable"),
                                ("alloc_penalty_interest", "Penalty interest receivable"),
                                ("alloc_default_interest", "Default interest receivable"),
                                ("alloc_interest_arrears", "Interest arrears receivable"),
                                ("alloc_interest_accrued", "Interest accrued receivable"),
                                ("alloc_principal_arrears", "Principal arrears receivable"),
                                ("alloc_principal_not_due", "Principal not due receivable"),
                            ):
                                amt = _f10(alloc.get(k))
                                if amt > 0:
                                    jr_rows.append({"Entry": "CR", "Account": lbl, "Amount": amt})
                    cap_target = _f10(det.get("carry_amount"))
                    if action == "LOAN_MODIFICATION_SPLIT":
                        split_net = det.get("split_net_by_leg")
                        if isinstance(split_net, list) and split_net:
                            cap_target = _f10(sum(_f10(x) for x in split_net))
                    snap = dict(det.get("bucket_snapshot") or {})
                    pnd = _f10(snap.get("principal_not_due"))
                    cap_needed = _f10(max(0.0, cap_target - pnd))
                    if cap_needed > 1e-10:
                        rem = cap_needed
                        move = {
                            "principal_arrears": 0.0,
                            "regular_interest_accrued": 0.0,
                            "regular_interest_arrears": 0.0,
                            "default_interest_asset": 0.0,
                            "penalty_interest_asset": 0.0,
                            "fees_charges_arrears": 0.0,
                        }
                        cap_order = (
                            ("principal_arrears", "principal_arrears"),
                            ("interest_accrued_balance", "regular_interest_accrued"),
                            ("interest_arrears_balance", "regular_interest_arrears"),
                            ("default_interest_balance", "default_interest_asset"),
                            ("penalty_interest_balance", "penalty_interest_asset"),
                            ("fees_charges_balance", "fees_charges_arrears"),
                        )
                        for state_k, tag_k in cap_order:
                            avail = _f10(snap.get(state_k))
                            if rem <= 1e-10:
                                break
                            take = _f10(min(rem, avail))
                            if take > 0:
                                move[tag_k] = _f10(move.get(tag_k, 0.0) + take)
                                rem = _f10(rem - take)
                        if rem > 1e-6:
                            st.warning(
                                f"Capitalisation preview short by {rem:,.2f}; "
                                "restructure amount may exceed movable non-principal balances."
                            )
                        if _f10(sum(move.values())) > 1e-10:
                            try:
                                from accounting.service import AccountingService

                                svc = AccountingService()
                                cap_payload = {"loan_principal": as_10dp(_f10(sum(move.values())))}
                                for k, v in move.items():
                                    if _f10(v) > 0:
                                        cap_payload[k] = as_10dp(v)
                                sim_cap = svc.simulate_event(
                                    "LOAN_RESTRUCTURE_CAPITALISE",
                                    payload=cap_payload,
                                    loan_id=int(draft.get("loan_id") or 0),
                                )
                                for ln in (sim_cap.lines or []):
                                    jr_rows.append(
                                        {
                                            "Entry": "DR" if float(ln.get("debit") or 0) > 0 else "CR",
                                            "Account": f"{ln.get('account_name')} ({ln.get('account_code')})",
                                            "Amount": float(
                                                ln.get("debit")
                                                if float(ln.get("debit") or 0) > 0
                                                else ln.get("credit") or 0
                                            ),
                                        }
                                    )
                            except Exception as ex:
                                st.warning(f"Capitalisation journal preview unavailable: {ex}")
                    try:
                        from accounting.service import AccountingService

                        svc = AccountingService()
                        fp = dict(det.get("fee_and_proceeds") or {})
                        rf_pct = _f10(fp.get("restructure_fee_pct") or 0.0)
                        rf_amt = 0.0
                        if rf_pct > 0:
                            split_carry = det.get("split_carry_by_leg")
                            if isinstance(split_carry, list) and split_carry:
                                rf_amt = _f10(sum(_f10(x) for x in split_carry) * (rf_pct / 100.0))
                            else:
                                rf_amt = _f10(_f10(det.get("carry_amount") or 0.0) * (rf_pct / 100.0))
                        if rf_amt > 0:
                            sim_rf = svc.simulate_event(
                                "RESTRUCTURE_FEE_CHARGE",
                                payload={
                                    "loan_principal": as_10dp(rf_amt),
                                    "deferred_fee_liability": as_10dp(rf_amt),
                                },
                                loan_id=int(draft.get("loan_id") or 0),
                            )
                            for ln in (sim_rf.lines or []):
                                jr_rows.append(
                                    {
                                        "Entry": "DR" if float(ln.get("debit") or 0) > 0 else "CR",
                                        "Account": f"{ln.get('account_name')} ({ln.get('account_code')})",
                                        "Amount": float(
                                            ln.get("debit")
                                            if float(ln.get("debit") or 0) > 0
                                            else ln.get("credit") or 0
                                        ),
                                    }
                                )
                        writeoff = _f10(det.get("writeoff_amount"))
                        if writeoff > 0:
                            sim_wo = svc.simulate_event(
                                "PRINCIPAL_WRITEOFF",
                                payload={
                                    "allowance_credit_losses": as_10dp(writeoff),
                                    "loan_principal": as_10dp(writeoff),
                                },
                                loan_id=int(draft.get("loan_id") or 0),
                            )
                            for ln in (sim_wo.lines or []):
                                jr_rows.append(
                                    {
                                        "Entry": "DR" if float(ln.get("debit") or 0) > 0 else "CR",
                                        "Account": f"{ln.get('account_name')} ({ln.get('account_code')})",
                                        "Amount": float(
                                            ln.get("debit")
                                            if float(ln.get("debit") or 0) > 0
                                            else ln.get("credit") or 0
                                        ),
                                    }
                                )
                        topup = _f10(det.get("topup_amount"))
                        if topup > 0:
                            p_top = {
                                "loan_principal": as_10dp(topup),
                                "cash_operating": as_10dp(topup),
                                "deferred_fee_liability": as_10dp(0),
                            }
                            mod_det = det.get("modification_loan_details") or {}
                            cash_gl = str(mod_det.get("cash_gl_account_id") or "").strip()
                            if cash_gl:
                                p_top["account_overrides"] = {"cash_operating": cash_gl}
                            sim_tu = svc.simulate_event(
                                "LOAN_APPROVAL",
                                payload=p_top,
                                loan_id=int(draft.get("loan_id") or 0),
                            )
                            for ln in (sim_tu.lines or []):
                                jr_rows.append(
                                    {
                                        "Entry": "DR" if float(ln.get("debit") or 0) > 0 else "CR",
                                        "Account": f"{ln.get('account_name')} ({ln.get('account_code')})",
                                        "Amount": float(
                                            ln.get("debit")
                                            if float(ln.get("debit") or 0) > 0
                                            else ln.get("credit") or 0
                                        ),
                                    }
                                )
                        if rf_amt > 0:
                            st.caption(
                                "Restructure fee amortisation posts monthly via "
                                "`RESTRUCTURE_FEE_AMORTISATION` at month-end."
                            )
                    except Exception:
                        # Fallback labels when accounting simulation is unavailable.
                        writeoff = _f10(det.get("writeoff_amount"))
                        if writeoff > 0:
                            jr_rows.append(
                                {"Entry": "DR", "Account": "Impairment / write-off expense", "Amount": writeoff}
                            )
                            jr_rows.append({"Entry": "CR", "Account": "Principal receivable", "Amount": writeoff})
                        topup = _f10(det.get("topup_amount"))
                        if topup > 0:
                            jr_rows.append({"Entry": "DR", "Account": "Principal receivable", "Amount": topup})
                            jr_rows.append({"Entry": "CR", "Account": "Cash / bank", "Amount": topup})

                    oit = str(det.get("outstanding_interest_treatment") or "capitalise").strip().lower()
                    if oit == "capitalise":
                        snap = det.get("bucket_snapshot") or {}
                        cap_interest_accrued = _f10(snap.get("interest_accrued_balance")) - _f10(
                            liq_alloc_preview.get("alloc_interest_accrued")
                        )
                        cap_interest_arrears = _f10(snap.get("interest_arrears_balance")) - _f10(
                            liq_alloc_preview.get("alloc_interest_arrears")
                        )
                        cap_default = _f10(snap.get("default_interest_balance")) - _f10(
                            liq_alloc_preview.get("alloc_default_interest")
                        )
                        cap_penalty = _f10(snap.get("penalty_interest_balance")) - _f10(
                            liq_alloc_preview.get("alloc_penalty_interest")
                        )
                        cap_amt = _f10(
                            max(0.0, cap_interest_accrued)
                            + max(0.0, cap_interest_arrears)
                            + max(0.0, cap_default)
                            + max(0.0, cap_penalty)
                        )
                        if cap_amt > 0:
                            wo_now = _f10(det.get("writeoff_amount"))
                            if cap_needed > 1e-10:
                                st.info(
                                    f"Interest treatment is **capitalise** ({cap_amt:,.2f}). "
                                    f"Only **{cap_needed:,.2f}** is capitalised to make principal_not_due sufficient; "
                                    f"remaining excess is write-off (**{wo_now:,.2f}**)."
                                )
                            else:
                                st.info(
                                    f"Interest treatment is **capitalise** ({cap_amt:,.2f}), but no "
                                    "principal_not_due top-up is required; excess remains handled by write-off policy."
                                )
                        elif liq_alloc_preview:
                            st.info(
                                "Interest treatment is **capitalise**, but interest balances are already cleared "
                                "by liquidation preview; no additional capitalisation is required."
                            )
                with st.expander("Preview journals to be posted", expanded=True):
                    if jr_rows:
                        st.dataframe(pd.DataFrame(jr_rows), hide_index=True, width="stretch")
                    else:
                        st.caption("No projected journal lines for this draft.")

                qa_btn1, qa_btn2, qa_btn3 = st.columns(3)
                with qa_btn1:
                    if st.button("Approve draft", type="primary", key=f"reamod_approve_{pick_id}"):
                        try:
                            loan_out = approve_loan_approval_draft(int(pick_id), approved_by=created_by or "approver")
                            st.success(f"Draft #{pick_id} approved for loan #{loan_out}.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
                with qa_btn2:
                    sb_note = st.text_input(
                        "Send-back note",
                        value="",
                        key=f"reamod_sendback_note_{pick_id}",
                        placeholder="Reason for correction",
                    )
                    if st.button("Send back", type="secondary", key=f"reamod_sendback_{pick_id}"):
                        try:
                            send_back_loan_approval_draft(
                                int(pick_id),
                                note=sb_note or "Returned for correction",
                                actor=created_by or "approver",
                            )
                            st.success(f"Draft #{pick_id} sent back.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
                with qa_btn3:
                    if st.button("Dismiss", type="secondary", key=f"reamod_dismiss_{pick_id}"):
                        try:
                            dismiss_loan_approval_draft(
                                int(pick_id),
                                note="Dismissed in Reamortisation approvals",
                                actor=created_by or "approver",
                            )
                            st.success(f"Draft #{pick_id} dismissed.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))

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
