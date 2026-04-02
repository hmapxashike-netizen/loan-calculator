"""Loan management UI: update safe details, approve drafts, view schedule."""

from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st

from display_formatting import format_display_currency
from ui.components import render_centered_html_table

def render_update_loans_ui(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    customers_available: bool,
    list_customers,
    get_display_name,
    get_loans_by_customer,
    update_loan_safe_details,
    save_loan_approval_draft,
    provisions_config_ok: bool,
    list_provision_security_subtypes,
) -> None:
        """UI for updating non-financial loan details and requesting loan termination."""
        st.subheader("Update / Terminate loans")
        if not loan_management_available:
            st.error(f"Loan management module is not available. ({loan_management_error})")
            return
    
        update_flash = st.session_state.pop("update_loans_flash", None)
        if update_flash:
            st.success(update_flash)
    
        customers = list_customers() if customers_available else []
        if not customers:
            st.info("No customers available. Create a customer first.")
            return
    
        # ~45% + 45% + 10% spacer; small gap between the two selects for clarity
        _cust_col, _loan_col, _upd_sp = st.columns([9, 9, 2], gap="small", vertical_alignment="center")
        with _cust_col:
            cust_sel = st.selectbox(
                "Select Customer",
                [get_display_name(c["id"]) for c in customers],
                key="update_loan_cust",
            )
        cust_id = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel)

        loans = get_loans_by_customer(cust_id)
        loans_active = [l for l in loans if l.get("status") == "active"]

        loan_options = [
            (l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}")
            for l in loans_active
        ]

        loan_labels = [t[1] for t in loan_options]
        with _loan_col:
            if not loans_active:
                st.selectbox(
                    "Select loan to update",
                    ["(no active loans)"],
                    disabled=True,
                    key="update_loan_sel",
                )
            else:
                loan_sel_label = st.selectbox(
                    "Select loan to update",
                    loan_labels,
                    key="update_loan_sel",
                )

        if not loans_active:
            st.info("No active loans for this customer.")
            return

        loan_id = next(t[0] for t in loan_options if t[1] == loan_sel_label)
        loan = next(l for l in loans_active if l["id"] == loan_id)
    
        tab_edit, tab_term = st.tabs(["Edit Safe Details", "Terminate Loan Request"])
    
        with tab_edit:
            st.markdown("**Update Non-Financial Details**")
            st.caption("Changes here apply immediately and do not affect schedules or GL postings.")
    
            subtypes = []
            if provisions_config_ok and list_provision_security_subtypes:
                try:
                    subtypes = list_provision_security_subtypes()
                except Exception:
                    pass
            # Rows from provisions.config.list_security_subtypes: security_type, subtype_name (not "name").
            subtype_options = [("", "None / Unsecured")] + [
                (
                    str(s["id"]),
                    f"{s.get('security_type', '')} · {s.get('subtype_name', s.get('name', ''))}".strip(" ·")
                    or f"Subtype #{s.get('id')}",
                )
                for s in subtypes
            ]
    
            curr_sub_id = str(loan.get("collateral_security_subtype_id") or "")
            curr_idx = 0
            for i, opt in enumerate(subtype_options):
                if opt[0] == curr_sub_id:
                    curr_idx = i
                    break
    
            c1, c2, c3 = st.columns(3)
            with c1:
                new_sub_label = st.selectbox(
                    "Collateral Security Subtype",
                    [opt[1] for opt in subtype_options],
                    index=curr_idx,
                    key="update_loan_coll_sub"
                )
            with c2:
                new_chg = st.number_input(
                    "Collateral Charge Amount",
                    value=float(loan.get("collateral_charge_amount") or 0.0),
                    min_value=0.0,
                    step=100.0,
                    key="update_loan_coll_chg"
                )
            with c3:
                new_val = st.number_input(
                    "Collateral Valuation Amount",
                    value=float(loan.get("collateral_valuation_amount") or 0.0),
                    min_value=0.0,
                    step=100.0,
                    key="update_loan_coll_val"
                )
    
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Save Details", key="update_loan_save", type="primary"):
                new_sub_id = next(opt[0] for opt in subtype_options if opt[1] == new_sub_label)
                updates = {
                    "collateral_security_subtype_id": int(new_sub_id) if new_sub_id else None,
                    "collateral_charge_amount": new_chg if new_chg > 0 else None,
                    "collateral_valuation_amount": new_val if new_val > 0 else None,
                }
                try:
                    update_loan_safe_details(loan_id, updates)
                    st.session_state["update_loans_flash"] = f"Details updated for Loan #{loan_id}."
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update details: {e}")
    
        with tab_term:
            st.markdown("**Request Loan Termination**")
            st.caption(
                "If a mistake was made that affects schedules or accruals, the loan must be terminated. "
                "Termination soft-deletes the loan and inactivates its GL journals. "
                "You can then capture a new, corrected loan. "
                "**This action requires approval.**"
            )
    
            reason = st.text_area("Reason for termination", key="update_loan_term_reason")
    
            if st.button("Submit Termination Request", key="update_loan_term_btn", type="primary"):
                if not reason.strip():
                    st.error("A reason is required to request termination.")
                else:
                    try:
                        draft_details = dict(loan)
                        import decimal, datetime
                        for k, v in draft_details.items():
                            if isinstance(v, (datetime.date, datetime.datetime)):
                                draft_details[k] = v.isoformat()
                            elif isinstance(v, decimal.Decimal):
                                draft_details[k] = float(v)
    
                        draft_details["approval_action"] = "TERMINATE"
                        draft_details["termination_reason"] = reason.strip()
    
                        draft_id = save_loan_approval_draft(
                            customer_id=loan["customer_id"],
                            loan_type=loan["loan_type"],
                            details=draft_details,
                            schedule_df=None,
                            product_code=loan.get("product_code"),
                            created_by="ui_user",
                            status="PENDING",
                            loan_id=loan_id,
                        )
                        st.session_state["update_loans_flash"] = f"Termination request submitted (Draft #{draft_id})."
                        for k in list(st.session_state.keys()):
                            if k.startswith("update_loan_") and k != "update_loans_flash":
                                st.session_state.pop(k, None)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to submit termination request: {e}")
    


def render_approve_loans_ui(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    customers_available: bool,
    documents_available: bool,
    list_loan_approval_drafts,
    get_loan_approval_draft,
    get_display_name,
    list_documents,
    get_document,
    upload_document,
    approve_loan_approval_draft,
    send_back_loan_approval_draft,
    dismiss_loan_approval_draft,
    format_schedule_df,
) -> None:
        """Approval inbox for loan drafts submitted from capture Stage 2."""
        st.subheader("Approve loans")
        st.caption(
            "All loan drafts awaiting approval (new submissions and items returned for rework)."
        )
        if not loan_management_available:
            st.error(f"Loan management module is not available. ({loan_management_error})")
            return
        approve_flash = st.session_state.pop("approve_loans_flash_message", None)
        if approve_flash:
            st.success(str(approve_flash))
    
        f1, f2 = st.columns([4, 1])
        with f1:
            search_txt = st.text_input(
                "Search draft",
                placeholder="Draft ID / Customer ID / Product / Loan type",
                key="approve_loan_search",
            )
        with f2:
            st.write("")
            st.write("")
            if st.button("Clear selection", key="approve_clear_selection", width="stretch"):
                st.session_state.pop("approve_selected_draft_id", None)
                st.rerun()

        drafts = list_loan_approval_drafts(
            statuses=["PENDING", "REWORK"],
            search=search_txt.strip() or None,
            limit=500,
        )
        if not drafts:
            st.info("No loan drafts are awaiting approval.")
            return
    
        draft_options = [int(r["id"]) for r in drafts]
        selected_id = st.session_state.get("approve_selected_draft_id")
        if selected_id is not None and int(selected_id) not in draft_options:
            selected_id = None
            st.session_state.pop("approve_selected_draft_id", None)
    
        # When selected, show inspection panel FIRST (top), then keep inbox table below.
        if selected_id is not None:
            draft = get_loan_approval_draft(int(selected_id))
            if draft:
                details = draft.get("details_json") or {}
                schedule_rows = draft.get("schedule_json") or []
                df_schedule = pd.DataFrame(schedule_rows) if schedule_rows else pd.DataFrame()
                customer_name = (
                    get_display_name(int(draft["customer_id"]))
                    if customers_available
                    else f"Customer #{draft['customer_id']}"
                )
    
                st.markdown("### Draft inspection")
                p1, p2, p3, p4 = st.columns(4)
                with p1:
                    st.caption("Identity")
                    st.write(f"Draft: **{draft.get('id')}**")
                    st.write(f"Customer: **{customer_name}**")
                    st.write(f"Loan type: **{draft.get('loan_type')}**")
                    st.write(f"Product: **{draft.get('product_code') or '—'}**")
                with p2:
                    st.caption("Amounts")
                    st.write(f"Principal: **{float(details.get('principal') or 0):,.2f}**")
                    st.write(f"Disbursed: **{float(details.get('disbursed_amount') or 0):,.2f}**")
                    st.write(f"Installment: **{float(details.get('installment') or 0):,.2f}**")
                    st.write(f"Total payment: **{float(details.get('total_payment') or 0):,.2f}**")
                with p3:
                    st.caption("Pricing")
                    st.write(f"Annual rate: **{float(details.get('annual_rate') or 0) * 100:.2f}%**")
                    st.write(f"Monthly rate: **{float(details.get('monthly_rate') or 0) * 100:.2f}%**")
                    st.write(f"Penalty: **{float(details.get('penalty_rate_pct') or 0):.2f}%**")
                    st.write(f"Fees: **{float(details.get('drawdown_fee') or 0) * 100:.2f}% / {float(details.get('arrangement_fee') or 0) * 100:.2f}%**")
                with p4:
                    st.caption("Dates & status")
                    st.write(f"Tenor: **{int(details.get('term') or 0)} months**")
                    st.write(f"First repay: **{details.get('first_repayment_date') or '—'}**")
                    st.write(f"Disbursed on: **{details.get('disbursement_date') or '—'}**")
                    st.write(f"Status: **{draft.get('status')}**")
    
                with st.expander("View documents", expanded=False):
                    if documents_available:
                        docs = list_documents(entity_type="loan_approval_draft", entity_id=int(selected_id))
                        if not docs:
                            st.info("No documents attached to this draft.")
                        else:
                            doc_df = pd.DataFrame(docs)
                            show_doc_cols = [
                                c for c in ["category_name", "file_name", "file_size", "uploaded_by", "uploaded_at", "notes"]
                                if c in doc_df.columns
                            ]
                            st.dataframe(doc_df[show_doc_cols], width="stretch", hide_index=True, height=180)
                    else:
                        st.info("Document module is unavailable.")
    
                with st.expander("View schedule", expanded=False):
                    if df_schedule.empty:
                        st.info("No schedule found for this draft.")
                    else:
                        st.dataframe(format_schedule_df(df_schedule), width="stretch", hide_index=True, height=220)
    
                note = st.text_input("Reviewer note (optional)", key="approve_reviewer_note")
                st.caption(
                    "**Send back to schedule builder** sets the draft to REWORK so capture staff can reload it under "
                    "**Capture loan → See loans for rework**, adjust the schedule, and **Send for approval** again."
                )
                a1, a2, a3 = st.columns(3)
                with a1:
                    if st.button("Approve and create loan", type="primary", key="approve_create_loan_btn"):
                        try:
                            loan_id = approve_loan_approval_draft(int(selected_id), approved_by="approver_ui")
                            # Copy draft documents to final loan entity.
                            doc_count = 0
                            if documents_available:
                                docs = list_documents(entity_type="loan_approval_draft", entity_id=int(selected_id))
                                for row in docs:
                                    full = get_document(int(row["id"]))
                                    if not full:
                                        continue
                                    upload_document(
                                        "loan",
                                        int(loan_id),
                                        int(full["category_id"]),
                                        str(full["file_name"]),
                                        str(full["file_type"]),
                                        int(full["file_size"]),
                                        full["file_content"],
                                        uploaded_by="System User",
                                        notes=str(full.get("notes") or ""),
                                    )
                                    doc_count += 1
                            st.session_state["approve_loans_flash_message"] = (
                                f"Loan approved successfully. Loan #{loan_id} created. "
                                f"{doc_count} document(s) copied."
                            )
                            st.session_state.pop("approve_selected_draft_id", None)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not approve draft: {e}")
                with a2:
                    if st.button("Send back to schedule builder", key="approve_send_back_btn"):
                        try:
                            send_back_loan_approval_draft(int(selected_id), note=note or "", actor="approver_ui")
                            st.session_state["approve_loans_flash_message"] = (
                                f"Draft #{selected_id} sent back to capture (status REWORK). "
                                f"Open **Capture loan → See loans for rework** to edit the schedule."
                            )
                            st.session_state.pop("approve_selected_draft_id", None)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not send back draft: {e}")
                with a3:
                    if st.button("Dismiss draft", key="approve_dismiss_btn"):
                        try:
                            dismiss_loan_approval_draft(int(selected_id), note=note or "", actor="approver_ui")
                            st.session_state["approve_loans_flash_message"] = (
                                f"Draft #{selected_id} dismissed."
                            )
                            st.session_state.pop("approve_selected_draft_id", None)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not dismiss draft: {e}")
    
                st.divider()
    
        # Inbox table always visible; select a row via compact "Open draft" controls.
        st.markdown("### Draft inbox")
        df = pd.DataFrame(drafts)
        show_cols = [
            c
            for c in [
                "id",
                "customer_id",
                "loan_type",
                "product_code",
                "assigned_approver_id",
                "status",
                "submitted_at",
            ]
            if c in df.columns
        ]
        st.dataframe(df[show_cols], width="stretch", hide_index=True, height=280)
    
        o1, o2, o3 = st.columns([2, 1, 1])
        with o1:
            open_label_map = {}
            open_labels = []
            for r in drafts:
                rid = int(r["id"])
                lbl = f"Draft {rid} · Cust {r.get('customer_id')} · {r.get('loan_type')} · {r.get('status')}"
                open_labels.append(lbl)
                open_label_map[lbl] = rid
            draft_pick = st.selectbox("Open draft", open_labels, key="approve_open_pick")
        with o2:
            manual_id = st.number_input("Draft ID", min_value=1, step=1, value=int(open_label_map.get(draft_pick, draft_options[0])), key="approve_open_manual_id")
        with o3:
            st.write("")
            st.write("")
            if st.button("Inspect draft", key="approve_open_btn", width="stretch"):
                st.session_state["approve_selected_draft_id"] = int(manual_id)
                st.rerun()
    


def render_view_schedule_ui(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    customers_available: bool,
    list_customers,
    get_display_name,
    get_loan,
    get_loans_by_customer,
    get_schedule_lines,
    format_schedule_df,
    schedule_export_downloads,
) -> None:
        """View the amortization schedule of an existing loan."""
        if not loan_management_available:
            st.error(f"Loan management module is not available. ({loan_management_error})")
            return

        loan_id = None
        search_by = st.radio("Find loan by", ["Loan ID", "Customer"], key="view_sched_by", horizontal=True)

        if search_by == "Loan ID":
            _half_l, _half_r = st.columns([1, 1])
            with _half_l:
                id_col, btn_col = st.columns([2, 1])
                with id_col:
                    lid_input = st.number_input("Loan ID", min_value=1, value=1, step=1, key="view_sched_loan_id")
                with btn_col:
                    st.write("")
                    st.write("")
                    load_by_id = st.button("Load schedule", key="view_sched_load_by_id", use_container_width=True)
                if load_by_id:
                    loan = get_loan(int(lid_input)) if loan_management_available else None
                    if not loan:
                        st.warning(f"Loan #{lid_input} not found.")
                    else:
                        loan_id = int(lid_input)
                        st.session_state["view_schedule_loan_id"] = loan_id
                loan_id = st.session_state.get("view_schedule_loan_id")
            with _half_r:
                st.empty()
        else:
            if not customers_available:
                st.info("Customer module is required to select by customer.")
            else:
                customers_list = list_customers(status="active") or []
                if not customers_list:
                    st.info("No customers found.")
                else:
                    cust_options = [(c["id"], get_display_name(c["id"]) or f"Customer #{c['id']}") for c in customers_list]
                    cust_labels = [t[1] for t in cust_options]
                    cust_col, loan_col = st.columns([1, 1])
                    with cust_col:
                        cust_sel = st.selectbox("Customer", cust_labels, key="view_sched_cust")
                    cid = cust_options[cust_labels.index(cust_sel)][0] if cust_sel else None
                    with loan_col:
                        if not cid:
                            st.caption("Select a customer to choose a loan.")
                        else:
                            loans_list = get_loans_by_customer(cid)
                            if not loans_list:
                                st.info("No loans for this customer.")
                            else:
                                loan_options = [
                                    (
                                        l["id"],
                                        f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}",
                                    )
                                    for l in loans_list
                                ]
                                loan_labels = [t[1] for t in loan_options]
                                loan_sel = st.selectbox("Loan", loan_labels, key="view_sched_loan_sel")
                                if loan_sel:
                                    loan_id = loan_options[loan_labels.index(loan_sel)][0]
    
        if loan_id:
            try:
                lines = get_schedule_lines(loan_id)
            except Exception as e:
                st.error(f"Could not load schedule: {e}")
                lines = []
    
            if not lines:
                st.info("No schedule stored for this loan (or loan has no instalments yet).")
            else:
                loan_info = get_loan(loan_id)
                if loan_info:
                    _lt_raw = str(loan_info.get("loan_type", "") or "—").strip()
                    _lt_disp = escape(_lt_raw.replace("_", " ").title() if _lt_raw != "—" else "—")
                    _pr_disp = escape(format_display_currency(loan_info.get("principal")))
                    _cust_raw = (
                        get_display_name(loan_info.get("customer_id"))
                        if customers_available
                        else loan_info.get("customer_id")
                    )
                    _cust_disp = escape(str(_cust_raw) if _cust_raw is not None else "—")
                    st.markdown(
                        f"""
<div style="margin:0.2rem 0 1rem 0; font-size:1.02rem; line-height:1.7; color:#0f172a;">
  <span style="font-weight:700;">Loan</span>&nbsp;<span style="font-weight:400;">#{int(loan_id)}</span>
  <span style="display:inline-block; margin:0 1rem; color:#94a3b8; font-weight:300;">|</span>
  <span style="font-weight:700;">Type</span>&nbsp;<span style="font-weight:400;">{_lt_disp}</span>
  <span style="display:inline-block; margin:0 1rem; color:#94a3b8; font-weight:300;">|</span>
  <span style="font-weight:700;">Principal</span>&nbsp;<span style="font-weight:400;">{_pr_disp}</span>
  <span style="display:inline-block; margin:0 1rem; color:#94a3b8; font-weight:300;">|</span>
  <span style="font-weight:700;">Customer</span>&nbsp;<span style="font-weight:400;">{_cust_disp}</span>
</div>
""",
                        unsafe_allow_html=True,
                    )
                df = pd.DataFrame(lines)
                # Map DB column names to display names used by format_schedule_display
                col_map = {
                    "payment": "Payment",
                    "principal": "Principal",
                    "interest": "Interest",
                    "principal_balance": "Principal Balance",
                    "total_outstanding": "Total Outstanding",
                }
                df = df.rename(columns=col_map)
                display_cols = [c for c in ["Period", "Date", "Payment", "Principal", "Interest", "Principal Balance", "Total Outstanding"] if c in df.columns]
                df_display = df[display_cols] if display_cols else df
                _df_vs = format_schedule_df(df_display)
                render_centered_html_table(_df_vs, [str(c) for c in _df_vs.columns])
                schedule_export_downloads(
                    df_display, file_stem=f"loan_{loan_id}_schedule", key_prefix=f"dl_sched_loan_view_{loan_id}"
                )


