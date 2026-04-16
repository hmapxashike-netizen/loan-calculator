"""Teller: single repayment, batch upload, reverse receipt, borrowings, write-off recovery."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd
import streamlit as st


from style import BRAND_GREEN, BRAND_TEXT_MUTED, inject_style_block, render_sub_header, render_sub_sub_header

from ui.streamlit_feedback import run_with_spinner

_logger = logging.getLogger(__name__)


def _trace_ui_enabled() -> bool:
    return os.environ.get("FARNDACRED_TRACE_TELLER_UI", "").strip().lower() in ("1", "true", "yes", "on")


def _inject_teller_green_primary_submit_css_once() -> None:
    """Brand-green primary form submits for Teller CTAs (scoped by accessible name)."""
    if st.session_state.get("_farnda_teller_green_submit_css"):
        return
    st.session_state["_farnda_teller_green_submit_css"] = True
    g = BRAND_GREEN
    inject_style_block(
        f"""
[data-testid="stMain"] button[data-testid="stBaseButton-primaryFormSubmit"][aria-label*="Record repayment"],
[data-testid="stMain"] button[data-testid="stBaseButton-primaryFormSubmit"][aria-label*="Post borrowing payment"],
[data-testid="stMain"] button[data-testid="stBaseButton-primaryFormSubmit"][aria-label*="Post recovery receipt"],
[data-testid="stMain"] button[kind="primaryFormSubmit"][aria-label*="Record repayment"],
[data-testid="stMain"] button[kind="primaryFormSubmit"][aria-label*="Post borrowing payment"],
[data-testid="stMain"] button[kind="primaryFormSubmit"][aria-label*="Post recovery receipt"] {{
  background-color: {g} !important;
  color: #ffffff !important;
  border: none !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.12) !important;
}}
[data-testid="stMain"] button[data-testid="stBaseButton-primaryFormSubmit"][aria-label*="Record repayment"]:hover,
[data-testid="stMain"] button[data-testid="stBaseButton-primaryFormSubmit"][aria-label*="Post borrowing payment"]:hover,
[data-testid="stMain"] button[data-testid="stBaseButton-primaryFormSubmit"][aria-label*="Post recovery receipt"]:hover,
[data-testid="stMain"] button[kind="primaryFormSubmit"][aria-label*="Record repayment"]:hover,
[data-testid="stMain"] button[kind="primaryFormSubmit"][aria-label*="Post borrowing payment"]:hover,
[data-testid="stMain"] button[kind="primaryFormSubmit"][aria-label*="Post recovery receipt"]:hover {{
  filter: brightness(0.93) !important;
}}
"""
    )


def render_teller_ui(
    *,
    customers_available: bool,
    loan_management_available: bool,
    loan_management_error: str,
    list_customers,
    get_display_name,
    get_loans_by_customer,
    get_system_date,
    source_cash_gl_cached_labels_and_ids,
    source_cash_gl_widget_label: str,
    source_cash_gl_cache_empty_warning,
) -> None:
    t_ui0 = time.perf_counter()
    if not customers_available:
        st.error("Customer module is required for Teller. Check database connection.")
        return
    if not loan_management_available:
        st.error(f"Loan management module is not available. ({loan_management_error})")
        return

    _inject_teller_green_primary_submit_css_once()

    from accounting.service import AccountingService
    from services import teller_service

    try:
        from rbac.subfeature_access import (
            teller_can_batch_and_reverse,
            teller_can_scheduled_receipts,
            teller_can_single_receipt,
        )
    except Exception:

        def teller_can_single_receipt(user=None) -> bool:  # type: ignore[misc]
            return True

        def teller_can_batch_and_reverse(user=None) -> bool:  # type: ignore[misc]
            return True

        def teller_can_scheduled_receipts(user=None) -> bool:  # type: ignore[misc]
            return True

    _teller_sections: list[str] = []
    if teller_can_single_receipt():
        _teller_sections.append("Single repayment")
    if teller_can_batch_and_reverse():
        _teller_sections.extend(
            [
                "Batch payments",
                "Reverse receipt",
                "Receipt from fully written-off loan",
            ]
        )
    if teller_can_scheduled_receipts():
        _teller_sections.append("Scheduled receipts (data take-on)")
    if not _teller_sections:
        st.warning("You do not have permission for any Teller areas for this role.")
        return
    st.session_state.setdefault("teller_subnav", _teller_sections[0])
    if st.session_state["teller_subnav"] not in _teller_sections:
        st.session_state["teller_subnav"] = _teller_sections[0]
    st.markdown(
        '<p class="farnda-teller-section-nav" aria-hidden="true"></p>',
        unsafe_allow_html=True,
    )
    st.radio(
        "Teller section",
        _teller_sections,
        key="teller_subnav",
        horizontal=True,
        label_visibility="collapsed",
    )
    _teller_active = st.session_state["teller_subnav"]

    if _teller_active == "Single repayment":
        t0 = time.perf_counter()
        render_sub_sub_header("Single repayment capture")
        customers_list = list_customers(status="active") or []
        if _trace_ui_enabled():
            _logger.info(
                "TRACE teller.ui single_repayment list_customers rows=%s wall_s=%.3f",
                len(customers_list),
                time.perf_counter() - t0,
            )
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            t_opts0 = time.perf_counter()
            options = [
                (
                    c["id"],
                    (str(c.get("display_name") or "").strip() or get_display_name(c["id"]) or f"Customer #{c['id']}"),
                )
                for c in customers_list
            ]
            labels = [f"{name} (ID {cid})" for cid, name in options]
            idx = 0
            if "teller_customer_id" in st.session_state:
                try:
                    idx = next(i for i, (cid, _) in enumerate(options) if cid == st.session_state["teller_customer_id"])
                except StopIteration:
                    pass
            if _trace_ui_enabled():
                _logger.info(
                    "TRACE teller.ui single_repayment build_customer_options rows=%s wall_s=%.3f",
                    len(options),
                    time.perf_counter() - t_opts0,
                )
            t_top0 = time.perf_counter()
            pick_col1, pick_col2, pick_col3 = st.columns([1.15, 1.2, 0.85], gap="small")
            with pick_col1:
                st.caption("Customer")
                sel = st.selectbox(
                    "Select customer",
                    labels,
                    index=idx,
                    key="teller_cust_select",
                    label_visibility="collapsed",
                )
            cid = options[labels.index(sel)][0] if sel and labels else None
            st.session_state["teller_customer_id"] = cid
            if _trace_ui_enabled():
                _logger.info(
                    "TRACE teller.ui single_repayment top_row_customer_select wall_s=%.3f",
                    time.perf_counter() - t_top0,
                )

            if cid:
                t1 = time.perf_counter()
                loans_list = get_loans_by_customer(cid)
                if _trace_ui_enabled():
                    _logger.info(
                        "TRACE teller.ui single_repayment get_loans_by_customer customer_id=%s rows=%s wall_s=%.3f",
                        cid,
                        len(loans_list or []),
                        time.perf_counter() - t1,
                    )
                loans_active = [l for l in loans_list if l.get("status") == "active"]
                if not loans_active:
                    st.info("No active loans for this customer.")
                else:
                    loan_options = [
                        (l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}")
                        for l in loans_active
                    ]
                    loan_labels = [t[1] for t in loan_options]
                    t_loan_sel0 = time.perf_counter()
                    with pick_col2:
                        st.caption("Loan")
                        loan_sel = st.selectbox(
                            "Select loan",
                            loan_labels,
                            key="teller_loan_select",
                            label_visibility="collapsed",
                        )
                    loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None
                    if _trace_ui_enabled():
                        _logger.info(
                            "TRACE teller.ui single_repayment loan_select_widget rows=%s wall_s=%.3f",
                            len(loan_labels),
                            time.perf_counter() - t_loan_sel0,
                        )

                    if loan_id:
                        t2 = time.perf_counter()
                        summary = teller_service.fetch_teller_amount_due_summary(loan_id)
                        if _trace_ui_enabled():
                            _logger.info(
                                "TRACE teller.ui single_repayment amount_due_summary loan_id=%s ok=%s wall_s=%.3f",
                                loan_id,
                                bool(summary),
                                time.perf_counter() - t2,
                            )
                        amount_due = summary["amount_due_today"] if summary else None
                        help_text = None
                        if amount_due is not None and summary is not None:
                            help_text = (
                                f"Base arrears as at {summary.get('base_as_of_date')}: "
                                f"{float(summary.get('base_total_delinquency_arrears') or 0):,.2f}\n"
                                f"Less today's allocations to arrears buckets: "
                                f"{float(summary.get('today_allocations_to_delinquency') or 0):,.2f}\n"
                                f"Method: {summary.get('method')}"
                            )
                        t_amt_ui0 = time.perf_counter()
                        with pick_col3:
                            st.caption("Amount due today")
                            if amount_due is not None:
                                st.markdown(
                                    f'<p style="font-size:0.875rem;margin:0;color:{BRAND_TEXT_MUTED};">'
                                    f"{amount_due:,.2f}</p>",
                                    unsafe_allow_html=True,
                                )
                                if help_text:
                                    with st.popover("Breakdown", help="How amount due today is derived"):
                                        st.text(help_text)
                            else:
                                st.markdown(
                                    f'<p style="font-size:0.875rem;margin:0;color:{BRAND_TEXT_MUTED};">—</p>',
                                    unsafe_allow_html=True,
                                )
                        if _trace_ui_enabled():
                            _logger.info(
                                "TRACE teller.ui single_repayment amount_due_widgets wall_s=%.3f",
                                time.perf_counter() - t_amt_ui0,
                            )

                        now = datetime.now()
                        _sys = get_system_date()
                        st.caption(
                            "**Source cash / bank GL** — same control as **loan capture** step 1. "
                            "This choice applies to **this receipt only** (not the loan’s disbursement cash). "
                            "**System date** for posting is taken from configured system date."
                        )
                        t3 = time.perf_counter()
                        _t_cash_lab, _t_cash_ids = source_cash_gl_cached_labels_and_ids()
                        if _trace_ui_enabled():
                            _logger.info(
                                "TRACE teller.ui single_repayment source_cash_cache rows=%s wall_s=%.3f",
                                len(_t_cash_ids or []),
                                time.perf_counter() - t3,
                            )
                        t_form0 = time.perf_counter()
                        with st.form("teller_single_form", clear_on_submit=True):
                            row_a1, row_a2, row_a3 = st.columns(3, gap="small")
                            with row_a1:
                                st.caption("Source cash / bank GL (A100000 tree)")
                                if _t_cash_ids:
                                    _t_sel = st.selectbox(
                                        source_cash_gl_widget_label,
                                        range(len(_t_cash_lab)),
                                        format_func=lambda i: _t_cash_lab[i],
                                        key="teller_source_cash_gl",
                                        label_visibility="collapsed",
                                    )
                                    _src_cash_gl = _t_cash_ids[_t_sel]
                                else:
                                    source_cash_gl_cache_empty_warning()
                                    _src_cash_gl = None
                            with row_a2:
                                st.caption("Amount")
                                amount = st.number_input(
                                    "Amount",
                                    min_value=0.00,
                                    value=0.00,
                                    step=100.0,
                                    format="%.2f",
                                    key="teller_amount",
                                    label_visibility="collapsed",
                                )
                            with row_a3:
                                st.caption("Company reference (GL)")
                                company_ref = st.text_input(
                                    "Company reference (appears in general ledger)",
                                    placeholder="e.g. GL ref",
                                    key="teller_company_ref",
                                    label_visibility="collapsed",
                                )
                            row_b1, row_b2, row_b3 = st.columns(3, gap="small")
                            with row_b1:
                                st.caption("Customer reference (statement)")
                                customer_ref = st.text_input(
                                    "Customer reference (appears on loan statement)",
                                    placeholder="e.g. Receipt #123",
                                    key="teller_cust_ref",
                                    label_visibility="collapsed",
                                )
                            with row_b2:
                                st.caption("Value date")
                                value_date = st.date_input(
                                    "Value date",
                                    value=_sys,
                                    key="teller_value_date",
                                    label_visibility="collapsed",
                                )
                            with row_b3:
                                st.empty()
                            if _trace_ui_enabled():
                                _logger.info(
                                    "TRACE teller.ui single_repayment form_widgets_before_submit wall_s=%.3f",
                                    time.perf_counter() - t_form0,
                                )
                            submitted = st.form_submit_button("Record repayment", type="primary")
                            if submitted and amount > 0:
                                if not _src_cash_gl:
                                    st.error(
                                        "No source cash account is available. Rebuild the **source cash account cache** "
                                        "(System configurations → Accounting configurations), then try again."
                                    )
                                else:
                                    try:
                                        _system_dt = datetime.combine(_sys, now.time())

                                        def _record_repayment():
                                            return teller_service.record_repayment_with_allocation(
                                                loan_id=loan_id,
                                                amount=amount,
                                                payment_date=value_date,
                                                source_cash_gl_account_id=_src_cash_gl,
                                                customer_reference=customer_ref.strip() or None,
                                                company_reference=company_ref.strip() or None,
                                                value_date=value_date,
                                                system_date=_system_dt,
                                            )

                                        rid = run_with_spinner("Recording repayment…", _record_repayment)
                                        st.success(
                                            f"Repayment recorded. **Repayment ID: {rid}**. "
                                            "Any overpayment was credited to Unapplied Funds."
                                        )
                                    except Exception as e:
                                        st.error(f"Could not record repayment: {e}")
                                        st.exception(e)
        if _trace_ui_enabled():
            _logger.info(
                "TRACE teller.ui single_repayment overall wall_s=%.3f",
                time.perf_counter() - t0,
            )

    elif _teller_active == "Batch payments":
        t_batch0 = time.perf_counter()
        render_sub_sub_header("Batch payments")
        st.caption(
            "Upload an Excel file with repayment rows. **source_cash_gl_account_id** must be a UUID that appears in the "
            "**source cash account cache** (same list as Teller — leaves under **A100000**). Rebuild the cache under "
            "**System configurations → Accounting configurations** when the chart changes."
        )

        t_tpl0 = time.perf_counter()
        today = get_system_date().isoformat()
        _tpl_bytes = teller_service.build_batch_upload_template_excel_bytes(
            sample_system_date_iso=today
        )
        if _trace_ui_enabled():
            _logger.info(
                "TRACE teller.ui batch_payments build_template_bytes bytes=%s wall_s=%.3f",
                len(_tpl_bytes or b""),
                time.perf_counter() - t_tpl0,
            )
        t_up0 = time.perf_counter()
        b_col1, b_col2 = st.columns(2)
        with b_col1:
            st.download_button(
                "Download template (Excel)",
                data=_tpl_bytes,
                file_name="teller_batch_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="teller_download_template",
            )
        with b_col2:
            uploaded = st.file_uploader("Upload Excel file", type=["xlsx", "xls"], key="teller_batch_upload")
        if _trace_ui_enabled():
            _logger.info(
                "TRACE teller.ui batch_payments template_row_and_uploader wall_s=%.3f uploaded=%s",
                time.perf_counter() - t_up0,
                bool(uploaded),
            )
        if uploaded:
            try:
                t_read0 = time.perf_counter()
                df = pd.read_excel(uploaded, engine="openpyxl")
                if _trace_ui_enabled():
                    _logger.info(
                        "TRACE teller.ui batch_payments read_excel rows=%s cols=%s wall_s=%.3f",
                        len(df.index),
                        len(df.columns),
                        time.perf_counter() - t_read0,
                    )
                required = ["loan_id", "amount", "source_cash_gl_account_id"]
                missing = [c for c in required if c not in df.columns]
                if missing:
                    st.error(f"Missing columns: {', '.join(missing)}. Use the template.")
                else:
                    t_df0 = time.perf_counter()
                    st.dataframe(df.head(20), width="stretch", hide_index=True)
                    if len(df) > 20:
                        st.caption(f"Showing first 20 of {len(df)} rows.")
                    if _trace_ui_enabled():
                        _logger.info(
                            "TRACE teller.ui batch_payments preview_dataframe wall_s=%.3f",
                            time.perf_counter() - t_df0,
                        )
                    p_col1, p_col2 = st.columns(2)
                    with p_col1:
                        process_batch = st.button("Process batch", type="primary", key="teller_batch_process")
                    with p_col2:
                        st.caption(f"Rows loaded: {len(df)}")
                    if process_batch:
                        t_parse0 = time.perf_counter()
                        valid_rows, parse_errors = teller_service.parse_batch_repayment_rows_from_dataframe(
                            df,
                            fallback_payment_date_iso=get_system_date().isoformat(),
                        )
                        if _trace_ui_enabled():
                            _logger.info(
                                "TRACE teller.ui batch_payments parse_rows valid=%s parse_errors=%s wall_s=%.3f",
                                len(valid_rows),
                                len(parse_errors),
                                time.perf_counter() - t_parse0,
                            )
                        if parse_errors:
                            st.warning(f"Parse issues: {len(parse_errors)} row(s) skipped.")
                            with st.expander("Parse errors"):
                                for err in parse_errors:
                                    st.text(err)
                        if not valid_rows:
                            st.error("No valid rows to process. Ensure loan_id and amount are numeric and positive.")
                        else:
                            t_proc0 = time.perf_counter()

                            def _run_batch():
                                return teller_service.run_batch_repayments(valid_rows)

                            success, fail, errors = run_with_spinner(
                                "Processing batch repayments…",
                                _run_batch,
                            )
                            if _trace_ui_enabled():
                                _logger.info(
                                    "TRACE teller.ui batch_payments run_batch_repayments "
                                    "valid_in=%s success=%s fail=%s wall_s=%.3f",
                                    len(valid_rows),
                                    success,
                                    fail,
                                    time.perf_counter() - t_proc0,
                                )
                            st.success(f"Batch complete: **{success}** repaid, **{fail}** failed.")
                            if errors:
                                with st.expander("Processing errors"):
                                    for err in errors:
                                        st.text(err)
            except Exception as e:
                st.error(f"Could not read file: {e}")
                st.exception(e)
        if _trace_ui_enabled():
            _logger.info(
                "TRACE teller.ui batch_payments branch_overall wall_s=%.3f",
                time.perf_counter() - t_batch0,
            )

    elif _teller_active == "Reverse receipt":
        render_sub_sub_header("Reverse receipt")
        st.caption("Select a customer and loan, then enter a receipt ID or choose one from the list to reverse it.")

        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            options = [
                (
                    c["id"],
                    (str(c.get("display_name") or "").strip() or get_display_name(c["id"]) or f"Customer #{c['id']}"),
                )
                for c in customers_list
            ]
            labels = [f"{name} (ID {cid})" for cid, name in options]
            idx = 0
            if "teller_rev_customer_id" in st.session_state:
                try:
                    idx = next(i for i, (cid, _) in enumerate(options) if cid == st.session_state["teller_rev_customer_id"])
                except StopIteration:
                    pass
            rev_col1, rev_col2 = st.columns(2)
            with rev_col1:
                sel = st.selectbox("Select customer", labels, index=idx, key="teller_rev_cust_select")
            cid = options[labels.index(sel)][0] if sel and labels else None
            st.session_state["teller_rev_customer_id"] = cid

            if cid:
                loans_list = get_loans_by_customer(cid)
                loans_active = [l for l in loans_list if l.get("status") == "active"]
                if not loans_active:
                    st.info("No active loans for this customer.")
                else:
                    loan_options = [
                        (l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}")
                        for l in loans_active
                    ]
                    loan_labels = [t[1] for t in loan_options]
                    with rev_col2:
                        loan_sel = st.selectbox("Select loan", loan_labels, key="teller_rev_loan_select")
                    loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None

                    if loan_id:
                        today = get_system_date()
                        start_date = today - timedelta(days=365)
                        receipts = teller_service.list_recent_receipts_for_loan(
                            loan_id, start_date=start_date, end_date=today
                        )

                        col_id, col_list = st.columns(2)
                        with col_id:
                            manual_id = st.text_input("Receipt ID (optional)", key="teller_rev_manual_id")
                        with col_list:
                            if receipts:
                                receipt_options = []
                                for r in receipts:
                                    rid = int(r.get("id"))
                                    amt = float(r.get("amount") or 0)
                                    vdate = r.get("value_date") or r.get("payment_date")
                                    label = f"ID {rid} | {vdate} | Amount {amt:,.2f}"
                                    receipt_options.append((rid, label))
                                rec_labels = [t[1] for t in receipt_options]
                                sel_label = st.selectbox(
                                    "Or select from recent receipts",
                                    rec_labels if rec_labels else ["(No receipts)"],
                                    key="teller_rev_receipt_select",
                                )
                                selected_id = None
                                if rec_labels and sel_label in rec_labels:
                                    selected_id = receipt_options[rec_labels.index(sel_label)][0]
                            else:
                                st.info("No receipts found for this loan in the last 12 months.")
                                selected_id = None

                        if st.button("Reverse receipt", type="primary", key="teller_rev_button"):
                            target_id = None
                            if manual_id.strip():
                                try:
                                    target_id = int(manual_id.strip())
                                except ValueError:
                                    st.error("Receipt ID must be a number.")
                            elif selected_id is not None:
                                target_id = selected_id

                            if not target_id:
                                st.error("Enter a valid receipt ID or select a receipt from the list.")
                            else:
                                try:
                                    st.info(
                                        f"**Reversal in progress** for receipt **{target_id}**. "
                                        "This will (1) save the reversal and (2) replay/reallocate EOD for the loan."
                                    )
                                    with st.spinner("Reversing receipt and recalculating loan state…"):
                                        rev_result = teller_service.execute_reverse_repayment(target_id)

                                    st.success(
                                        f"Reversal saved for receipt **{target_id}**. "
                                        f"Reversal repayment id **{rev_result.reversal_repayment_id}**."
                                    )

                                    if rev_result.eod_rerun_success:
                                        st.success(
                                            f"Re-allocation/EOD replay **successful** for loan **{rev_result.loan_id}** "
                                            f"from **{rev_result.eod_from_date.isoformat()}** through "
                                            f"**{rev_result.eod_to_date.isoformat()}**."
                                        )
                                    else:
                                        st.error(
                                            f"Reversal was saved, but **re-allocation/EOD replay failed** for loan "
                                            f"**{rev_result.loan_id}** (window "
                                            f"**{rev_result.eod_from_date.isoformat()}** → "
                                            f"**{rev_result.eod_to_date.isoformat()}**). "
                                            f"**Failed stage:** `{(rev_result.eod_rerun_error or 'unknown')}`"
                                        )
                                except Exception as e:
                                    st.error(
                                        f"Could not reverse receipt **{target_id}**. "
                                        f"**Failed stage:** `reverse_repayment` | **Error:** {e}"
                                    )
                                    st.exception(e)

    elif _teller_active == "Receipt from fully written-off loan":
        acct_svc = AccountingService()
        render_sub_sub_header("Receipt from a fully written-off loan")
        st.caption(
            "Use this tab when you receive a recovery on a loan that has been fully written off. "
            "This uses the configured 'WRITEOFF_RECOVERY' journal template "
            "(Debit: CASH AND CASH EQUIVALENTS, Credit: BAD DEBTS RECOVERED)."
        )

        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            options = [
                (
                    c["id"],
                    (str(c.get("display_name") or "").strip() or get_display_name(c["id"]) or f"Customer #{c['id']}"),
                )
                for c in customers_list
            ]
            labels = [f"{name} (ID {cid})" for cid, name in options]
            idx = 0
            if "teller_wr_customer_id" in st.session_state:
                try:
                    idx = next(
                        i for i, (cid, _) in enumerate(options) if cid == st.session_state["teller_wr_customer_id"]
                    )
                except StopIteration:
                    pass
            wr_col1, wr_col2 = st.columns(2)
            with wr_col1:
                sel = st.selectbox("Select customer", labels, index=idx, key="teller_wr_cust_select")
            cid = options[labels.index(sel)][0] if sel and labels else None
            st.session_state["teller_wr_customer_id"] = cid

            if cid:
                loans_list = get_loans_by_customer(cid)
                # Include all loans; recoveries can apply to closed/written-off loans.
                if not loans_list:
                    st.info("No loans found for this customer.")
                else:
                    loan_options = [
                        (
                            l["id"],
                            f"Loan #{l['id']} | Status: {l.get('status', 'unknown')} | Principal: {l.get('principal', 0):,.2f}",
                        )
                        for l in loans_list
                    ]
                    loan_labels = [t[1] for t in loan_options]
                    with wr_col2:
                        loan_sel = st.selectbox(
                            "Select written-off loan (or target loan)", loan_labels, key="teller_wr_loan"
                        )
                    loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None

                    if loan_id:
                        _sys = get_system_date()

                        with st.form("teller_writeoff_recovery_form"):
                            st.caption("Journal entry date follows the configured **system date**; only **value date** is entered below.")
                            wr1, wr2, wr3 = st.columns(3, gap="small")
                            with wr1:
                                st.caption("Receipt value date")
                                value_date = st.date_input(
                                    "Receipt value date",
                                    value=_sys,
                                    key="teller_wr_value_date",
                                    label_visibility="collapsed",
                                )
                            with wr2:
                                st.caption("Recovery amount")
                                amount = st.number_input(
                                    "Recovery amount",
                                    min_value=0.01,
                                    value=100.00,
                                    step=10.00,
                                    format="%.2f",
                                    key="teller_wr_amount",
                                    label_visibility="collapsed",
                                )
                            with wr3:
                                st.caption("Customer reference (optional)")
                                customer_ref = st.text_input(
                                    "Customer reference (optional)",
                                    placeholder="e.g. Recovery receipt #123",
                                    key="teller_wr_cust_ref",
                                    label_visibility="collapsed",
                                )
                            wr4, wr5, wr6 = st.columns(3, gap="small")
                            with wr4:
                                st.caption("Company reference (optional)")
                                company_ref = st.text_input(
                                    "Company reference (optional)",
                                    placeholder="e.g. GL ref",
                                    key="teller_wr_company_ref",
                                    label_visibility="collapsed",
                                )
                            with wr5:
                                st.empty()
                            with wr6:
                                st.empty()
                            submitted = st.form_submit_button("Post recovery receipt", type="primary")

                            if submitted and amount > 0:
                                try:

                                    def _post_writeoff_recovery():
                                        teller_service.post_writeoff_recovery_journal(
                                            acct_svc,
                                            loan_id=int(loan_id),
                                            value_date=value_date,
                                            amount=Decimal(str(amount)),
                                            customer_reference=customer_ref,
                                            company_reference=company_ref,
                                            created_by="teller_ui",
                                        )

                                    run_with_spinner("Posting recovery receipt…", _post_writeoff_recovery)
                                    st.success(
                                        f"Recovery receipt posted successfully for loan #{loan_id}. "
                                        "The GL will debit CASH AND CASH EQUIVALENTS and credit BAD DEBTS RECOVERED."
                                    )
                                except Exception as e:
                                    st.error(f"Error posting recovery receipt journal: {e}")
                                    st.exception(e)

    elif _teller_active == "Scheduled receipts (data take-on)":
        render_sub_sub_header("Scheduled receipts (data take-on)")
        st.warning(
            "**Reserved for migration / data take-on.** Each row must have **value date strictly after** "
            "the system business date. No allocation or repayment GL until **EOD on that value date** "
            "(enable *Activate scheduled receipts* under System configurations → EOD). "
            "Use **Cancel** below if you captured a row in error before its value date."
        )
        _sys = get_system_date()
        _sample_vd = (_sys + timedelta(days=1)).isoformat()
        st.caption(
            "Upload the same columns as **Batch payments** (`loan_id`, `amount`, `payment_date`, `value_date`, "
            "references, `source_cash_gl_account_id`). Template uses a sample **future** value date."
        )
        tpl_b = teller_service.build_scheduled_batch_upload_template_excel_bytes(
            sample_future_value_date_iso=_sample_vd
        )
        dl_col, up_col = st.columns(2)
        with dl_col:
            st.download_button(
                "Download scheduled batch template (Excel)",
                data=tpl_b,
                file_name="teller_scheduled_batch_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="teller_scheduled_download_template",
            )
        with up_col:
            sched_up = st.file_uploader(
                "Upload scheduled receipts (Excel)", type=["xlsx", "xls"], key="teller_scheduled_upload"
            )
        st.checkbox(
            "I confirm these are intentional future-dated scheduled receipts (not normal Teller mistakes).",
            key="teller_scheduled_confirm",
        )
        if sched_up:
            try:
                sdf = pd.read_excel(sched_up, engine="openpyxl")
                st.dataframe(sdf.head(20), width="stretch", hide_index=True)
                if len(sdf.index) > 20:
                    st.caption(f"Showing first 20 of {len(sdf.index)} rows.")
                if st.button("Process scheduled batch", type="primary", key="teller_scheduled_process"):
                    if not st.session_state.get("teller_scheduled_confirm"):
                        st.error("Confirm the checkbox above before processing.")
                    else:
                        valid_s, parse_err_s = teller_service.parse_batch_repayment_rows_from_dataframe(
                            sdf,
                            fallback_payment_date_iso=_sys.isoformat(),
                        )
                        if parse_err_s:
                            st.warning(f"Parse issues: {len(parse_err_s)} row(s) skipped.")
                            with st.expander("Parse errors"):
                                for err in parse_err_s:
                                    st.text(err)
                        if not valid_s:
                            st.error("No valid rows. Check loan_id, amount, and source_cash_gl_account_id.")
                        else:

                            def _run_sched_batch():
                                return teller_service.run_batch_scheduled_repayments(valid_s)

                            ok_s, fail_s, err_s = run_with_spinner(
                                "Recording scheduled receipts…",
                                _run_sched_batch,
                            )
                            st.success(f"Scheduled batch: **{ok_s}** recorded, **{fail_s}** failed.")
                            if err_s:
                                with st.expander("Processing errors"):
                                    for err in err_s:
                                        st.text(err)
            except Exception as e:
                st.error(f"Could not read file: {e}")
                st.exception(e)

        st.divider()
        render_sub_sub_header("List scheduled receipts for a loan")
        ls1, ls2 = st.columns(2)
        with ls1:
            list_loan = st.number_input("Loan ID", min_value=1, step=1, value=1, key="teller_sched_list_loan")
        with ls2:
            st.caption("Shows rows with status **scheduled** only.")
        if st.button("Refresh list", key="teller_sched_list_btn"):
            try:
                rows = teller_service.list_scheduled_rows_for_loan(int(list_loan))
                if not rows:
                    st.info("No scheduled receipts for this loan.")
                else:
                    st.dataframe(rows, width="stretch", hide_index=True)
            except Exception as e:
                st.error(str(e))

        st.divider()
        render_sub_sub_header("Cancel scheduled receipt (before value date)")
        c1, c2, c3 = st.columns(3, gap="small")
        with c1:
            can_rid = st.number_input("Repayment ID", min_value=1, step=1, value=1, key="teller_sched_cancel_rid")
        with c2:
            can_reason = st.text_input(
                "Reason (required)",
                placeholder="e.g. Duplicate capture",
                key="teller_sched_cancel_reason",
                label_visibility="collapsed",
            )
        with c3:
            st.caption("Requires reason; audit trail retained.")
        if st.button("Cancel scheduled receipt", type="secondary", key="teller_sched_cancel_btn"):
            try:
                from middleware import get_current_user

                u = get_current_user()
                who = str((u or {}).get("email") or (u or {}).get("username") or (u or {}).get("id") or "teller_ui")

                def _do_cancel():
                    teller_service.execute_cancel_scheduled_repayment(
                        int(can_rid),
                        reason=can_reason.strip(),
                        cancelled_by=who,
                    )

                run_with_spinner("Cancelling…", _do_cancel)
                st.success(f"Repayment **{int(can_rid)}** cancelled (status set to cancelled).")
            except Exception as e:
                st.error(str(e))

    if _trace_ui_enabled():
        _logger.info("TRACE teller.ui overall wall_s=%.3f active=%s", time.perf_counter() - t_ui0, _teller_active)
