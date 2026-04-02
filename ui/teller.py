"""Teller: single repayment, batch upload, reverse receipt, borrowings, write-off recovery."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd
import streamlit as st

from ui.components import render_green_page_title


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
    if not customers_available:
        st.error("Customer module is required for Teller. Check database connection.")
        return
    if not loan_management_available:
        st.error(f"Loan management module is not available. ({loan_management_error})")
        return

    from accounting.service import AccountingService
    from services import teller_service

    render_green_page_title("Teller")

    acct_svc = AccountingService()

    tab_single, tab_batch, tab_reverse, tab_borrowing_payment, tab_writeoff_recovery = st.tabs(
        [
            "Single repayment",
            "Batch payments",
            "Reverse receipt",
            "Payment of borrowings",
            "Receipt from fully written-off loan",
        ]
    )

    with tab_single:
        st.subheader("Single repayment capture")
        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            options = [(c["id"], get_display_name(c["id"])) for c in customers_list]
            labels = [f"{name} (ID {cid})" for cid, name in options]
            idx = 0
            if "teller_customer_id" in st.session_state:
                try:
                    idx = next(i for i, (cid, _) in enumerate(options) if cid == st.session_state["teller_customer_id"])
                except StopIteration:
                    pass
            pick_col1, pick_col2 = st.columns(2)
            with pick_col1:
                sel = st.selectbox("Select customer", labels, index=idx, key="teller_cust_select")
            cid = options[labels.index(sel)][0] if sel and labels else None
            st.session_state["teller_customer_id"] = cid

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
                    with pick_col2:
                        loan_sel = st.selectbox("Select loan", loan_labels, key="teller_loan_select")
                    loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None

                    if loan_id:
                        summary = teller_service.fetch_teller_amount_due_summary(loan_id)
                        amount_due = summary["amount_due_today"] if summary else None

                        if amount_due is not None and summary is not None:
                            help_text = (
                                f"Base arrears as at {summary.get('base_as_of_date')}: {float(summary.get('base_total_delinquency_arrears') or 0):,.2f}\n"
                                f"Less today's allocations to arrears buckets: {float(summary.get('today_allocations_to_delinquency') or 0):,.2f}\n"
                                f"Method: {summary.get('method')}"
                            )
                            st.metric(
                                label="Amount Due Today",
                                value=f"{amount_due:,.2f}",
                                help=help_text,
                            )

                        now = datetime.now()
                        _sys = get_system_date()
                        st.caption(
                            "**Source cash / bank GL** — same control as **loan capture** step 1. "
                            "This choice applies to **this receipt only** (not the loan’s disbursement cash)."
                        )
                        _t_cash_lab, _t_cash_ids = source_cash_gl_cached_labels_and_ids()
                        with st.form("teller_single_form", clear_on_submit=True):
                            if _t_cash_ids:
                                _t_sel = st.selectbox(
                                    source_cash_gl_widget_label,
                                    range(len(_t_cash_lab)),
                                    format_func=lambda i: _t_cash_lab[i],
                                    key="teller_source_cash_gl",
                                )
                                _src_cash_gl = _t_cash_ids[_t_sel]
                            else:
                                source_cash_gl_cache_empty_warning()
                                _src_cash_gl = None
                            f_col1, f_col2 = st.columns(2)
                            with f_col1:
                                amount = st.number_input("Amount", min_value=0.00, value=0.00, step=100.0, format="%.2f", key="teller_amount")
                                customer_ref = st.text_input("Customer reference (appears on loan statement)", placeholder="e.g. Receipt #123", key="teller_cust_ref")
                            with f_col2:
                                company_ref = st.text_input("Company reference (appears in general ledger)", placeholder="e.g. GL ref", key="teller_company_ref")
                            col1, col2 = st.columns(2)
                            with col1:
                                value_date = st.date_input("Value date", value=_sys, key="teller_value_date")
                            with col2:
                                system_date = st.date_input("System date", value=_sys, key="teller_system_date")
                            submitted = st.form_submit_button("Record repayment")
                            if submitted and amount > 0:
                                if not _src_cash_gl:
                                    st.error(
                                        "No source cash account is available. Rebuild the **source cash account cache** "
                                        "(System configurations → Accounting configurations), then try again."
                                    )
                                else:
                                    try:
                                        rid = teller_service.record_repayment_with_allocation(
                                            loan_id=loan_id,
                                            amount=amount,
                                            payment_date=value_date,
                                            source_cash_gl_account_id=_src_cash_gl,
                                            customer_reference=customer_ref.strip() or None,
                                            company_reference=company_ref.strip() or None,
                                            value_date=value_date,
                                            system_date=datetime.combine(system_date, now.time()),
                                        )
                                        st.success(
                                            f"Repayment recorded. **Repayment ID: {rid}**. "
                                            "Any overpayment was credited to Unapplied Funds."
                                        )
                                    except Exception as e:
                                        st.error(f"Could not record repayment: {e}")
                                        st.exception(e)

    with tab_batch:
        st.subheader("Batch payments")
        st.caption(
            "Upload an Excel file with repayment rows. **source_cash_gl_account_id** must be a UUID that appears in the "
            "**source cash account cache** (same list as Teller — leaves under **A100000**). Rebuild the cache under "
            "**System configurations → Accounting configurations** when the chart changes."
        )

        today = get_system_date().isoformat()
        _tpl_bytes = teller_service.build_batch_upload_template_excel_bytes(
            sample_system_date_iso=today
        )
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
        if uploaded:
            try:
                df = pd.read_excel(uploaded, engine="openpyxl")
                required = ["loan_id", "amount", "source_cash_gl_account_id"]
                missing = [c for c in required if c not in df.columns]
                if missing:
                    st.error(f"Missing columns: {', '.join(missing)}. Use the template.")
                else:
                    st.dataframe(df.head(20), width="stretch", hide_index=True)
                    if len(df) > 20:
                        st.caption(f"Showing first 20 of {len(df)} rows.")
                    p_col1, p_col2 = st.columns(2)
                    with p_col1:
                        process_batch = st.button("Process batch", type="primary", key="teller_batch_process")
                    with p_col2:
                        st.caption(f"Rows loaded: {len(df)}")
                    if process_batch:
                        valid_rows, parse_errors = teller_service.parse_batch_repayment_rows_from_dataframe(
                            df,
                            fallback_payment_date_iso=get_system_date().isoformat(),
                        )
                        if parse_errors:
                            st.warning(f"Parse issues: {len(parse_errors)} row(s) skipped.")
                            with st.expander("Parse errors"):
                                for err in parse_errors:
                                    st.text(err)
                        if not valid_rows:
                            st.error("No valid rows to process. Ensure loan_id and amount are numeric and positive.")
                        else:
                            success, fail, errors = teller_service.run_batch_repayments(valid_rows)
                            st.success(f"Batch complete: **{success}** repaid, **{fail}** failed.")
                            if errors:
                                with st.expander("Processing errors"):
                                    for err in errors:
                                        st.text(err)
            except Exception as e:
                st.error(f"Could not read file: {e}")
                st.exception(e)

    with tab_reverse:
        st.subheader("Reverse receipt")
        st.caption("Select a customer and loan, then enter a receipt ID or choose one from the list to reverse it.")

        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            options = [(c["id"], get_display_name(c["id"])) for c in customers_list]
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

    with tab_borrowing_payment:
        st.subheader("Payment of borrowings")
        st.caption(
            "Use this tab to post payments made to external lenders/borrowings. "
            "This uses the configured 'BORROWING_REPAYMENT' journal template."
        )

        _sys = get_system_date()

        with st.form("teller_borrowing_payment_form"):
            bw_col1, bw_col2 = st.columns(2)
            with bw_col1:
                value_date = st.date_input("Payment value date", value=_sys, key="teller_borrowing_value_date")
                amount = st.number_input(
                    "Payment amount",
                    min_value=0.01,
                    value=1000.00,
                    step=100.00,
                    format="%.2f",
                    key="teller_borrowing_amount",
                )
                reference = st.text_input(
                    "Reference",
                    placeholder="e.g. Borrowing repayment ref",
                    key="teller_borrowing_ref",
                )
            with bw_col2:
                st.date_input("System date", value=_sys, key="teller_borrowing_system_date")
                description = st.text_input(
                    "Narration (Description)",
                    placeholder="e.g. Payment of borrowing to financier X",
                    key="teller_borrowing_desc",
                )

            submitted = st.form_submit_button("Post borrowing payment")
            if submitted:
                try:
                    teller_service.post_borrowing_repayment_journal(
                        acct_svc,
                        value_date=value_date,
                        amount=Decimal(str(amount)),
                        reference=reference,
                        description=description.strip() or "Payment of borrowings",
                        created_by="teller_ui",
                    )
                    st.success("Borrowing payment journal posted successfully.")
                except Exception as e:
                    st.error(f"Error posting borrowing payment journal: {e}")
                    st.exception(e)

    with tab_writeoff_recovery:
        st.subheader("Receipt from a fully written-off loan")
        st.caption(
            "Use this tab when you receive a recovery on a loan that has been fully written off. "
            "This uses the configured 'WRITEOFF_RECOVERY' journal template "
            "(Debit: CASH AND CASH EQUIVALENTS, Credit: BAD DEBTS RECOVERED)."
        )

        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            options = [(c["id"], get_display_name(c["id"])) for c in customers_list]
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
                            wrf_col1, wrf_col2 = st.columns(2)
                            with wrf_col1:
                                value_date = st.date_input(
                                    "Receipt value date", value=_sys, key="teller_wr_value_date"
                                )
                                amount = st.number_input(
                                    "Recovery amount",
                                    min_value=0.01,
                                    value=100.00,
                                    step=10.00,
                                    format="%.2f",
                                    key="teller_wr_amount",
                                )
                                customer_ref = st.text_input(
                                    "Customer reference (optional)",
                                    placeholder="e.g. Recovery receipt #123",
                                    key="teller_wr_cust_ref",
                                )
                            with wrf_col2:
                                st.date_input(
                                    "System date", value=_sys, key="teller_wr_system_date"
                                )
                                company_ref = st.text_input(
                                    "Company reference (optional)",
                                    placeholder="e.g. GL ref",
                                    key="teller_wr_company_ref",
                                )
                            submitted = st.form_submit_button("Post recovery receipt")

                            if submitted and amount > 0:
                                try:
                                    teller_service.post_writeoff_recovery_journal(
                                        acct_svc,
                                        loan_id=int(loan_id),
                                        value_date=value_date,
                                        amount=Decimal(str(amount)),
                                        customer_reference=customer_ref,
                                        company_reference=company_ref,
                                        created_by="teller_ui",
                                    )
                                    st.success(
                                        f"Recovery receipt posted successfully for loan #{loan_id}. "
                                        "The GL will debit CASH AND CASH EQUIVALENTS and credit BAD DEBTS RECOVERED."
                                    )
                                except Exception as e:
                                    st.error(f"Error posting recovery receipt journal: {e}")
                                    st.exception(e)
