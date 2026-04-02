"""Loan capture: flat panel (details, schedule, review, documents)."""

from __future__ import annotations

from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

from display_formatting import format_display_amount
from ui.components import render_centered_html_table

from loans import (
    add_months,
    days_in_month,
    is_last_day_of_month,
    parse_schedule_dates_from_table,
    recompute_customised_from_payments,
)

_CAPTURE_LOAN_DOC_TYPE_NAMES = {
    "Signed Loan Agreement",
    "Facility Letter",
    "Term Sheet",
    "Business Plan",
    "Application Form",
    "Application Letter",
    "Purchase Orders",
    "Offtake Agreement",
    "Supply Agreement",
    "Other",
}


# Loan capture workspace: flat panel, brand colours (#16A34A / #0F766E align with sidebar logo styling).
# Do not revert to tabbed Details/Schedule + separate review step without explicit product sign-off.
_FCAP_BRAND_GREEN = "#16A34A"
_FCAP_BRAND_TEAL = "#0F766E"


def _render_capture_loan_documents_staging(
    *,
    documents_available: bool,
    list_document_categories,
    widget_suffix: str = "",
) -> None:
    """
    Upload + list staged loan documents for capture. ``widget_suffix`` e.g. ``'_rev'`` for review step
    (separate Streamlit widget keys from the schedule-builder step).
    """
    suf = widget_suffix
    if "loan_docs_staged" not in st.session_state:
        st.session_state["loan_docs_staged"] = []
    staged_loan_docs = st.session_state["loan_docs_staged"]
    if not documents_available:
        st.info("Document module is unavailable.")
        return
    doc_cats = list_document_categories(active_only=True)
    name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in _CAPTURE_LOAN_DOC_TYPE_NAMES}
    if not name_to_cat:
        st.info("No matching loan document categories configured.")
        return
    dcol1, dcol2 = st.columns(2)
    with dcol1:
        doc_type = st.selectbox(
            "Doc Type",
            sorted(name_to_cat.keys()),
            key=f"loan_doc_type{suf}",
        )
        other_label = ""
        if doc_type == "Other":
            other_label = st.text_input(
                "If Other, Describe The Document",
                key=f"loan_doc_other_label{suf}",
            )
    with dcol2:
        f = st.file_uploader("Choose File", type=["pdf", "png", "jpg", "jpeg"], key=f"loan_doc_file{suf}")
    if st.button("Add To List", type="secondary", key=f"loan_doc_add{suf}") and f is not None:
        cat = name_to_cat[doc_type]
        label = other_label.strip() if doc_type == "Other" else ""
        staged_loan_docs.append(
            {
                "category_id": cat["id"],
                "category_name": doc_type,
                "file": f,
                "notes": label or "",
            }
        )
        st.session_state["loan_docs_staged"] = staged_loan_docs
        st.success(f"Staged {f.name} as {doc_type}.")
        st.rerun()
    if staged_loan_docs:
        st.markdown("**Staged documents:**")
        for idx, row in enumerate(staged_loan_docs, start=1):
            cat_name = row.get("category_name") or "Document"
            _lbl = (row.get("notes") or "").strip()
            _line = f"{idx}. {row['file'].name} · {cat_name}"
            if _lbl:
                _line = f"{_line} · {_lbl}"
            st.write(_line)


def _fcapture_inject_css_once() -> None:
    """Loan capture only: scoped via :has(.fcapture-scope). Bump _fcapture_panel_css_v* when CSS changes."""
    if st.session_state.get("_fcapture_panel_css_v10"):
        return
    st.session_state["_fcapture_panel_css_v10"] = True
    _g = _FCAP_BRAND_GREEN
    _t = _FCAP_BRAND_TEAL
    st.markdown(
        f"""
<style>
/* ---- Loan capture: compact panel (marker .fcapture-scope on page) ---- */
main .block-container:has(.fcapture-scope) {{
  --fcap-green: {_g};
  --fcap-teal: {_t};
  --fcap-ink: #0a0a0a;
  color: var(--fcap-ink);
  font-size: 1.08rem;
  line-height: 1.32;
  padding-top: 0.35rem !important;
  padding-bottom: 0.45rem !important;
}}
main .block-container:has(.fcapture-scope) h3 {{
  margin-top: 0.2rem !important;
  margin-bottom: 0.15rem !important;
  font-size: 1.25rem !important;
}}
main .block-container:has(.fcapture-scope) .stMarkdown,
main .block-container:has(.fcapture-scope) label,
main .block-container:has(.fcapture-scope) [data-testid="stWidgetLabel"] {{
  color: var(--fcap-ink) !important;
}}
main .block-container:has(.fcapture-scope) [data-testid="stCaptionContainer"] {{
  margin-top: 0.05rem !important;
  margin-bottom: 0.08rem !important;
}}
main .block-container:has(.fcapture-scope) [data-testid="stCaptionContainer"] p {{
  font-size: 0.95rem !important;
  color: #334155 !important;
  margin-bottom: 0 !important;
}}
main .block-container:has(.fcapture-scope) [data-baseweb="select"] span,
main .block-container:has(.fcapture-scope) [data-baseweb="input"] input {{
  font-size: 0.98em !important;
}}
main .block-container:has(.fcapture-scope) [data-testid="stDataFrame"] {{
  font-size: 0.95em !important;
}}
main .block-container:has(.fcapture-scope) [data-baseweb="select"]:focus-within {{
  transform: translateX(-4px);
  transition: transform 0.16s ease, box-shadow 0.16s ease;
  box-shadow: -2px 0 0 0 var(--fcap-teal);
  border-radius: 4px;
}}
main .block-container:has(.fcapture-scope) .stTextInput input:focus-visible,
main .block-container:has(.fcapture-scope) .stNumberInput input:focus-visible,
main .block-container:has(.fcapture-scope) [data-baseweb="input"] input:focus-visible {{
  outline: 2px solid var(--fcap-teal) !important;
  outline-offset: 2px;
  transform: translateX(-3px);
  transition: transform 0.16s ease;
}}
main .block-container:has(.fcapture-scope) [data-baseweb="popover"] {{
  z-index: 900 !important;
}}
.fcapture-soft {{
  border: 0;
  border-top: 1px solid #94a3b8;
  margin: 0.1rem 0;
}}
main .block-container:has(.fcapture-scope) [data-testid="stVerticalBlockBorderWrapper"] {{
  padding: 0.35rem 0.45rem 0.4rem 0.45rem !important;
}}
main .block-container:has(.fcapture-scope) [data-testid="stVerticalBlock"] > div {{
  gap: 0.22rem !important;
}}
/* Do not restyle Loan Management segmented subnav (same block-container once .fcapture-scope exists). */
main .block-container:has(.fcapture-scope) [data-testid="stHorizontalBlock"]:not(:has(.farnda-lm-segbar-root)) {{
  gap: 0.35rem !important;
}}
/* Draft panel triggers: hyperlink-style tertiary buttons */
main .block-container:has(.fcapture-scope) button[data-testid="baseButton-tertiary"] {{
  color: #1d4ed8 !important;
  font-weight: 600 !important;
  text-decoration: underline !important;
  text-underline-offset: 0.12em;
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  padding: 0.05rem 0.25rem 0.15rem 0 !important;
  min-height: auto !important;
}}
main .block-container:has(.fcapture-scope) button[data-testid="baseButton-tertiary"]:hover {{
  color: #1e40af !important;
}}
</style>
        """,
        unsafe_allow_html=True,
    )


def _fcapture_clear_session_after_submit() -> None:
    """Clear loan capture session keys after successful send (widget state resets on rerun; amounts default via value=0)."""
    for k in list(st.session_state.keys()):
        if (k.startswith("capture_") or k.startswith("cap_")) and k not in (
            "capture_flash_message",
            "_fcapture_panel_css_v10",
        ):
            st.session_state.pop(k, None)
    st.session_state["loan_docs_staged"] = []
    st.session_state["capture_loan_step"] = 0


def render_capture_loan_ui(
    *,
    documents_available: bool,
    list_document_categories,
    upload_document,
    loan_management_available: bool,
    list_customers,
    get_display_name,
    list_products,
    get_product_config_from_db,
    list_loan_purposes,
    get_loan_purpose_by_id,
    users_for_rm_available: bool,
    list_users_for_selection,
    agents_available: bool,
    list_agents,
    get_cached_source_cash_account_entries,
    source_cash_gl_cached_labels_and_ids,
    source_cash_gl_cache_empty_warning,
    list_loan_approval_drafts,
    get_loan_approval_draft,
    provisions_config_ok: bool,
    list_provision_security_subtypes,
    provision_schema_ready_fn,
    get_system_config,
    get_consumer_schemes,
    get_product_rate_basis,
    get_system_date,
    format_schedule_df,
    money_df_column_config,
    schedule_editor_disabled_amounts,
    compute_consumer_schedule,
    compute_term_schedule,
    compute_bullet_schedule,
    first_repayment_from_customised_table,
    pct_to_monthly,
) -> None:
    """Capture loan: single flat panel — details, schedule, review, documents; brand-styled section rules."""
    from services import capture_service

    if "capture_loan_step" not in st.session_state:
        st.session_state["capture_loan_step"] = 0

    def _stage1_session_details() -> dict:
        return {
            "agent_id": st.session_state.get("capture_agent_id"),
            "relationship_manager_id": st.session_state.get("capture_relationship_manager_id"),
            "disbursement_bank_option_id": None,
            "cash_gl_account_id": st.session_state.get("capture_cash_gl_account_id"),
            "collateral_security_subtype_id": st.session_state.get("capture_collateral_subtype_pick"),
            "collateral_charge_amount": st.session_state.get("capture_collateral_charge"),
            "collateral_valuation_amount": st.session_state.get("capture_collateral_valuation"),
            "loan_purpose_id": st.session_state.get("capture_loan_purpose_id"),
        }

    def _step1_source_cash_gl_valid() -> bool:
        entries = get_cached_source_cash_account_entries()
        cid = st.session_state.get("capture_cash_gl_account_id")
        cid_s = None if cid is None else str(cid).strip() or None
        ok, msg = capture_service.validate_source_cash_gl_selection(cid_s, entries)
        if not ok and msg:
            st.error(msg)
        return ok

    def _capture_details_for_queue() -> dict:
        details = dict(st.session_state.get("capture_loan_details") or {})
        s1 = _stage1_session_details()
        return capture_service.merge_details_with_stage1(details, s1)

    def _save_capture_staged_draft() -> None:
        """Persist STAGED draft: key loan session + schedule (if any) for capture staff to resume."""
        pre = capture_service.validate_staged_save_prerequisites(
            rework_source_draft_id=st.session_state.get("capture_rework_source_draft_id"),
            customer_id=st.session_state.get("capture_customer_id"),
            loan_type=st.session_state.get("capture_loan_type"),
        )
        if pre:
            st.error(pre)
            return
        if not _step1_source_cash_gl_valid():
            return
        df_sched = st.session_state.get("capture_loan_schedule_df")
        details_to_save = _capture_details_for_queue()
        result = capture_service.persist_staged_capture_draft(
            customer_id=int(st.session_state.get("capture_customer_id")),
            loan_type=str(st.session_state.get("capture_loan_type")),
            product_code=st.session_state.get("capture_product_code"),
            details_to_save=details_to_save,
            schedule_df=df_sched,
            existing_stage1_draft_id=st.session_state.get("capture_stage1_draft_id"),
        )
        if not result.ok:
            st.error(result.error or "Save failed.")
            return
        if result.new_stage1_draft_id is not None:
            st.session_state["capture_stage1_draft_id"] = result.new_stage1_draft_id
        st.session_state["capture_flash_message"] = result.flash_message
        st.rerun()

    def _submit_capture_send_for_approval() -> None:
        try:
            details = st.session_state.get("capture_loan_details") or {}
            df_schedule = st.session_state.get("capture_loan_schedule_df")
            cid = st.session_state.get("capture_customer_id")
            ltype = st.session_state.get("capture_loan_type")
            pcode = st.session_state.get("capture_product_code")
            pre = capture_service.validate_send_for_approval_prerequisites(
                details, df_schedule, cid, ltype
            )
            if pre:
                st.error(pre)
                return
            if not _step1_source_cash_gl_valid():
                return
            details_to_queue = _capture_details_for_queue()
            source_draft_id = st.session_state.get("capture_rework_source_draft_id")
            stage1_draft_id = st.session_state.get("capture_stage1_draft_id")
            draft_id = capture_service.resolve_and_submit_approval_draft(
                customer_id=int(cid),
                loan_type=str(ltype),
                product_code=pcode,
                details_to_queue=details_to_queue,
                df_schedule=df_schedule,
                source_rework_draft_id=int(source_draft_id) if source_draft_id is not None else None,
                stage1_draft_id=int(stage1_draft_id) if stage1_draft_id is not None else None,
            )
            upload_fn = upload_document if documents_available else None
            staged_loan_docs = list(st.session_state.get("loan_docs_staged") or [])
            doc_count, doc_errs = capture_service.attach_loan_draft_documents_from_staging(
                int(draft_id),
                staged_loan_docs,
                upload_document_fn=upload_fn,
            )
            for de in doc_errs:
                st.error(de)
            st.session_state["capture_flash_message"] = capture_service.build_approval_flash_after_submit(
                had_source_rework=source_draft_id is not None,
                draft_id=int(draft_id),
                doc_count=doc_count,
            )
            _fcapture_clear_session_after_submit()
            st.rerun()
        except Exception as e:
            st.error(f"Could not send draft for approval: {e}")

    def _loan_type_display_map(raw: str) -> str:
        _m = {
            "consumer_loan": "Consumer Loan",
            "term_loan": "Term Loan",
            "bullet_loan": "Bullet Loan",
            "customised_repayments": "Customised Repayments",
        }
        return _m.get(raw, raw)

    def _apply_rework_draft_from_row(draft: dict) -> None:
        st.session_state["capture_open_draft_panel"] = None
        draft_loan_type = str(draft.get("loan_type") or "")
        display_type = _loan_type_display_map(draft_loan_type)
        det = draft.get("details_json") or {}
        sched = draft.get("schedule_json") or []
        st.session_state["capture_customer_id"] = int(draft.get("customer_id"))
        st.session_state["capture_loan_type"] = display_type
        st.session_state["capture_product_code"] = draft.get("product_code")
        st.session_state["capture_loan_details"] = det
        st.session_state["capture_loan_schedule_df"] = pd.DataFrame(sched)
        st.session_state["capture_agent_id"] = det.get("agent_id")
        st.session_state["capture_relationship_manager_id"] = det.get("relationship_manager_id")
        st.session_state.pop("capture_disbursement_bank_option_id", None)
        st.session_state["capture_cash_gl_account_id"] = det.get("cash_gl_account_id")
        _cs = det.get("collateral_security_subtype_id")
        if _cs is not None:
            try:
                st.session_state["capture_collateral_subtype_pick"] = int(_cs)
            except (TypeError, ValueError):
                st.session_state.pop("capture_collateral_subtype_pick", None)
        else:
            st.session_state.pop("capture_collateral_subtype_pick", None)
        st.session_state["capture_collateral_charge"] = float(det.get("collateral_charge_amount") or 0)
        st.session_state["capture_collateral_valuation"] = float(det.get("collateral_valuation_amount") or 0)
        _lp_rw = det.get("loan_purpose_id")
        if _lp_rw is not None and str(_lp_rw).strip() != "":
            try:
                st.session_state["capture_loan_purpose_id"] = int(_lp_rw)
            except (TypeError, ValueError):
                st.session_state.pop("capture_loan_purpose_id", None)
        else:
            st.session_state.pop("capture_loan_purpose_id", None)
        st.session_state["capture_rework_source_draft_id"] = int(draft.get("id"))
        st.session_state.pop("capture_stage1_draft_id", None)
        for _wk in (
            "cap_customer_sel",
            "cap_product_sel",
            "cap_rm_t1",
            "cap_agent_sel_t0",
            "cap_cash_gl_sel_t0",
            "cap_loan_purpose_sel",
        ):
            st.session_state.pop(_wk, None)
        st.session_state["loan_docs_staged"] = []
        st.session_state["capture_loan_step"] = 0
        st.session_state["capture_flash_message"] = (
            f"Loaded rework draft #{draft.get('id')} — edit the form above, then **Send For Approval**."
        )
        st.rerun()

    def _apply_staged_draft_from_row(draft_s: dict) -> None:
        st.session_state["capture_open_draft_panel"] = None
        draft_loan_type_s = str(draft_s.get("loan_type") or "")
        display_type_s = _loan_type_display_map(draft_loan_type_s)
        det_s = draft_s.get("details_json") or {}
        sched_rows = draft_s.get("schedule_json") or []
        df_res = pd.DataFrame(sched_rows)
        has_sched = df_res is not None and not df_res.empty
        st.session_state["capture_customer_id"] = int(draft_s.get("customer_id"))
        st.session_state["capture_loan_type"] = display_type_s
        st.session_state["capture_product_code"] = draft_s.get("product_code")
        st.session_state["capture_agent_id"] = det_s.get("agent_id")
        st.session_state["capture_relationship_manager_id"] = det_s.get("relationship_manager_id")
        st.session_state.pop("capture_disbursement_bank_option_id", None)
        st.session_state["capture_cash_gl_account_id"] = det_s.get("cash_gl_account_id")
        _cs_s = det_s.get("collateral_security_subtype_id")
        if _cs_s is not None:
            try:
                st.session_state["capture_collateral_subtype_pick"] = int(_cs_s)
            except (TypeError, ValueError):
                st.session_state.pop("capture_collateral_subtype_pick", None)
        else:
            st.session_state.pop("capture_collateral_subtype_pick", None)
        st.session_state["capture_collateral_charge"] = float(det_s.get("collateral_charge_amount") or 0)
        st.session_state["capture_collateral_valuation"] = float(det_s.get("collateral_valuation_amount") or 0)
        _lp_st = det_s.get("loan_purpose_id")
        if _lp_st is not None and str(_lp_st).strip() != "":
            try:
                st.session_state["capture_loan_purpose_id"] = int(_lp_st)
            except (TypeError, ValueError):
                st.session_state.pop("capture_loan_purpose_id", None)
        else:
            st.session_state.pop("capture_loan_purpose_id", None)
        st.session_state["capture_stage1_draft_id"] = int(draft_s.get("id"))
        st.session_state.pop("capture_rework_source_draft_id", None)
        if has_sched:
            st.session_state["capture_loan_details"] = det_s
            st.session_state["capture_loan_schedule_df"] = df_res
            st.session_state["capture_loan_step"] = 0
            _msg = f"Resumed staged draft #{draft_s.get('id')} — scroll to **Review**, **Documents**, and **Actions**."
        else:
            st.session_state.pop("capture_loan_details", None)
            st.session_state.pop("capture_loan_schedule_df", None)
            st.session_state["capture_loan_step"] = 0
            _msg = f"Resumed staged draft #{draft_s.get('id')} — continue from **Details** and **Schedule** above."
        for _wk in (
            "cap_customer_sel",
            "cap_product_sel",
            "cap_rm_t1",
            "cap_agent_sel_t0",
            "cap_cash_gl_sel_t0",
            "cap_loan_purpose_sel",
        ):
            st.session_state.pop(_wk, None)
        st.session_state["loan_docs_staged"] = []
        st.session_state["capture_flash_message"] = _msg
        st.rerun()

    flash_msg = st.session_state.pop("capture_flash_message", None)
    if flash_msg:
        st.success(str(flash_msg))
    if st.session_state.pop("capture_require_docs_prompt", False):
        st.info("Upload supporting loan documents before **Send For Approval**.")
    if st.session_state.get("capture_rework_note"):
        st.warning(str(st.session_state.pop("capture_rework_note")))
    if int(st.session_state.get("capture_loan_step") or 0) in (1, 2):
        st.session_state["capture_loan_step"] = 0
    _fcapture_inject_css_once()
    st.markdown(
        '<span class="fcapture-scope" aria-hidden="true"></span>',
        unsafe_allow_html=True,
    )
    st.session_state.setdefault("capture_open_draft_panel", None)
    _open_panel = st.session_state.get("capture_open_draft_panel")
    _lnk1, _lnk2, _lnk_sp = st.columns([1.15, 1.25, 3])
    with _lnk1:
        if st.button(
            "See Loans for Rework",
            key="cap_open_rework_panel",
            type="tertiary",
        ):
            st.session_state["capture_open_draft_panel"] = (
                None if _open_panel == "rework" else "rework"
            )
            st.rerun()
    with _lnk2:
        if st.button(
            "Resume Capture Draft",
            key="cap_open_staged_panel",
            type="tertiary",
        ):
            st.session_state["capture_open_draft_panel"] = (
                None if _open_panel == "staged" else "staged"
            )
            st.rerun()

    if st.session_state.get("capture_open_draft_panel") == "rework":
        st.subheader("See Loans For Rework")
        srch = st.text_input(
            "Search Rework Drafts",
            placeholder="Draft ID / Customer ID / Product / Loan Type",
            key="cap_rework_search",
        )
        rework_rows = list_loan_approval_drafts(
            status="REWORK",
            search=srch.strip() or None,
            limit=200,
        )
        if rework_rows:
            rw_df = pd.DataFrame(rework_rows)
            rw_cols = [
                c
                for c in [
                    "id",
                    "customer_id",
                    "loan_type",
                    "product_code",
                    "assigned_approver_id",
                    "submitted_at",
                ]
                if c in rw_df.columns
            ]
            st.dataframe(
                rw_df[rw_cols],
                width="stretch",
                hide_index=True,
                height=min(160, 40 + min(len(rework_rows), 12) * 28),
            )
            for r in rework_rows:
                rid = int(r["id"])
                _rw_lbl = (
                    f"Load Draft #{rid} · Customer {r.get('customer_id')} · "
                    f"{r.get('product_code', '—')} · {r.get('loan_type', '—')}"
                )
                if st.button(_rw_lbl, key=f"cap_rework_row_{rid}"):
                    draft = get_loan_approval_draft(rid)
                    if not draft:
                        st.error(f"Draft #{rid} not found.")
                    else:
                        _apply_rework_draft_from_row(draft)

    if st.session_state.get("capture_open_draft_panel") == "staged":
        st.subheader("Resume Capture Draft")
        stg_srch = st.text_input(
            "Search Staged Drafts",
            placeholder="Draft ID / Customer ID / Product",
            key="cap_staged_search",
        )
        staged_rows = list_loan_approval_drafts(
            status="STAGED",
            search=stg_srch.strip() or None,
            limit=200,
        )
        if staged_rows:
            stg_df = pd.DataFrame(staged_rows)
            stg_cols = [
                c
                for c in [
                    "id",
                    "customer_id",
                    "loan_type",
                    "product_code",
                    "assigned_approver_id",
                    "submitted_at",
                ]
                if c in stg_df.columns
            ]
            st.dataframe(
                stg_df[stg_cols],
                width="stretch",
                hide_index=True,
                height=min(140, 36 + min(len(staged_rows), 10) * 28),
            )
            for r in staged_rows:
                sid = int(r["id"])
                _st_lbl = (
                    f"Load Staged Draft #{sid} · Customer {r.get('customer_id')} · "
                    f"{r.get('product_code', '—')} · {r.get('loan_type', '—')}"
                )
                if st.button(_st_lbl, key=f"cap_staged_row_{sid}"):
                    draft_s = get_loan_approval_draft(sid)
                    if not draft_s:
                        st.error(f"Draft #{sid} not found.")
                    else:
                        _apply_staged_draft_from_row(draft_s)

    # -------- Loan capture: flat panel (details → schedule → review → actions) --------
    with st.container(border=True):
        st.subheader("Details")
        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.warning("No active customers. Add a customer first under **Customers**.")
        else:
            options = [(c["id"], get_display_name(c["id"]) or f"Customer #{c['id']}") for c in customers_list]
            _cust_idx_opts = list(range(len(options)))
            _default_ci = 0
            _cid_pre = st.session_state.get("capture_customer_id")
            if _cid_pre is not None:
                try:
                    _default_ci = next(i for i, o in enumerate(options) if int(o[0]) == int(_cid_pre))
                except StopIteration:
                    _default_ci = 0
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                choice = st.selectbox(
                    "Customer",
                    _cust_idx_opts,
                    index=_default_ci,
                    format_func=lambda i: options[i][1],
                    key="cap_customer_sel",
                )
            lt_display = {
                "consumer_loan": "Consumer Loan",
                "term_loan": "Term Loan",
                "bullet_loan": "Bullet Loan",
                "customised_repayments": "Customised Repayments",
            }
            if choice is not None:
                st.session_state["capture_customer_id"] = options[choice][0]
            product_opts = (
                list_products(active_only=True)
                if (choice is not None and loan_management_available)
                else []
            )
            with c2:
                if choice is None:
                    st.text_input(
                        "Product",
                        value="",
                        disabled=True,
                        key="cap_product_ph",
                    )
                    st.session_state["capture_product_code"] = None
                    st.session_state["capture_loan_type"] = "Term Loan"
                elif not product_opts:
                    st.warning("No products.")
                    st.session_state["capture_product_code"] = None
                    st.session_state["capture_loan_type"] = "Term Loan"
                else:
                    product_labels = [f"{p['code']} – {p['name']}" for p in product_opts]
                    prod_options = list(range(len(product_labels)))
                    prod_format = (lambda i: product_labels[i]) if product_labels else (lambda i: "(No products)")
                    _default_pi = 0
                    _pcode_pre = st.session_state.get("capture_product_code")
                    if _pcode_pre:
                        try:
                            _default_pi = next(i for i, p in enumerate(product_opts) if p.get("code") == _pcode_pre)
                        except StopIteration:
                            _default_pi = 0
                    product_sel_idx = st.selectbox(
                        "Product",
                        prod_options,
                        index=_default_pi,
                        format_func=prod_format,
                        key="cap_product_sel",
                    )
                    if product_sel_idx is not None and 0 <= product_sel_idx < len(product_opts):
                        _lp = product_opts[product_sel_idx]["loan_type"]
                        st.session_state["capture_product_code"] = product_opts[product_sel_idx]["code"]
                        st.session_state["capture_loan_type"] = lt_display.get(_lp, _lp)
                    else:
                        st.session_state["capture_product_code"] = None
                        st.session_state["capture_loan_type"] = "Term Loan"
            with c3:
                if users_for_rm_available:
                    users_rm = list_users_for_selection()
                    rm_opts = [(None, "(None)")] + [(u["id"], f"{u['full_name']} ({u['email']})") for u in users_rm]
                    rm_labels = [t[1] for t in rm_opts]
                    rm_ids = [t[0] for t in rm_opts]
                    _default_rmi = 0
                    _rm_pre = st.session_state.get("capture_relationship_manager_id")
                    if _rm_pre is not None:
                        try:
                            _default_rmi = next(
                                i
                                for i, rid in enumerate(rm_ids)
                                if rid is not None and str(rid) == str(_rm_pre)
                            )
                        except StopIteration:
                            _default_rmi = 0
                    rm_sel = st.selectbox(
                        "Relationship Manager",
                        rm_labels,
                        index=_default_rmi,
                        key="cap_rm_t1",
                    )
                    st.session_state["capture_relationship_manager_id"] = rm_ids[rm_labels.index(rm_sel)] if rm_sel else None
                else:
                    st.session_state["capture_relationship_manager_id"] = None
            with c4:
                if agents_available:
                    try:
                        agents_list_cap = list_agents(status="active") or []
                    except Exception:
                        agents_list_cap = []
                    agent_labels_cap = ["(None)"] + [a["name"] for a in agents_list_cap]
                    agent_ids_cap = [None] + [a["id"] for a in agents_list_cap]
                    default_agent_idx = 0
                    _aid_pre = st.session_state.get("capture_agent_id")
                    if _aid_pre is not None:
                        try:
                            default_agent_idx = next(
                                i
                                for i, aid in enumerate(agent_ids_cap)
                                if aid is not None and str(aid) == str(_aid_pre)
                            )
                        except StopIteration:
                            default_agent_idx = 0
                    sel_agent_label = st.selectbox(
                        "Agent",
                        agent_labels_cap,
                        index=default_agent_idx,
                        key="cap_agent_sel_t0",
                    )
                    st.session_state["capture_agent_id"] = agent_ids_cap[agent_labels_cap.index(sel_agent_label)] if sel_agent_label else None
                else:
                    st.session_state["capture_agent_id"] = None

            c5, c6, _, _ = st.columns(4)
            with c5:
                _cg_lab, _cg_ids = source_cash_gl_cached_labels_and_ids()
                if _cg_ids:
                    _cg_default = 0
                    _prev_cg = st.session_state.get("capture_cash_gl_account_id")
                    if _prev_cg and str(_prev_cg) in _cg_ids:
                        _cg_default = _cg_ids.index(str(_prev_cg))
                    _cg_i = st.selectbox(
                        "Source Cash GL",
                        range(len(_cg_lab)),
                        format_func=lambda i: _cg_lab[i],
                        index=_cg_default,
                        key="cap_cash_gl_sel_t0",
                    )
                    st.session_state["capture_cash_gl_account_id"] = _cg_ids[_cg_i]
                else:
                    st.session_state["capture_cash_gl_account_id"] = None
                    source_cash_gl_cache_empty_warning()
            with c6:
                _purposes_all: list = []
                if loan_management_available:
                    try:
                        _purposes_all = list_loan_purposes(active_only=False)
                    except Exception as _cap_lp_ex:
                        st.warning(f"Loan purposes list failed: {_cap_lp_ex}")
                _purposes_active = [p for p in _purposes_all if p.get("is_active", True)]
                _purposes_for_dropdown = list(_purposes_active)
                _cur_lp = st.session_state.get("capture_loan_purpose_id")
                if _cur_lp is not None and str(_cur_lp).strip() != "":
                    try:
                        _ci_lp = int(_cur_lp)
                        if not any(int(p["id"]) == _ci_lp for p in _purposes_for_dropdown):
                            _row_inact = get_loan_purpose_by_id(_ci_lp)
                            if _row_inact:
                                _purposes_for_dropdown.append(_row_inact)
                    except (TypeError, ValueError):
                        pass
                _opts_lp_ids: list[int | None] = [None]
                _opts_lp_labels = ["(None)"]
                for _p in _purposes_for_dropdown:
                    _opts_lp_ids.append(int(_p["id"]))
                    _lab_p = str(_p.get("name") or "")
                    if not _p.get("is_active", True):
                        _lab_p = f"{_lab_p} (inactive)"
                    _opts_lp_labels.append(_lab_p)
                _default_lp_i = 0
                if _cur_lp is not None and str(_cur_lp).strip() != "":
                    try:
                        _want_lp = int(_cur_lp)
                        if _want_lp in _opts_lp_ids:
                            _default_lp_i = _opts_lp_ids.index(_want_lp)
                    except (TypeError, ValueError):
                        _default_lp_i = 0
                if not _purposes_active:
                    if _purposes_all:
                        st.warning(
                            "All **loan purposes in the database are inactive**. Activate one under "
                            "**System configurations → Loan purposes**, or add a new active purpose."
                        )
                    else:
                        st.warning(
                            "No loan purposes in the database. Add them under **System configurations → Loan purposes**."
                        )
                _sel_lp_ix = st.selectbox(
                    "Loan Purpose",
                    list(range(len(_opts_lp_labels))),
                    index=min(_default_lp_i, max(0, len(_opts_lp_labels) - 1)),
                    format_func=lambda i, labs=_opts_lp_labels: labs[i],
                    key="cap_loan_purpose_sel",
                )
                st.session_state["capture_loan_purpose_id"] = _opts_lp_ids[int(_sel_lp_ix)]

        st.markdown('<hr class="fcapture-soft"/>', unsafe_allow_html=True)
        with st.expander("Collateral (IFRS)", expanded=False):
            if (
                not provisions_config_ok
                or list_provision_security_subtypes is None
                or provision_schema_ready_fn is None
            ):
                st.warning("Collateral tables unavailable — run **scripts/run_migration_53.py**.")
            else:
                _sch_ok, _sch_msg = provision_schema_ready_fn()
                if not _sch_ok:
                    st.warning(_sch_msg)
                else:
                    _subs = list_provision_security_subtypes(active_only=True) or []
                    if not _subs:
                        st.info("Add subtypes under **System configurations → IFRS provision config**.")
                    else:
                        _sid_opts = [int(s["id"]) for s in _subs]
                        _pick_cur = st.session_state.get("capture_collateral_subtype_pick")
                        if _pick_cur is not None and int(_pick_cur) not in _sid_opts:
                            st.session_state.pop("capture_collateral_subtype_pick", None)
                        _sid_lbl = {
                            int(s["id"]): f"{s['security_type']} · {s['subtype_name']} (haircut {s['typical_haircut_pct']}%)"
                            for s in _subs
                        }
                        _cc1, _cc2, _cc3, _ = st.columns([2, 1, 1, 1])
                        with _cc1:
                            st.selectbox(
                                "Collateral Subtype",
                                _sid_opts,
                                format_func=lambda i, m=_sid_lbl: m.get(int(i), str(i)),
                                key="capture_collateral_subtype_pick",
                            )
                        with _cc2:
                            st.number_input(
                                "Charge Amount",
                                min_value=0.0,
                                step=0.01,
                                key="capture_collateral_charge",
                            )
                        with _cc3:
                            st.number_input(
                                "Valuation",
                                min_value=0.0,
                                step=0.01,
                                key="capture_collateral_valuation",
                            )

        st.subheader("Schedule")
        cid = st.session_state.get("capture_customer_id")
        ltype = st.session_state.get("capture_loan_type")
        product_code = st.session_state.get("capture_product_code") or "—"
        if not cid or not ltype:
            st.info("Select **Customer** And **Product** Under **Details** First.")
        else:
            if st.session_state.get("capture_loan_details") is not None or st.session_state.get("capture_loan_schedule_df") is not None:
                if st.button("Clear Saved Schedule", key="cap_clear_t2"):
                    st.session_state.pop("capture_loan_details", None)
                    st.session_state.pop("capture_loan_schedule_df", None)
                    st.rerun()
            product_cfg_for_basis = get_product_config_from_db(product_code) or {}
            product_rate_basis = get_product_rate_basis(product_cfg_for_basis, fallback=None)
            product_gls = product_cfg_for_basis.get("global_loan_settings") or {}
            interest_method = product_gls.get("interest_method")
            if interest_method not in {"Reducing balance", "Flat rate"}:
                st.error(
                    f"Selected product `{product_code}` must define "
                    f"product_config:{product_code}.global_loan_settings.interest_method as "
                    f"'Reducing balance' or 'Flat rate'."
                )
                st.stop()
            flat_rate = interest_method == "Flat rate"
            rate_label = (
                "Interest Rate (% Per Annum)"
                if product_rate_basis == "Per annum"
                else "Interest Rate (% Per Month)"
            )
            if ltype == "Consumer Loan":
                cfg = get_system_config()
                schemes = get_consumer_schemes()
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("consumer_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]

                _cl_pr_opts = ["Net Proceeds", "Principal (Total Loan Amount)"]
                _cl_rt_opts = [
                    "Anniversary Date (Same Day Each Month)",
                    "Last Day Of Each Month",
                ]
                cl_r1c1, cl_r1c2, cl_r1c3, cl_r1c4 = st.columns(4)
                with cl_r1c1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_cl_currency",
                    )
                    _pi_lab = st.selectbox(
                        "What are you Entering?",
                        _cl_pr_opts,
                        key="cap_cl_principal_mode",
                    )
                    input_tf = _pi_lab == "Principal (Total Loan Amount)"
                with cl_r1c2:
                    loan_required = st.number_input(
                        "Loan Amount",
                        min_value=0.0,
                        value=0.0,
                        step=10.0,
                        format="%.2f",
                        key="cap_cl_principal",
                    )
                    loan_term = st.number_input("Term (Months)", 1, 60, 6, key="cap_cl_term")
                with cl_r1c3:
                    disbursement_date = datetime.combine(
                        st.date_input("Disbursement Date", get_system_date(), key="cap_cl_start"),
                        datetime.min.time(),
                    )
                    default_first_rep = add_months(disbursement_date, 1).date()
                    first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="cap_cl_first_rep")
                    first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
                with cl_r1c4:
                    _rt_lab = st.selectbox("Repayments On", _cl_rt_opts, key="cap_cl_repay_timing")
                    use_anniversary = _rt_lab.startswith("Anniversary")
                cl_schedule_valid = use_anniversary or is_last_day_of_month(first_repayment_date)
                if not cl_schedule_valid:
                    last_day = days_in_month(first_repayment_date.year, first_repayment_date.month)
                    example = datetime(first_repayment_date.year, first_repayment_date.month, last_day).strftime("%d-%b-%Y")
                    st.error(
                        "When repayments are on the **last day of each month**, the First Repayment Date must be the last day of its month. "
                        f"For {first_repayment_date.strftime('%B %Y')} the last day is **{example}**."
                    )
                # Product-per-scheme: derive consumer schedule rates from selected product.
                # This removes redundancy in the capture flow (scheme is implicit in product_code).
                product_cfg = get_product_config_from_db(product_code) or {}
                product_rate_basis = get_product_rate_basis(product_cfg)
                default_rates = (product_cfg.get("default_rates") or {}).get("consumer_loan") or {}
                interest_pct = default_rates.get("interest_pct")
                admin_fee_pct = default_rates.get("admin_fee_pct")

                if interest_pct is not None and admin_fee_pct is not None:
                    interest_pct_month = pct_to_monthly(interest_pct, product_rate_basis)
                    if interest_pct_month is None:
                        st.error(
                            f"Selected product `{product_code}` has invalid interest_pct for consumer_loan (must be numeric)."
                        )
                        st.stop()

                    base_rate = float(interest_pct_month) / 100.0
                    admin_fee = float(admin_fee_pct) / 100.0

                    matched = next(
                        (
                            s
                            for s in schemes
                            if abs(float(s.get("interest_rate_pct", 0.0)) - float(interest_pct_month)) < 1e-6
                            and abs(float(s.get("admin_fee_pct", 0.0)) - float(admin_fee_pct)) < 1e-6
                        ),
                        None,
                    )
                    scheme = str(matched["name"]) if matched and matched.get("name") else "Other"
                else:
                    st.error(
                        f"Selected product `{product_code}` must define "
                        f"`product_config:{product_code}.default_rates.consumer_loan.interest_pct` and "
                        f"`product_config:{product_code}.default_rates.consumer_loan.admin_fee_pct`."
                    )
                    st.stop()

                # Product-per-scheme: regular interest/admin come from product defaults.
                # Penalty/default interest is also derived from product config, but we do NOT expose
                # a penalty override field in the consumer capture flow.
                penalty_pct = (product_cfg.get("penalty_rates") or {}).get("consumer_loan")
                if penalty_pct is None:
                    st.error(
                        f"Selected product `{product_code}` must define "
                        f"`product_config:{product_code}.penalty_rates.consumer_loan`."
                    )
                    st.stop()

                penalty_pct_month = pct_to_monthly(penalty_pct, product_rate_basis)
                if penalty_pct_month is None:
                    st.error(
                        f"Selected product `{product_code}` has invalid penalty_rates.consumer_loan (must be numeric)."
                    )
                    st.stop()

                penalty_pct = float(penalty_pct_month or 0.0)
                penalty_quotation_product = product_cfg.get("penalty_interest_quotation")
                if not penalty_quotation_product:
                    st.error(
                        f"Selected product `{product_code}` must define "
                        f"`product_config:{product_code}.penalty_interest_quotation`."
                    )
                    st.stop()

                _rates_mode = st.selectbox(
                    "Regular Interest & Admin Fee",
                    ["Use Product Defaults", "Override Manually"],
                    key="cap_cl_rates_mode",
                )
                override_rates = _rates_mode.startswith("Override")
                if override_rates:
                    override_interest_label = (
                        "Regular Interest Rate (% Per Annum)"
                        if product_rate_basis == "Per annum"
                        else "Regular Interest Rate (% Per Month)"
                    )
                    override_interest_pct = st.number_input(
                        override_interest_label,
                        min_value=0.0,
                        max_value=100.0,
                        value=round(float(interest_pct or 0.0), 4),
                        step=0.1,
                        key="cap_cl_override_interest_pct",
                    )
                    override_admin_fee_pct = st.number_input(
                        "Administration Fee (%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=round(float(admin_fee) * 100.0, 4),
                        step=0.1,
                        key="cap_cl_override_admin_fee_pct",
                    )

                    # Convert the overridden rate into the internal "Per month" form
                    # because the consumer schedule computation uses that basis internally.
                    override_interest_pct_month = pct_to_monthly(override_interest_pct, product_rate_basis)
                    if override_interest_pct_month is None:
                        st.error("Invalid override interest rate for the selected product rate basis.")
                        st.stop()
                    base_rate = float(override_interest_pct_month) / 100.0
                    admin_fee = float(override_admin_fee_pct) / 100.0

                    # Remap scheme name based on overridden rates (or use "Other").
                    scheme_interest_pct_for_match = override_interest_pct_month
                    matched_override = next(
                        (
                            s
                            for s in schemes
                            if abs(float(s.get("interest_rate_pct", 0.0)) - float(scheme_interest_pct_for_match)) < 1e-6
                            and abs(float(s.get("admin_fee_pct", 0.0)) - float(override_admin_fee_pct)) < 1e-6
                        ),
                        None,
                    )
                    scheme = (
                        str(matched_override["name"])
                        if matched_override and matched_override.get("name")
                        else "Other"
                    )
                if cl_schedule_valid:
                    details, df_schedule = compute_consumer_schedule(
                        loan_required, loan_term, disbursement_date, base_rate, admin_fee, input_tf,
                        "Per month", flat_rate, scheme=scheme,
                        first_repayment_date=first_repayment_date, use_anniversary=use_anniversary,
                    )
                    details["currency"] = currency
                    details["penalty_rate_pct"] = penalty_pct
                    details["penalty_quotation"] = penalty_quotation_product
                    _df_cl = format_schedule_df(df_schedule)
                    render_centered_html_table(_df_cl, [str(c) for c in _df_cl.columns])
                    if st.button("Use This Schedule", type="secondary", key="cap_cl_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_schedule
                        st.session_state["capture_loan_step"] = 0
                        st.session_state["capture_require_docs_prompt"] = True
                        try:
                            st.toast("Schedule saved — scroll down for review, documents, and actions.", icon="✅")
                        except Exception:
                            pass
                        st.rerun()

            elif ltype == "Term Loan":
                cfg = get_system_config()
                product_cfg = get_product_config_from_db(product_code) or {}
                product_rate_basis = get_product_rate_basis(product_cfg)
                dr = (product_cfg.get("default_rates") or {}).get("term_loan") or {}
                required = ["interest_pct", "drawdown_pct", "arrangement_pct"]
                missing = [k for k in required if dr.get(k) is None]
                if missing:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.default_rates.term_loan "
                        f"keys: {', '.join(missing)}."
                    )
                    st.stop()
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("term_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]

                _term_pr_opts = ["Net Proceeds", "Principal (Total Loan Amount)"]
                _term_grace_opts = [
                    "No Grace Period",
                    "Principal Moratorium",
                    "Principal And Interest Moratorium",
                ]
                _term_rt_opts = ["Anniversary Date", "Last Day Of Month"]
                tt1, tt2, tt3, tt4 = st.columns(4)
                with tt1:
                    _tp_lab = st.selectbox("What are you Entering?", _term_pr_opts, key="cap_term_principal_mode")
                    input_tf = _tp_lab == "Principal (Total Loan Amount)"
                with tt2:
                    grace_type = st.selectbox("Grace Period", _term_grace_opts, key="cap_term_grace_sel")
                with tt3:
                    _trt = st.selectbox("Repayments On", _term_rt_opts, key="cap_term_timing_sel")
                    use_anniversary = _trt.startswith("Anniversary")
                with tt4:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_term_currency",
                    )

                tcol1, tcol2, tcol3, tcol4 = st.columns(4)
                with tcol1:
                    loan_required = st.number_input(
                        "Loan Amount",
                        min_value=0.0,
                        value=0.0,
                        step=100.0,
                        format="%.2f",
                        key="cap_term_principal",
                    )
                    loan_term = st.number_input("Term (Months)", 1, 120, 24, key="cap_term_months")
                with tcol2:
                    disbursement_date = datetime.combine(
                        st.date_input("Disbursement Date", get_system_date(), key="cap_term_disb"),
                        datetime.min.time(),
                    )
                with tcol3:
                    drawdown_pct = (
                        st.number_input(
                            "Drawdown Fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("drawdown_pct")),
                            step=0.1,
                            key="cap_term_drawdown",
                        )
                        / 100.0
                    )
                with tcol4:
                    arrangement_pct = (
                        st.number_input(
                            "Arrangement Fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("arrangement_pct")),
                            step=0.1,
                            key="cap_term_arrangement",
                        )
                        / 100.0
                    )
                def_penalty = (product_cfg.get("penalty_rates") or {}).get("term_loan")
                if def_penalty is None:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_rates.term_loan."
                    )
                    st.stop()
                penalty_quotation_product = product_cfg.get("penalty_interest_quotation")
                if not penalty_quotation_product:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_interest_quotation."
                    )
                    st.stop()
                tpen1, tpen2, tpen3, tpen4 = st.columns(4)
                with tpen1:
                    rate_pct = st.number_input(
                        rate_label,
                        0.0,
                        100.0,
                        float(dr.get("interest_pct") or 0.0),
                        step=0.1,
                        key="cap_term_rate",
                    )
                with tpen2:
                    penalty_label = (
                        "Penalty Interest (% Per Annum)"
                        if product_rate_basis == "Per annum"
                        else "Penalty Interest (% Per Month)"
                    )
                    penalty_pct = st.number_input(
                        penalty_label,
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_term_penalty",
                    )
                with tpen3:
                    default_first = add_months(disbursement_date, 1).date()
                    first_rep = datetime.combine(
                        st.date_input("First Repayment Date", default_first, key="cap_term_first_rep"),
                        datetime.min.time(),
                    )
                moratorium_months = 0
                with tpen4:
                    if grace_type == "Principal Moratorium":
                        moratorium_months = st.number_input(
                            "Moratorium (Months)", 1, 60, 3, key="cap_term_moratorium_p"
                        )
                    elif grace_type == "Principal And Interest Moratorium":
                        moratorium_months = st.number_input(
                            "Moratorium (Months)", 1, 60, 3, key="cap_term_moratorium_pi"
                        )
                if not use_anniversary and not is_last_day_of_month(first_rep):
                    st.error("When repayments are on last day of month, first repayment date must be the last day of that month.")
                else:
                    details, df_schedule = compute_term_schedule(
                        loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                        input_tf, grace_type, moratorium_months, first_rep, use_anniversary,
                        product_rate_basis, flat_rate,
                    )
                    details["currency"] = currency
                    penalty_pct_monthly = pct_to_monthly(penalty_pct, product_rate_basis)
                    if penalty_pct_monthly is None:
                        st.error("Invalid penalty interest for the selected product rate basis.")
                        st.stop()
                    details["penalty_rate_pct"] = float(penalty_pct_monthly)
                    details["penalty_quotation"] = penalty_quotation_product
                    _df_term = format_schedule_df(df_schedule)
                    render_centered_html_table(_df_term, [str(c) for c in _df_term.columns])
                    if st.button("Use This Schedule", type="secondary", key="cap_term_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_schedule
                        st.session_state["capture_loan_step"] = 0
                        st.session_state["capture_require_docs_prompt"] = True
                        try:
                            st.toast("Schedule saved — scroll down for review, documents, and actions.", icon="✅")
                        except Exception:
                            pass
                        st.rerun()

            elif ltype == "Bullet Loan":
                cfg = get_system_config()
                product_cfg = get_product_config_from_db(product_code) or {}
                dr = (product_cfg.get("default_rates") or {}).get("bullet_loan") or {}
                required = ["interest_pct", "drawdown_pct", "arrangement_pct"]
                missing = [k for k in required if dr.get(k) is None]
                if missing:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.default_rates.bullet_loan "
                        f"keys: {', '.join(missing)}."
                    )
                    st.stop()
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("bullet_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]
                _bul_type_opts = [
                    "Straight Bullet (No Interim Payments)",
                    "Bullet With Interest Payments",
                ]
                _bul_pr_opts = ["Net Proceeds", "Principal (Total Loan Amount)"]
                _bul_rt_opts = ["Anniversary Date", "Last Day Of Month"]
                br1c1, br1c2, br1c3, br1c4 = st.columns(4)
                with br1c1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_bullet_currency",
                    )
                with br1c2:
                    bullet_type = st.selectbox("Bullet Type", _bul_type_opts, key="cap_bullet_type_sel")
                with br1c3:
                    _bpr = st.selectbox("What are you Entering?", _bul_pr_opts, key="cap_bullet_principal_mode")
                    input_tf = _bpr == "Principal (Total Loan Amount)"
                with br1c4:
                    loan_required = st.number_input(
                        "Loan Amount",
                        min_value=0.0,
                        value=0.0,
                        step=100.0,
                        format="%.2f",
                        key="cap_bullet_principal",
                    )
                br2c1, br2c2, br2c3, br2c4 = st.columns(4)
                with br2c1:
                    loan_term = st.number_input("Term (Months)", 1, 120, 12, key="cap_bullet_term")
                with br2c2:
                    disbursement_date = datetime.combine(
                        st.date_input("Disbursement Date", get_system_date(), key="cap_bullet_disb"),
                        datetime.min.time(),
                    )
                with br2c3:
                    rate_pct = st.number_input(
                        rate_label,
                        0.0,
                        100.0,
                        float(dr.get("interest_pct")),
                        step=0.1,
                        key="cap_bullet_rate",
                    )
                with br2c4:
                    drawdown_pct = (
                        st.number_input(
                            "Drawdown Fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("drawdown_pct")),
                            step=0.1,
                            key="cap_bullet_drawdown",
                        )
                        / 100.0
                    )
                def_penalty = (product_cfg.get("penalty_rates") or {}).get("bullet_loan")
                if def_penalty is None:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_rates.bullet_loan."
                    )
                    st.stop()
                penalty_quotation_product = product_cfg.get("penalty_interest_quotation")
                if not penalty_quotation_product:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_interest_quotation."
                    )
                    st.stop()
                br3c1, br3c2, br3c3, br3c4 = st.columns(4)
                with br3c1:
                    arrangement_pct = (
                        st.number_input(
                            "Arrangement Fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("arrangement_pct")),
                            step=0.1,
                            key="cap_bullet_arrangement",
                        )
                        / 100.0
                    )
                with br3c2:
                    penalty_label = (
                        "Penalty Interest (% Per Annum)"
                        if product_rate_basis == "Per annum"
                        else "Penalty Interest (% Per Month)"
                    )
                    penalty_pct = st.number_input(
                        penalty_label,
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_bullet_penalty",
                    )
                penalty_pct_monthly = pct_to_monthly(penalty_pct, product_rate_basis)
                if penalty_pct_monthly is None:
                    st.error("Invalid penalty interest for the selected product rate basis.")
                    st.stop()
                first_rep = None
                use_anniversary = True
                if "with interest" in bullet_type.lower():
                    with br3c3:
                        default_first = add_months(disbursement_date, 1).date()
                        first_rep = datetime.combine(
                            st.date_input("First Repayment Date", default_first, key="cap_bullet_first_rep"),
                            datetime.min.time(),
                        )
                    with br3c4:
                        _brt = st.selectbox("Interest Payments On", _bul_rt_opts, key="cap_bullet_timing_sel")
                        use_anniversary = _brt.startswith("Anniversary")
                    if not use_anniversary and not is_last_day_of_month(first_rep):
                        st.error("First repayment date must be last day of month when using last day of month.")
                    else:
                        details, df_schedule = compute_bullet_schedule(
                            loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                            input_tf, bullet_type, first_rep, use_anniversary, product_rate_basis, flat_rate,
                        )
                        details["currency"] = currency
                        details["penalty_rate_pct"] = float(penalty_pct_monthly)
                        details["penalty_quotation"] = penalty_quotation_product
                        _df_bul = format_schedule_df(df_schedule)
                        render_centered_html_table(_df_bul, [str(c) for c in _df_bul.columns])
                        if st.button("Use This Schedule", type="secondary", key="cap_bullet_use"):
                            st.session_state["capture_loan_details"] = details
                            st.session_state["capture_loan_schedule_df"] = df_schedule
                            st.session_state["capture_loan_step"] = 0
                            st.session_state["capture_require_docs_prompt"] = True
                            try:
                                st.toast("Schedule saved — scroll down for review, documents, and actions.", icon="✅")
                            except Exception:
                                pass
                            st.rerun()
                else:
                    details, df_schedule = compute_bullet_schedule(
                        loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                        input_tf, bullet_type, None, True, product_rate_basis, flat_rate,
                    )
                    details["currency"] = currency
                    details["penalty_rate_pct"] = float(penalty_pct_monthly)
                    details["penalty_quotation"] = penalty_quotation_product
                    _df_bul2 = format_schedule_df(df_schedule)
                    render_centered_html_table(_df_bul2, [str(c) for c in _df_bul2.columns])
                    if st.button("Use This Schedule", type="secondary", key="cap_bullet_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_schedule
                        st.session_state["capture_loan_step"] = 0
                        st.session_state["capture_require_docs_prompt"] = True
                        try:
                            st.toast("Schedule saved — scroll down for review, documents, and actions.", icon="✅")
                        except Exception:
                            pass
                        st.rerun()

            else:
                # Customised Repayments
                cfg = get_system_config()
                product_cfg = get_product_config_from_db(product_code) or {}
                dr = (product_cfg.get("default_rates") or {}).get("customised_repayments") or {}
                required = ["interest_pct", "drawdown_pct", "arrangement_pct"]
                missing = [k for k in required if dr.get(k) is None]
                if missing:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.default_rates.customised_repayments "
                        f"keys: {', '.join(missing)}."
                    )
                    st.stop()
                accepted_currencies = cfg.get(
                    "accepted_currencies", [cfg.get("base_currency", "USD")]
                )
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get(
                    "customised_repayments", cfg.get("base_currency", "USD")
                )
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]
                _cust_pr_opts = ["Net Proceeds", "Principal (Total Loan Amount)"]
                _cust_shape_opts = ["Regular (Fixed Dates)", "Irregular (Editable Dates)"]
                _cust_rt_opts = ["Anniversary Date", "Last Day Of Month"]
                cu1, cu2, cu3, cu4 = st.columns(4)
                with cu1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_cust_currency",
                    )
                    _cupr = st.selectbox("What are you Entering?", _cust_pr_opts, key="cap_cust_principal_mode")
                    input_tf = _cupr == "Principal (Total Loan Amount)"
                with cu2:
                    loan_required = st.number_input(
                        "Loan Amount", min_value=0.0, value=0.0, step=100.0, format="%.2f", key="cap_cust_principal"
                    )
                    loan_term = st.number_input("Term (Months)", 1, 120, 12, key="cap_cust_term")
                with cu3:
                    disbursement_date = datetime.combine(
                        st.date_input("Disbursement Date", get_system_date(), key="cap_cust_start"),
                        datetime.min.time(),
                    )
                    _shape = st.selectbox(
                        "Schedule Shape",
                        _cust_shape_opts,
                        key="cap_cust_shape",
                    )
                    irregular = _shape.startswith("Irregular")
                with cu4:
                    _crt = st.selectbox("Repayments On", _cust_rt_opts, key="cap_cust_timing_sel")
                    use_anniversary = _crt.startswith("Anniversary")
                default_first = add_months(disbursement_date, 1).date()
                if not use_anniversary:
                    default_first = default_first.replace(day=days_in_month(default_first.year, default_first.month))
                first_rep_derived = st.session_state.get("cap_cust_first_rep_derived")
                first_rep_display = first_rep_derived.date() if first_rep_derived is not None else default_first
                first_rep = datetime.combine(first_rep_display, datetime.min.time())

                cu5, cu6, cu7, cu8 = st.columns(4)
                with cu5:
                    rate_pct = st.number_input(
                        rate_label,
                        0.0,
                        100.0,
                        float(dr.get("interest_pct")),
                        step=0.1,
                        key="cap_cust_rate",
                    )
                with cu6:
                    drawdown_pct = (
                        st.number_input(
                            "Drawdown Fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct")), step=0.1, key="cap_cust_drawdown"
                        )
                        / 100.0
                    )
                with cu7:
                    arrangement_pct = (
                        st.number_input(
                            "Arrangement Fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct")), step=0.1, key="cap_cust_arrangement"
                        )
                        / 100.0
                    )

                def_penalty = (product_cfg.get("penalty_rates") or {}).get("customised_repayments")
                if def_penalty is None:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_rates.customised_repayments."
                    )
                    st.stop()

                penalty_quotation_product = product_cfg.get("penalty_interest_quotation")
                if not penalty_quotation_product:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_interest_quotation."
                    )
                    st.stop()

                penalty_label = (
                    "Penalty Interest (% Per Annum)"
                    if product_rate_basis == "Per annum"
                    else "Penalty Interest (% Per Month)"
                )
                with cu8:
                    penalty_pct = st.number_input(
                        penalty_label,
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_cust_penalty",
                    )
                penalty_pct_monthly = pct_to_monthly(penalty_pct, product_rate_basis)
                if penalty_pct_monthly is None:
                    st.error("Invalid penalty interest for the selected product rate basis.")
                    st.stop()
                total_fee = drawdown_pct + arrangement_pct
                if input_tf:
                    total_facility = loan_required
                else:
                    total_facility = loan_required / (1.0 - total_fee)
                annual_rate = (
                    (rate_pct / 100.0) * 12.0
                    if product_rate_basis == "Per month"
                    else (rate_pct / 100.0)
                )

                cap_key = "cap_cust_df"
                cap_params = (round(total_facility, 2), loan_term, disbursement_date.strftime("%Y-%m-%d"), irregular)
                if cap_key not in st.session_state or st.session_state.get("cap_cust_params") != cap_params:
                    st.session_state["cap_cust_params"] = cap_params
                    schedule_dates_init = repayment_dates(disbursement_date, first_rep, int(loan_term), use_anniversary)
                    rows = [{"Period": 0, "Date": disbursement_date.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": round(total_facility, 2), "Total Outstanding": round(total_facility, 2)}]
                    for i, dt in enumerate(schedule_dates_init, 1):
                        rows.append({"Period": i, "Date": dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0})
                    st.session_state[cap_key] = pd.DataFrame(rows)
                    st.session_state.pop("cap_cust_first_rep_derived", None)

                df_cap = st.session_state[cap_key].copy()
                # Always derive schedule_dates from table so recompute matches displayed dates
                schedule_dates = parse_schedule_dates_from_table(df_cap, start_date=disbursement_date)
                df_cap = recompute_customised_from_payments(df_cap, total_facility, schedule_dates, annual_rate, flat_rate, disbursement_date)
                st.session_state[cap_key] = df_cap
                st.session_state["cap_cust_first_rep_derived"] = first_repayment_from_customised_table(df_cap)

                date_editable = irregular
                if irregular:
                    if st.button("Add Row", type="secondary", key="cap_cust_add_row"):
                        last_df = st.session_state[cap_key]
                        if len(last_df) > 0:
                            try:
                                last_date_str = str(last_df.at[len(last_df) - 1, "Date"]).strip()[:32]
                                last_dt = datetime.combine(datetime.strptime(last_date_str, "%d-%b-%Y").date(), datetime.min.time())
                            except (ValueError, TypeError):
                                last_dt = add_months(disbursement_date, len(last_df))
                            next_dt = add_months(last_dt, 1)
                            if not use_anniversary:
                                next_dt = next_dt.replace(day=days_in_month(next_dt.year, next_dt.month))
                            new_row = {"Period": len(last_df), "Date": next_dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0}
                            st.session_state[cap_key] = pd.concat([last_df, pd.DataFrame([new_row])], ignore_index=True)
                            st.rerun()

                edited = st.data_editor(
                    df_cap,
                    column_config=money_df_column_config(
                        df_cap,
                        overrides={
                            "Period": st.column_config.NumberColumn(disabled=True),
                            "Date": st.column_config.TextColumn(
                                disabled=not date_editable,
                            ),
                        },
                        column_disabled=schedule_editor_disabled_amounts,
                    ),
                    width="stretch",
                    hide_index=True,
                    key="cap_cust_editor",
                )
                if not edited.equals(df_cap):
                    schedule_dates_edit = parse_schedule_dates_from_table(edited, start_date=disbursement_date)
                    df_cap = recompute_customised_from_payments(edited, total_facility, schedule_dates_edit, annual_rate, flat_rate, disbursement_date)
                    st.session_state[cap_key] = df_cap
                    st.session_state["cap_cust_first_rep_derived"] = first_repayment_from_customised_table(df_cap)
                    st.rerun()

                # Show first repayment date from current table (first row with payment > 0)
                first_rep_from_current = first_repayment_from_customised_table(df_cap)
                first_rep_label = first_rep_from_current.strftime("%d-%b-%Y") if first_rep_from_current else default_first.strftime("%d-%b-%Y") + " (no payment yet)"
                st.markdown(f"**First repayment date (from table):** {first_rep_label}")

                # For save: first repayment = first row with non-zero payment; end = last date in table
                first_rep_for_save = first_repayment_from_customised_table(df_cap) or first_rep
                end_date_from_table = schedule_dates[-1] if schedule_dates else disbursement_date

                final_to = float(df_cap.at[len(df_cap) - 1, "Total Outstanding"]) if len(df_cap) > 1 else total_facility
                if abs(final_to) < 0.01:
                    details = {
                        "principal": total_facility, "disbursed_amount": loan_required, "term": loan_term,
                        "annual_rate": annual_rate, "drawdown_fee": drawdown_pct, "arrangement_fee": arrangement_pct,
                        "disbursement_date": disbursement_date, "end_date": end_date_from_table,
                        "first_repayment_date": first_rep_for_save, "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
                        "penalty_rate_pct": float(penalty_pct_monthly),
                        "penalty_quotation": penalty_quotation_product,
                        "currency": currency,
                    }
                    if st.button("Use This Schedule", type="secondary", key="cap_cust_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_cap
                        st.session_state["capture_loan_step"] = 0
                        st.session_state["capture_require_docs_prompt"] = True
                        try:
                            st.toast("Schedule saved — scroll down for review, documents, and actions.", icon="✅")
                        except Exception:
                            pass
                        st.rerun()
                else:
                    st.warning("Clear the schedule (Total Outstanding = $0) before using it.")
        st.markdown('<hr class="fcapture-soft"/>', unsafe_allow_html=True)
        _rv_det = st.session_state.get("capture_loan_details")
        _rv_df = st.session_state.get("capture_loan_schedule_df")
        _rv_cid = st.session_state.get("capture_customer_id")
        _rv_lt = st.session_state.get("capture_loan_type")
        if _rv_det and _rv_df is not None and _rv_cid and _rv_lt:
            st.subheader("Review & Submit")
            sum_col1, sum_col2, sum_col3, sum_col4 = st.columns(4)
            with sum_col1:
                st.markdown(f"**Customer:** {get_display_name(_rv_cid)} ({_rv_cid})")
                st.markdown(
                    f"**Product:** {st.session_state.get('capture_product_code') or '—'} · **{_rv_lt}**"
                )
                _rv_lpid = st.session_state.get("capture_loan_purpose_id")
                if _rv_lpid is not None and str(_rv_lpid).strip() != "" and loan_management_available:
                    try:
                        _rv_pur = get_loan_purpose_by_id(int(_rv_lpid))
                        _rv_pnm = (_rv_pur or {}).get("name")
                        if _rv_pnm:
                            st.markdown(f"**Loan purpose:** {_rv_pnm}")
                        else:
                            st.markdown(f"**Loan purpose:** ID {_rv_lpid}")
                    except (TypeError, ValueError):
                        pass
            with sum_col2:
                st.markdown(f"**Principal:** {_rv_det.get('principal', 0):,.2f}")
                st.markdown(
                    f"**Disbursed** {_rv_det.get('disbursed_amount', 0):,.2f} · **Term** {_rv_det.get('term', 0)} mo"
                )
            product_code_for_rate = st.session_state.get("capture_product_code")
            product_cfg_for_rate = get_product_config_from_db(product_code_for_rate) or {}
            rate_basis_for_display = (product_cfg_for_rate.get("global_loan_settings") or {}).get("rate_basis")
            if rate_basis_for_display not in {"Per month", "Per annum"}:
                st.error(
                    f"Selected product `{product_code_for_rate}` must define rate_basis "
                    "as 'Per month' or 'Per annum'."
                )
                st.stop()
            monthly_dec = None
            annual_dec = None
            if _rv_det.get("monthly_rate") is not None:
                monthly_dec = float(_rv_det.get("monthly_rate") or 0.0)
                annual_dec = monthly_dec * 12.0
            if _rv_det.get("annual_rate") is not None:
                annual_dec = float(_rv_det.get("annual_rate") or 0.0)
                monthly_dec = annual_dec / 12.0
            with sum_col3:
                if rate_basis_for_display == "Per month":
                    rate_display_pct = (monthly_dec or 0.0) * 100.0
                    st.markdown(f"**Int. (pm):** {rate_display_pct:.2f}%")
                else:
                    rate_display_pct = (annual_dec or 0.0) * 100.0
                    st.markdown(f"**Int. (pa):** {rate_display_pct:.2f}%")
                pen_rate_pct = _rv_det.get("metadata", {}).get(
                    "penalty_rate_pct", _rv_det.get("penalty_rate_pct", 0)
                )
                if rate_basis_for_display == "Per month":
                    pen_display_pct = float(pen_rate_pct or 0.0)
                    st.markdown(f"**Penalty (pm):** {pen_display_pct:.2f}%")
                else:
                    pen_display_pct = float(pen_rate_pct or 0.0) * 12.0
                    st.markdown(f"**Penalty (pa):** {pen_display_pct:.2f}%")
            with sum_col4:
                d_fee_amt = _rv_det.get("drawdown_fee_amount")
                a_fee_amt = _rv_det.get("arrangement_fee_amount")
                adm_fee_amt = _rv_det.get("admin_fee_amount")
                prin_raw = _rv_det.get("principal", _rv_det.get("facility", 0))
                if d_fee_amt is None:
                    d_fee_amt = float(prin_raw) * float(_rv_det.get("drawdown_fee") or 0)
                if a_fee_amt is None:
                    a_fee_amt = float(prin_raw) * float(_rv_det.get("arrangement_fee") or 0)
                if adm_fee_amt is None:
                    adm_fee_amt = float(prin_raw) * float(_rv_det.get("admin_fee") or 0)
                fees = float(d_fee_amt) + float(a_fee_amt) + float(adm_fee_amt)
                st.markdown(f"**Fees:** {format_display_amount(fees, system_config=get_system_config())}")
            st.markdown('<hr class="fcapture-soft"/>', unsafe_allow_html=True)
            st.subheader("Journal Preview (On Approval)")
            from accounting.service import AccountingService
            from loan_management import build_loan_approval_journal_payload

            try:
                payload_preview = build_loan_approval_journal_payload(_rv_det)
                _cash_gl_prev = (_rv_det or {}).get("cash_gl_account_id") or st.session_state.get(
                    "capture_cash_gl_account_id"
                )
                if _cash_gl_prev:
                    _ao_prev = dict(payload_preview.get("account_overrides") or {})
                    _ao_prev["cash_operating"] = str(_cash_gl_prev).strip()
                    payload_preview["account_overrides"] = _ao_prev
                sim = AccountingService().simulate_event("LOAN_APPROVAL", payload=payload_preview)
                if sim.lines:
                    if not sim.balanced and sim.warning:
                        st.warning(sim.warning)
                    df_preview = pd.DataFrame(
                        [
                            {
                                "Account": f"{line['account_name']} ({line['account_code']})",
                                "Debit": float(line["debit"]),
                                "Credit": float(line["credit"]),
                            }
                            for line in sim.lines
                        ]
                    )
                    st.dataframe(
                        df_preview,
                        use_container_width=True,
                        hide_index=True,
                        height=min(220, 42 + len(sim.lines) * 36),
                        column_config=money_df_column_config(df_preview),
                    )
                else:
                    st.info("No LOAN_APPROVAL template lines.")
            except Exception as e:
                st.warning(f"Journal preview unavailable: {e}")
            st.subheader("Repayment Schedule")
            _df_rv = format_schedule_df(_rv_df)
            render_centered_html_table(_df_rv, [str(c) for c in _df_rv.columns])

        st.subheader("Documents")
        _render_capture_loan_documents_staging(
            documents_available=documents_available,
            list_document_categories=list_document_categories,
            widget_suffix="",
        )

        if st.session_state.get("capture_rework_source_draft_id") is not None:
            st.info("Rework session — edit as needed, then send again for approval.")

        has_schedule = (
            st.session_state.get("capture_loan_details") is not None
            and st.session_state.get("capture_loan_schedule_df") is not None
        )
        st.subheader("Actions")
        ba1, ba2, ba3, ba4 = st.columns(4)
        with ba1:
            if st.button(
                "Clear Form",
                type="secondary",
                key="cap_clear_all",
            ):
                _fcapture_clear_session_after_submit()
                st.session_state["capture_flash_message"] = "Form cleared."
                st.rerun()
        with ba2:
            if st.button(
                "Save Continue Later",
                type="secondary",
                key="cap_save_staged_schedule",
            ):
                _save_capture_staged_draft()
        with ba3:
            if st.button(
                "Send for Approval",
                type="secondary",
                key="cap_send_for_approval",
                disabled=not has_schedule,
            ):
                _submit_capture_send_for_approval()
        with ba4:
            if st.button(
                "Dismiss Draft",
                type="secondary",
                key="cap_dismiss_t3",
            ):
                for k in list(st.session_state.keys()):
                    if (k.startswith("capture_") or k.startswith("cap_")) and k not in (
                        "capture_flash_message",
                        "_fcapture_panel_css_v10",
                    ):
                        st.session_state.pop(k, None)
                st.session_state["loan_docs_staged"] = []
                st.session_state["capture_loan_step"] = 0
                st.session_state["capture_flash_message"] = "Capture session dismissed."
                st.rerun()
