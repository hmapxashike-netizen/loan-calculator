"""Loan modification tab: approval-gated workflow (balances, amounts, schedules, draft, documents)."""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any, Callable

import pandas as pd
import streamlit as st

from decimal_utils import as_10dp, amounts_equal_at_2dp
from loans import (
    add_months,
    days_in_month,
    is_last_day_of_month,
    parse_schedule_dates_from_table,
    recompute_customised_from_payments,
    repayment_dates,
)
from services.modification_capture_bridge import (
    EOD_SUMMARY_BUCKET_ROWS,
    as_of_balance_date,
    bucket_snapshot_for_json,
    loan_type_db,
    loan_type_display,
)
from style import BRAND_GREEN, BRAND_TEXT_MUTED, inject_style_block, render_sub_sub_header
from ui.components import inject_tertiary_hyperlink_css_once, schedule_readonly_dataframe_column_config
from ui.streamlit_feedback import run_with_spinner

def _quote_interest_for_basis(value: float, from_b: str, to_b: str) -> float:
    """Map a quoted interest % from loan A basis to another product basis."""
    if from_b == to_b:
        return float(value)
    if from_b == "Per annum" and to_b == "Per month":
        return float(value) / 12.0
    if from_b == "Per month" and to_b == "Per annum":
        return float(value) * 12.0
    return float(value)


def _default_central_interest_pct(cfg: dict[str, Any], lt_display: str) -> float:
    key = {
        "Consumer Loan": "consumer_loan",
        "Term Loan": "term_loan",
        "Bullet Loan": "bullet_loan",
        "Customised Repayments": "customised_repayments",
    }.get(lt_display, "term_loan")
    dr = (cfg.get("default_rates") or {}).get(key) or {}
    return float(dr.get("interest_pct") or 0.0)


def _default_central_penalty_pct(cfg: dict[str, Any], lt_display: str) -> float:
    pmap = {
        "Consumer Loan": "consumer_loan",
        "Term Loan": "term_loan",
        "Bullet Loan": "bullet_loan",
        "Customised Repayments": "customised_repayments",
    }
    pk = pmap.get(lt_display, "term_loan")
    p = (cfg.get("penalty_rates") or {}).get(pk)
    return float(p) if p is not None else 0.0


def _two_col_label_amount_table_html(*, label: str, amount: float) -> str:
    """Item + amount row: first column left, amount header and cell centered."""
    h0 = html.escape("Item")
    h1 = html.escape("Amount")
    c0 = html.escape(label)
    c1 = html.escape(f"{float(as_10dp(amount)):,.2f}")
    return (
        '<div style="overflow-x:auto;">'
        '<table style="border-collapse:collapse;font-size:0.95rem;width:100%;max-width:28rem;">'
        "<thead><tr>"
        f'<th style="text-align:left;padding:0.35rem 0.6rem;border-bottom:1px solid #e2e8f0;">{h0}</th>'
        f'<th style="text-align:center;padding:0.35rem 0.6rem;border-bottom:1px solid #e2e8f0;">{h1}</th>'
        "</tr></thead><tbody><tr>"
        f'<td style="text-align:left;padding:0.35rem 0.6rem;vertical-align:middle;">{c0}</td>'
        f'<td style="text-align:center;padding:0.35rem 0.6rem;vertical-align:middle;">{c1}</td>'
        "</tr></tbody></table></div>"
    )


def _parse_amount_grouped(text: str) -> float | None:
    """Parse user amount; allows commas and spaces (e.g. 1,001,000.00)."""
    t = (text or "").strip().replace(",", "").replace(" ", "")
    if t == "" or t in {".", "-", "-."}:
        return None
    try:
        v = float(t)
    except ValueError:
        return None
    if v < 0:
        return None
    return float(v)


def _make_reamod_grouped_amount_on_change(key: str, *, empty_as_zero: bool = False):
    """Return Streamlit on_change callback to rewrite value as grouped 2dp (e.g. 1,001,000.00)."""

    def _cb() -> None:
        raw = st.session_state.get(key)
        if raw is None:
            return
        s = str(raw).strip()
        if s == "":
            if empty_as_zero:
                st.session_state[key] = "0.00"
            return
        p = _parse_amount_grouped(s)
        if p is None:
            return
        canon = f"{float(as_10dp(p)):,.2f}"
        if s != canon:
            st.session_state[key] = canon

    return _cb


def _compact_readonly_amount(
    label: str,
    value_str: str,
    *,
    help_text: str | None = None,
    text_align: str = "left",
) -> None:
    """Body-sized read-only figure (avoids st.metric’s oversized value typography)."""
    lab = html.escape(label)
    val = html.escape(value_str)
    ta = text_align if text_align in ("left", "center", "right") else "left"
    title = f' title="{html.escape(help_text, quote=True)}"' if help_text else ""
    st.markdown(
        f'<p style="font-size:1rem;line-height:1.45;margin:0.15rem 0 0 0;color:inherit;text-align:{ta};"{title}>'
        f'<span style="font-weight:600;">{lab}</span> '
        f'<span style="font-weight:500;">{val}</span></p>',
        unsafe_allow_html=True,
    )


def _inject_reamod_use_schedule_button_css_once() -> None:
    """Brand-green **Use this schedule** (shown under preview or under customised data_editor)."""
    k = "_farnda_reamod_use_sched_btn_css_v1"
    if st.session_state.get(k):
        return
    st.session_state[k] = True
    g = BRAND_GREEN
    inject_style_block(
        f"""
[data-testid="stMain"] button[data-testid="stBaseButton-primary"][aria-label="Use this schedule"],
[data-testid="stMain"] button[kind="primary"][aria-label="Use this schedule"] {{
  background-color: {g} !important;
  color: #ffffff !important;
  border: none !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.12) !important;
}}
[data-testid="stMain"] button[data-testid="stBaseButton-primary"][aria-label="Use this schedule"]:hover,
[data-testid="stMain"] button[kind="primary"][aria-label="Use this schedule"]:hover {{
  filter: brightness(0.93) !important;
}}
"""
    )


def _inject_reamod_save_clear_green_css_once() -> None:
    """Brand-green **Save and Continue Later** and **Clear session schedules**."""
    k = "_farnda_reamod_save_clear_green_css"
    if st.session_state.get(k):
        return
    st.session_state[k] = True
    g = BRAND_GREEN
    inject_style_block(
        f"""
[data-testid="stMain"] button[data-testid="stBaseButton-primary"][aria-label="Save and Continue Later"],
[data-testid="stMain"] button[kind="primary"][aria-label="Save and Continue Later"],
[data-testid="stMain"] button[data-testid="stBaseButton-primary"][aria-label="Clear session schedules"],
[data-testid="stMain"] button[kind="primary"][aria-label="Clear session schedules"] {{
  background-color: {g} !important;
  color: #ffffff !important;
  border: none !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.12) !important;
}}
[data-testid="stMain"] button[data-testid="stBaseButton-primary"][aria-label="Save and Continue Later"]:hover,
[data-testid="stMain"] button[kind="primary"][aria-label="Save and Continue Later"]:hover,
[data-testid="stMain"] button[data-testid="stBaseButton-primary"][aria-label="Clear session schedules"]:hover,
[data-testid="stMain"] button[kind="primary"][aria-label="Clear session schedules"]:hover {{
  filter: brightness(0.93) !important;
}}
"""
    )


def _reamod_field_caption(text: str, *, align: str = "left") -> None:
    st.markdown(
        f'<p style="margin:0 0 0.2rem 0;font-size:0.8rem;color:{BRAND_TEXT_MUTED};text-align:{align};">'
        f"{html.escape(text)}</p>",
        unsafe_allow_html=True,
    )


def _leg_letter(i: int) -> str:
    return chr(ord("A") + i)


def _leg_session_keys(i: int) -> tuple[str, str]:
    if i == 0:
        return "reamod_details_a", "reamod_df_a"
    if i == 1:
        return "reamod_details_b", "reamod_df_b"
    L = _leg_letter(i).lower()
    return f"reamod_details_{L}", f"reamod_df_{L}"


def _mod_gross_principal(
    carry_leg: float,
    top_leg: float,
    restruct_fee: float,
    top_fee_combined: float,
) -> tuple[float, float, float]:
    """Returns (gross_carry, gross_top, combined_gross). Fees are decimals in [0,1)."""
    rf = float(restruct_fee)
    gc = float(as_10dp(carry_leg / (1.0 - rf))) if rf < 1.0 and carry_leg > 0 else float(as_10dp(carry_leg))
    tf = float(top_fee_combined)
    if top_leg <= 0:
        return gc, 0.0, float(as_10dp(gc))
    gt = float(as_10dp(top_leg / (1.0 - tf))) if tf < 1.0 else float(as_10dp(top_leg))
    return gc, gt, float(as_10dp(gc + gt))


_REMOD_DOC_NAMES = {
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


def _render_remod_documents_staging(
    *,
    documents_available: bool,
    list_document_categories: Callable[..., list[dict]],
) -> None:
    if "reamod_docs_staged" not in st.session_state:
        st.session_state["reamod_docs_staged"] = []
    staged = st.session_state["reamod_docs_staged"]
    if not documents_available:
        st.info("Document module is unavailable.")
        return
    doc_cats = list_document_categories(active_only=True)
    name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in _REMOD_DOC_NAMES}
    if not name_to_cat:
        st.info("No matching loan document categories configured.")
        return
    d1, d2 = st.columns(2)
    with d1:
        doc_type = st.selectbox("Doc type", sorted(name_to_cat.keys()), key="reamod_doc_type")
        other_label = ""
        if doc_type == "Other":
            other_label = st.text_input("If Other, describe", key="reamod_doc_other")
    with d2:
        f = st.file_uploader("File", type=["pdf", "png", "jpg", "jpeg"], key="reamod_doc_file")
    if st.button("Add to list", type="tertiary", key="reamod_doc_add") and f is not None:
        cat = name_to_cat[doc_type]
        staged.append(
            {
                "category_id": cat["id"],
                "category_name": doc_type,
                "file": f,
                "notes": other_label.strip() if doc_type == "Other" else "",
            }
        )
        st.session_state["reamod_docs_staged"] = staged
        st.success(f"Staged {f.name}.")
        st.rerun()
    if staged:
        st.caption("Staged documents (attached on submit)")
        for i, row in enumerate(staged, start=1):
            st.write(f"{i}. {row['file'].name} · {row.get('category_name') or ''}")


def _attach_staged_docs(
    draft_id: int,
    staged: list[dict],
    upload_document_fn: Callable[..., Any] | None,
) -> tuple[int, list[str]]:
    if not upload_document_fn or not staged:
        return 0, []
    errors: list[str] = []
    count = 0
    for row in staged:
        f = row["file"]
        try:
            upload_document_fn(
                "loan_approval_draft",
                int(draft_id),
                int(row["category_id"]),
                f.name,
                f.type,
                f.size,
                f.getvalue(),
                uploaded_by="System User",
                notes=str(row.get("notes") or ""),
            )
            count += 1
        except Exception as ex:
            errors.append(f"{f.name}: {ex}")
    return count, errors


def _reamod_collateral_patch_dict_only(pol: str) -> dict[str, Any]:
    """Collateral patch without widgets (Keep / confirmed Clear)."""
    if pol == "Clear":
        return {"collateral_cleared": True}
    return {}


def _reamod_collateral_clear_confirm_block(*, key_prefix: str) -> None:
    """One-step confirmation after user clicks Clear (does not open Documents)."""
    pend_key = f"{key_prefix}_coll_clear_pending"
    if not st.session_state.get(pend_key):
        return
    st.warning(
        "Collateral will be **cleared** when you save or submit this modification. "
        "Confirm, or cancel to keep the previous choice."
    )
    _cy, _cn = st.columns(2, gap="small")
    pol_key = f"{key_prefix}_coll_pol"
    with _cy:
        if st.button("Confirm clear", type="primary", key=f"{key_prefix}_coll_clr_conf"):
            st.session_state[pol_key] = "Clear"
            st.session_state[pend_key] = False
            st.session_state["reamod_coll_clear_flash"] = key_prefix
            st.rerun()
    with _cn:
        if st.button("Cancel", type="secondary", key=f"{key_prefix}_coll_clr_can"):
            st.session_state[pend_key] = False
            st.rerun()


def _reamod_collateral_link_row(*, key_prefix: str, heading: str = "Collateral") -> str:
    """Brand subheading + tertiary links; opens Documents only for Keep existing / Replace."""
    inject_tertiary_hyperlink_css_once()
    pol_key = f"{key_prefix}_coll_pol"
    pend_key = f"{key_prefix}_coll_clear_pending"
    if pol_key not in st.session_state:
        st.session_state[pol_key] = "Keep existing"
    c0, c1, c2, c3 = st.columns([1.2, 1.05, 0.72, 0.55], gap="small")
    with c0:
        h = heading.rstrip(":").strip() or "Collateral"
        render_sub_sub_header(f"{h}:")
    with c1:
        if st.button("Keep existing", type="tertiary", key=f"{key_prefix}_coll_keep"):
            st.session_state[pol_key] = "Keep existing"
            st.session_state[pend_key] = False
            st.session_state["reamod_docs_open"] = True
    with c2:
        if st.button("Replace", type="tertiary", key=f"{key_prefix}_coll_rep"):
            st.session_state[pol_key] = "Replace"
            st.session_state[pend_key] = False
            st.session_state["reamod_docs_open"] = True
    with c3:
        if st.button("Clear", type="tertiary", key=f"{key_prefix}_coll_clr"):
            st.session_state[pend_key] = True
    return str(st.session_state[pol_key])


def _reamod_collateral_replace_patch(
    *,
    key_prefix: str,
    provisions_config_ok: bool,
    list_provision_security_subtypes: Callable[[], list[dict]] | None,
    loan: dict[str, Any],
) -> dict[str, Any]:
    subtypes: list[dict] = []
    if provisions_config_ok and list_provision_security_subtypes:
        try:
            subtypes = list_provision_security_subtypes()
        except Exception:
            pass
    opts = [("", "None / Unsecured")] + [
        (
            str(s["id"]),
            f"{s.get('security_type', '')} · {s.get('subtype_name', s.get('name', ''))}".strip(" ·")
            or f"Subtype #{s.get('id')}",
        )
        for s in subtypes
    ]
    curr = str(loan.get("collateral_security_subtype_id") or "")
    idx = next((i for i, o in enumerate(opts) if o[0] == curr), 0)
    c1, c2, c3 = st.columns(3)
    with c1:
        lab = st.selectbox(
            "Security subtype",
            [o[1] for o in opts],
            index=idx,
            key=f"{key_prefix}_coll_sub",
        )
    sub_id = next(o[0] for o in opts if o[1] == lab)
    with c2:
        chg = st.number_input(
            "Charge amount",
            min_value=0.0,
            value=float(loan.get("collateral_charge_amount") or 0.0),
            key=f"{key_prefix}_coll_chg",
        )
    with c3:
        val = st.number_input(
            "Valuation",
            min_value=0.0,
            value=float(loan.get("collateral_valuation_amount") or 0.0),
            key=f"{key_prefix}_coll_val",
        )
    return {
        "collateral_security_subtype_id": int(sub_id) if sub_id else None,
        "collateral_charge_amount": float(as_10dp(chg)) if chg > 0 else None,
        "collateral_valuation_amount": float(as_10dp(val)) if val > 0 else None,
    }


def render_loan_modification_tab(
    *,
    list_customers: Callable[[], list[dict]],
    get_display_name: Callable[[int], str],
    get_system_date: Callable[[], Any],
    get_loan_for_modification: Callable[[int], dict | None],
    list_products: Callable[..., list[dict]],
    get_product_config_from_db: Callable[[str], dict | None],
    get_system_config: Callable[[], dict],
    get_consumer_schemes: Callable[[], list[dict]],
    get_product_rate_basis: Callable[..., str],
    compute_consumer_schedule: Callable[..., tuple[dict, pd.DataFrame]],
    compute_term_schedule: Callable[..., tuple[dict, pd.DataFrame]],
    compute_bullet_schedule: Callable[..., tuple[dict, pd.DataFrame]],
    pct_to_monthly: Callable[..., float | None],
    save_loan_approval_draft: Callable[..., int],
    update_loan_approval_draft_staged: Callable[..., None],
    resubmit_loan_approval_draft: Callable[..., int],
    documents_available: bool,
    list_document_categories: Callable[..., list[dict]],
    upload_document: Callable[..., Any] | None,
    provisions_config_ok: bool,
    list_provision_security_subtypes: Callable[[], list[dict]] | None,
    source_cash_gl_cached_labels_and_ids: list[tuple[str, str]] | None,
    created_by: str | None,
    money_df_column_config: Callable[..., Any] | None = None,
    schedule_editor_disabled_amounts: dict[str, bool] | None = None,
    first_repayment_from_customised_table: Callable[[pd.DataFrame], Any] | None = None,
) -> None:
    from loan_management.daily_state import get_loan_daily_state_balances

    render_sub_sub_header("Select Customer and Loan to Modify")

    customers = list_customers() or []
    if not customers:
        st.info("No customers. Create a customer first.")
        return

    # Outside `reamod_*` namespace so we can wipe every reamod_ key on first visit without exception.
    _FARNDA_REMOD_TAB_INIT = "_farnda_reamod_mod_tab_init_v2"
    if _FARNDA_REMOD_TAB_INIT not in st.session_state:
        for _rk in list(st.session_state.keys()):
            if isinstance(_rk, str) and _rk.startswith("reamod_"):
                st.session_state.pop(_rk, None)
        st.session_state.pop("reamod_mod_tab_session_init", None)
        st.session_state[_FARNDA_REMOD_TAB_INIT] = True

    PLACEHOLDER_CUST = "-Select Customer-"
    PLACEHOLDER_LOAN = "-Select Loan-"

    h1, h2, h3 = st.columns([1, 1, 1], gap="small")
    with h1:
        st.caption("Customer")
        cust_choices = [PLACEHOLDER_CUST] + [get_display_name(c["id"]) for c in customers]
        _cust_cur = st.session_state.get("reamod_hdr_cust")
        if _cust_cur is None or _cust_cur not in cust_choices:
            st.session_state["reamod_hdr_cust"] = PLACEHOLDER_CUST
        cust_sel = st.selectbox("Customer", cust_choices, key="reamod_hdr_cust", label_visibility="collapsed")
    with h2:
        st.caption("Loan")
        from loan_management import get_loans_by_customer

        if cust_sel == PLACEHOLDER_CUST:
            st.selectbox(
                "Loan",
                [PLACEHOLDER_LOAN],
                key="reamod_hdr_loan",
                label_visibility="collapsed",
                disabled=True,
            )
        else:
            cust_id = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel)
            loans = get_loans_by_customer(cust_id)
            loans_active = [l for l in loans if l.get("status") == "active"]
            if not loans_active:
                st.info("No active loans.")
                st.stop()
            loan_opts = [
                (l["id"], f"#{l['id']} · {l.get('loan_type', '')} · {float(l.get('principal') or 0):,.2f}")
                for l in loans_active
            ]
            labels = [x[1] for x in loan_opts]
            _prev_loan = st.session_state.get("reamod_hdr_loan")
            if _prev_loan is None or _prev_loan not in labels:
                st.session_state["reamod_hdr_loan"] = labels[0]
            loan_lab = st.selectbox("Loan", labels, key="reamod_hdr_loan", label_visibility="collapsed")
            loan_id = loan_opts[labels.index(loan_lab)][0]
    with h3:
        st.caption("Restructure date")
        _sys = get_system_date()
        restructure_date = st.date_input(
            "Restructure date",
            value=_sys,
            max_value=_sys,
            key="reamod_hdr_date",
            label_visibility="collapsed",
        )

    if cust_sel == PLACEHOLDER_CUST:
        st.info("Select a **customer** to load loans and continue.")
        st.stop()

    info = get_loan_for_modification(loan_id)
    if not info:
        st.warning("Could not load loan.")
        return
    loan = info["loan"]
    last_due = info.get("last_due_date")

    gate_ok = True
    if last_due and restructure_date > last_due:
        st.error("Restructure date cannot be after the last due date.")
        gate_ok = False

    as_of = as_of_balance_date(restructure_date)
    bal = get_loan_daily_state_balances(loan_id, as_of) if gate_ok else None

    if gate_ok:
        if bal is None:
            st.warning(
                f"No **loan_daily_state** row on or before **{as_of.isoformat()}**. "
                "Run EOD for that date (or pick a later restructure date) to see the breakdown."
            )
        else:
            render_sub_sub_header(f"Balance Outstanding as of {as_of.strftime('%d/%m/%Y')}")
            row_eod: dict[str, float] = {}
            for lab, key in EOD_SUMMARY_BUCKET_ROWS:
                row_eod[lab] = float(as_10dp(bal.get(key) or 0))
            row_eod["Total outstanding (total_exposure)"] = float(as_10dp(bal.get("total_exposure") or 0))
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

    outstanding = float(as_10dp(bal.get("total_exposure") or 0)) if bal else 0.0

    if not gate_ok or bal is None:
        st.stop()

    st.divider()
    render_sub_sub_header("Split Strategy")
    _carry_disp_key = "reamod_carry_display"
    _carry_loan_key = "reamod_carry_bound_loan_id"
    _top_disp_key = "reamod_topup_display"
    _top_loan_key = "reamod_topup_bound_loan_id"
    if st.session_state.get(_carry_loan_key) != loan_id:
        st.session_state[_carry_loan_key] = int(loan_id)
        st.session_state[_carry_disp_key] = f"{float(as_10dp(outstanding)):,.2f}"
    if st.session_state.get(_top_loan_key) != loan_id:
        st.session_state[_top_loan_key] = int(loan_id)
        st.session_state[_top_disp_key] = "0.00"

    st.caption("Loans from this modification")
    _mod_loan_opts = [
        "1 — single new facility on this loan",
        "2 — split into Loans A + B",
        "3 — split into Loans A + B + C",
        "4 or more — use termination + new loans instead",
    ]
    _mod_ix = st.selectbox(
        "Loans from this modification",
        range(len(_mod_loan_opts)),
        format_func=lambda i: _mod_loan_opts[i],
        key="reamod_mod_loan_intent",
        label_visibility="collapsed",
        help="Supports up to three facilities (A–C). Four or more require terminating the loan and booking new loans.",
    )
    if int(_mod_ix) == len(_mod_loan_opts) - 1:
        st.warning(
            "This screen supports **up to three** facilities from one modification (**Loans A–C**). For **four or "
            "more** loans, **terminate** this loan and capture **new** loans for each facility."
        )
        st.stop()
    n_legs = int(_mod_ix) + 1
    is_split = n_legs >= 2

    if not is_split:
        # Empty carry is not an error: default to EOD outstanding (covers split→single and cleared field).
        _carry_s = str(st.session_state.get(_carry_disp_key, "")).strip()
        if not _carry_s:
            st.session_state[_carry_disp_key] = f"{float(as_10dp(outstanding)):,.2f}"
            st.rerun()
        _parsed_carry = _parse_amount_grouped(str(st.session_state.get(_carry_disp_key, "")))
        _top_raw = str(st.session_state.get(_top_disp_key, "0")).strip() or "0"
        _parsed_top = _parse_amount_grouped(_top_raw)

        a1, a2, a3 = st.columns(3, gap="small")
        with a1:
            _reamod_field_caption("Amount to restructure", align="left")
            st.text_input(
                "Amount to restructure",
                key=_carry_disp_key,
                label_visibility="collapsed",
                on_change=_make_reamod_grouped_amount_on_change(_carry_disp_key, empty_as_zero=False),
                help="Thousands separators optional (e.g. 1,001,000.00). Top-up is booked on loan A.",
            )
        with a2:
            _reamod_field_caption("Top-up (additional disbursement)", align="center")
            _tp_l, _tp_m, _tp_r = st.columns([0.12, 1.0, 0.12], gap="small")
            with _tp_m:
                st.text_input(
                    "Top-up (additional disbursement)",
                    key=_top_disp_key,
                    label_visibility="collapsed",
                    on_change=_make_reamod_grouped_amount_on_change(_top_disp_key, empty_as_zero=True),
                    help="Optional; thousands separators allowed (e.g. 50,000.00). Drawn on loan **A** only.",
                )
        with a3:
            _tf_l, _tf_m, _tf_r = st.columns([0.12, 1.0, 0.12], gap="small")
            with _tf_m:
                if _parsed_carry is not None and _parsed_top is not None:
                    _tf_preview = float(as_10dp(_parsed_carry + _parsed_top))
                    _compact_readonly_amount(
                        "Total facility",
                        f"{_tf_preview:,.2f}",
                        text_align="center",
                    )
                else:
                    _compact_readonly_amount("Total facility", "—", text_align="center")

        if _parsed_carry is None:
            st.error("Enter a valid **Amount to restructure** (non-negative number; commas allowed).")
            st.stop()
        if _parsed_top is None:
            st.error("Enter a valid **Top-up** amount (non-negative; commas allowed), or **0.00**.")
            st.stop()
        carry = float(as_10dp(_parsed_carry))
        top_up = float(as_10dp(_parsed_top))
        total_facility = float(as_10dp(carry + top_up))
    else:
        carry = float(as_10dp(outstanding))
        top_up = 0.0
        total_facility = float(as_10dp(outstanding))
        st.caption(
            "Split refinances **full EOD outstanding** across the new loans; allocate via **Net** under **Modified Loans**."
        )

    if not is_split and carry > outstanding + 1e-6:
        st.error("Amount to restructure cannot exceed total outstanding (total_exposure).")
        st.stop()

    excess_outstanding = float(as_10dp(max(0.0, outstanding - total_facility)))
    if is_split:
        excess_policy = "SPLIT"
        base_writeoff = float(as_10dp(outstanding - total_facility))
    else:
        base_writeoff = (
            float(as_10dp(outstanding - total_facility)) if excess_outstanding > 1e-6 else 0.0
        )
        excess_policy = "WRITE_OFF" if excess_outstanding > 1e-6 else "NONE"

    if not is_split:
        _compact_readonly_amount(
            "Excess outstanding",
            f"{excess_outstanding:,.2f}",
            help_text="EOD **total_exposure** minus total facility (not refinanced into the new facility).",
        )
        if excess_outstanding > 1e-6:
            st.info(
                f"**Write-off at approval:** **{excess_outstanding:,.2f}** — EOD outstanding not covered by total "
                f"facility will be written off when this modification is approved."
            )
        else:
            st.caption("No excess outstanding: total facility matches EOD **total_exposure** (2dp).")

    st.divider()
    products = list_products(active_only=True) or []
    if not products:
        st.error("No active loan products.")
        st.stop()
    prod_labels = [f"{p['code']} · {p['name']}" for p in products]

    net_legs: list[float] = []
    split_product_codes: list[str] = []
    split_lt_display: list[str] = []
    split_lt_db: list[str] = []
    split_alloc_writeoff = 0.0

    if is_split:
        render_sub_sub_header("Modified Loans")
        net_legs = []
        nc = st.columns(n_legs)
        for i in range(n_legs):
            with nc[i]:
                _lab = f"Loan {_leg_letter(i)}"
                if i == 0:
                    _lab += " (amount to restructure)"
                st.caption(_lab)
                _net_default = float(as_10dp(total_facility)) if i == 0 else 0.0
                net_legs.append(
                    st.number_input(
                        "Net",
                        min_value=0.0,
                        value=_net_default,
                        step=100.0,
                        format="%.2f",
                        key=f"reamod_net_{i}",
                        label_visibility="collapsed",
                    )
                )

        sum_net = float(as_10dp(sum(net_legs)))
        _net_facility_ok = sum_net <= total_facility + 1e-6 or amounts_equal_at_2dp(
            as_10dp(sum_net), as_10dp(total_facility)
        )
        if not _net_facility_ok:
            _leg_labels = " + ".join(f"Loan {_leg_letter(i)}" for i in range(n_legs))
            st.error(
                f"**Total facility** must be **greater than or equal to** the combined net proceeds "
                f"({_leg_labels}). Increase amount to restructure / top-up, or lower one or more net amounts."
            )
            st.stop()

        if top_up > 1e-9 and net_legs and float(net_legs[0]) + 1e-9 < float(top_up):
            st.error(
                "Loan **A** net proceeds must be **>= top-up** (top-up is drawn on loan A only)."
            )
            st.stop()
        split_alloc_writeoff = float(as_10dp(max(0.0, total_facility - sum_net)))

        pc_cols = st.columns(n_legs)
        for i in range(n_legs):
            with pc_cols[i]:
                _plab = f"Loan {_leg_letter(i)}"
                if i == 0:
                    _plab += " (restructure)"
                st.caption(_plab)
                ix_i = st.selectbox(
                    "Product",
                    range(len(prod_labels)),
                    format_func=lambda j, labels=prod_labels: labels[j],
                    key=f"reamod_prod_{i}",
                    label_visibility="collapsed",
                )
                pr = products[int(ix_i)]
                split_product_codes.append(str(pr["code"]))
                split_lt_display.append(loan_type_display(str(pr.get("loan_type") or "")))
                split_lt_db.append(loan_type_db(split_lt_display[-1]))
    else:
        p1, _p2, _p3 = st.columns(3, gap="small")
        with p1:
            ix = st.selectbox(
                "Product", range(len(prod_labels)), format_func=lambda i: prod_labels[i], key="reamod_prod"
            )
            prod_a = products[int(ix)]
        product_code_a = str(prod_a["code"])
        lt_display_a = loan_type_display(str(prod_a.get("loan_type") or ""))
        lt_db_a = loan_type_db(lt_display_a)
        split_product_codes = [product_code_a]
        split_lt_display = [lt_display_a]
        split_lt_db = [lt_db_a]
        net_legs = [total_facility]

    if is_split:
        product_code_a = split_product_codes[0]
        lt_display_a = split_lt_display[0]
        lt_db_a = split_lt_db[0]

    split_product_code_b = split_product_codes[1] if len(split_product_codes) > 1 else product_code_a
    lt_db_b = split_lt_db[1] if len(split_lt_db) > 1 else lt_db_a

    top_alloc = [float(as_10dp(top_up if i == 0 else 0.0)) for i in range(len(net_legs))]
    carry_legs = [float(as_10dp(net_legs[i] - top_alloc[i])) for i in range(len(net_legs))]
    if is_split:
        sum_carry_split = float(as_10dp(sum(carry_legs)))
        if amounts_equal_at_2dp(as_10dp(sum_net), as_10dp(total_facility)):
            if not amounts_equal_at_2dp(as_10dp(sum_carry_split), as_10dp(carry)):
                st.error(
                    "When net proceeds **fully allocate** total facility, Σ(net − top-up on A) must equal "
                    "**Amount to restructure** (2dp). Increase or redistribute nets, or leave unallocated facility "
                    "to be written off (reduce total nets below total facility)."
                )
                st.stop()
        elif split_alloc_writeoff <= 1e-9:
            if not amounts_equal_at_2dp(as_10dp(sum_carry_split), as_10dp(carry)):
                st.error(
                    "Σ(net − top-up on A) must equal **Amount to restructure** (2dp) unless you leave part of "
                    "total facility unallocated (written off at approval)."
                )
                st.stop()

    pa = float(as_10dp(carry_legs[0])) if carry_legs else 0.0
    pb = float(as_10dp(carry_legs[1])) if len(carry_legs) > 1 else 0.0

    writeoff_amount = float(as_10dp(base_writeoff + split_alloc_writeoff))

    if is_split:
        sum_nets = float(as_10dp(sum(net_legs)))
        balancing_writeoff = float(as_10dp(outstanding - sum_nets))
        if not amounts_equal_at_2dp(as_10dp(writeoff_amount), as_10dp(balancing_writeoff)):
            st.error(
                "Split reconciliation (2dp): total **write-off at approval** must equal "
                "**EOD total outstanding − Σ(nets)**. Adjust net proceeds or facility amounts."
            )
            st.stop()

    disbursement_dt = datetime.combine(restructure_date, datetime.min.time())
    cfg_a = get_product_config_from_db(product_code_a) or {}
    gls_a = cfg_a.get("global_loan_settings") or {}
    im = gls_a.get("interest_method")
    if im not in {"Reducing balance", "Flat rate"}:
        st.error(f"Product `{product_code_a}` must set global_loan_settings.interest_method.")
        st.stop()

    dr_cons = (cfg_a.get("default_rates") or {}).get("consumer_loan") or {}
    dr_term = (cfg_a.get("default_rates") or {}).get("term_loan") or {}
    dr_bul = (cfg_a.get("default_rates") or {}).get("bullet_loan") or {}
    dr_custom = (cfg_a.get("default_rates") or {}).get("customised_repayments") or {}
    prb_a = get_product_rate_basis(cfg_a)

    if not is_split:
        st.divider()
        render_sub_sub_header("Terms & schedule")
        t1, t2, t3, t4 = st.columns(4, gap="small")
        with t1:
            fee_pct = (
                st.number_input(
                    "Restructure fee (%) on restructured outstanding",
                    min_value=0.0,
                    max_value=99.99,
                    value=0.0,
                    step=0.05,
                    key="reamod_fee_pct",
                )
                / 100.0
            )
        rate_lbl_int = (
            "Interest rate (% per annum)" if prb_a == "Per annum" else "Interest rate (% per month)"
        )
        rate_lbl_pen = (
            "Penalty rate (% per annum)" if prb_a == "Per annum" else "Penalty rate (% per month)"
        )
        _d_int = _default_central_interest_pct(cfg_a, lt_display_a)
        _d_pen = _default_central_penalty_pct(cfg_a, lt_display_a)
        with t2:
            central_interest_pct = st.number_input(
                rate_lbl_int,
                min_value=0.0,
                max_value=100.0,
                value=float(_d_int),
                step=0.05,
                key="reamod_central_interest_pct",
            )
        with t3:
            central_penalty_pct = st.number_input(
                rate_lbl_pen,
                min_value=0.0,
                max_value=100.0,
                value=float(_d_pen),
                step=0.05,
                key="reamod_central_penalty_pct",
            )
        with t4:
            term_m = st.number_input("Term (months)", 1, 360, 12, key="reamod_term")

        top_fee_consumer = float(dr_cons.get("admin_fee_pct") or 0) / 100.0
        top_fee_draw = float(dr_term.get("drawdown_pct") or 0) / 100.0
        top_fee_arr = float(dr_term.get("arrangement_pct") or 0) / 100.0
        cash_gl_id: str | None = None
        if top_up > 1e-9:
            _gl_list = source_cash_gl_cached_labels_and_ids
            if lt_display_a == "Consumer Loan":
                _tu_cols = st.columns(2 if _gl_list else 1, gap="small")
                with _tu_cols[0]:
                    top_fee_consumer = (
                        st.number_input(
                            "Admin fee on top-up (%)",
                            min_value=0.0,
                            max_value=99.99,
                            value=float(dr_cons.get("admin_fee_pct") or 0),
                            step=0.05,
                            key="reamod_top_admin_pct",
                        )
                        / 100.0
                    )
                if _gl_list:
                    _labels = [x[0] for x in _gl_list]
                    _ids = [x[1] for x in _gl_list]
                    _def_i = next(
                        (i for i, x in enumerate(_ids) if x == str(loan.get("cash_gl_account_id") or "").strip()),
                        0,
                    )
                    with _tu_cols[1]:
                        _pick = st.selectbox(
                            "Source cash / bank GL (top-up)",
                            range(len(_labels)),
                            format_func=lambda i: _labels[i],
                            index=_def_i,
                            key="reamod_cash_gl",
                        )
                        cash_gl_id = _ids[int(_pick)]
                elif loan.get("cash_gl_account_id"):
                    cash_gl_id = str(loan.get("cash_gl_account_id")).strip()
            elif lt_display_a in ("Term Loan", "Bullet Loan", "Customised Repayments"):
                if lt_display_a == "Term Loan":
                    dr_use = dr_term
                elif lt_display_a == "Bullet Loan":
                    dr_use = dr_bul
                else:
                    dr_use = dr_custom
                _tu_cols = st.columns(3 if _gl_list else 2, gap="small")
                with _tu_cols[0]:
                    top_fee_draw = (
                        st.number_input(
                            "Drawdown fee on top-up (%)",
                            min_value=0.0,
                            max_value=99.99,
                            value=float(dr_use.get("drawdown_pct") or 0),
                            step=0.05,
                            key="reamod_top_dd_pct",
                        )
                        / 100.0
                    )
                with _tu_cols[1]:
                    top_fee_arr = (
                        st.number_input(
                            "Arrangement fee on top-up (%)",
                            min_value=0.0,
                            max_value=99.99,
                            value=float(dr_use.get("arrangement_pct") or 0),
                            step=0.05,
                            key="reamod_top_arr_pct",
                        )
                        / 100.0
                    )
                if _gl_list:
                    _labels = [x[0] for x in _gl_list]
                    _ids = [x[1] for x in _gl_list]
                    _def_i = next(
                        (i for i, x in enumerate(_ids) if x == str(loan.get("cash_gl_account_id") or "").strip()),
                        0,
                    )
                    with _tu_cols[2]:
                        _pick = st.selectbox(
                            "Source cash / bank GL (top-up)",
                            range(len(_labels)),
                            format_func=lambda i: _labels[i],
                            index=_def_i,
                            key="reamod_cash_gl",
                        )
                        cash_gl_id = _ids[int(_pick)]
                elif loan.get("cash_gl_account_id"):
                    cash_gl_id = str(loan.get("cash_gl_account_id")).strip()
            else:
                st.info("Top-up fee inputs apply to Consumer / Term / Bullet / Customised on loan A.")
                if _gl_list:
                    _labels = [x[0] for x in _gl_list]
                    _ids = [x[1] for x in _gl_list]
                    _def_i = next(
                        (i for i, x in enumerate(_ids) if x == str(loan.get("cash_gl_account_id") or "").strip()),
                        0,
                    )
                    _one = st.columns(1, gap="small")
                    with _one[0]:
                        _pick = st.selectbox(
                            "Source cash / bank GL (top-up)",
                            range(len(_labels)),
                            format_func=lambda i: _labels[i],
                            index=_def_i,
                            key="reamod_cash_gl",
                        )
                        cash_gl_id = _ids[int(_pick)]
                elif loan.get("cash_gl_account_id"):
                    cash_gl_id = str(loan.get("cash_gl_account_id")).strip()

        if fee_pct >= 1.0:
            st.error("Restructure fee must be below 100%.")
            st.stop()

        # Only fees that apply to this loan type (do not add consumer admin% to Term/Bullet/Customised top-up).
        if lt_display_a == "Consumer Loan":
            top_fee_combined = float(as_10dp(top_fee_consumer))
        elif lt_display_a in ("Term Loan", "Bullet Loan", "Customised Repayments"):
            top_fee_combined = float(as_10dp(top_fee_draw + top_fee_arr))
        else:
            top_fee_combined = float(as_10dp(top_fee_consumer + top_fee_draw + top_fee_arr))
        if top_up > 1e-9 and top_fee_combined >= 1.0:
            st.error(
                "Combined top-up fees for this loan type must total **below 100%**."
            )
            st.stop()

        _gross_restruct = (
            float(as_10dp(carry / (1.0 - fee_pct))) if fee_pct < 1.0 and carry > 1e-12 else float(as_10dp(carry))
        )
        if top_up <= 1e-12:
            principal_schedule_single = _gross_restruct
        else:
            principal_schedule_single = float(
                as_10dp(_gross_restruct + float(as_10dp(top_up / (1.0 - top_fee_combined))))
            )

        _compact_readonly_amount(
            "Principal Amount:",
            f"{principal_schedule_single:,.2f}",
            help_text=(
                "Principal Amount = Amount to Restructure ÷ (1 − Restructure%) + Top Up ÷ (1 − sum of "
                "top-up fee %). **Consumer:** administration only. **Term / Bullet / Customised:** drawdown + "
                "arrangement only (administration on top-up = 0% for those types)."
            ),
        )
        notes = ""
    else:
        cash_gl_id = None
        fee_pct = 0.0
        principal_schedule_single = 0.0
        notes = ""
        # Placeholders for closure; split path always passes per-leg values into _gen_one_leg.
        term_m = 12
        central_interest_pct = 0.0
        central_penalty_pct = 0.0
        top_fee_consumer = float(dr_cons.get("admin_fee_pct") or 0) / 100.0
        top_fee_draw = float(dr_term.get("drawdown_pct") or 0) / 100.0
        top_fee_arr = float(dr_term.get("arrangement_pct") or 0) / 100.0
        if lt_display_a == "Consumer Loan":
            top_fee_combined = float(as_10dp(top_fee_consumer))
        elif lt_display_a in ("Term Loan", "Bullet Loan", "Customised Repayments"):
            top_fee_combined = float(as_10dp(top_fee_draw + top_fee_arr))
        else:
            top_fee_combined = float(as_10dp(top_fee_consumer + top_fee_draw + top_fee_arr))

    oit = "capitalise"

    def _gen_one_leg(
        *,
        leg: str,
        lt_display: str,
        product_code: str,
        cfg: dict,
        carry_leg: float,
        top_leg: float,
        net_total_leg: float,
        top_fee_on_leg: float,
        fee_pct_leg: float | None = None,
        term_months_leg: int | None = None,
        central_interest_pct_leg: float | None = None,
        central_penalty_pct_leg: float | None = None,
        schedule_gross_override: float | None = None,
        use_key: str,
        session_details_key: str,
        session_df_key: str,
    ) -> None:
        schemes = get_consumer_schemes()
        prb = get_product_rate_basis(cfg)
        gls_leg = cfg.get("global_loan_settings") or {}
        im_leg = gls_leg.get("interest_method")
        if im_leg not in {"Reducing balance", "Flat rate"}:
            st.error(f"Product `{product_code}` must set global_loan_settings.interest_method.")
            return
        flat_leg = im_leg == "Flat rate"
        disbursement_date = disbursement_dt
        default_fr = add_months(disbursement_date, 1).date()

        _fp = float(fee_pct_leg) if fee_pct_leg is not None else float(fee_pct)
        _tm = int(term_months_leg) if term_months_leg is not None else int(term_m)
        _ci = float(central_interest_pct_leg) if central_interest_pct_leg is not None else float(
            central_interest_pct
        )
        _cp = float(central_penalty_pct_leg) if central_penalty_pct_leg is not None else float(
            central_penalty_pct
        )

        if schedule_gross_override is not None:
            combined_gross = float(as_10dp(schedule_gross_override))
        else:
            tf_use = float(top_fee_on_leg) if top_leg > 1e-12 else 0.0
            _gc, _gt, combined_gross = _mod_gross_principal(carry_leg, top_leg, _fp, tf_use)
        if combined_gross <= 0:
            st.error("Gross facility for this leg must be positive.")
            return
        if net_total_leg > combined_gross + 1e-6:
            st.error("Leg net proceeds cannot exceed gross schedule principal for this leg.")
            return

        _has_top = top_leg > 1e-12
        if combined_gross > 1e-12:
            _f_eff = max(0.0, min(0.9999, 1.0 - (net_total_leg / combined_gross)))
        else:
            _f_eff = 0.0
        _stf = top_fee_draw + top_fee_arr
        if _stf > 1e-12:
            _draw_eff = _f_eff * (top_fee_draw / _stf)
            _arr_eff = _f_eff * (top_fee_arr / _stf)
        else:
            _draw_eff, _arr_eff = _f_eff, 0.0

        interest_quoted_leg = _quote_interest_for_basis(_ci, prb, prb)
        penalty_pm_central = pct_to_monthly(_cp, prb)
        if penalty_pm_central is None:
            st.error("Invalid penalty rate for this leg’s product rate basis.")
            return

        try:
            if lt_display == "Consumer Loan":
                dr = (cfg.get("default_rates") or {}).get("consumer_loan") or {}
                if dr.get("admin_fee_pct") is None:
                    st.error("Product missing default_rates.consumer_loan.admin_fee_pct.")
                    return
                fe_b, fe_c = st.columns(2, gap="small")
                with fe_b:
                    fr = st.date_input("First repayment date", default_fr, key=f"reamod_fr_{leg}")
                with fe_c:
                    rt = st.selectbox(
                        "Timing of payment",
                        ["Anniversary Date", "Last Day Of Month"],
                        key=f"reamod_rt_{leg}",
                    )
                first_rep = datetime.combine(fr, datetime.min.time())
                use_ann = rt.startswith("Anniversary")
                if not use_ann and not is_last_day_of_month(first_rep):
                    st.error("For last-day-of-month timing, first repayment must be the last day of that month.")
                    return
                interest_pct_month = pct_to_monthly(interest_quoted_leg, prb)
                if interest_pct_month is None:
                    return
                base_rate = float(interest_pct_month) / 100.0
                admin_for_match = float(_f_eff * 100.0) if _has_top else 0.0
                matched = next(
                    (
                        s
                        for s in schemes
                        if abs(float(s.get("interest_rate_pct", 0.0)) - float(interest_pct_month)) < 1e-6
                        and abs(float(s.get("admin_fee_pct", 0.0)) - admin_for_match) < 1e-6
                    ),
                    None,
                )
                scheme = str(matched["name"]) if matched and matched.get("name") else "Other"
                if _has_top:
                    details, df_s = compute_consumer_schedule(
                        net_total_leg,
                        _tm,
                        disbursement_date,
                        base_rate,
                        float(_f_eff),
                        False,
                        "Per month",
                        flat_leg,
                        scheme=scheme,
                        first_repayment_date=first_rep,
                        use_anniversary=use_ann,
                    )
                    details["admin_fee"] = float(top_fee_consumer)
                else:
                    details, df_s = compute_consumer_schedule(
                        combined_gross,
                        _tm,
                        disbursement_date,
                        base_rate,
                        0.0,
                        True,
                        "Per month",
                        flat_leg,
                        scheme=scheme,
                        first_repayment_date=first_rep,
                        use_anniversary=use_ann,
                    )
                details["penalty_rate_pct"] = float(penalty_pm_central)
                pq = cfg.get("penalty_interest_quotation")
                if not pq:
                    st.error(f"Product `{product_code}` must set penalty_interest_quotation.")
                    return
                details["penalty_quotation"] = pq
            elif lt_display == "Term Loan":
                dr = (cfg.get("default_rates") or {}).get("term_loan") or {}
                for k in ("interest_pct", "drawdown_pct", "arrangement_pct"):
                    if dr.get(k) is None:
                        st.error(f"Product missing default_rates.term_loan.{k}.")
                        return
                if _has_top:
                    draw_leg, arr_leg = float(_draw_eff), float(_arr_eff)
                else:
                    draw_leg, arr_leg = 0.0, 0.0
                _tg, _tfr, _trt = st.columns(3, gap="small")
                with _tg:
                    grace = st.selectbox(
                        "Grace period",
                        ["No Grace Period", "Principal Moratorium", "Principal And Interest Moratorium"],
                        key=f"reamod_grace_{leg}",
                    )
                with _tfr:
                    fr = st.date_input("First repayment date", default_fr, key=f"reamod_fr_{leg}")
                with _trt:
                    rt = st.selectbox(
                        "Timing of payment",
                        ["Anniversary Date", "Last Day Of Month"],
                        key=f"reamod_rt_{leg}",
                    )
                mor = 0
                if grace == "Principal Moratorium":
                    mor = st.number_input("Moratorium (months)", 1, 120, 3, key=f"reamod_mor_p_{leg}")
                elif grace == "Principal And Interest Moratorium":
                    mor = st.number_input("Moratorium (months)", 1, 120, 3, key=f"reamod_mor_pi_{leg}")
                first_rep = datetime.combine(fr, datetime.min.time())
                use_ann = rt.startswith("Anniversary")
                if not use_ann and not is_last_day_of_month(first_rep):
                    st.error("For last-day-of-month timing, first repayment must be the last day of that month.")
                    return
                if _has_top:
                    details, df_s = compute_term_schedule(
                        net_total_leg,
                        _tm,
                        disbursement_date,
                        float(interest_quoted_leg),
                        draw_leg,
                        arr_leg,
                        False,
                        grace,
                        mor,
                        first_rep,
                        use_ann,
                        prb,
                        flat_leg,
                    )
                    details["drawdown_fee"] = float(top_fee_draw)
                    details["arrangement_fee"] = float(top_fee_arr)
                else:
                    details, df_s = compute_term_schedule(
                        combined_gross,
                        _tm,
                        disbursement_date,
                        float(interest_quoted_leg),
                        0.0,
                        0.0,
                        True,
                        grace,
                        mor,
                        first_rep,
                        use_ann,
                        prb,
                        flat_leg,
                    )
                details["penalty_rate_pct"] = float(penalty_pm_central)
                pq = cfg.get("penalty_interest_quotation")
                if not pq:
                    st.error(f"Product `{product_code}` must set penalty_interest_quotation.")
                    return
                details["penalty_quotation"] = pq
            elif lt_display == "Bullet Loan":
                dr = (cfg.get("default_rates") or {}).get("bullet_loan") or {}
                for k in ("interest_pct", "drawdown_pct", "arrangement_pct"):
                    if dr.get(k) is None:
                        st.error(f"Product missing default_rates.bullet_loan.{k}.")
                        return
                if _has_top:
                    draw_leg, arr_leg = float(_draw_eff), float(_arr_eff)
                else:
                    draw_leg, arr_leg = 0.0, 0.0
                _bb1, _bb2, _bb3 = st.columns([1.5, 1.0, 1.0], gap="small")
                with _bb1:
                    _reamod_field_caption("Bullet type", align="left")
                    btype = st.selectbox(
                        "Bullet type",
                        ["Straight Bullet (No Interim Payments)", "Bullet With Interest Payments"],
                        key=f"reamod_bt_{leg}",
                        label_visibility="collapsed",
                    )
                with _bb2:
                    _reamod_field_caption("First repayment date", align="center")
                    _bc2l, _bc2m, _bc2r = st.columns([0.12, 1.0, 0.12])
                    with _bc2m:
                        fr = st.date_input(
                            "First repayment date",
                            value=default_fr,
                            key=f"reamod_fr_{leg}",
                            label_visibility="collapsed",
                        )
                with _bb3:
                    _reamod_field_caption("Timing of payment", align="center")
                    _bc3l, _bc3m, _bc3r = st.columns([0.12, 1.0, 0.12])
                    with _bc3m:
                        rt = st.selectbox(
                            "Timing of payment",
                            ["Anniversary Date", "Last Day Of Month"],
                            key=f"reamod_rt_{leg}",
                            label_visibility="collapsed",
                        )
                first_rep = datetime.combine(fr, datetime.min.time())
                use_ann = rt.startswith("Anniversary")
                if not use_ann and not is_last_day_of_month(first_rep):
                    st.error("For last-day-of-month timing, first repayment must be the last day of that month.")
                    return
                first_b = None
                if "With Interest" in btype:
                    first_b = first_rep
                if _has_top:
                    details, df_s = compute_bullet_schedule(
                        net_total_leg,
                        _tm,
                        disbursement_date,
                        float(interest_quoted_leg),
                        draw_leg,
                        arr_leg,
                        False,
                        btype,
                        first_b,
                        use_ann,
                        prb,
                        flat_leg,
                    )
                    details["drawdown_fee"] = float(top_fee_draw)
                    details["arrangement_fee"] = float(top_fee_arr)
                else:
                    details, df_s = compute_bullet_schedule(
                        combined_gross,
                        _tm,
                        disbursement_date,
                        float(interest_quoted_leg),
                        0.0,
                        0.0,
                        True,
                        btype,
                        first_b,
                        use_ann,
                        prb,
                        flat_leg,
                    )
                details["penalty_rate_pct"] = float(penalty_pm_central)
                pq = cfg.get("penalty_interest_quotation")
                if not pq:
                    st.error(f"Product `{product_code}` must set penalty_interest_quotation.")
                    return
                details["penalty_quotation"] = pq
            else:
                if (
                    money_df_column_config is None
                    or first_repayment_from_customised_table is None
                    or schedule_editor_disabled_amounts is None
                ):
                    st.warning("Customised repayments need table helpers from the host app.")
                    return
                dr = (cfg.get("default_rates") or {}).get("customised_repayments") or {}
                for k in ("interest_pct", "drawdown_pct", "arrangement_pct"):
                    if dr.get(k) is None:
                        st.error(f"Product missing default_rates.customised_repayments.{k}.")
                        return
                pq = cfg.get("penalty_interest_quotation")
                if not pq:
                    st.error(f"Product `{product_code}` must set penalty_interest_quotation.")
                    return
                if _has_top:
                    draw_leg = float(_draw_eff)
                    arr_leg = float(_arr_eff)
                    total_fee_leg = draw_leg + arr_leg
                    if total_fee_leg >= 1.0:
                        st.error("Drawdown + arrangement must be below 100%.")
                        return
                    implied_net = float(as_10dp(combined_gross * (1.0 - total_fee_leg)))
                    if not amounts_equal_at_2dp(as_10dp(implied_net), as_10dp(net_total_leg)):
                        st.error(
                            "Top-up drawdown/arrangement fees are inconsistent with this leg’s net and gross (2dp): "
                            f"implied net **{implied_net:,.2f}** vs leg net **{net_total_leg:,.2f}**."
                        )
                        return
                else:
                    draw_leg = 0.0
                    arr_leg = 0.0
                    total_fee_leg = 0.0
                    exp_net = float(as_10dp(combined_gross * (1.0 - _fp)))
                    if not amounts_equal_at_2dp(as_10dp(net_total_leg), as_10dp(exp_net)):
                        st.error(
                            "This leg’s net proceeds must equal **restructure gross × (1 − restructure fee %)** (2dp): "
                            f"expected **{exp_net:,.2f}**, got **{net_total_leg:,.2f}**."
                        )
                        return
                _cust1, _cust2, _cust3 = st.columns([1.05, 0.95, 0.95], gap="small")
                with _cust1:
                    _reamod_field_caption("Schedule shape", align="left")
                    _shape = st.selectbox(
                        "Schedule shape",
                        ["Regular (Fixed Dates)", "Irregular (Editable Dates)"],
                        key=f"reamod_cust_shape_{leg}",
                        label_visibility="collapsed",
                    )
                irregular = _shape.startswith("Irregular")
                with _cust2:
                    _reamod_field_caption("First repayment date", align="center")
                    _cfr_l, _cfr_m, _cfr_r = st.columns([0.12, 1.0, 0.12])
                    with _cfr_m:
                        fr = st.date_input(
                            "First repayment date",
                            value=default_fr,
                            key=f"reamod_fr_{leg}",
                            label_visibility="collapsed",
                        )
                with _cust3:
                    _reamod_field_caption("Timing of payment", align="center")
                    _crt_l, _crt_m, _crt_r = st.columns([0.12, 1.0, 0.12])
                    with _crt_m:
                        rt = st.selectbox(
                            "Timing of payment",
                            ["Anniversary Date", "Last Day Of Month"],
                            key=f"reamod_rt_{leg}",
                            label_visibility="collapsed",
                        )
                first_rep = datetime.combine(fr, datetime.min.time())
                use_ann = rt.startswith("Anniversary")
                if not use_ann and not is_last_day_of_month(first_rep):
                    st.error("For last-day-of-month timing, first repayment must be the last day of that month.")
                    return
                total_facility_tbl = float(as_10dp(combined_gross))
                annual_rate = (
                    (float(interest_quoted_leg) / 100.0) * 12.0
                    if prb == "Per month"
                    else (float(interest_quoted_leg) / 100.0)
                )
                cap_key = f"reamod_cust_df_{leg}"
                cap_params = (
                    round(total_facility_tbl, 2),
                    _tm,
                    disbursement_date.strftime("%Y-%m-%d"),
                    irregular,
                    round(draw_leg, 6),
                    round(arr_leg, 6),
                    round(float(interest_quoted_leg), 6),
                )
                params_key = f"reamod_cust_params_{leg}"
                if cap_key not in st.session_state or st.session_state.get(params_key) != cap_params:
                    st.session_state[params_key] = cap_params
                    schedule_dates_init = repayment_dates(
                        disbursement_date, first_rep, _tm, use_anniversary=use_ann
                    )
                    rows = [
                        {
                            "Period": 0,
                            "Date": disbursement_date.strftime("%d-%b-%Y"),
                            "Payment": 0.0,
                            "Interest": 0.0,
                            "Principal": 0.0,
                            "Principal Balance": round(total_facility_tbl, 2),
                            "Total Outstanding": round(total_facility_tbl, 2),
                        }
                    ]
                    for i, dt in enumerate(schedule_dates_init, 1):
                        rows.append(
                            {
                                "Period": i,
                                "Date": dt.strftime("%d-%b-%Y"),
                                "Payment": 0.0,
                                "Interest": 0.0,
                                "Principal": 0.0,
                                "Principal Balance": 0.0,
                                "Total Outstanding": 0.0,
                            }
                        )
                    st.session_state[cap_key] = pd.DataFrame(rows)
                df_cap = st.session_state[cap_key].copy()
                schedule_dates_c = parse_schedule_dates_from_table(df_cap, start_date=disbursement_date)
                df_cap = recompute_customised_from_payments(
                    df_cap,
                    total_facility_tbl,
                    schedule_dates_c,
                    annual_rate,
                    flat_leg,
                    disbursement_date,
                )
                st.session_state[cap_key] = df_cap
                date_editable = irregular
                if irregular:
                    if st.button("Add row", type="secondary", key=f"reamod_cust_add_{leg}"):
                        last_df = st.session_state[cap_key]
                        if len(last_df) > 0:
                            try:
                                last_date_str = str(last_df.at[len(last_df) - 1, "Date"]).strip()[:32]
                                last_dt = datetime.combine(
                                    datetime.strptime(last_date_str, "%d-%b-%Y").date(),
                                    datetime.min.time(),
                                )
                            except (ValueError, TypeError):
                                last_dt = add_months(disbursement_date, len(last_df))
                            next_dt = add_months(last_dt, 1)
                            if not use_ann:
                                next_dt = next_dt.replace(day=days_in_month(next_dt.year, next_dt.month))
                            new_row = {
                                "Period": len(last_df),
                                "Date": next_dt.strftime("%d-%b-%Y"),
                                "Payment": 0.0,
                                "Interest": 0.0,
                                "Principal": 0.0,
                                "Principal Balance": 0.0,
                                "Total Outstanding": 0.0,
                            }
                            st.session_state[cap_key] = pd.concat(
                                [last_df, pd.DataFrame([new_row])], ignore_index=True
                            )
                            st.rerun()
                edited = st.data_editor(
                    df_cap,
                    column_config=money_df_column_config(
                        df_cap,
                        overrides={
                            "Period": {
                                **st.column_config.NumberColumn(disabled=True),
                                "alignment": "left",
                            },
                            "Date": {
                                **st.column_config.TextColumn(disabled=not date_editable),
                                "alignment": "left",
                            },
                        },
                        column_disabled=schedule_editor_disabled_amounts,
                        money_column_alignment="right",
                    ),
                    width="stretch",
                    hide_index=True,
                    key=f"reamod_cust_ed_{leg}",
                )
                if not edited.equals(df_cap):
                    schedule_dates_edit = parse_schedule_dates_from_table(edited, start_date=disbursement_date)
                    df_cap = recompute_customised_from_payments(
                        edited,
                        total_facility_tbl,
                        schedule_dates_edit,
                        annual_rate,
                        flat_leg,
                        disbursement_date,
                    )
                    st.session_state[cap_key] = df_cap
                    st.rerun()
                schedule_dates_final = parse_schedule_dates_from_table(df_cap, start_date=disbursement_date)
                first_rep_for_save = first_repayment_from_customised_table(df_cap) or first_rep
                end_date_from_table = schedule_dates_final[-1] if schedule_dates_final else disbursement_date
                final_to = float(df_cap.at[len(df_cap) - 1, "Total Outstanding"]) if len(df_cap) > 1 else total_facility_tbl
                if abs(final_to) >= 0.01:
                    st.info("Reduce **Total Outstanding** on the last row to near zero before saving this schedule.")
                    return
                scfg = get_system_config()
                lt_db_leg = loan_type_db(lt_display)
                currency = (scfg.get("loan_default_currencies") or {}).get(
                    lt_db_leg, scfg.get("base_currency", "USD")
                )
                details = {
                    "principal": total_facility_tbl,
                    "disbursed_amount": float(as_10dp(net_total_leg)),
                    "term": _tm,
                    "annual_rate": annual_rate,
                    "drawdown_fee": draw_leg,
                    "arrangement_fee": arr_leg,
                    "disbursement_date": disbursement_date,
                    "end_date": end_date_from_table,
                    "first_repayment_date": first_rep_for_save,
                    "payment_timing": "anniversary" if use_ann else "last_day_of_month",
                    "penalty_rate_pct": float(penalty_pm_central),
                    "penalty_quotation": pq,
                    "currency": currency,
                }
                if _has_top:
                    details["drawdown_fee"] = float(top_fee_draw)
                    details["arrangement_fee"] = float(top_fee_arr)
                df_s = df_cap

            details["principal"] = float(as_10dp(combined_gross))
            details["disbursed_amount"] = float(as_10dp(net_total_leg))
            scfg = get_system_config()
            lt_db_leg = loan_type_db(lt_display)
            details["currency"] = (scfg.get("loan_default_currencies") or {}).get(
                lt_db_leg, scfg.get("base_currency", "USD")
            )
            details["product_code"] = product_code
            if cash_gl_id:
                details["cash_gl_account_id"] = cash_gl_id

            _inject_reamod_use_schedule_button_css_once()
            # Customised: editable st.data_editor above is the schedule; others use same Streamlit grid read-only.
            if lt_display != "Customised Repayments":
                if money_df_column_config is not None:
                    st.dataframe(
                        df_s,
                        column_config=schedule_readonly_dataframe_column_config(
                            df_s,
                            money_df_column_config=money_df_column_config,
                        ),
                        hide_index=True,
                        width="stretch",
                    )
                else:
                    st.dataframe(df_s, hide_index=True, width="stretch")

            def _persist_schedule_to_session() -> None:
                st.session_state[session_details_key] = details
                st.session_state[session_df_key] = df_s
                st.success(f"Schedule {leg} saved in session.")
                st.rerun()

            if st.button("Use this schedule", type="primary", key=use_key):
                _persist_schedule_to_session()
        except Exception as ex:
            st.error(str(ex))

    n_sched = n_legs if is_split else 1
    st.divider()
    for si in range(n_sched):
        letter = _leg_letter(si)
        lt_d = split_lt_display[si]
        pc_leg = split_product_codes[si]
        cfg_leg = get_product_config_from_db(pc_leg) or {}
        prb_leg = get_product_rate_basis(cfg_leg)
        sk_det, sk_df = _leg_session_keys(si)
        use_k = f"reamod_use_{letter}"

        if is_split and si == 0 and split_alloc_writeoff > 1e-9:
            # Not a split "leg": this slice is written off at approval (no schedule). Per-leg
            # restructure fee % only applies to funded legs; write-off amount is informational.
            _reamod_field_caption("Unallocated facility (write-off at approval)", align="left")
            st.markdown(
                _two_col_label_amount_table_html(
                    label="Write-off",
                    amount=float(split_alloc_writeoff),
                ),
                unsafe_allow_html=True,
            )

        if is_split:
            render_sub_sub_header(f"Terms & schedule - Loan {letter}")
            _rl_int = (
                "Interest rate (% per annum)" if prb_leg == "Per annum" else "Interest rate (% per month)"
            )
            _rl_pen = (
                "Penalty rate (% per annum)" if prb_leg == "Per annum" else "Penalty rate (% per month)"
            )
            _di = _default_central_interest_pct(cfg_leg, lt_d)
            _dp = _default_central_penalty_pct(cfg_leg, lt_d)
            _tc1, _tc2, _tc3, _tc4 = st.columns(4, gap="small")
            with _tc1:
                _fpv = st.number_input(
                    "Restructure fee (%) on restructured outstanding",
                    min_value=0.0,
                    max_value=99.99,
                    value=0.0,
                    step=0.05,
                    key=f"reamod_fee_pct_{letter}",
                )
                fee_pct_leg = float(_fpv) / 100.0
            with _tc2:
                ci_leg = st.number_input(
                    _rl_int,
                    min_value=0.0,
                    max_value=100.0,
                    value=float(_di),
                    step=0.05,
                    key=f"reamod_central_interest_pct_{letter}",
                )
            with _tc3:
                cp_leg = st.number_input(
                    _rl_pen,
                    min_value=0.0,
                    max_value=100.0,
                    value=float(_dp),
                    step=0.05,
                    key=f"reamod_central_penalty_pct_{letter}",
                )
            with _tc4:
                tm_leg = st.number_input("Term (months)", 1, 360, 12, key=f"reamod_term_{letter}")
            if fee_pct_leg >= 1.0:
                st.error("Restructure fee must be below 100%.")
                st.stop()
            term_months_leg = int(tm_leg)
            central_interest_pct_leg = float(ci_leg)
            central_penalty_pct_leg = float(cp_leg)
        else:
            render_sub_sub_header("Schedule")
            fee_pct_leg = float(fee_pct)
            term_months_leg = int(term_m)
            central_interest_pct_leg = float(central_interest_pct)
            central_penalty_pct_leg = float(central_penalty_pct)

        _gen_one_leg(
            leg=letter,
            lt_display=lt_d,
            product_code=pc_leg,
            cfg=cfg_leg,
            carry_leg=carry_legs[si],
            top_leg=top_alloc[si],
            net_total_leg=net_legs[si],
            top_fee_on_leg=top_fee_combined if top_alloc[si] > 1e-12 else 0.0,
            fee_pct_leg=fee_pct_leg,
            term_months_leg=term_months_leg,
            central_interest_pct_leg=central_interest_pct_leg,
            central_penalty_pct_leg=central_penalty_pct_leg,
            schedule_gross_override=principal_schedule_single if not is_split else None,
            use_key=use_k,
            session_details_key=sk_det,
            session_df_key=sk_df,
        )

    if is_split:
        fee_pct = float(st.session_state.get("reamod_fee_pct_A", 0.0) or 0.0) / 100.0

    st.divider()
    pol_a = _reamod_collateral_link_row(
        key_prefix="reamod",
        heading="Collateral — Loan A" if is_split else "Collateral",
    )
    _reamod_collateral_clear_confirm_block(key_prefix="reamod")
    pol_b = "Keep existing"
    if is_split:
        pol_b = _reamod_collateral_link_row(
            key_prefix="reamod_b",
            heading="Collateral — Loan B",
        )
        _reamod_collateral_clear_confirm_block(key_prefix="reamod_b")

    _flash_pref = st.session_state.pop("reamod_coll_clear_flash", None)
    if _flash_pref == "reamod":
        st.success("Collateral for **loan A** is set to **clear** when you save or submit.")
    elif _flash_pref == "reamod_b":
        st.success("Collateral for **loan B** is set to **clear** when you save or submit.")

    patch_a: dict[str, Any] = {}
    patch_b: dict[str, Any] = {}
    _docs_open = bool(st.session_state.get("reamod_docs_open", False))
    if _docs_open:
        with st.expander("Documents", expanded=True):
            if pol_a == "Replace":
                patch_a = _reamod_collateral_replace_patch(
                    key_prefix="reamod",
                    provisions_config_ok=provisions_config_ok,
                    list_provision_security_subtypes=list_provision_security_subtypes,
                    loan=loan,
                )
            else:
                patch_a = _reamod_collateral_patch_dict_only(pol_a)
            if is_split:
                if pol_b == "Replace":
                    patch_b = _reamod_collateral_replace_patch(
                        key_prefix="reamod_b",
                        provisions_config_ok=provisions_config_ok,
                        list_provision_security_subtypes=list_provision_security_subtypes,
                        loan=loan,
                    )
                else:
                    patch_b = _reamod_collateral_patch_dict_only(pol_b)
            _render_remod_documents_staging(
                documents_available=documents_available,
                list_document_categories=list_document_categories,
            )
    else:
        patch_a = _reamod_collateral_patch_dict_only(pol_a)
        if is_split:
            patch_b = _reamod_collateral_patch_dict_only(pol_b)

    st.divider()
    _inject_reamod_save_clear_green_css_once()
    c_save1, c_save2, c_save3 = st.columns(3, gap="medium")
    with c_save1:
        if st.button("Save and Continue Later", type="primary", key="reamod_save_staged"):
            try:
                snap = bucket_snapshot_for_json(bal)
                base_details: dict[str, Any] = {
                    "approval_action": "LOAN_MODIFICATION_SPLIT" if is_split else "LOAN_MODIFICATION",
                    "source_loan_id": int(loan_id),
                    "restructure_date": restructure_date.isoformat(),
                    "as_of_balance_date": as_of.isoformat(),
                    "bucket_snapshot": snap,
                    "excess_policy": excess_policy,
                    "carry_amount": str(as_10dp(carry)),
                    "topup_amount": str(as_10dp(top_up)),
                    "total_facility": str(as_10dp(total_facility)),
                    "outstanding_snapshot": str(as_10dp(outstanding)),
                    "writeoff_amount": str(
                        as_10dp(writeoff_amount if excess_policy in ("WRITE_OFF", "SPLIT") else 0)
                    ),
                    "outstanding_interest_treatment": oit,
                    "modification_notes": notes.strip(),
                    "fee_and_proceeds": {
                        "restructure_fee_pct": str(as_10dp(fee_pct * 100 if fee_pct else 0)),
                        "schedule_principal_gross": str(as_10dp(principal_schedule_single))
                        if not is_split
                        else None,
                        "net_proceeds_by_leg": [str(as_10dp(x)) for x in net_legs],
                        "topup_fee_consumer_pct": str(as_10dp(top_fee_consumer * 100))
                        if top_up > 1e-9 and lt_display_a == "Consumer Loan"
                        else None,
                        "topup_fee_drawdown_pct": str(as_10dp(top_fee_draw * 100))
                        if top_up > 1e-9
                        and lt_display_a in ("Term Loan", "Bullet Loan", "Customised Repayments")
                        else None,
                        "topup_fee_arrangement_pct": str(as_10dp(top_fee_arr * 100))
                        if top_up > 1e-9
                        and lt_display_a in ("Term Loan", "Bullet Loan", "Customised Repayments")
                        else None,
                    },
                }
                df_a = st.session_state.get("reamod_df_a") or pd.DataFrame()
                df_b = st.session_state.get("reamod_df_b") if is_split else None
                det_a = dict(st.session_state.get("reamod_details_a") or {})
                det_a.update(patch_a)
                if cash_gl_id:
                    det_a.setdefault("cash_gl_account_id", cash_gl_id)
                base_details["modification_loan_details"] = det_a
                if is_split:
                    det_list: list[dict[str, Any]] = []
                    for si in range(n_legs):
                        skd, _ = _leg_session_keys(si)
                        di = dict(st.session_state.get(skd) or {})
                        if si == 0:
                            di.update(patch_a)
                        elif si == 1:
                            di.update(patch_b)
                        if cash_gl_id:
                            di.setdefault("cash_gl_account_id", cash_gl_id)
                        det_list.append(di)
                    extra_scheds: list[Any] = []
                    for si in range(2, n_legs):
                        _, skf = _leg_session_keys(si)
                        dfx = st.session_state.get(skf)
                        extra_scheds.append(
                            dfx.to_dict(orient="records")
                            if dfx is not None and isinstance(dfx, pd.DataFrame) and not dfx.empty
                            else []
                        )
                    base_details["split_leg_count"] = int(n_legs)
                    base_details["split_product_codes"] = list(split_product_codes)
                    base_details["split_loan_types"] = list(split_lt_db)
                    base_details["split_loan_details_list"] = det_list
                    base_details["split_schedules_extra"] = extra_scheds
                    base_details["split_net_by_leg"] = [str(as_10dp(x)) for x in net_legs]
                    base_details["split_carry_by_leg"] = [str(as_10dp(x)) for x in carry_legs]
                    base_details["split"] = {
                        "principal_a": str(as_10dp(pa)),
                        "principal_b": str(as_10dp(pb)),
                    }
                    base_details["split_product_code_b"] = split_product_code_b
                    base_details["split_loan_details_a"] = det_list[0]
                    base_details["split_loan_details_b"] = det_list[1]
                    base_details["split_loan_type_b"] = lt_db_b
                staged_id = st.session_state.get("reamod_staged_draft_id")
                if staged_id:
                    update_loan_approval_draft_staged(
                        int(staged_id),
                        int(cust_id),
                        lt_db_a,
                        base_details,
                        df_a if not df_a.empty else pd.DataFrame(),
                        product_code=product_code_a,
                        schedule_df_secondary=df_b if is_split and df_b is not None else None,
                    )
                    flash = f"Updated staged draft #{staged_id}."
                else:
                    new_id = save_loan_approval_draft(
                        int(cust_id),
                        lt_db_a,
                        base_details,
                        df_a if not df_a.empty else pd.DataFrame(),
                        product_code=product_code_a,
                        created_by=created_by,
                        status="STAGED",
                        loan_id=int(loan_id),
                        schedule_df_secondary=df_b if is_split and df_b is not None else None,
                    )
                    st.session_state["reamod_staged_draft_id"] = int(new_id)
                    flash = f"Created staged draft #{new_id}."
                st.success(flash)
                st.rerun()
            except Exception as ex:
                st.error(str(ex))
    with c_save2:
        if st.button("Clear session schedules", type="primary", key="reamod_clear_sess"):
            for k in (
                "reamod_details_a",
                "reamod_df_a",
                "reamod_details_b",
                "reamod_df_b",
                "reamod_staged_draft_id",
            ):
                st.session_state.pop(k, None)
            for letter in ("c", "d", "e"):
                st.session_state.pop(f"reamod_details_{letter}", None)
                st.session_state.pop(f"reamod_df_{letter}", None)
            for letter in ("A", "B", "C", "D", "E"):
                st.session_state.pop(f"reamod_cust_df_{letter}", None)
                st.session_state.pop(f"reamod_cust_params_{letter}", None)
            st.session_state.pop("reamod_docs_staged", None)
            st.session_state.pop("reamod_docs_open", None)
            st.session_state.pop("reamod_coll_clear_pending", None)
            st.session_state.pop("reamod_b_coll_clear_pending", None)
            st.session_state.pop("reamod_coll_clear_flash", None)
            st.session_state.pop("reamod_carry_display", None)
            st.session_state.pop("reamod_carry_bound_loan_id", None)
            st.session_state.pop("reamod_topup_display", None)
            st.session_state.pop("reamod_topup_bound_loan_id", None)
            st.rerun()

    with c_save3:
        submitted = st.button("Submit for approval", type="primary", key="reamod_submit")
    if submitted:
        df_a = st.session_state.get("reamod_df_a")
        if df_a is None or (isinstance(df_a, pd.DataFrame) and df_a.empty):
            st.error("Generate a schedule and click **Use this schedule** for the retained / A leg.")
            st.stop()
        if is_split:
            for si in range(n_legs):
                _, skf = _leg_session_keys(si)
                df_leg = st.session_state.get(skf)
                if df_leg is None or (isinstance(df_leg, pd.DataFrame) and df_leg.empty):
                    st.error(
                        f"Split workflow requires schedule {_leg_letter(si)} — use **Use this schedule** on that leg."
                    )
                    st.stop()
        try:
            snap = bucket_snapshot_for_json(bal)
            det_a = dict(st.session_state.get("reamod_details_a") or {})
            det_a.update(patch_a)
            if cash_gl_id:
                det_a.setdefault("cash_gl_account_id", cash_gl_id)
            details: dict[str, Any] = {
                "approval_action": "LOAN_MODIFICATION_SPLIT" if is_split else "LOAN_MODIFICATION",
                "source_loan_id": int(loan_id),
                "restructure_date": restructure_date.isoformat(),
                "as_of_balance_date": as_of.isoformat(),
                "bucket_snapshot": snap,
                "excess_policy": excess_policy,
                "carry_amount": str(as_10dp(carry)),
                "topup_amount": str(as_10dp(top_up)),
                "total_facility": str(as_10dp(total_facility)),
                "outstanding_snapshot": str(as_10dp(outstanding)),
                "writeoff_amount": str(
                    as_10dp(writeoff_amount if excess_policy in ("WRITE_OFF", "SPLIT") else 0)
                ),
                "outstanding_interest_treatment": oit,
                "modification_notes": notes.strip(),
                "modification_loan_details": det_a,
                "fee_and_proceeds": {
                    "restructure_fee_pct": str(as_10dp(fee_pct * 100 if fee_pct else 0)),
                    "schedule_principal_gross": str(as_10dp(principal_schedule_single))
                    if not is_split
                    else None,
                    "net_proceeds_by_leg": [str(as_10dp(x)) for x in net_legs],
                    "topup_fee_consumer_pct": str(as_10dp(top_fee_consumer * 100))
                    if top_up > 1e-9 and lt_display_a == "Consumer Loan"
                    else None,
                    "topup_fee_drawdown_pct": str(as_10dp(top_fee_draw * 100))
                    if top_up > 1e-9
                    and lt_display_a in ("Term Loan", "Bullet Loan", "Customised Repayments")
                    else None,
                    "topup_fee_arrangement_pct": str(as_10dp(top_fee_arr * 100))
                    if top_up > 1e-9
                    and lt_display_a in ("Term Loan", "Bullet Loan", "Customised Repayments")
                    else None,
                },
            }
            if is_split:
                det_list_submit: list[dict[str, Any]] = []
                for si in range(n_legs):
                    skd, _ = _leg_session_keys(si)
                    di = dict(st.session_state.get(skd) or {})
                    if si == 0:
                        di.update(patch_a)
                    elif si == 1:
                        di.update(patch_b)
                    if cash_gl_id:
                        di.setdefault("cash_gl_account_id", cash_gl_id)
                    det_list_submit.append(di)
                extra_scheds_s: list[Any] = []
                for si in range(2, n_legs):
                    _, skf = _leg_session_keys(si)
                    dfx = st.session_state.get(skf)
                    extra_scheds_s.append(
                        dfx.to_dict(orient="records")
                        if dfx is not None and isinstance(dfx, pd.DataFrame) and not dfx.empty
                        else []
                    )
                details["split_leg_count"] = int(n_legs)
                details["split_product_codes"] = list(split_product_codes)
                details["split_loan_types"] = list(split_lt_db)
                details["split_loan_details_list"] = det_list_submit
                details["split_schedules_extra"] = extra_scheds_s
                details["split_net_by_leg"] = [str(as_10dp(x)) for x in net_legs]
                details["split_carry_by_leg"] = [str(as_10dp(x)) for x in carry_legs]
                details["split"] = {"principal_a": str(as_10dp(pa)), "principal_b": str(as_10dp(pb))}
                details["split_loan_details_a"] = det_list_submit[0]
                details["split_loan_details_b"] = det_list_submit[1]
                details["split_loan_type_b"] = lt_db_b
                details["split_product_code_b"] = split_product_code_b
            staged_id = st.session_state.get("reamod_staged_draft_id")
            df_a_f = st.session_state.get("reamod_df_a")
            df_b_f = st.session_state.get("reamod_df_b") if is_split else None

            def _do_submit() -> int:
                if staged_id:
                    return resubmit_loan_approval_draft(
                        int(staged_id),
                        int(cust_id),
                        lt_db_a,
                        details,
                        df_a_f,
                        product_code=product_code_a,
                        created_by=created_by,
                        schedule_df_secondary=df_b_f if is_split else None,
                    )
                return save_loan_approval_draft(
                    int(cust_id),
                    lt_db_a,
                    details,
                    df_a_f,
                    product_code=product_code_a,
                    created_by=created_by,
                    status="PENDING",
                    loan_id=int(loan_id),
                    schedule_df_secondary=df_b_f if is_split else None,
                )

            new_draft_id = run_with_spinner("Submitting draft…", _do_submit)
            staged_docs = list(st.session_state.get("reamod_docs_staged") or [])
            dc, errs = _attach_staged_docs(int(new_draft_id), staged_docs, upload_document)
            for e in errs:
                st.warning(e)
            st.success(f"Draft #{new_draft_id} submitted ({dc} document(s)). Open **Approve loans**.")
            st.session_state.pop("reamod_staged_draft_id", None)
            st.session_state.pop("reamod_docs_staged", None)
            st.session_state.pop("reamod_details_a", None)
            st.session_state.pop("reamod_df_a", None)
            st.session_state.pop("reamod_details_b", None)
            st.session_state.pop("reamod_df_b", None)
            for letter in ("c", "d", "e"):
                st.session_state.pop(f"reamod_details_{letter}", None)
                st.session_state.pop(f"reamod_df_{letter}", None)
            for letter in ("A", "B", "C", "D", "E"):
                st.session_state.pop(f"reamod_cust_df_{letter}", None)
                st.session_state.pop(f"reamod_cust_params_{letter}", None)
            st.rerun()
        except Exception as ex:
            st.error(str(ex))
