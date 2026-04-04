"""Loan modification tab: approval-gated workflow (balances, amounts, schedules, draft, documents)."""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any, Callable

import pandas as pd
import streamlit as st

from decimal_utils import as_10dp, amounts_equal_at_2dp
from loans import add_months, is_last_day_of_month
from services.modification_capture_bridge import (
    EOD_SUMMARY_BUCKET_ROWS,
    as_of_balance_date,
    bucket_snapshot_for_json,
    loan_type_db,
    loan_type_display,
)
from style import render_sub_sub_header
from ui.streamlit_feedback import run_with_spinner

def _compact_readonly_amount(label: str, value_str: str, *, help_text: str | None = None) -> None:
    """Body-sized read-only figure (avoids st.metric’s oversized value typography)."""
    lab = html.escape(label)
    val = html.escape(value_str)
    title = f' title="{html.escape(help_text, quote=True)}"' if help_text else ""
    st.markdown(
        f'<p style="font-size:1rem;line-height:1.45;margin:0.15rem 0 0 0;color:inherit;"{title}>'
        f'<span style="font-weight:600;">{lab}</span> '
        f'<span style="font-weight:500;">{val}</span></p>',
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


def _collateral_widgets(
    *,
    key_prefix: str,
    provisions_config_ok: bool,
    list_provision_security_subtypes: Callable[[], list[dict]] | None,
    loan: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Returns (policy: keep|replace|clear, patch for modification_loan_details)."""
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
    pol = st.radio(
        "Collateral",
        ["Keep existing", "Replace", "Clear"],
        horizontal=True,
        key=f"{key_prefix}_coll_pol",
    )
    patch: dict[str, Any] = {}
    if pol == "Keep existing":
        return pol, patch
    if pol == "Clear":
        return pol, {"collateral_cleared": True}
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
    patch = {
        "collateral_security_subtype_id": int(sub_id) if sub_id else None,
        "collateral_charge_amount": float(as_10dp(chg)) if chg > 0 else None,
        "collateral_valuation_amount": float(as_10dp(val)) if val > 0 else None,
    }
    return pol, patch


def render_loan_modification_tab(
    *,
    list_customers: Callable[[], list[dict]],
    get_display_name: Callable[[int], str],
    get_system_date: Callable[[], Any],
    get_loan_for_modification: Callable[[int], dict | None],
    format_schedule_df: Callable[[pd.DataFrame], pd.DataFrame],
    schedule_export_downloads: Callable[..., None],
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
) -> None:
    from loan_management.daily_state import get_loan_daily_state_balances

    render_sub_sub_header("Loan modification (approval workflow)")
    st.caption(
        "Balances use **EOD loan daily state** as of **restructure date minus one day**. "
        "Submit sends a draft to **Approve loans**; approver posts write-off / top-up GL and applies the modification."
    )

    customers = list_customers() or []
    if not customers:
        st.info("No customers. Create a customer first.")
        return

    h1, h2, h3 = st.columns([1, 1, 1], gap="small")
    with h1:
        st.caption("Customer")
        cust_labels = [get_display_name(c["id"]) for c in customers]
        cust_sel = st.selectbox("Customer", cust_labels, key="reamod_hdr_cust", label_visibility="collapsed")
        cust_id = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel)
    with h2:
        st.caption("Loan")
        from loan_management import get_loans_by_customer

        loans = get_loans_by_customer(cust_id)
        loans_active = [l for l in loans if l.get("status") == "active"]
        if not loans_active:
            st.info("No active loans.")
            return
        loan_opts = [
            (l["id"], f"#{l['id']} · {l.get('loan_type', '')} · {float(l.get('principal') or 0):,.2f}")
            for l in loans_active
        ]
        labels = [x[1] for x in loan_opts]
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

    info = get_loan_for_modification(loan_id)
    if not info:
        st.warning("Could not load loan.")
        return
    loan = info["loan"]
    last_due = info.get("last_due_date")
    st.caption(f"Schedule v{info['schedule_version']} · Last due: {last_due or '—'}")

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
            render_sub_sub_header(f"EOD breakdown as of {as_of.isoformat()} (restructure date − 1 day)")
            eod_row: dict[str, Any] = {}
            for lab, key in EOD_SUMMARY_BUCKET_ROWS:
                eod_row[lab] = float(bal.get(key) or 0)
            eod_row["Total outstanding (total_exposure)"] = float(bal.get("total_exposure") or 0)
            eod_df = pd.DataFrame([eod_row])
            eod_col_cfg = {
                col: st.column_config.NumberColumn(col, format="%.2f", width="small")
                for col in eod_df.columns
            }
            st.dataframe(
                eod_df,
                hide_index=True,
                width="stretch",
                height=72,
                column_config=eod_col_cfg,
            )

    outstanding = float(as_10dp(bal.get("total_exposure") or 0)) if bal else 0.0

    if not gate_ok or bal is None:
        st.stop()

    st.divider()
    render_sub_sub_header("Restructure amounts")
    a1, a2, a3 = st.columns(3, gap="small")
    with a1:
        carry = st.number_input(
            "Amount to restructure",
            min_value=0.0,
            value=float(as_10dp(outstanding)),
            step=100.0,
            format="%.2f",
            key="reamod_carry",
            help="Becomes **loan A** in **The Split** (restructured principal; top-up is also booked on loan A).",
        )
    with a2:
        top_up = st.number_input(
            "Top-up (additional disbursement)",
            min_value=0.0,
            value=0.0,
            step=100.0,
            format="%.2f",
            key="reamod_topup",
        )
    with a3:
        total_facility = float(as_10dp(carry + top_up))
        _compact_readonly_amount("Total facility", f"{total_facility:,.2f}")

    if carry > outstanding + 1e-6:
        st.error("Amount to restructure cannot exceed total outstanding (total_exposure).")
        st.stop()

    excess_outstanding = float(as_10dp(max(0.0, outstanding - total_facility)))
    has_excess_for_policy = total_facility + 1e-9 < outstanding and not amounts_equal_at_2dp(
        as_10dp(total_facility), as_10dp(outstanding)
    )

    n_legs = 2
    excess_choice: str | None = None
    ex1, ex2, ex3 = st.columns([0.95, 1.05, 1.2], gap="small")
    with ex1:
        _compact_readonly_amount(
            "Excess outstanding",
            f"{excess_outstanding:,.2f}",
            help_text=(
                "EOD total_exposure minus total facility. If positive, pick Excess policy "
                "(and The Split options) in this row."
            ),
        )
    excess_policy = "NONE"
    base_writeoff = 0.0
    with ex2:
        if has_excess_for_policy:
            st.caption("Excess policy")
            excess_choice = st.selectbox(
                "Excess policy",
                ["Write off difference", "The Split"],
                key="reamod_excess",
                label_visibility="collapsed",
                help="**The Split:** max **4** loans (**A–D**). **A** = amount to restructure + top-up.",
            )
            base_writeoff = float(as_10dp(outstanding - total_facility))
            if excess_choice.startswith("Write off"):
                excess_policy = "WRITE_OFF"
            else:
                excess_policy = "SPLIT"
        else:
            st.caption("No excess (2dp).")
            excess_choice = None

    with ex3:
        if has_excess_for_policy and excess_choice is not None and not excess_choice.startswith("Write off"):
            st.caption("Additional loans (B–D)")
            _add_ix = st.selectbox(
                "Additional loans (B–D)",
                [0, 1, 2],
                index=0,
                format_func=lambda j: (
                    "1 additional → 2 loans (A+B)",
                    "2 additional → 3 loans (A–C)",
                    "3 additional → 4 loans (A–D)",
                )[int(j)],
                key="reamod_split_additional",
                label_visibility="collapsed",
                help="Loan **A** is always included; choose how many **extra** loans (**B–D**).",
            )
            n_legs = 2 + int(_add_ix)
        elif has_excess_for_policy:
            st.caption("—")
        else:
            st.empty()

    is_split = excess_policy == "SPLIT"

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
        render_sub_sub_header("The Split")
        st.caption(
            "**Amount to restructure** is **loan A**; top-up is on **A** only (A net must be >= top-up). "
            "**B–D** take the rest of the facility. "
            "If Σ(net) = **total facility**, then Σ(net − top-up on A) must equal **amount to restructure** (2dp). "
            "If Σ(net) < total facility, the shortfall is written off at approval. "
            "**Reconciliation (2dp):** approval **write-off** (excess + any unallocated facility) is the figure that "
            "balances **Σ(nets)** to **EOD total outstanding**; top-up sits in loan **A** net."
        )
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
        if sum_net > total_facility + 1e-6:
            st.error("Sum of net proceeds cannot exceed **Total facility**.")
            st.stop()
        if top_up > 1e-9 and net_legs and float(net_legs[0]) + 1e-9 < float(top_up):
            st.error(
                "Loan **A** net proceeds must be **>= top-up** (top-up is drawn on loan A only)."
            )
            st.stop()
        split_alloc_writeoff = float(as_10dp(max(0.0, total_facility - sum_net)))
        if split_alloc_writeoff > 1e-9:
            _col_fee, _col_wo = st.columns([1.0, 1.05], gap="small")
            with _col_fee:
                st.caption("Restructure fee (restructured outstanding)")
                st.number_input(
                    "Restructure fee (%) on restructured outstanding",
                    min_value=0.0,
                    max_value=99.99,
                    value=0.0,
                    step=0.05,
                    key="reamod_fee_pct",
                    label_visibility="collapsed",
                )
            with _col_wo:
                st.caption("Unallocated facility (write-off at approval)")
                _wo_df = pd.DataFrame(
                    [{"Item": "Write-off", "Amount": float(split_alloc_writeoff)}]
                )
                st.dataframe(
                    _wo_df,
                    hide_index=True,
                    width="content",
                    height=56,
                    column_config={
                        "Item": st.column_config.TextColumn("Item", width="small"),
                        "Amount": st.column_config.NumberColumn("Amount", format="%.2f", width="small"),
                    },
                )

        st.caption("Product per loan")
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
            st.caption("Product")
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
                "**EOD total outstanding − Σ(nets)**. Adjust net proceeds, facility amounts, or excess policy."
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

    st.divider()
    render_sub_sub_header("Terms & schedule (net proceeds + restructure fee %; disbursement = restructure date)")
    _fee_beside_split_wo = is_split and split_alloc_writeoff > 1e-9
    t1, t2, t3, t4 = st.columns(4, gap="small")
    with t1:
        if _fee_beside_split_wo:
            st.caption("Restructure fee — set above next to unallocated table")
            fee_pct = float(st.session_state.get("reamod_fee_pct", 0.0) or 0.0) / 100.0
        else:
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
    top_fee_consumer = float(dr_cons.get("admin_fee_pct") or 0) / 100.0
    top_fee_draw = float(dr_term.get("drawdown_pct") or 0) / 100.0
    top_fee_arr = float(dr_term.get("arrangement_pct") or 0) / 100.0
    if top_up > 1e-9:
        st.caption("Top-up fees (apply to top-up only; restructure fee applies to restructured outstanding)")
        if lt_display_a == "Consumer Loan":
            with t2:
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
        elif lt_display_a in ("Term Loan", "Bullet Loan"):
            dr_use = dr_term if lt_display_a == "Term Loan" else dr_bul
            with t2:
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
            with t3:
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
        else:
            st.info("Top-up fee defaults apply to Consumer / Term / Bullet products.")
    with t4:
        term_m = st.number_input("Term (months)", 1, 360, 12, key="reamod_term")

    if fee_pct >= 1.0:
        st.error("Restructure fee must be below 100%.")
        st.stop()

    top_fee_combined = top_fee_draw + top_fee_arr if lt_display_a in ("Term Loan", "Bullet Loan") else top_fee_consumer

    principal_schedule_single = (
        float(as_10dp(total_facility / (1.0 - fee_pct))) if fee_pct < 1.0 else float(as_10dp(total_facility))
    )

    if not is_split:
        n1, n2, n3, n4 = st.columns(4, gap="small")
        with n1:
            _compact_readonly_amount("Principal (schedule)", f"{principal_schedule_single:,.2f}")
            st.caption("Total facility ÷ (1 − restructure fee). Drives the amortisation schedule.")
        with n4:
            notes = st.text_area("Notes (optional)", key="reamod_notes", height=68)
    else:
        notes = st.text_area("Notes (optional)", key="reamod_notes", height=68)

    oit = "capitalise"

    cash_gl_id: str | None = None
    if top_up > 1e-9:
        if source_cash_gl_cached_labels_and_ids:
            labels = [x[0] for x in source_cash_gl_cached_labels_and_ids]
            ids = [x[1] for x in source_cash_gl_cached_labels_and_ids]
            default_cash = str(loan.get("cash_gl_account_id") or "").strip()
            def_i = next((i for i, x in enumerate(ids) if x == default_cash), 0)
            g1, _g2, _g3, _g4 = st.columns(4, gap="small")
            with g1:
                pick = st.selectbox(
                    "Source cash / bank GL (top-up)",
                    range(len(labels)),
                    format_func=lambda i: labels[i],
                    index=def_i,
                    key="reamod_cash_gl",
                )
                cash_gl_id = ids[int(pick)]
        elif loan.get("cash_gl_account_id"):
            cash_gl_id = str(loan.get("cash_gl_account_id")).strip()

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
        _c_fr, _c_rt, _c_pad0 = st.columns([1.0, 1.15, 3.85], gap="small")
        with _c_fr:
            fr = st.date_input(f"First repayment ({leg})", default_fr, key=f"reamod_fr_{leg}")
        first_rep = datetime.combine(fr, datetime.min.time())
        with _c_rt:
            rt = st.selectbox(
                f"Repayments on ({leg})",
                ["Anniversary Date", "Last Day Of Month"],
                key=f"reamod_rt_{leg}",
            )
        use_ann = rt.startswith("Anniversary")
        if not use_ann and not is_last_day_of_month(first_rep):
            st.error("For last-day-of-month timing, first repayment must be the last day of that month.")
            return

        if schedule_gross_override is not None:
            combined_gross = float(as_10dp(schedule_gross_override))
        else:
            tf_use = float(top_fee_on_leg) if top_leg > 1e-12 else 0.0
            _gc, _gt, combined_gross = _mod_gross_principal(carry_leg, top_leg, fee_pct, tf_use)
        if combined_gross <= 0:
            st.error("Gross facility for this leg must be positive.")
            return

        try:
            if lt_display == "Consumer Loan":
                dr = (cfg.get("default_rates") or {}).get("consumer_loan") or {}
                if dr.get("interest_pct") is None or dr.get("admin_fee_pct") is None:
                    st.error("Product missing default_rates.consumer_loan.")
                    return
                interest_pct_month = pct_to_monthly(float(dr["interest_pct"]), prb)
                if interest_pct_month is None:
                    return
                base_rate = float(interest_pct_month) / 100.0
                admin_fee_scheme = float(dr["admin_fee_pct"]) / 100.0
                penalty_pct = (cfg.get("penalty_rates") or {}).get("consumer_loan")
                if penalty_pct is None:
                    st.error("Product missing penalty_rates.consumer_loan.")
                    return
                pm = pct_to_monthly(penalty_pct, prb)
                if pm is None:
                    return
                matched = next(
                    (
                        s
                        for s in schemes
                        if abs(float(s.get("interest_rate_pct", 0.0)) - float(interest_pct_month)) < 1e-6
                        and abs(float(s.get("admin_fee_pct", 0.0)) - float(admin_fee_scheme * 100.0)) < 1e-6
                    ),
                    None,
                )
                scheme = str(matched["name"]) if matched and matched.get("name") else "Other"
                details, df_s = compute_consumer_schedule(
                    combined_gross,
                    int(term_m),
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
                details["penalty_rate_pct"] = float(pm)
                details["penalty_quotation"] = cfg.get("penalty_interest_quotation") or ""
            elif lt_display == "Term Loan":
                dr = (cfg.get("default_rates") or {}).get("term_loan") or {}
                for k in ("interest_pct", "drawdown_pct", "arrangement_pct"):
                    if dr.get(k) is None:
                        st.error(f"Product missing default_rates.term_loan.{k}.")
                        return
                rate_pct = float(dr["interest_pct"])
                def_pen = (cfg.get("penalty_rates") or {}).get("term_loan")
                if def_pen is None:
                    st.error("Product missing penalty_rates.term_loan.")
                    return
                _c_pen, _c_gr, _c_pg = st.columns([1.0, 1.35, 3.65], gap="small")
                with _c_pen:
                    penalty_pct = st.number_input(
                        f"Penalty ({leg})",
                        0.0,
                        100.0,
                        float(def_pen),
                        key=f"reamod_pen_term_{leg}",
                    )
                with _c_gr:
                    grace = st.selectbox(
                        f"Grace ({leg})",
                        ["No Grace Period", "Principal Moratorium", "Principal And Interest Moratorium"],
                        key=f"reamod_grace_{leg}",
                    )
                mor = 0
                if grace == "Principal Moratorium":
                    _c_m, _c_mp = st.columns([1.0, 4.0], gap="small")
                    with _c_m:
                        mor = st.number_input(f"Moratorium ({leg})", 1, 120, 3, key=f"reamod_mor_p_{leg}")
                elif grace == "Principal And Interest Moratorium":
                    _c_m, _c_mp = st.columns([1.0, 4.0], gap="small")
                    with _c_m:
                        mor = st.number_input(f"Moratorium ({leg})", 1, 120, 3, key=f"reamod_mor_pi_{leg}")
                details, df_s = compute_term_schedule(
                    combined_gross,
                    int(term_m),
                    disbursement_date,
                    rate_pct,
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
                pm = pct_to_monthly(penalty_pct, prb)
                if pm is None:
                    return
                details["penalty_rate_pct"] = float(pm)
                details["penalty_quotation"] = cfg.get("penalty_interest_quotation") or ""
            elif lt_display == "Bullet Loan":
                dr = (cfg.get("default_rates") or {}).get("bullet_loan") or {}
                for k in ("interest_pct", "drawdown_pct", "arrangement_pct"):
                    if dr.get(k) is None:
                        st.error(f"Product missing default_rates.bullet_loan.{k}.")
                        return
                _c_bt, _c_btp = st.columns([1.65, 3.35], gap="small")
                with _c_bt:
                    btype = st.selectbox(
                        f"Bullet type ({leg})",
                        ["Straight Bullet (No Interim Payments)", "Bullet With Interest Payments"],
                        key=f"reamod_bt_{leg}",
                    )
                first_b = None
                if "With Interest" in btype:
                    first_b = first_rep
                details, df_s = compute_bullet_schedule(
                    combined_gross,
                    int(term_m),
                    disbursement_date,
                    float(dr["interest_pct"]),
                    0.0,
                    0.0,
                    True,
                    btype,
                    first_b,
                    use_ann,
                    prb,
                    flat_leg,
                )
                def_pen = (cfg.get("penalty_rates") or {}).get("bullet_loan")
                if def_pen is None:
                    st.error("Product missing penalty_rates.bullet_loan.")
                    return
                _c_bpen, _c_bpenp = st.columns([1.0, 4.0], gap="small")
                with _c_bpen:
                    penalty_pct = st.number_input(
                        f"Penalty ({leg})",
                        0.0,
                        100.0,
                        float(def_pen),
                        key=f"reamod_pen_bul_{leg}",
                    )
                pm = pct_to_monthly(penalty_pct, prb)
                if pm is None:
                    return
                details["penalty_rate_pct"] = float(pm)
                details["penalty_quotation"] = cfg.get("penalty_interest_quotation") or ""
            else:
                st.info("Customised repayments: use **Loan calculators** to build a schedule and a future enhancement will import it here.")
                return

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

            st.dataframe(format_schedule_df(df_s), width="stretch", hide_index=True, height=220)
            schedule_export_downloads(
                df_s,
                file_stem=f"loan_{loan_id}_mod_{leg}_preview",
                key_prefix=f"reamod_dl_{leg}_{loan_id}",
            )
            _c_use, _c_usep = st.columns([1.35, 3.65], gap="small")
            with _c_use:
                if st.button("Use this schedule", type="tertiary", key=use_key):
                    st.session_state[session_details_key] = details
                    st.session_state[session_df_key] = df_s
                    st.success(f"Schedule {leg} saved in session.")
                    st.rerun()
        except Exception as ex:
            st.error(str(ex))

    n_sched = n_legs if is_split else 1
    for si in range(n_sched):
        letter = _leg_letter(si)
        lt_d = split_lt_display[si]
        pc_leg = split_product_codes[si]
        cfg_leg = get_product_config_from_db(pc_leg) or {}
        sk_det, sk_df = _leg_session_keys(si)
        use_k = f"reamod_use_{letter}"
        if not is_split:
            render_sub_sub_header("Schedule — retained loan")
        else:
            render_sub_sub_header(f"Schedule — Loan {letter}")
        if lt_d == "Customised Repayments":
            st.info("Switch product or use calculators for customised schedules.")
        else:
            _gen_one_leg(
                leg=letter,
                lt_display=lt_d,
                product_code=pc_leg,
                cfg=cfg_leg,
                carry_leg=carry_legs[si],
                top_leg=top_alloc[si],
                net_total_leg=net_legs[si],
                top_fee_on_leg=top_fee_combined if top_alloc[si] > 1e-12 else 0.0,
                schedule_gross_override=principal_schedule_single if not is_split else None,
                use_key=use_k,
                session_details_key=sk_det,
                session_df_key=sk_df,
            )
            if st.session_state.get(sk_df) is not None:
                st.caption(f"Session: schedule {letter} ready.")

    st.divider()
    st.markdown("**Collateral**")
    pol_a, patch_a = _collateral_widgets(
        key_prefix="reamod",
        provisions_config_ok=provisions_config_ok,
        list_provision_security_subtypes=list_provision_security_subtypes,
        loan=loan,
    )
    patch_b: dict[str, Any] = {}
    pol_b = "Keep existing"
    if is_split:
        st.caption("Leg B collateral")
        pol_b, patch_b = _collateral_widgets(
            key_prefix="reamod_b",
            provisions_config_ok=provisions_config_ok,
            list_provision_security_subtypes=list_provision_security_subtypes,
            loan=loan,
        )

    st.divider()
    with st.expander("Documents", expanded=False):
        _render_remod_documents_staging(
            documents_available=documents_available,
            list_document_categories=list_document_categories,
        )

    st.divider()
    c_save1, c_save2 = st.columns(2)
    with c_save1:
        if st.button("Save staged draft", type="secondary", key="reamod_save_staged"):
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
                        if top_up > 1e-9 and lt_display_a in ("Term Loan", "Bullet Loan")
                        else None,
                        "topup_fee_arrangement_pct": str(as_10dp(top_fee_arr * 100))
                        if top_up > 1e-9 and lt_display_a in ("Term Loan", "Bullet Loan")
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
        if st.button("Clear session schedules", key="reamod_clear_sess"):
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
            st.session_state.pop("reamod_docs_staged", None)
            st.rerun()

    if st.button("Submit for approval", type="primary", key="reamod_submit"):
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
                    if top_up > 1e-9 and lt_display_a in ("Term Loan", "Bullet Loan")
                    else None,
                    "topup_fee_arrangement_pct": str(as_10dp(top_fee_arr * 100))
                    if top_up > 1e-9 and lt_display_a in ("Term Loan", "Bullet Loan")
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
            st.rerun()
        except Exception as ex:
            st.error(str(ex))
