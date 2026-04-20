"""Loan management UI: update safe details, approve drafts, view schedule."""

from __future__ import annotations

import io
from collections import defaultdict
from collections.abc import Callable
from decimal import Decimal
from html import escape

import pandas as pd
import streamlit as st

from decimal_utils import as_10dp


from style import render_main_header, render_sub_header, render_sub_sub_header

from display_formatting import format_display_currency
from ui.components import inject_tertiary_hyperlink_css_once, render_centered_html_table
from ui.streamlit_feedback import run_with_spinner


def _normalize_document_file_bytes(raw: object) -> bytes:
    if raw is None:
        return b""
    if isinstance(raw, memoryview):
        return raw.tobytes()
    if isinstance(raw, bytearray):
        return bytes(raw)
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, str):
        return raw.encode("utf-8")
    return bytes(raw)


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
        render_sub_sub_header("Update / Terminate loans")
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
    
        _upd_sub_labels = ["Edit Safe Details", "Terminate Loan Request"]
        st.session_state.setdefault("update_loans_subtab", _upd_sub_labels[0])
        if st.session_state["update_loans_subtab"] not in _upd_sub_labels:
            st.session_state["update_loans_subtab"] = _upd_sub_labels[0]
        st.radio(
            "Update loans panel",
            _upd_sub_labels,
            key="update_loans_subtab",
            horizontal=True,
            label_visibility="collapsed",
        )
        _upd_panel = st.session_state["update_loans_subtab"]

        if _upd_panel == "Edit Safe Details":
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
                    run_with_spinner(
                        "Updating loan details…",
                        lambda: update_loan_safe_details(loan_id, updates),
                    )
                    st.session_state["update_loans_flash"] = f"Details updated for Loan #{loan_id}."
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update details: {e}")
    
        else:
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
    
                        draft_id = run_with_spinner(
                            "Submitting termination request…",
                            lambda: save_loan_approval_draft(
                            customer_id=loan["customer_id"],
                            loan_type=loan["loan_type"],
                            details=draft_details,
                            schedule_df=None,
                            product_code=loan.get("product_code"),
                            created_by="ui_user",
                            status="PENDING",
                            loan_id=loan_id,
                            ),
                        )
                        st.session_state["update_loans_flash"] = f"Termination request submitted (Draft #{draft_id})."
                        for k in list(st.session_state.keys()):
                            if k.startswith("update_loan_") and k != "update_loans_flash":
                                st.session_state.pop(k, None)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to submit termination request: {e}")
    
def render_batch_loan_capture_ui(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    customers_available: bool,
    list_customers,
    get_display_name,
    save_loan,
    list_customers_for_loan_batch_link=None,
    source_cash_gl_cached_labels_and_ids: Callable[[], tuple[list[str], list[str]]] | None = None,
) -> None:
        """Batch loan migration: preview CSVs, then commit loans directly to the database."""
        try:
            from rbac.subfeature_access import loan_management_can_batch_capture

            if not loan_management_can_batch_capture():
                st.error("You do not have permission for batch loan capture (migration).")
                st.stop()
        except Exception:
            st.error("You do not have permission for batch loan capture (migration).")
            st.stop()
        render_sub_sub_header("Batch Loan Capture (Migration)")
        st.caption(
            "Upload a loan header CSV and schedule CSV, preview the rows, then **Commit loans to database** "
            "(same pattern as customer batch import — no approval queue). "
            "Link each loan to a customer using **customer_ref** (same value as customer **migration_ref**), "
            "or **customer_display_name** (matches the UI label), or a valid **customer_id**. "
            "Run `python scripts/run_migration_75.py` once so migration_ref is stored. "
            "When the **source cash account cache** is populated, choose a **default source cash GL** below "
            "(used when **cash_gl_account_id** is blank in the CSV); a non-empty per-row value overrides it. "
            "Schedule **Date** values should use a **four-digit year** (YYYY-MM-DD or dd-Mon-2024). "
            "If the database column was ever VARCHAR(10), run `python scripts/run_migration_76.py` before imports. "
            "Regenerate `test_schedules.csv` with `scripts/generate_test_loans.py` if dates look like `…-202` (truncated year)."
        )
        if not loan_management_available:
            st.error(f"Loan management module is not available. ({loan_management_error})")
            return
        if not customers_available:
            st.error("Customer module is required before loading migration loans.")
            return
    
        customers = list_customers() or []
        valid_customer_ids = {int(c["id"]) for c in customers if c.get("id") is not None}
    
        template_loans = pd.DataFrame(
            [
                {
                    "import_key": "LN-001",
                    "customer_ref": "CUST-0001",
                    "customer_display_name": "",
                    "customer_id": "",
                    "loan_type": "Term Loan",
                    "product_code": "",
                    "principal": 1000.00,
                    "disbursed_amount": 980.00,
                    "term": 6,
                    "annual_rate": 24.0,
                    "monthly_rate": 2.0,
                    "drawdown_fee_amount": 20.0,
                    "arrangement_fee_amount": 0.0,
                    "admin_fee_amount": 0.0,
                    "drawdown_fee": 0.0,
                    "arrangement_fee": 0.0,
                    "admin_fee": 0.0,
                    "disbursement_date": "2026-01-01",
                    "first_repayment_date": "2026-02-01",
                    "end_date": "2026-07-01",
                    "installment": 176.67,
                    "total_payment": 1060.00,
                    "payment_timing": "anniversary",
                    "cash_gl_account_id": "",
                    "loan_purpose_id": "",
                    "agent_id": "",
                    "relationship_manager_id": "",
                }
            ]
        )
        template_schedule = pd.DataFrame(
            [
                {
                    "import_key": "LN-001",
                    "Period": 1,
                    "Date": "2026-02-01",
                    "Payment": 176.67,
                    "Principal": 156.67,
                    "Interest": 20.00,
                    "Principal Balance": 843.33,
                    "Total Outstanding": 843.33,
                },
                {
                    "import_key": "LN-001",
                    "Period": 2,
                    "Date": "2026-03-01",
                    "Payment": 176.67,
                    "Principal": 159.80,
                    "Interest": 16.87,
                    "Principal Balance": 683.53,
                    "Total Outstanding": 683.53,
                },
            ]
        )
        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                "Download Loan Header Template",
                data=template_loans.to_csv(index=False).encode("utf-8"),
                file_name="loan_batch_template.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with dl2:
            st.download_button(
                "Download Loan Schedule Template",
                data=template_schedule.to_csv(index=False).encode("utf-8"),
                file_name="loan_schedule_batch_template.csv",
                mime="text/csv",
                use_container_width=True,
            )
        loan_csv = st.file_uploader(
            "Upload loan header CSV",
            type=["csv"],
            key="loan_batch_header_upload",
        )
        sched_csv = st.file_uploader(
            "Upload schedule CSV",
            type=["csv"],
            key="loan_batch_schedule_upload",
        )
        if loan_csv is None or sched_csv is None:
            return
    
        try:
            loans_df = pd.read_csv(loan_csv)
            sched_df = pd.read_csv(sched_csv)
        except Exception as e:
            st.error(f"Could not read CSV file(s): {e}")
            return
    
        st.markdown("**Loan rows preview**")
        st.dataframe(loans_df.head(50), hide_index=True, width="stretch")
        st.markdown("**Schedule rows preview**")
        st.dataframe(sched_df.head(50), hide_index=True, width="stretch")

        required_loan_cols = {"import_key", "loan_type", "principal", "term", "disbursement_date"}
        required_sched_cols = {"import_key", "Period", "Date", "Payment", "Principal", "Interest"}
        miss_loan = sorted(c for c in required_loan_cols if c not in loans_df.columns)
        miss_sched = sorted(c for c in required_sched_cols if c not in sched_df.columns)
        if miss_loan:
            st.error(f"Loan CSV missing required column(s): {', '.join(miss_loan)}")
            return
        if miss_sched:
            st.error(f"Schedule CSV missing required column(s): {', '.join(miss_sched)}")
            return

        batch_default_source_cash_gl_id: str | None = None
        if source_cash_gl_cached_labels_and_ids:
            try:
                _cg_lab, _cg_ids = source_cash_gl_cached_labels_and_ids()
            except Exception:
                _cg_lab, _cg_ids = [], []
            if _cg_ids:
                st.markdown("**Source cash GL**")
                _cg_i = st.selectbox(
                    "Default source cash / bank GL (used when CSV cash_gl_account_id is empty; per-row overrides)",
                    range(len(_cg_lab)),
                    format_func=lambda i: _cg_lab[i],
                    key="loan_batch_default_cash_gl_sel",
                )
                batch_default_source_cash_gl_id = _cg_ids[_cg_i]

        if not st.button("Commit loans to database", type="primary", key="loan_batch_commit"):
            return

        def _opt_float(raw: object) -> float | None:
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                return None
            s = str(raw).strip()
            if not s:
                return None
            return float(as_10dp(float(s)))

        def _opt_int(raw: object) -> int | None:
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                return None
            s = str(raw).strip()
            if not s:
                return None
            return int(float(s))

        def _opt_text(raw: object) -> str | None:
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                return None
            s = str(raw).strip()
            return s or None

        def _run_batch_commit() -> tuple[list[dict], list[dict]]:
            grouped_sched: dict[str, pd.DataFrame] = {
                str(k): g.copy()
                for k, g in sched_df.groupby(sched_df["import_key"].astype(str), dropna=False)
            }
            created_rows: list[dict] = []
            error_rows: list[dict] = []

            ref_exact: dict[str, int] = {}
            ref_lower: dict[str, int] = {}
            display_sets: dict[str, set[int]] = defaultdict(set)
            legal_sets: dict[str, set[int]] = defaultdict(set)
            indiv_sets: dict[str, set[int]] = defaultdict(set)
            if list_customers_for_loan_batch_link:
                link_rows = list_customers_for_loan_batch_link() or []
                for lr in link_rows:
                    cid = int(lr["id"])
                    mr = lr.get("migration_ref")
                    if mr is not None and str(mr).strip():
                        s = str(mr).strip()
                        ref_exact[s] = cid
                        ref_lower[s.lower()] = cid
                    ctype = str(lr.get("type") or "")
                    if ctype == "individual":
                        nm = (lr.get("individual_name") or "").strip()
                        if nm:
                            display_sets[nm.lower()].add(cid)
                            indiv_sets[nm.lower()].add(cid)
                    elif ctype == "corporate":
                        tn = (lr.get("trading_name") or "").strip()
                        ln = (lr.get("legal_name") or "").strip()
                        disp = tn if tn else ln
                        if disp:
                            display_sets[disp.lower()].add(cid)
                        if ln:
                            legal_sets[ln.lower()].add(cid)
                        if tn:
                            display_sets[tn.lower()].add(cid)

            def _resolve_customer_id_for_row(row: object) -> tuple[int, str]:
                """Returns (customer_id, how_matched)."""
                cid0 = _opt_int(row.get("customer_id"))
                if cid0 is not None and cid0 in valid_customer_ids:
                    return cid0, "customer_id"

                cref = _opt_text(row.get("customer_ref")) or _opt_text(row.get("migration_ref"))
                if cref:
                    hit = ref_exact.get(cref) or ref_lower.get(cref.lower())
                    if hit is not None and hit in valid_customer_ids:
                        return hit, "customer_ref"
                    raise ValueError(
                        f"customer_ref '{cref}' not found. Import customers with the same migration_ref, "
                        "or fix the spelling."
                    )

                cdn = _opt_text(row.get("customer_display_name"))
                if cdn:
                    ids = list(display_sets.get(cdn.lower(), set()))
                    if len(ids) == 1:
                        return ids[0], "customer_display_name"
                    if len(ids) > 1:
                        raise ValueError(
                            f"customer_display_name '{cdn}' matches {len(ids)} customers; use customer_ref or customer_id."
                        )
                    raise ValueError(f"customer_display_name '{cdn}' not found.")

                cle = _opt_text(row.get("customer_legal_name"))
                if cle:
                    ids = list(legal_sets.get(cle.lower(), set()))
                    if len(ids) == 1:
                        return ids[0], "customer_legal_name"
                    if len(ids) > 1:
                        raise ValueError(
                            f"customer_legal_name '{cle}' is ambiguous; use customer_ref or customer_id."
                        )
                    raise ValueError(f"customer_legal_name '{cle}' not found.")

                cin = _opt_text(row.get("customer_individual_name"))
                if cin:
                    ids = list(indiv_sets.get(cin.lower(), set()))
                    if len(ids) == 1:
                        return ids[0], "customer_individual_name"
                    if len(ids) > 1:
                        raise ValueError(
                            f"customer_individual_name '{cin}' is ambiguous; use customer_ref or customer_id."
                        )
                    raise ValueError(f"customer_individual_name '{cin}' not found.")

                if cid0 is not None:
                    raise ValueError(f"customer_id {cid0} does not exist.")
                raise ValueError(
                    "Provide customer_ref (recommended), customer_display_name, customer_legal_name, "
                    "customer_individual_name, or a valid customer_id."
                )

            for idx, row in loans_df.iterrows():
                row_no = int(idx) + 2  # include CSV header row
                try:
                    import_key = str(row.get("import_key") or "").strip()
                    if not import_key:
                        raise ValueError("import_key is required.")
                    sched_rows = grouped_sched.get(import_key)
                    if sched_rows is None or sched_rows.empty:
                        raise ValueError(f"No schedule rows found for import_key '{import_key}'.")

                    customer_id, link_how = _resolve_customer_id_for_row(row)

                    loan_type = _opt_text(row.get("loan_type"))
                    if not loan_type:
                        raise ValueError("loan_type is required.")

                    principal = _opt_float(row.get("principal"))
                    term = _opt_int(row.get("term"))
                    disb_date = _opt_text(row.get("disbursement_date"))
                    if principal is None:
                        raise ValueError("principal is required.")
                    if term is None or term <= 0:
                        raise ValueError("term must be a positive integer.")
                    if not disb_date:
                        raise ValueError("disbursement_date is required.")

                    disbursed = _opt_float(row.get("disbursed_amount"))
                    meta = {"migration_import_key": import_key, "batch_customer_link": link_how}
                    cref_meta = _opt_text(row.get("customer_ref")) or _opt_text(row.get("migration_ref"))
                    if cref_meta:
                        meta["batch_customer_ref"] = cref_meta

                    row_cash_gl = _opt_text(row.get("cash_gl_account_id"))
                    cash_gl_for_save = row_cash_gl or batch_default_source_cash_gl_id

                    details = {
                        "principal": principal,
                        "disbursed_amount": disbursed if disbursed is not None else principal,
                        "term": term,
                        "annual_rate": _opt_float(row.get("annual_rate")),
                        "monthly_rate": _opt_float(row.get("monthly_rate")),
                        "penalty_rate_pct": _opt_float(row.get("penalty_rate_pct")),
                        "drawdown_fee_amount": _opt_float(row.get("drawdown_fee_amount")),
                        "arrangement_fee_amount": _opt_float(row.get("arrangement_fee_amount")),
                        "admin_fee_amount": _opt_float(row.get("admin_fee_amount")),
                        "drawdown_fee": _opt_float(row.get("drawdown_fee")),
                        "arrangement_fee": _opt_float(row.get("arrangement_fee")),
                        "admin_fee": _opt_float(row.get("admin_fee")),
                        "disbursement_date": disb_date,
                        "first_repayment_date": _opt_text(row.get("first_repayment_date")),
                        "end_date": _opt_text(row.get("end_date")),
                        "installment": _opt_float(row.get("installment")),
                        "total_payment": _opt_float(row.get("total_payment")),
                        "payment_timing": _opt_text(row.get("payment_timing")),
                        "cash_gl_account_id": cash_gl_for_save,
                        "loan_purpose_id": _opt_int(row.get("loan_purpose_id")),
                        "agent_id": _opt_int(row.get("agent_id")),
                        "relationship_manager_id": _opt_int(row.get("relationship_manager_id")),
                        "metadata": meta,
                    }
                    details = {k: v for k, v in details.items() if v is not None}

                    schedule_use = sched_rows.copy()
                    for col in (
                        "Period",
                        "Payment",
                        "Principal",
                        "Interest",
                        "Principal Balance",
                        "Total Outstanding",
                    ):
                        if col in schedule_use.columns:
                            schedule_use[col] = pd.to_numeric(schedule_use[col], errors="coerce")
                    schedule_use = schedule_use.sort_values(by=["Period"]).reset_index(drop=True)
                    schedule_use = schedule_use.drop(columns=[c for c in ["import_key"] if c in schedule_use.columns])

                    loan_id = save_loan(
                        int(customer_id),
                        str(loan_type),
                        details,
                        schedule_use,
                        product_code=_opt_text(row.get("product_code")),
                    )
                    created_rows.append(
                        {
                            "row": row_no,
                            "import_key": import_key,
                            "customer_id": customer_id,
                            "linked_via": link_how,
                            "customer_name": get_display_name(customer_id),
                            "loan_id": int(loan_id),
                        }
                    )
                except Exception as e:
                    error_rows.append({"row": row_no, "import_key": row.get("import_key"), "error": str(e)})
            return created_rows, error_rows

        try:
            created_rows, error_rows = run_with_spinner("Saving loans to database…", _run_batch_commit)
        except Exception as ex:
            st.error(f"Batch commit failed: {ex}")
            return

        st.success(f"Committed {len(created_rows)} loan(s) to the database.")
        if created_rows:
            st.dataframe(pd.DataFrame(created_rows), hide_index=True, width="stretch")
        if error_rows:
            st.warning(f"{len(error_rows)} row(s) failed.")
            st.dataframe(pd.DataFrame(error_rows), hide_index=True, width="stretch")


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
    money_df_column_config=None,
) -> None:
        """Approval inbox for loan drafts submitted from capture Stage 2."""
        render_sub_sub_header("Approve loans")
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
                for _k in list(st.session_state.keys()):
                    if str(_k).startswith("approve_doc_preview_"):
                        st.session_state.pop(_k, None)
                st.rerun()
    
        drafts = list_loan_approval_drafts(
            statuses=["PENDING", "REWORK"],
            search=search_txt.strip() or None,
            limit=500,
        )
        if not drafts:
            st.info("No loan drafts are awaiting approval.")
            return

        with st.expander("Dismiss batch (clear this approval queue)", expanded=False):
            st.warning(
                f"This will **dismiss all {len(drafts)} draft(s)** currently listed (same search and "
                "**PENDING** / **REWORK**). Use this after a bad migration upload or to reset the queue."
            )
            st.caption(
                "Note: the inbox list is capped for performance. The batch dismiss will re-fetch up to 5,000 "
                "matching drafts at click-time."
            )
            _bd_note = st.text_input(
                "Optional note stored on each draft",
                key="approve_batch_dismiss_note",
                placeholder="e.g. Clearing test import batch",
            )
            _bd_conf = st.text_input(
                'Type **DISMISS BATCH** to confirm',
                key="approve_batch_dismiss_confirm",
            )
            if st.button("Dismiss entire batch", type="primary", key="approve_batch_dismiss_go"):
                if (_bd_conf or "").strip() != "DISMISS BATCH":
                    st.error('Confirmation must be exactly: DISMISS BATCH')
                else:
                    # Re-fetch drafts at click time so the batch matches the latest queue (and not just
                    # the first page already loaded above).
                    drafts_now = list_loan_approval_drafts(
                        statuses=["PENDING", "REWORK"],
                        search=search_txt.strip() or None,
                        limit=5000,
                    )
                    errs: list[str] = []
                    n_ok = 0

                    def _run_batch_dismiss():
                        nonlocal n_ok, errs
                        for r in drafts_now:
                            try:
                                dismiss_loan_approval_draft(
                                    int(r["id"]),
                                    note=(_bd_note or "").strip() or "Batch dismiss (Approve loans)",
                                    actor="approver_ui",
                                )
                                n_ok += 1
                            except Exception as ex:
                                errs.append(f"Draft #{r.get('id')}: {ex}")

                    run_with_spinner("Dismissing drafts…", _run_batch_dismiss)
                    st.session_state.pop("approve_selected_draft_id", None)
                    for _k in list(st.session_state.keys()):
                        if str(_k).startswith("approve_doc_preview_"):
                            st.session_state.pop(_k, None)
                    if errs:
                        st.session_state["approve_loans_flash_message"] = (
                            f"Dismissed {n_ok} draft(s); {len(errs)} error(s). "
                            f"(Showing up to 50 errors in console log.)"
                        )
                        # Keep a short copy for UI after rerun.
                        st.session_state["approve_batch_dismiss_errors"] = errs[:50]
                    else:
                        st.session_state["approve_loans_flash_message"] = f"Dismissed {n_ok} draft(s)."
                        st.session_state.pop("approve_batch_dismiss_errors", None)
                    st.rerun()

        with st.expander("Approve / Commit batch (create loans)", expanded=False):
            st.warning(
                f"This will **approve and commit** drafts currently listed (same search and "
                "**PENDING** / **REWORK**). This creates loans and posts GL where applicable."
            )
            st.caption(
                "The inbox list is capped for performance. Batch approve will re-fetch up to 2,000 matching drafts at click-time."
            )
            _ba_note = st.text_input(
                "Optional note (stored in your logs only; approval function does not persist this note)",
                key="approve_batch_approve_note",
                placeholder="e.g. Approving migration batch",
            )
            _ba_conf = st.text_input(
                "Type **APPROVE BATCH** to confirm",
                key="approve_batch_approve_confirm",
            )
            if st.button("Approve / Commit entire batch", type="primary", key="approve_batch_approve_go"):
                if (_ba_conf or "").strip() != "APPROVE BATCH":
                    st.error("Confirmation must be exactly: APPROVE BATCH")
                else:
                    drafts_now = list_loan_approval_drafts(
                        statuses=["PENDING", "REWORK"],
                        search=search_txt.strip() or None,
                        limit=2000,
                    )
                    errs_a: list[str] = []
                    ok_rows: list[str] = []

                    def _run_batch_approve():
                        nonlocal errs_a, ok_rows
                        for r in drafts_now:
                            did = int(r["id"])
                            try:
                                lid = approve_loan_approval_draft(did, approved_by="approver_ui")
                                ok_rows.append(f"Draft #{did} -> Loan #{int(lid)}")
                            except Exception as ex:
                                errs_a.append(f"Draft #{did}: {ex}")

                    run_with_spinner("Approving drafts…", _run_batch_approve)
                    st.session_state.pop("approve_selected_draft_id", None)
                    for _k in list(st.session_state.keys()):
                        if str(_k).startswith("approve_doc_preview_"):
                            st.session_state.pop(_k, None)
                    if errs_a:
                        st.session_state["approve_loans_flash_message"] = (
                            f"Approved {len(ok_rows)} draft(s); {len(errs_a)} error(s)."
                        )
                        st.session_state["approve_batch_approve_errors"] = errs_a[:50]
                        st.session_state["approve_batch_approve_ok"] = ok_rows[:50]
                    else:
                        st.session_state["approve_loans_flash_message"] = f"Approved {len(ok_rows)} draft(s)."
                        st.session_state["approve_batch_approve_ok"] = ok_rows[:50]
                        st.session_state.pop("approve_batch_approve_errors", None)
                    st.rerun()

        _ba_ok = st.session_state.pop("approve_batch_approve_ok", None)
        if _ba_ok:
            with st.expander("Batch approve results (first 50)", expanded=False):
                st.code("\n".join(_ba_ok))
        _ba_errs = st.session_state.pop("approve_batch_approve_errors", None)
        if _ba_errs:
            with st.expander("Batch approve errors (first 50)", expanded=False):
                st.code("\n".join(_ba_errs))

        # Show persisted batch-dismiss errors (if any) after rerun.
        _bd_errs = st.session_state.pop("approve_batch_dismiss_errors", None)
        if _bd_errs:
            with st.expander("Batch dismiss errors (first 50)", expanded=False):
                st.code("\n".join(_bd_errs))
    
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
                sec_rows = draft.get("schedule_json_secondary") or []
                df_schedule_b = pd.DataFrame(sec_rows) if sec_rows else pd.DataFrame()
                customer_name = (
                    get_display_name(int(draft["customer_id"]))
                    if customers_available
                    else f"Customer #{draft['customer_id']}"
                )
                ap_action = str(details.get("approval_action") or "").strip().upper()
    
                st.markdown("### Draft inspection")
                p1, p2, p3, p4 = st.columns(4)
                with p1:
                    st.caption("Identity")
                    st.write(f"Draft: **{draft.get('id')}**")
                    st.write(f"Customer: **{customer_name}**")
                    st.write(f"Loan type: **{draft.get('loan_type')}**")
                    st.write(f"Product: **{draft.get('product_code') or '—'}**")
                    if ap_action:
                        st.write(f"Action: **{ap_action}**")
                if ap_action in ("LOAN_MODIFICATION", "LOAN_MODIFICATION_SPLIT"):
                    mod = details.get("modification_loan_details") or {}
                    src_lid = draft.get("loan_id") or details.get("source_loan_id")
                    with p2:
                        st.caption("Modification amounts")
                        st.write(f"Source loan: **{src_lid or '—'}**")
                        st.write(f"Restructure date: **{details.get('restructure_date') or '—'}**")
                        st.write(f"EOD snapshot date: **{details.get('as_of_balance_date') or '—'}**")
                        st.write(f"Total facility: **{float(str(details.get('total_facility') or details.get('principal') or 0)):,.2f}**")
                        st.write(f"Top-up: **{float(str(details.get('topup_amount') or 0)):,.2f}**")
                        st.write(f"Write-off on approve: **{float(str(details.get('writeoff_amount') or 0)):,.2f}**")
                    with p3:
                        st.caption("Policy & interest")
                        st.write(f"Excess policy: **{details.get('excess_policy') or '—'}**")
                        st.write(
                            "Interest treatment (restructure): **capitalise** "
                            "(automatic; excess handled via write-off / split policy above)."
                        )
                        st.write(
                            f"New principal (header): **{float(mod.get('principal') or 0):,.2f}** · "
                            f"Instalment: **{float(mod.get('installment') or 0):,.2f}**"
                        )
                    with p4:
                        st.caption("Status")
                        st.write(f"Draft status: **{draft.get('status')}**")
                        if ap_action == "LOAN_MODIFICATION_SPLIT":
                            sp = details.get("split") or {}
                            nsp = int(details.get("split_leg_count") or 2)
                            st.write(
                                f"Modified Loans: **{nsp}** loan(s) (schedules A–{chr(ord('A') + min(nsp, 4) - 1)}; "
                                "**A** = amount to restructure)"
                            )
                            st.write(
                                f"Split A/B principal: **{float(str(sp.get('principal_a') or 0)):,.2f}** / "
                                f"**{float(str(sp.get('principal_b') or 0)):,.2f}**"
                            )
                            st.write(f"Product B: **{details.get('split_product_code_b') or '—'}**")
                elif ap_action == "TERMINATE":
                    with p2:
                        st.caption("Termination")
                        st.write(f"Loan to terminate: **{draft.get('loan_id') or '—'}**")
                    with p3:
                        st.caption("Reason")
                        st.write(f"{escape(str(details.get('termination_reason') or details.get('notes') or '—'))}")
                    with p4:
                        st.caption("Status")
                        st.write(f"**{draft.get('status')}**")
                else:
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
                        st.write(
                            f"Fees: **{float(details.get('drawdown_fee') or 0) * 100:.2f}% / "
                            f"{float(details.get('arrangement_fee') or 0) * 100:.2f}%**"
                        )
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
                            inject_tertiary_hyperlink_css_once()
                            _preview_key = f"approve_doc_preview_{int(selected_id)}"
                            st.caption("Click a **file name** to preview PDFs and images here. Other types: download from the preview panel.")
                            _h0, _h1, _h2, _h3, _h4 = st.columns([1.15, 2.35, 0.65, 0.95, 1.1], gap="small")
                            with _h0:
                                st.markdown(
                                    '<p style="margin:0;font-weight:600;font-size:0.8rem;text-align:left;">Category</p>',
                                    unsafe_allow_html=True,
                                )
                            with _h1:
                                st.markdown(
                                    '<p style="margin:0;font-weight:600;font-size:0.8rem;text-align:left;">File name</p>',
                                    unsafe_allow_html=True,
                                )
                            with _h2:
                                st.markdown(
                                    '<p style="margin:0;font-weight:600;font-size:0.8rem;text-align:center;">KB</p>',
                                    unsafe_allow_html=True,
                                )
                            with _h3:
                                st.markdown(
                                    '<p style="margin:0;font-weight:600;font-size:0.8rem;text-align:center;">Uploaded</p>',
                                    unsafe_allow_html=True,
                                )
                            with _h4:
                                st.markdown(
                                    '<p style="margin:0;font-weight:600;font-size:0.8rem;text-align:center;">By</p>',
                                    unsafe_allow_html=True,
                                )
                            for _d in docs:
                                _did = int(_d["id"])
                                _r0, _r1, _r2, _r3, _r4 = st.columns([1.15, 2.35, 0.65, 0.95, 1.1], gap="small")
                                with _r0:
                                    st.markdown(
                                        f'<p style="margin:0;font-size:0.88rem;text-align:left;">'
                                        f"{escape(str(_d.get('category_name') or '—'))}</p>",
                                        unsafe_allow_html=True,
                                    )
                                with _r1:
                                    if st.button(
                                        str(_d.get("file_name") or "document"),
                                        key=f"approve_open_doc_{_did}",
                                        type="tertiary",
                                    ):
                                        st.session_state[_preview_key] = _did
                                        st.rerun()
                                with _r2:
                                    try:
                                        _kb = (
                                            round(int(_d.get("file_size") or 0) / 1024, 1)
                                            if _d.get("file_size") is not None
                                            else "—"
                                        )
                                    except (TypeError, ValueError):
                                        _kb = "—"
                                    st.markdown(
                                        f'<p style="margin:0;font-size:0.88rem;text-align:center;">{_kb}</p>',
                                        unsafe_allow_html=True,
                                    )
                                with _r3:
                                    _ua = _d.get("uploaded_at")
                                    _ua_s = str(_ua)[:16] if _ua is not None else "—"
                                    st.markdown(
                                        f'<p style="margin:0;font-size:0.88rem;text-align:center;">'
                                        f"{escape(_ua_s)}</p>",
                                        unsafe_allow_html=True,
                                    )
                                with _r4:
                                    st.markdown(
                                        f'<p style="margin:0;font-size:0.88rem;text-align:center;">'
                                        f"{escape(str(_d.get('uploaded_by') or '—'))}</p>",
                                        unsafe_allow_html=True,
                                    )
                            _allowed_doc_ids = {int(d["id"]) for d in docs}
                            _pid = st.session_state.get(_preview_key)
                            if _pid is not None:
                                try:
                                    _pid_i = int(_pid)
                                except (TypeError, ValueError):
                                    st.session_state.pop(_preview_key, None)
                                    _pid_i = None
                                if _pid_i is not None and _pid_i not in _allowed_doc_ids:
                                    st.session_state.pop(_preview_key, None)
                                    _pid_i = None
                                if _pid_i is not None:
                                    full = get_document(_pid_i)
                                    if not full:
                                        st.warning("Document could not be loaded.")
                                        st.session_state.pop(_preview_key, None)
                                    else:
                                        st.divider()
                                        _fn = str(full.get("file_name") or "document")
                                        st.markdown(f"**Preview:** {escape(_fn)}")
                                        _raw = _normalize_document_file_bytes(full.get("file_content"))
                                        _mime = str(full.get("file_type") or "").lower()
                                        _fn_l = _fn.lower()
                                        _is_pdf = _mime == "application/pdf" or _fn_l.endswith(".pdf")
                                        _is_img = _mime.startswith("image/") or _fn_l.endswith(
                                            (".png", ".jpg", ".jpeg", ".gif", ".webp")
                                        )
                                        if _is_pdf and _raw:
                                            st.pdf(io.BytesIO(_raw))
                                        elif _is_img and _raw:
                                            st.image(_raw)
                                        else:
                                            st.info(
                                                "In-app preview is not available for this file type. "
                                                "Use **Download** to open it locally."
                                            )
                                            st.download_button(
                                                "Download",
                                                data=_raw,
                                                file_name=_fn,
                                                mime=str(full.get("file_type") or "application/octet-stream"),
                                                key=f"approve_preview_dl_{_pid_i}",
                                            )
                                        if st.button(
                                            "Close preview",
                                            key=f"approve_close_doc_{int(selected_id)}_{_pid_i}",
                                        ):
                                            st.session_state.pop(_preview_key, None)
                                            st.rerun()
                    else:
                        st.info("Document module is unavailable.")
    
                with st.expander("View schedule", expanded=False):
                    if ap_action == "LOAN_MODIFICATION_SPLIT":
                        extras = details.get("split_schedules_extra") or []
                        leg_frames: list[pd.DataFrame] = [df_schedule, df_schedule_b]
                        if isinstance(extras, list):
                            for block in extras:
                                leg_frames.append(
                                    pd.DataFrame(block) if isinstance(block, list) else pd.DataFrame()
                                )
                        n_legs = int(details.get("split_leg_count") or 2)
                        n_legs = max(2, min(n_legs, 4))
                        tab_labels = [f"Schedule {chr(ord('A') + i)}" for i in range(n_legs)]
                        dfs_use = leg_frames[:n_legs]
                        while len(dfs_use) < n_legs:
                            dfs_use.append(pd.DataFrame())
                        if any(not d.empty for d in dfs_use):
                            _leg_key = f"approve_split_schedule_leg_{int(selected_id)}"
                            st.session_state.setdefault(_leg_key, tab_labels[0])
                            if st.session_state.get(_leg_key) not in tab_labels:
                                st.session_state[_leg_key] = tab_labels[0]
                            st.radio(
                                "Schedule leg",
                                tab_labels,
                                key=_leg_key,
                                horizontal=True,
                                label_visibility="collapsed",
                            )
                            ti = tab_labels.index(st.session_state[_leg_key])
                            st.caption(f"Loan {chr(ord('A') + ti)}")
                            if ti < len(dfs_use) and not dfs_use[ti].empty:
                                st.dataframe(
                                    format_schedule_df(dfs_use[ti]),
                                    width="stretch",
                                    hide_index=True,
                                    height=220,
                                )
                            else:
                                st.info("No schedule rows for this leg.")
                        elif df_schedule.empty:
                            st.info("No schedule found for this draft.")
                        else:
                            st.dataframe(
                                format_schedule_df(df_schedule),
                                width="stretch",
                                hide_index=True,
                                height=220,
                            )
                    elif df_schedule.empty:
                        st.info("No schedule found for this draft.")
                    else:
                        st.dataframe(
                            format_schedule_df(df_schedule),
                            width="stretch",
                            hide_index=True,
                            height=220,
                        )

                with st.expander("Journal preview (simulation)", expanded=False):
                    try:
                        from accounting.service import AccountingService
                        from loan_management import build_loan_approval_journal_payload

                        svc = AccountingService()
                        src_lid = draft.get("loan_id")
                        wo = float(str(details.get("writeoff_amount") or 0))
                        tu = float(str(details.get("topup_amount") or 0))
                        mod_det = details.get("modification_loan_details") or {}
                        cash_gl = mod_det.get("cash_gl_account_id")
                        if ap_action in ("LOAN_MODIFICATION", "LOAN_MODIFICATION_SPLIT") and src_lid:
                            if wo > 0:
                                p_wo = {
                                    "allowance_credit_losses": as_10dp(Decimal(str(wo))),
                                    "loan_principal": as_10dp(Decimal(str(wo))),
                                }
                                sim_wo = svc.simulate_event(
                                    "PRINCIPAL_WRITEOFF",
                                    payload=p_wo,
                                    loan_id=int(src_lid),
                                )
                                if sim_wo.lines:
                                    st.caption("Principal write-off (on approve)")
                                    df_j = pd.DataFrame(
                                        [
                                            {
                                                "Account": f"{ln['account_name']} ({ln['account_code']})",
                                                "Debit": float(ln["debit"]),
                                                "Credit": float(ln["credit"]),
                                            }
                                            for ln in sim_wo.lines
                                        ]
                                    )
                                    cfg_j = dict(money_df_column_config(df_j)) if money_df_column_config else {}
                                    for jc in ("Debit", "Credit"):
                                        if jc in cfg_j and isinstance(cfg_j[jc], dict):
                                            cfg_j[jc] = {**cfg_j[jc], "alignment": "right"}
                                    st.dataframe(
                                        df_j,
                                        width="stretch",
                                        hide_index=True,
                                        height=min(200, 40 + len(sim_wo.lines) * 34),
                                        column_config=cfg_j or None,
                                    )
                                    if not sim_wo.balanced and sim_wo.warning:
                                        st.warning(sim_wo.warning)
                            if tu > 0:
                                payload_tu = dict(
                                    build_loan_approval_journal_payload(
                                        {
                                            "principal": tu,
                                            "disbursed_amount": tu,
                                            "drawdown_fee": 0.0,
                                            "arrangement_fee": 0.0,
                                            "admin_fee": 0.0,
                                        }
                                    )
                                )
                                if cash_gl:
                                    payload_tu["account_overrides"] = {"cash_operating": str(cash_gl).strip()}
                                st.caption("Top-up disbursement (on approve)")
                                sim_tu = svc.simulate_event(
                                    "LOAN_APPROVAL",
                                    payload=payload_tu,
                                    loan_id=int(src_lid),
                                )
                                if sim_tu.lines:
                                    df_t = pd.DataFrame(
                                        [
                                            {
                                                "Account": f"{ln['account_name']} ({ln['account_code']})",
                                                "Debit": float(ln["debit"]),
                                                "Credit": float(ln["credit"]),
                                            }
                                            for ln in sim_tu.lines
                                        ]
                                    )
                                    cfg_t = dict(money_df_column_config(df_t)) if money_df_column_config else {}
                                    for jc in ("Debit", "Credit"):
                                        if jc in cfg_t and isinstance(cfg_t[jc], dict):
                                            cfg_t[jc] = {**cfg_t[jc], "alignment": "right"}
                                    st.dataframe(
                                        df_t,
                                        width="stretch",
                                        hide_index=True,
                                        height=min(200, 40 + len(sim_tu.lines) * 34),
                                        column_config=cfg_t or None,
                                    )
                                    if not sim_tu.balanced and sim_tu.warning:
                                        st.warning(sim_tu.warning)
                        elif not ap_action or ap_action == "":
                            cap_det = {**details}
                            if cap_det.get("principal"):
                                payload_cap = dict(build_loan_approval_journal_payload(cap_det))
                                cg = cap_det.get("cash_gl_account_id")
                                if cg:
                                    payload_cap["account_overrides"] = {"cash_operating": str(cg).strip()}
                                sim_c = svc.simulate_event("LOAN_APPROVAL", payload=payload_cap, loan_id=None)
                                if sim_c.lines:
                                    df_c = pd.DataFrame(
                                        [
                                            {
                                                "Account": f"{ln['account_name']} ({ln['account_code']})",
                                                "Debit": float(ln["debit"]),
                                                "Credit": float(ln["credit"]),
                                            }
                                            for ln in sim_c.lines
                                        ]
                                    )
                                    cfg_c = dict(money_df_column_config(df_c)) if money_df_column_config else {}
                                    for jc in ("Debit", "Credit"):
                                        if jc in cfg_c and isinstance(cfg_c[jc], dict):
                                            cfg_c[jc] = {**cfg_c[jc], "alignment": "right"}
                                    st.dataframe(
                                        df_c,
                                        width="stretch",
                                        hide_index=True,
                                        height=min(220, 40 + len(sim_c.lines) * 34),
                                        column_config=cfg_c or None,
                                    )
                                    if not sim_c.balanced and sim_c.warning:
                                        st.warning(sim_c.warning)
                        else:
                            st.info("No journal simulation for this action type.")
                    except Exception as _je:
                        st.warning(f"Journal preview unavailable: {_je}")
    
                note = st.text_input("Reviewer note (optional)", key="approve_reviewer_note")
                st.caption(
                    "**Send back to schedule builder** sets the draft to REWORK so capture staff can reload it under "
                    "**Capture loan → See loans for rework**, adjust the schedule, and **Send for approval** again."
                )
                if ap_action == "TERMINATE":
                    _approve_label = "Approve termination"
                elif ap_action == "LOAN_MODIFICATION":
                    _approve_label = "Approve modification"
                elif ap_action == "LOAN_MODIFICATION_SPLIT":
                    _approve_label = "Approve split & new loans"
                else:
                    _approve_label = "Approve and create loan"

                a1, a2, a3 = st.columns(3)
                with a1:
                    if st.button(_approve_label, type="primary", key="approve_create_loan_btn"):
                        try:

                            def _approve_and_copy_docs() -> tuple[int, int, str]:
                                new_loan_id = approve_loan_approval_draft(
                                    int(selected_id), approved_by="approver_ui"
                                )
                                doc_count = 0
                                refreshed = get_loan_approval_draft(int(selected_id))
                                dj = (refreshed or {}).get("details_json") or {}
                                _sids = dj.get("split_created_loan_ids")
                                if isinstance(_sids, list) and _sids:
                                    targets = []
                                    for _x in _sids:
                                        try:
                                            targets.append(int(_x))
                                        except (TypeError, ValueError):
                                            pass
                                    if not targets:
                                        targets = [int(new_loan_id)]
                                else:
                                    targets = [int(new_loan_id)]
                                    id_b = dj.get("split_created_loan_id_b")
                                    if id_b is not None:
                                        try:
                                            targets.append(int(id_b))
                                        except (TypeError, ValueError):
                                            pass
                                if documents_available:
                                    docs = list_documents(
                                        entity_type="loan_approval_draft", entity_id=int(selected_id)
                                    )
                                    for lid in targets:
                                        for row in docs:
                                            full = get_document(int(row["id"]))
                                            if not full:
                                                continue
                                            upload_document(
                                                "loan",
                                                int(lid),
                                                int(full["category_id"]),
                                                str(full["file_name"]),
                                                str(full["file_type"]),
                                                int(full["file_size"]),
                                                full["file_content"],
                                                uploaded_by="System User",
                                                notes=str(full.get("notes") or ""),
                                            )
                                            doc_count += 1
                                ap_done = str(dj.get("approval_action") or ap_action or "").strip().upper()
                                if ap_done == "LOAN_MODIFICATION":
                                    msg = (
                                        f"Modification approved. Loan #{new_loan_id}. "
                                        f"{doc_count} document copy operation(s)."
                                    )
                                elif ap_done == "LOAN_MODIFICATION_SPLIT":
                                    _ids_s = ", ".join(f"#{t}" for t in targets)
                                    msg = (
                                        f"Split approved. New loans {_ids_s}. "
                                        f"{doc_count} document copy operation(s)."
                                    )
                                elif ap_done == "TERMINATE":
                                    msg = (
                                        f"Termination approved for loan #{new_loan_id}. "
                                        f"{doc_count} document copy operation(s)."
                                    )
                                else:
                                    msg = (
                                        f"Loan approved successfully. Loan #{new_loan_id} created. "
                                        f"{doc_count} document(s) copied."
                                    )
                                return int(new_loan_id), doc_count, msg

                            loan_id, doc_count, flash_msg = run_with_spinner(
                                "Approving draft and copying documents…",
                                _approve_and_copy_docs,
                            )
                            st.session_state["approve_loans_flash_message"] = flash_msg
                            st.session_state.pop("approve_selected_draft_id", None)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not approve draft: {e}")
                with a2:
                    if st.button("Send back to schedule builder", key="approve_send_back_btn"):
                        try:
                            run_with_spinner(
                                "Sending draft back…",
                                lambda: send_back_loan_approval_draft(
                                    int(selected_id), note=note or "", actor="approver_ui"
                                ),
                            )
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
                            run_with_spinner(
                                "Dismissing draft…",
                                lambda: dismiss_loan_approval_draft(
                                    int(selected_id), note=note or "", actor="approver_ui"
                                ),
                            )
                            st.session_state["approve_loans_flash_message"] = (
                                f"Draft #{selected_id} dismissed."
                            )
                            st.session_state.pop("approve_selected_draft_id", None)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not dismiss draft: {e}")
    
                st.divider()
    
        # Inbox table always visible; draft ID opens inspection (same as Inspect draft).
        st.markdown("### Draft inbox")
        inject_tertiary_hyperlink_css_once()

        def _approve_inbox_fmt_submitted(v: object) -> str:
            if v is None:
                return "—"
            s = str(v).strip()
            return s[:19] if len(s) >= 19 else s

        _inbox_page_size = 35
        _n_drafts = len(drafts)
        _n_pages = max(1, (_n_drafts + _inbox_page_size - 1) // _inbox_page_size)
        if _n_pages > 1:
            st.session_state.setdefault("approve_inbox_page", 1)
            try:
                _pg_i = int(st.session_state["approve_inbox_page"])
            except (TypeError, ValueError):
                _pg_i = 1
            if _pg_i < 1 or _pg_i > _n_pages:
                _pg_i = min(max(1, _pg_i), _n_pages)
                st.session_state["approve_inbox_page"] = _pg_i
            _pg1, _pg2, _pg3 = st.columns([1, 2, 3])
            with _pg1:
                st.number_input(
                    "Page",
                    min_value=1,
                    max_value=_n_pages,
                    step=1,
                    key="approve_inbox_page",
                )
                _cur_pg = int(st.session_state["approve_inbox_page"])
            with _pg2:
                st.caption(
                    f"Showing {min(_n_drafts, (_cur_pg - 1) * _inbox_page_size + 1)}–"
                    f"{min(_n_drafts, _cur_pg * _inbox_page_size)} of {_n_drafts} drafts."
                )
            with _pg3:
                st.empty()
        else:
            st.session_state.pop("approve_inbox_page", None)
            _cur_pg = 1
        _slice_start = (_cur_pg - 1) * _inbox_page_size
        _slice = drafts[_slice_start : _slice_start + _inbox_page_size]

        _w = [0.52, 0.72, 1.05, 1.05, 0.92, 0.88, 1.35]
        _hc = st.columns(_w)
        _hdrs = [
            ("ID", "left"),
            ("Customer ID", "center"),
            ("Loan type", "center"),
            ("Product", "center"),
            ("Assigned", "center"),
            ("Status", "center"),
            ("Submitted", "center"),
        ]
        for _hi, (_ht, _al) in enumerate(_hdrs):
            with _hc[_hi]:
                st.markdown(
                    f'<p style="text-align:{_al};margin:0 0 0.2rem 0;font-weight:600;font-size:0.82rem;">'
                    f"{escape(_ht)}</p>",
                    unsafe_allow_html=True,
                )
        for r in _slice:
            rid = int(r["id"])
            rc = st.columns(_w)
            with rc[0]:
                if st.button(str(rid), key=f"approve_inbox_open_id_{rid}", type="tertiary"):
                    st.session_state["approve_selected_draft_id"] = rid
                    st.rerun()
            with rc[1]:
                st.markdown(
                    f'<p style="text-align:center;margin:0;font-size:0.88rem;">'
                    f"{escape(str(r.get('customer_id', '')))}</p>",
                    unsafe_allow_html=True,
                )
            with rc[2]:
                st.markdown(
                    f'<p style="text-align:center;margin:0;font-size:0.88rem;">'
                    f"{escape(str(r.get('loan_type') or '—'))}</p>",
                    unsafe_allow_html=True,
                )
            with rc[3]:
                st.markdown(
                    f'<p style="text-align:center;margin:0;font-size:0.88rem;">'
                    f"{escape(str(r.get('product_code') or '—'))}</p>",
                    unsafe_allow_html=True,
                )
            with rc[4]:
                st.markdown(
                    f'<p style="text-align:center;margin:0;font-size:0.88rem;">'
                    f"{escape(str(r.get('assigned_approver_id') or '—'))}</p>",
                    unsafe_allow_html=True,
                )
            with rc[5]:
                st.markdown(
                    f'<p style="text-align:center;margin:0;font-size:0.88rem;">'
                    f"{escape(str(r.get('status') or '—'))}</p>",
                    unsafe_allow_html=True,
                )
            with rc[6]:
                st.markdown(
                    f'<p style="text-align:center;margin:0;font-size:0.88rem;">'
                    f"{escape(_approve_inbox_fmt_submitted(r.get('submitted_at')))}</p>",
                    unsafe_allow_html=True,
                )
    
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


