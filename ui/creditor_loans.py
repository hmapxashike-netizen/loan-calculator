"""Creditor (borrowing) mirror facilities UI — layout aligned with loan management (compact rows)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any

import pandas as pd
import streamlit as st

from style import render_sub_header, render_sub_sub_header
from display_formatting import format_display_currency
from ui.streamlit_feedback import run_with_spinner


def _creditor_perm(user: dict | None, key: str) -> bool:
    if not user:
        return False
    role = (user.get("role") or "").strip().upper()
    if role == "SUPERADMIN":
        return True
    try:
        from rbac.service import get_permission_keys_for_role_key, rbac_tables_ready

        if not rbac_tables_ready():
            return role in ("ADMIN", "SUPERADMIN")
        return key in get_permission_keys_for_role_key(role)
    except Exception:
        return role in ("ADMIN", "SUPERADMIN")


def _render_creditor_drawdown_doc_staging(
    *,
    documents_available: bool,
    list_document_categories: Callable[..., Any] | None,
    upload_document: Callable[..., Any] | None,
) -> None:
    """Stage files for attachment on drawdown commit (entity_type ``creditor_drawdown``)."""
    st.session_state.setdefault("cl_dd_docs_staged", [])
    staged: list[dict[str, Any]] = st.session_state["cl_dd_docs_staged"]
    if not documents_available or not upload_document or not list_document_categories:
        st.caption("Document uploads require the documents module and configuration.")
        return
    try:
        cats = list_document_categories(active_only=True) or []
    except Exception as ex:
        st.warning(f"Could not load document categories: {ex}")
        return
    names = sorted({str(c["name"]) for c in cats if c.get("name")})
    if not names:
        st.info("No document categories configured.")
        return
    cat_by_name = {str(c["name"]): c for c in cats if c.get("name")}
    d1, d2 = st.columns([2, 2], gap="small")
    with d1:
        pick = st.selectbox("Category", names, key="cl_dd_doc_cat")
    with d2:
        fu = st.file_uploader("File", type=["pdf", "png", "jpg", "jpeg"], key="cl_dd_doc_file")
    n1, n2 = st.columns([3, 1], gap="small")
    with n1:
        doc_notes = st.text_input("Notes (optional)", key="cl_dd_doc_notes")
    with n2:
        st.write("")
        st.write("")
        if st.button("Stage", key="cl_dd_doc_add") and fu is not None:
            row = cat_by_name[pick]
            staged.append(
                {
                    "category_id": int(row["id"]),
                    "file": fu,
                    "notes": (doc_notes or "").strip(),
                }
            )
            st.session_state["cl_dd_docs_staged"] = staged
            st.rerun()
    if staged:
        st.caption("**Staged for commit:** " + " · ".join(f"{r['file'].name}" for r in staged))


def render_creditor_loans_ui(
    *,
    get_system_date,
    get_cached_source_cash_account_entries,
    documents_available: bool = False,
    list_document_categories: Callable[..., Any] | None = None,
    upload_document: Callable[..., Any] | None = None,
    money_df_column_config: Callable[..., Any] | None = None,
) -> None:
    from middleware import get_current_user

    user = get_current_user()
    if not _creditor_perm(user, "creditor_loans.view"):
        st.error("You do not have permission to view Creditor loans.")
        return

    render_sub_header("Creditor loans (borrowings)")
    st.caption(
        "Mirror schedule and daily state are **separate** from debtor loans. "
        "Bank reconciliation and statement true-ups remain **manual** via Journals."
    )

    tabs = [
        "Counterparties",
        "Capture facility",
        "Capture drawdown",
        "Receipts",
        "Write-off",
        "Drawdowns & schedules",
    ]
    tab_objs = st.tabs(tabs)

    with tab_objs[0]:
        render_sub_sub_header("Counterparties (lenders)")
        st.session_state.setdefault("cp_counterparty_panel", None)
        _cp_toast = st.session_state.pop("cp_toast", None)
        if _cp_toast:
            st.success(_cp_toast)

        try:
            from creditor_loans.persistence import list_counterparties

            cps = list_counterparties(active_only=False)
        except Exception as e:
            st.error(f"Could not load counterparties: {e}")
            cps = []

        can_edit_cp = _creditor_perm(user, "creditor_loans.counterparties")
        if not can_edit_cp:
            st.warning("You need **creditor_loans.counterparties** to add or edit counterparties.")

        if can_edit_cp:
            ba, bb, _ = st.columns([1, 1, 6], gap="small")
            with ba:
                if st.button(
                    "Add",
                    key="cp_toggle_add",
                    help="Show or hide the new counterparty form.",
                ):
                    cur = st.session_state.get("cp_counterparty_panel")
                    st.session_state["cp_counterparty_panel"] = None if cur == "add" else "add"
            with bb:
                if st.button(
                    "Edit",
                    key="cp_toggle_edit",
                    help="Show or hide status update.",
                ):
                    cur = st.session_state.get("cp_counterparty_panel")
                    st.session_state["cp_counterparty_panel"] = None if cur == "edit" else "edit"

        _panel = st.session_state.get("cp_counterparty_panel") if can_edit_cp else None

        if can_edit_cp and _panel == "add":
            c1, c2, c3, c4 = st.columns([2, 2, 2, 2], gap="small")
            with c1:
                nm = st.text_input("Name", key="cp_name")
            with c2:
                refc = st.text_input("Reference code", key="cp_ref")
            with c3:
                tax = st.text_input("Tax / registration id", key="cp_tax")
            with c4:
                cp_st = st.selectbox(
                    "Status",
                    ["active", "inactive", "deleted"],
                    index=0,
                    key="cp_status_new",
                )
            if st.button("Save counterparty", type="primary", key="cp_save"):
                try:
                    from creditor_loans.persistence import create_counterparty

                    def _go():
                        create_counterparty(
                            nm.strip(),
                            reference_code=refc or None,
                            tax_id=tax or None,
                            status=cp_st,
                        )

                    run_with_spinner("Saving…", _go)
                    st.session_state["cp_toast"] = "Counterparty saved."
                    st.session_state["cp_counterparty_panel"] = None
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
            st.caption("Creditor ID is the database **id** shown in the table below.")

        if can_edit_cp and _panel == "edit":
            st.caption("Creditor ID is the database **id** shown in the table below.")
            try:
                from creditor_loans.persistence import update_counterparty_status

                cps_all = cps
                if cps_all:
                    u1, u2, u3 = st.columns([2, 2, 2], gap="small")
                    with u1:
                        upd_lab = st.selectbox(
                            "Counterparty (status update)",
                            [f"{c['name']} (#{c['id']})" for c in cps_all],
                            key="cp_upd_pick",
                        )
                    upd_id = next(int(c["id"]) for c in cps_all if f"{c['name']} (#{c['id']})" == upd_lab)
                    with u2:
                        upd_st = st.selectbox(
                            "New status",
                            ["active", "inactive", "deleted"],
                            key="cp_upd_status",
                        )
                    with u3:
                        st.write("")
                        st.write("")
                        if st.button("Apply status", key="cp_upd_go"):
                            try:

                                def _u():
                                    update_counterparty_status(upd_id, upd_st)

                                run_with_spinner("Updating…", _u)
                                st.session_state["cp_toast"] = "Status updated."
                                st.session_state["cp_counterparty_panel"] = None
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                else:
                    st.info("No counterparties to update yet.")
            except Exception:
                pass

        if cps:
            df_cp = pd.DataFrame(cps)
            if "id" in df_cp.columns:
                df_cp = df_cp.rename(columns={"id": "creditor_id"})
            st.dataframe(df_cp, hide_index=True, width="stretch", height=220)
        else:
            st.info("No counterparties yet.")

    with tab_objs[1]:
        render_sub_sub_header("Capture facility (limit, expiry, facility fee)")
        st.session_state.setdefault("cf_panel", None)
        _cf_toast = st.session_state.pop("cf_toast", None)
        if _cf_toast:
            st.success(_cf_toast)

        try:
            from creditor_loans.persistence import list_facilities as _list_fac_tbl

            facilities_tbl = _list_fac_tbl(status=None)
        except Exception as e:
            st.error(str(e))
            facilities_tbl = []

        can_cf = _creditor_perm(user, "creditor_loans.capture")
        if not can_cf:
            st.warning("You need **creditor_loans.capture**.")

        if can_cf:
            b1, b2, _ = st.columns([1, 1, 8], gap="small")
            with b1:
                if st.button("Add", key="cf_toggle_add", help="Show or hide new facility form."):
                    curp = st.session_state.get("cf_panel")
                    st.session_state["cf_panel"] = None if curp == "add" else "add"
            with b2:
                if st.button("Edit", key="cf_toggle_edit", help="Show or hide facility update."):
                    curp = st.session_state.get("cf_panel")
                    st.session_state["cf_panel"] = None if curp == "edit" else "edit"

        _cfp = st.session_state.get("cf_panel") if can_cf else None

        if can_cf and _cfp == "add":
            try:
                from creditor_loans.persistence import create_facility, list_counterparties

                cps = list_counterparties(active_only=True)
                if not cps:
                    st.info("Add an **active** counterparty first (Counterparties tab).")
                else:
                    f1, f2, f3, f4 = st.columns([2, 2, 2, 2], gap="small")
                    with f1:
                        cp_lab = st.selectbox("Counterparty", [f"{c['name']} (#{c['id']})" for c in cps], key="cf_cp")
                    cp_id = next(c["id"] for c in cps if f"{c['name']} (#{c['id']})" == cp_lab)
                    with f2:
                        flim = st.number_input("Facility limit", min_value=0.01, value=100000.0, key="cf_lim")
                    with f3:
                        fexp = st.date_input("Facility expiry", value=None, key="cf_exp")
                    with f4:
                        ffee = st.number_input("Facility fee (deferred)", min_value=0.0, value=0.0, key="cf_fee")
                    if st.button("Save facility", type="primary", key="cf_save"):
                        try:

                            def _sf():
                                return create_facility(
                                    int(cp_id),
                                    facility_limit=float(flim),
                                    facility_expiry_date=fexp,
                                    facility_fee_amount=float(ffee),
                                )

                            new_fid = run_with_spinner("Saving facility…", _sf)
                            st.session_state["cf_toast"] = (
                                f"Facility **#{new_fid}** created. Use **Capture drawdown** to fund a tranche."
                            )
                            st.session_state["cf_panel"] = None
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))
            except Exception as e:
                st.error(str(e))

        if can_cf and _cfp == "edit":
            try:
                from creditor_loans.persistence import update_facility

                if not facilities_tbl:
                    st.info("No facilities yet — use **Add**.")
                else:
                    flabs = [f"CF-{f['id']} — {f.get('counterparty_name', '')}" for f in facilities_tbl]
                    fx = st.selectbox("Facility", range(len(facilities_tbl)), format_func=lambda i: flabs[i], key="cf_edit_pick")
                    row = facilities_tbl[int(fx)]
                    fid = int(row["id"])
                    e1, e2, e3, e4 = st.columns([2, 2, 2, 2], gap="small")
                    with e1:
                        e_lim = st.number_input(
                            "Facility limit",
                            min_value=0.01,
                            value=float(row.get("facility_limit") or 0),
                            key="cf_edit_lim",
                        )
                    with e2:
                        exp_raw = row.get("facility_expiry_date")
                        e_exp = st.date_input(
                            "Facility expiry",
                            value=exp_raw if hasattr(exp_raw, "year") else None,
                            key="cf_edit_exp",
                        )
                    with e3:
                        e_fee = st.number_input(
                            "Facility fee (deferred)",
                            min_value=0.0,
                            value=float(row.get("facility_fee_amount") or 0),
                            key="cf_edit_fee",
                        )
                    with e4:
                        st_sel = str(row.get("status") or "active").lower()
                        ix = 0 if st_sel == "active" else (1 if st_sel == "inactive" else 2)
                        e_st = st.selectbox(
                            "Status",
                            ["active", "inactive", "deleted"],
                            index=min(ix, 2),
                            key="cf_edit_st",
                        )
                    if st.button("Save changes", type="primary", key="cf_edit_save"):

                        def _uf():
                            update_facility(
                                fid,
                                facility_limit=float(e_lim),
                                facility_expiry_date=e_exp,
                                facility_fee_amount=float(e_fee),
                                status=str(e_st),
                            )

                        run_with_spinner("Saving…", _uf)
                        st.session_state["cf_toast"] = f"Facility **CF-{fid}** updated."
                        st.session_state["cf_panel"] = None
                        st.rerun()
            except Exception as e:
                st.error(str(e))

        if facilities_tbl:
            _show = [
                {
                    "facility_id": f.get("id"),
                    "counterparty": f.get("counterparty_name"),
                    "facility_limit": f.get("facility_limit"),
                    "facility_expiry_date": f.get("facility_expiry_date"),
                    "facility_fee_amount": f.get("facility_fee_amount"),
                    "status": f.get("status"),
                }
                for f in facilities_tbl
            ]
            st.dataframe(pd.DataFrame(_show), hide_index=True, width="stretch", height=260)
        else:
            st.info("No facilities yet.")

    with tab_objs[2]:
        render_sub_sub_header("Capture drawdown (schedule + GL)")
        st.session_state.setdefault("dd_panel", None)
        _dd_toast = st.session_state.pop("dd_toast", None)
        if _dd_toast:
            st.success(_dd_toast)

        try:
            from creditor_loans.persistence import list_creditor_loans as _list_dd_tbl

            draws_tbl = _list_dd_tbl(status=None)
        except Exception as e:
            st.error(str(e))
            draws_tbl = []

        can_dd = _creditor_perm(user, "creditor_loans.capture")
        if not can_dd:
            st.warning("You need **creditor_loans.capture**.")

        if can_dd:
            db1, db2, _ = st.columns([1, 1, 8], gap="small")
            with db1:
                if st.button("Add", key="dd_toggle_add", help="Show or hide new drawdown capture."):
                    curp = st.session_state.get("dd_panel")
                    st.session_state["dd_panel"] = None if curp == "add" else "add"
            with db2:
                if st.button("Edit", key="dd_toggle_edit", help="Show or hide drawdown status / accrual."):
                    curp = st.session_state.get("dd_panel")
                    st.session_state["dd_panel"] = None if curp == "edit" else "edit"

        _ddp = st.session_state.get("dd_panel") if can_dd else None

        if can_dd and _ddp == "edit":
            try:
                from creditor_loans.persistence import update_creditor_drawdown

                if not draws_tbl:
                    st.info("No drawdowns yet — use **Add**.")
                else:
                    dd_labels = [
                        f"DD-{d['id']} CF-{d.get('creditor_facility_id', '')} — {d.get('counterparty_name', '')}"
                        for d in draws_tbl
                    ]
                    dix = st.selectbox(
                        "Drawdown",
                        range(len(draws_tbl)),
                        format_func=lambda i: dd_labels[i],
                        key="dd_edit_pick",
                    )
                    drow = draws_tbl[int(dix)]
                    did = int(drow["id"])
                    s1, s2 = st.columns(2)
                    with s1:
                        st_cur = str(drow.get("status") or "active").lower()
                        st_opts = ["active", "inactive"]
                        st_i = 0 if st_cur == "active" else 1
                        e_st = st.selectbox("Status", st_opts, index=st_i, key="dd_edit_st")
                    with s2:
                        am = str(drow.get("accrual_mode") or "periodic_schedule")
                        am_i = 1 if am == "daily_mirror" else 0
                        e_am = st.selectbox(
                            "Accrual mode",
                            ["periodic_schedule", "daily_mirror"],
                            format_func=lambda x: (
                                "Periodic (schedule)" if x == "periodic_schedule" else "Daily mirror"
                            ),
                            index=am_i,
                            key="dd_edit_am",
                        )
                    st.caption("Principal and schedule are not edited here.")
                    if st.button("Save changes", type="primary", key="dd_edit_save"):
                        try:

                            def _dd_u():
                                update_creditor_drawdown(did, status=str(e_st), accrual_mode=str(e_am))

                            run_with_spinner("Saving…", _dd_u)
                            st.session_state["dd_toast"] = f"Drawdown **DD-{did}** updated."
                            st.session_state["dd_panel"] = None
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
            except Exception as e:
                st.error(str(e))

        if can_dd and _ddp == "add":
            try:
                from core.config_manager import get_global_loan_settings
                from creditor_loans.debtor_engine_schedule import (
                    build_creditor_schedule_dataframe,
                    debtor_schedule_engine,
                    recompute_creditor_customised_from_editor,
                )
                from creditor_loans.persistence import list_creditor_loan_types, list_facilities
                from loans import add_months, days_in_month, is_last_day_of_month

                facs_row = list_facilities(status="active")
                types = list_creditor_loan_types()
                if not facs_row:
                    st.info("Create a **facility** first (Capture facility tab).")
                elif not types:
                    st.error("No creditor loan types in database (run migration 84).")
                else:
                    glob = get_global_loan_settings()
                    rate_basis = str(glob.get("rate_basis") or "Per month")
                    flat_rate = glob.get("interest_method") == "Flat rate"
                    per_ann = rate_basis == "Per annum"

                    fac_opts = [
                        f"CF-{f['id']} — {f.get('counterparty_name', '')} (limit {f.get('facility_limit', '')})"
                        for f in facs_row
                    ]
                    lt_opts = [f"{t['label']} ({t['code']})" for t in types]
                    r1c1, r1c2, r1c3, r1c4 = st.columns([2, 2, 2, 2], gap="small")
                    with r1c1:
                        fac_ix = st.selectbox(
                            "Facility",
                            range(len(facs_row)),
                            format_func=lambda i: fac_opts[i],
                            key="cl_fac_pick",
                        )
                    fac_id = int(facs_row[int(fac_ix)]["id"])
                    with r1c2:
                        lt_lab = st.selectbox("Loan type", lt_opts, key="cl_lt")
                    lt_code = next(t["code"] for t in types if f"{t['label']} ({t['code']})" == lt_lab)
                    type_row = next(t for t in types if t["code"] == lt_code)
                    behavior = type_row.get("behavior_json") or {}
                    engine = debtor_schedule_engine(behavior)

                    with r1c3:
                        principal = st.number_input("Principal", min_value=0.01, value=10000.0, key="cl_prin")
                    with r1c4:
                        term_m = st.number_input("Term (months)", min_value=1, value=12, key="cl_term")

                    r2c1, r2c2, r2c3 = st.columns([2, 2, 2], gap="small")
                    with r2c1:
                        disb = st.date_input("Disbursement date", value=get_system_date(), key="cl_disb")
                    disb_dt = datetime.combine(disb, datetime.min.time())
                    default_fr = add_months(disb_dt, 1).date()
                    with r2c2:
                        repay_on = st.selectbox(
                            "Repayments on",
                            ["Anniversary date (same day each month)", "Last day of each month"],
                            key="cl_repay_on",
                        )
                        use_anniversary = repay_on.startswith("Anniversary")
                    with r2c3:
                        if not use_anniversary:
                            default_fr = default_fr.replace(day=days_in_month(default_fr.year, default_fr.month))
                        first_rep = st.date_input("First repayment date", value=default_fr, key="cl_first_rep")
                    first_rep_dt = datetime.combine(first_rep, datetime.min.time())

                    consumer_m = 1.0
                    rate_headline = 12.0
                    bullet_type_label = "Straight bullet (no interim payments)"
                    r3c1, r3c2, r3c3 = st.columns([2, 2, 2], gap="small")
                    with r3c1:
                        if engine == "consumer_30_360":
                            st.caption("30/360 consumer engine (same as debtor calculator).")
                            consumer_m = st.number_input(
                                "Per month rate %",
                                min_value=0.0001,
                                value=1.0,
                                step=0.05,
                                format="%.4f",
                                key="cl_cons_m",
                            )
                            rate_headline = 0.0
                        else:
                            lab = "Annual rate %" if per_ann else "Per month rate %"
                            rate_headline = st.number_input(
                                lab,
                                min_value=0.0,
                                value=12.0 if per_ann else 1.0,
                                step=0.1,
                                format="%.4f",
                                key="cl_rate_head",
                            )
                    with r3c2:
                        if engine == "bullet_actual_360":
                            bullet_type_label = st.selectbox(
                                "Bullet type",
                                [
                                    "Straight bullet (no interim payments)",
                                    "Bullet with interest payments",
                                ],
                                key="cl_bullet_kind",
                            )
                    with r3c3:
                        end_d = st.date_input("End date (optional)", value=None, key="cl_end")

                    r3b1, r3b2, r3b3 = st.columns([2, 2, 2], gap="small")
                    with r3b1:
                        acc_mode = st.selectbox(
                            "Accrual mode",
                            ["periodic_schedule", "daily_mirror"],
                            format_func=lambda x: (
                                "Periodic (schedule due dates)"
                                if x == "periodic_schedule"
                                else "Daily mirror (daily accrual state)"
                            ),
                            index=0,
                            key="cl_accrual",
                        )
                    with r3b2:
                        st.number_input(
                            "Penalty rate % (reserved)",
                            min_value=0.0,
                            value=0.0,
                            disabled=True,
                            key="cl_pen_ph",
                        )
                    with r3b3:
                        st.caption(
                            "Drawdown fee amortises month-end via **BORROWING_FEES_AMORTISATION**. "
                            "Facility fee uses the **Capture facility** amount."
                        )

                    entries = get_cached_source_cash_account_entries()
                    labels = [f"{e['code']} — {e['name']}" for e in entries] if entries else []
                    ids = [str(e["id"]) for e in entries] if entries else []
                    r4c1, r4c2, r4c3 = st.columns([2, 2, 2], gap="small")
                    with r4c1:
                        cash_ix = (
                            st.selectbox(
                                "Cash / bank GL",
                                range(len(labels)),
                                format_func=lambda i: labels[i] if labels else "(none)",
                                key="cl_cash",
                            )
                            if labels
                            else None
                        )
                    with r4c2:
                        ddf = st.number_input("Drawdown fees", min_value=0.0, value=0.0, key="cl_ddf")
                    with r4c3:
                        st.caption(f"Schedule engine: **{engine}** (debtor `loans.py`).")

                    if st.button("Build schedule & preview", key="cl_prev"):
                        try:
                            if engine == "consumer_30_360":
                                if not use_anniversary and not is_last_day_of_month(first_rep_dt):
                                    ld = days_in_month(first_rep_dt.year, first_rep_dt.month)
                                    ex = datetime(first_rep_dt.year, first_rep_dt.month, ld).strftime("%d-%b-%Y")
                                    st.error(
                                        "For last-day-of-month repayments, first repayment must be the last "
                                        f"calendar day of its month (e.g. **{ex}**)."
                                    )
                                else:
                                    cm = float(consumer_m) / 100.0
                                    sch = build_creditor_schedule_dataframe(
                                        behavior_json=behavior,
                                        principal=float(principal),
                                        term_months=int(term_m),
                                        disbursement_date=disb_dt,
                                        rate_pct=float(rate_headline),
                                        rate_basis=rate_basis,
                                        flat_rate=flat_rate,
                                        use_anniversary=use_anniversary,
                                        first_repayment_date=first_rep_dt,
                                        consumer_monthly_rate=cm,
                                        bullet_type_label=bullet_type_label,
                                    )
                                    st.session_state["cl_sched_df"] = sch
                            elif engine == "bullet_actual_360":
                                if "with" in bullet_type_label.lower() or "interest" in bullet_type_label.lower():
                                    if not use_anniversary and not is_last_day_of_month(first_rep_dt):
                                        ld = days_in_month(first_rep_dt.year, first_rep_dt.month)
                                        ex = datetime(first_rep_dt.year, first_rep_dt.month, ld).strftime("%d-%b-%Y")
                                        st.error(
                                            "For last-day-of-month interest payments, first repayment must be the "
                                            f"last calendar day of its month (e.g. **{ex}**)."
                                        )
                                    else:
                                        st.session_state["cl_sched_df"] = build_creditor_schedule_dataframe(
                                            behavior_json=behavior,
                                            principal=float(principal),
                                            term_months=int(term_m),
                                            disbursement_date=disb_dt,
                                            rate_pct=float(rate_headline),
                                            rate_basis=rate_basis,
                                            flat_rate=flat_rate,
                                            use_anniversary=use_anniversary,
                                            first_repayment_date=first_rep_dt,
                                            consumer_monthly_rate=None,
                                            bullet_type_label=bullet_type_label,
                                        )
                                else:
                                    st.session_state["cl_sched_df"] = build_creditor_schedule_dataframe(
                                        behavior_json=behavior,
                                        principal=float(principal),
                                        term_months=int(term_m),
                                        disbursement_date=disb_dt,
                                        rate_pct=float(rate_headline),
                                        rate_basis=rate_basis,
                                        flat_rate=flat_rate,
                                        use_anniversary=use_anniversary,
                                        first_repayment_date=None,
                                        consumer_monthly_rate=None,
                                        bullet_type_label=bullet_type_label,
                                    )
                            else:
                                if float(rate_headline) <= 0 and engine != "customised_actual_360":
                                    st.error("Enter a positive interest rate.")
                                elif not use_anniversary and not is_last_day_of_month(first_rep_dt):
                                    ld = days_in_month(first_rep_dt.year, first_rep_dt.month)
                                    ex = datetime(first_rep_dt.year, first_rep_dt.month, ld).strftime("%d-%b-%Y")
                                    st.error(
                                        "For last-day-of-month repayments, first repayment must be the last "
                                        f"calendar day of its month (e.g. **{ex}**)."
                                    )
                                else:
                                    st.session_state["cl_sched_df"] = build_creditor_schedule_dataframe(
                                        behavior_json=behavior,
                                        principal=float(principal),
                                        term_months=int(term_m),
                                        disbursement_date=disb_dt,
                                        rate_pct=float(rate_headline),
                                        rate_basis=rate_basis,
                                        flat_rate=flat_rate,
                                        use_anniversary=use_anniversary,
                                        first_repayment_date=first_rep_dt,
                                        consumer_monthly_rate=None,
                                        bullet_type_label=bullet_type_label,
                                    )
                        except Exception as ex:
                            st.error(str(ex))

                    if st.session_state.get("cl_sched_df") is not None:
                        st.caption(
                            "Review **journal preview** and optional **documents**, then **Commit drawdown** "
                            "(save + post GL when disbursement is on or before the business date)."
                        )
                        if engine == "customised_actual_360":
                            st.caption("Edit **Payment** (and **Date** if needed); schedule recomputes each run.")
                            edited = st.data_editor(
                                st.session_state["cl_sched_df"],
                                num_rows="fixed",
                                hide_index=True,
                                width="stretch",
                                key="cl_sched_editor",
                            )
                            try:
                                st.session_state["cl_sched_df"] = recompute_creditor_customised_from_editor(
                                    edited,
                                    principal=float(principal),
                                    disbursement_date=disb_dt,
                                    rate_pct=float(rate_headline),
                                    rate_basis=rate_basis,
                                    flat_rate=flat_rate,
                                )
                            except Exception as ex:
                                st.warning(f"Recompute: {ex}")
                        else:
                            st.dataframe(st.session_state["cl_sched_df"], height=240, width="stretch")

                        render_sub_sub_header("Journal preview (drawdown)")
                        try:
                            from eod.system_business_date import get_effective_date

                            _biz_dd = get_effective_date()
                        except Exception:
                            _biz_dd = disb
                        if disb > _biz_dd:
                            st.caption(
                                "Disbursement date is **after** the current business date — the drawdown is still "
                                "saved on commit; **BORROWING_DRAWDOWN** posts when disbursement is on or before "
                                "the business date."
                            )
                        try:
                            from accounting.service import AccountingService
                            from creditor_loans.save_creditor_loan import build_borrowing_drawdown_journal_payload

                            _cash_gl_prev = ids[int(cash_ix)] if (labels and cash_ix is not None) else None
                            _pl = build_borrowing_drawdown_journal_payload(
                                principal=float(principal),
                                drawdown_fee_amount=float(ddf),
                                arrangement_fee_amount=0.0,
                                cash_gl_account_id=_cash_gl_prev,
                            )
                            sim = AccountingService().simulate_event("BORROWING_DRAWDOWN", payload=dict(_pl))
                            if sim.lines:
                                if not sim.balanced and sim.warning:
                                    st.warning(sim.warning)
                                df_j = pd.DataFrame(
                                    [
                                        {
                                            "Account": f"{line['account_name']} ({line['account_code']})",
                                            "Debit": float(line["debit"]),
                                            "Credit": float(line["credit"]),
                                        }
                                        for line in sim.lines
                                    ]
                                )
                                _jp_cfg = (
                                    dict(money_df_column_config(df_j))
                                    if callable(money_df_column_config)
                                    else None
                                )
                                if _jp_cfg and isinstance(_jp_cfg, dict):
                                    for _jc in ("Debit", "Credit"):
                                        if _jc in _jp_cfg and isinstance(_jp_cfg[_jc], dict):
                                            _jp_cfg[_jc] = {**_jp_cfg[_jc], "alignment": "right"}
                                st.dataframe(
                                    df_j,
                                    width="stretch",
                                    hide_index=True,
                                    height=min(220, 42 + len(sim.lines) * 36),
                                    column_config=_jp_cfg,
                                )
                            else:
                                st.info("No BORROWING_DRAWDOWN template lines (check accounting templates).")
                        except Exception as ex:
                            st.warning(f"Journal preview unavailable: {ex}")

                        render_sub_sub_header("Documents")
                        _render_creditor_drawdown_doc_staging(
                            documents_available=documents_available,
                            list_document_categories=list_document_categories,
                            upload_document=upload_document,
                        )

                        if st.button("Commit drawdown", type="primary", key="cl_commit"):
                            try:
                                from creditor_loans.save_creditor_loan import save_creditor_loan

                                def _save():
                                    if engine == "consumer_30_360":
                                        mdec = float(consumer_m) / 100.0
                                        ann_st = float(mdec * 12.0)
                                        mo_st = float(mdec)
                                    elif per_ann:
                                        ann_st = float(rate_headline) / 100.0
                                        mo_st = float(ann_st / 12.0)
                                    else:
                                        mo_st = float(rate_headline) / 100.0
                                        ann_st = float(mo_st * 12.0)
                                    details = {
                                        "principal": float(principal),
                                        "disbursement_date": disb,
                                        "end_date": end_d,
                                        "annual_rate": float(ann_st),
                                        "monthly_rate": float(mo_st),
                                        "term": int(term_m),
                                        "facility": float(principal),
                                        "drawdown_fee_amount": float(ddf),
                                        "arrangement_fee_amount": 0.0,
                                        "accrual_mode": str(acc_mode),
                                    }
                                    if labels and cash_ix is not None:
                                        details["cash_gl_account_id"] = ids[int(cash_ix)]
                                    return save_creditor_loan(
                                        creditor_facility_id=int(fac_id),
                                        creditor_loan_type_code=str(lt_code),
                                        details=details,
                                        schedule_df=st.session_state["cl_sched_df"],
                                        post_drawdown_gl=True,
                                    )

                                new_id = run_with_spinner("Saving drawdown…", _save)
                                staged_docs = list(st.session_state.pop("cl_dd_docs_staged", []) or [])
                                doc_errs: list[str] = []
                                doc_ok = 0
                                if upload_document and staged_docs:
                                    created_by = (
                                        str(user.get("username") or user.get("email") or "creditor_ui").strip()
                                        or "creditor_ui"
                                    )
                                    for row in staged_docs:
                                        f = row["file"]
                                        try:
                                            upload_document(
                                                "creditor_drawdown",
                                                int(new_id),
                                                int(row["category_id"]),
                                                f.name,
                                                f.type,
                                                f.size,
                                                f.getvalue(),
                                                uploaded_by=created_by,
                                                notes=str(row.get("notes") or ""),
                                            )
                                            doc_ok += 1
                                        except Exception as de:
                                            doc_errs.append(f"{f.name}: {de}")
                                _toast = f"Drawdown **#{new_id}** saved (under facility **CF-{fac_id}**)."
                                if doc_ok:
                                    _toast += f" {doc_ok} document(s) attached."
                                if doc_errs:
                                    _toast += " Document errors: " + "; ".join(doc_errs)
                                st.session_state["dd_toast"] = _toast
                                st.session_state["cl_sched_df"] = None
                                st.session_state["dd_panel"] = None
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
            except Exception as e:
                st.error(str(e))

        if draws_tbl:
            df_dd = pd.DataFrame(draws_tbl)
            _keep = [
                c
                for c in (
                    "id",
                    "creditor_facility_id",
                    "counterparty_name",
                    "principal",
                    "status",
                    "accrual_mode",
                    "disbursement_date",
                )
                if c in df_dd.columns
            ]
            st.dataframe(df_dd[_keep], hide_index=True, width="stretch", height=280)
        else:
            st.info("No drawdowns yet.")

    with tab_objs[3]:
        render_sub_sub_header("Creditor receipts")
        if not _creditor_perm(user, "creditor_loans.receipts"):
            st.warning("You need **creditor_loans.receipts**.")
        else:
            from accounting.service import AccountingService
            from services import teller_service

            created_by = str(user.get("username") or user.get("email") or "creditor_ui")
            biz = get_system_date()
            entries = get_cached_source_cash_account_entries()
            lbl2 = [f"{e['code']} — {e['name']}" for e in entries] if entries else []
            id2 = [str(e["id"]) for e in entries] if entries else []
            st.caption(
                "Drawdown receipts update the creditor mirror. **GL-only borrowing repayment** (expander below) "
                "posts **BORROWING_REPAYMENT** when there is no drawdown row or you only need the cash journals."
            )
            try:
                from creditor_loans import get_creditor_daily_state_balances
                from creditor_loans.persistence import list_creditor_loans

                facs = list_creditor_loans(status="active")
            except Exception as e:
                st.error(str(e))
                facs = []

            if facs:
                fopts = [
                    f"DD-{f['id']} CF-{f.get('creditor_facility_id', '')} {f.get('counterparty_name','')}"
                    for f in facs
                ]
                f1, f2, f3, f4 = st.columns([2, 2, 2, 2], gap="small")
                with f1:
                    flab = st.selectbox("Drawdown", fopts, key="cr_fac")
                fid = next(
                    int(f["id"])
                    for f in facs
                    if f"DD-{f['id']} CF-{f.get('creditor_facility_id', '')} {f.get('counterparty_name','')}"
                    == flab
                )
                bal = get_creditor_daily_state_balances(int(fid), biz) or {}
                with f2:
                    st.metric("Principal not due", format_display_currency(bal.get("principal_not_due", 0)))
                with f3:
                    st.metric("Interest arrears", format_display_currency(bal.get("interest_arrears_balance", 0)))
                with f4:
                    st.metric("Principal arrears", format_display_currency(bal.get("principal_arrears", 0)))
                r1, r2, r3, r4, r5 = st.columns([2, 2, 2, 2, 2], gap="small")
                with r1:
                    amt = st.number_input("Amount", min_value=0.01, value=100.0, key="cr_amt")
                with r2:
                    pdt = st.date_input("Payment date", value=biz, key="cr_pd")
                with r3:
                    vdt = st.date_input("Value date", value=biz, key="cr_vd")
                with r4:
                    cix = st.selectbox("Source cash GL", range(len(lbl2)), format_func=lambda i: lbl2[i], key="cr_cash") if lbl2 else None
                with r5:
                    ref = st.text_input("Reference", key="cr_ref")
                if lbl2 and cix is not None and st.button("Post receipt", type="primary", key="cr_post"):
                    try:
                        from loan_management import load_system_config_from_db
                        from creditor_loans.repayment_record import record_creditor_repayment

                        def _p():
                            return record_creditor_repayment(
                                int(fid),
                                float(amt),
                                pdt,
                                id2[int(cix)],
                                value_date=vdt,
                                reference=ref or None,
                                system_config=load_system_config_from_db(),
                            )

                        rid = run_with_spinner("Posting…", _p)
                        st.success(f"Repayment **#{rid}** posted.")
                    except Exception as ex:
                        st.error(str(ex))
            else:
                st.info("No active drawdowns — use **GL-only borrowing repayment** below for cash journals.")

            with st.expander("GL-only borrowing repayment (no facility record)", expanded=False):
                with st.form("cr_gl_borrowing_form"):
                    g1, g2, g3 = st.columns([2, 2, 2], gap="small")
                    with g1:
                        gl_vd = st.date_input("Value date", value=biz, key="cr_gl_vd")
                    with g2:
                        gl_prin = st.number_input("Principal portion", min_value=0.0, value=0.0, step=100.0, key="cr_gl_prin")
                    with g3:
                        gl_int = st.number_input("Interest portion", min_value=0.0, value=0.0, step=10.0, key="cr_gl_int")
                    g4, g5, g6 = st.columns([2, 2, 2], gap="small")
                    with g4:
                        gl_ref = st.text_input("Reference", key="cr_gl_ref")
                    with g5:
                        gl_desc = st.text_input("Narration", key="cr_gl_desc")
                    with g6:
                        gl_cix = (
                            st.selectbox("Source cash GL", range(len(lbl2)), format_func=lambda i: lbl2[i], key="cr_gl_cash")
                            if lbl2
                            else None
                        )
                    submitted_gl = st.form_submit_button("Post borrowing journals", type="primary")
                    if submitted_gl:
                        if not lbl2 or gl_cix is None:
                            st.error("Select a source cash GL account.")
                        else:
                            try:
                                acct_svc = AccountingService()

                                def _gl():
                                    teller_service.post_borrowing_repayment_journal(
                                        acct_svc,
                                        value_date=gl_vd,
                                        principal=Decimal(str(gl_prin)),
                                        interest=Decimal(str(gl_int)),
                                        reference=gl_ref or None,
                                        description=(gl_desc or "").strip() or "Payment of borrowings",
                                        created_by=created_by,
                                        account_overrides={"cash_operating": id2[int(gl_cix)]},
                                    )

                                run_with_spinner("Posting borrowing journals…", _gl)
                                st.success("Borrowing repayment journal posted.")
                            except Exception as ex:
                                st.error(str(ex))

    with tab_objs[4]:
        render_sub_sub_header("Write-off (creditor)")
        if not _creditor_perm(user, "creditor_loans.writeoff"):
            st.warning("You need **creditor_loans.writeoff** (superadmin-gated by default).")
        else:
            try:
                from creditor_loans.persistence import list_creditor_loans

                facs = list_creditor_loans(status="active")
                if not facs:
                    st.info("No active drawdowns.")
                else:
                    w1, w2, w3, w4 = st.columns([2, 2, 2, 2], gap="small")
                    with w1:
                        wlab = st.selectbox(
                            "Drawdown",
                            [f"DD-{f['id']} CF-{f.get('creditor_facility_id', '')}" for f in facs],
                            key="wo_fac",
                        )
                    wid = int(wlab.split()[0].replace("DD-", ""))
                    with w2:
                        pwo = st.number_input("Principal write-off", min_value=0.0, value=0.0, key="wo_p")
                    with w3:
                        iwo = st.number_input("Interest payable write-off", min_value=0.0, value=0.0, key="wo_i")
                    with w4:
                        ed = st.date_input("Entry date", value=get_system_date(), key="wo_ed")
                    if st.button("Post write-off journals", type="primary", key="wo_go"):
                        try:
                            from creditor_loans.write_off import post_creditor_writeoff

                            def _w():
                                post_creditor_writeoff(
                                    wid,
                                    principal_amount=Decimal(str(pwo)),
                                    interest_amount=Decimal(str(iwo)),
                                    entry_date=ed,
                                    created_by=str(user.get("username") or user.get("email") or "creditor_ui"),
                                )

                            run_with_spinner("Posting…", _w)
                            st.success("Write-off posted.")
                        except Exception as ex:
                            st.error(str(ex))
            except Exception as e:
                st.error(str(e))

    with tab_objs[5]:
        render_sub_sub_header("Drawdowns & mirror schedule")
        try:
            from creditor_loans.persistence import list_creditor_loans, get_creditor_schedule_lines

            facs = list_creditor_loans(status=None)
            if not facs:
                st.info("No drawdowns.")
            else:
                fvs = [
                    f"DD-{f['id']} CF-{f.get('creditor_facility_id', '')} — {f.get('counterparty_name','')}"
                    for f in facs
                ]
                sel = st.selectbox("Select drawdown", fvs, key="fv_sel")
                fid = next(
                    int(f["id"])
                    for f in facs
                    if f"DD-{f['id']} CF-{f.get('creditor_facility_id', '')} — {f.get('counterparty_name','')}"
                    == sel
                )
                lines = get_creditor_schedule_lines(fid)
                if lines:
                    st.dataframe(pd.DataFrame(lines), hide_index=True, width="stretch", height=320)
                else:
                    st.info("No schedule lines.")
        except Exception as e:
            st.error(str(e))
