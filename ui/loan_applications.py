"""Streamlit UI: loan application (prospect) pipeline — list, create, update, link to booked loan."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
import time
from typing import Any

import streamlit as st

from customers.national_id_validation import (
    NATIONAL_ID_FORMAT_HELP,
    is_valid_national_id_format,
    normalize_national_id_input,
)
from style import render_sub_sub_header
from ui.streamlit_feedback import run_with_spinner

_SK_CUSTOMER = "loan_app_selected_customer_id"
# Set when user opens Add Individual / Add Corporate from loan application wizard; cleared after return or when leaving those subtabs.
_SK_RETURN_FROM_CUSTOMER = "loan_app_return_after_customer_create"
# Queued before st.rerun; app main() applies this before st.sidebar.radio so sidebar state does not clobber programmatic nav.
_SK_FORCE_CUSTOMERS_NAV = "_loan_app_force_customers_nav"
# Queued before st.rerun; main.py must apply before streamlit-option-menu binds (same pattern as Customers nav).
_SK_FORCE_LOAN_CAPTURE_NAV = "_loan_app_force_loan_capture_nav"
_SK_INNER_TAB_INTENT = "loan_applications_inner_tab_intent"
_RETURN_INTENT_TTL_SECONDS = 900


def _apply_main_nav_at_startup(
    section: str,
    *,
    loan_mgmt_subnav: str | None = None,
    customers_subnav: str | None = None,
) -> None:
    """
    Set keys for ``main.py`` / ``app.py`` section routing before widgets bind.

    Must run only from ``consume_loan_app_navigation_intent()`` **before** the sidebar widget is created.
    Never call after sidebar widgets instantiate.
    """
    sec = str(section).strip()
    if not sec:
        return
    st.session_state["farnda_app_section_nav"] = sec
    st.session_state["farnda_main_nav_choice"] = sec
    if loan_mgmt_subnav:
        st.session_state["loan_mgmt_subnav"] = str(loan_mgmt_subnav)
    if customers_subnav is not None:
        st.session_state["customers_subnav"] = str(customers_subnav)


def queue_main_nav_after_widgets(
    section: str,
    *,
    loan_mgmt_subnav: str | None = None,
    customers_subnav: str | None = None,
) -> None:
    """Queue a nav change for the **next** run (safe from buttons inside page content after sidebar mounted)."""
    sec = str(section).strip()
    if not sec:
        return
    if customers_subnav is not None:
        st.session_state[_SK_FORCE_CUSTOMERS_NAV] = {
            "section": sec,
            "customers_subnav": str(customers_subnav),
        }
        return
    payload: dict[str, str] = {"section": sec}
    if loan_mgmt_subnav:
        payload["loan_mgmt_subnav"] = str(loan_mgmt_subnav)
    st.session_state[_SK_FORCE_LOAN_CAPTURE_NAV] = payload


def _customers_form_access_blocker() -> str | None:
    """Return a human-readable blocker when user cannot open Customers Add tabs."""
    try:
        from middleware import get_current_user
        from rbac.guards import user_can_open_nav_section
        user = get_current_user()
        if not user_can_open_nav_section(user, "Customers"):
            return "You do not have permission to open the Customers section."
    except Exception:
        pass
    try:
        from rbac.subfeature_access import customers_can_workspace
        if not customers_can_workspace():
            return "Your role can view Customers, but cannot open Add Individual/Add Corporate."
    except Exception:
        pass
    try:
        from subscription.access import RESTRICTED_NAV_SECTIONS, get_subscription_snapshot
        snap = get_subscription_snapshot()
        if snap and not snap.enforcement_skipped:
            if snap.terminated:
                return "Subscription is terminated; Customers section is unavailable."
            if snap.restricted_nav and "Customers" not in RESTRICTED_NAV_SECTIONS:
                return "Subscription restricted mode is blocking Customers navigation."
    except Exception:
        pass
    return None


def _loan_capture_jump_blocker() -> str | None:
    """Return a message when the user cannot open Loan management (needed for Jump to Loan Capture)."""
    try:
        from middleware import get_current_user
        from rbac.guards import user_can_open_nav_section

        user = get_current_user()
        if not user_can_open_nav_section(user, "Loan management"):
            return (
                "Your role cannot open **Loan management**. Ask an administrator to grant the "
                "**Loan management** navigation permission for your role (same area as Loan Capture)."
            )
    except Exception:
        pass
    try:
        from subscription.access import RESTRICTED_NAV_SECTIONS, get_subscription_snapshot

        snap = get_subscription_snapshot()
        if snap and not snap.enforcement_skipped:
            if snap.terminated:
                return "Subscription is terminated; Loan management is unavailable."
            if snap.restricted_nav and "Loan management" not in RESTRICTED_NAV_SECTIONS:
                return "Subscription restricted mode is blocking Loan management."
    except Exception:
        pass
    return None


def _queue_inner_tab_switch(tab_name: str) -> None:
    """Queue Loan Applications inner-tab switch for next rerun (safe after widget instantiation)."""
    st.session_state[_SK_INNER_TAB_INTENT] = tab_name


def consume_loan_app_navigation_intent() -> None:
    """Apply queued jumps before sidebar navigation widgets bind (``main.py`` option_menu + ``app.py`` radio)."""
    pay_lm = st.session_state.pop(_SK_FORCE_LOAN_CAPTURE_NAV, None)
    if isinstance(pay_lm, dict):
        sec_lm = pay_lm.get("section")
        sub_lm = pay_lm.get("loan_mgmt_subnav")
        if sec_lm:
            _apply_main_nav_at_startup(
                str(sec_lm),
                loan_mgmt_subnav=str(sub_lm) if sub_lm else None,
            )
        elif sub_lm:
            st.session_state["loan_mgmt_subnav"] = str(sub_lm)

    payload = st.session_state.pop(_SK_FORCE_CUSTOMERS_NAV, None)
    if not isinstance(payload, dict):
        return
    sec = payload.get("section")
    sub = payload.get("customers_subnav")
    if sec:
        _apply_main_nav_at_startup(str(sec), customers_subnav=str(sub) if sub else None)
    elif sub:
        st.session_state["customers_subnav"] = str(sub)


def navigate_to_customers_add_for_loan_application(*, subnav: str) -> None:
    """Open Customers → Add Individual or Add Corporate; after save, customer UI returns to Loan applications (stage 2)."""
    if subnav not in ("Add Individual", "Add Corporate"):
        return
    blocker = _customers_form_access_blocker()
    if blocker:
        st.error(blocker)
        return
    now = time.time()
    st.session_state[_SK_RETURN_FROM_CUSTOMER] = {
        "source": "loan_applications",
        "subnav": subnav,
        "armed_at": now,
        "expires_at": now + float(_RETURN_INTENT_TTL_SECONDS),
    }
    st.session_state[_SK_FORCE_CUSTOMERS_NAV] = {
        "section": "Customers",
        "customers_subnav": subnav,
    }
    try:
        st.toast("Opening Customers…", icon="👤")
    except Exception:
        pass
    st.rerun()


def _national_id_snapshot(cust: dict | None) -> str | None:
    if not cust:
        return None
    ind = cust.get("individual")
    if ind:
        s = (ind.get("national_id") or "").strip()
        return s if s else None
    corp = cust.get("corporate") or {}
    for k in ("reg_number", "tin"):
        s = (corp.get(k) or "").strip()
        if s:
            return s
    return None


def _sector_name(sectors: list[dict[str, Any]], sector_id: object) -> str:
    if sector_id is None:
        return "—"
    try:
        sid = int(sector_id)
    except (TypeError, ValueError):
        return str(sector_id)
    for s in sectors:
        if int(s["id"]) == sid:
            return str(s.get("name") or "").strip() or str(sid)
    return str(sid)


def _subsector_name(subsectors: list[dict[str, Any]], subsector_id: object) -> str:
    if subsector_id is None:
        return "—"
    try:
        ssid = int(subsector_id)
    except (TypeError, ValueError):
        return str(subsector_id)
    for ss in subsectors:
        if int(ss["id"]) == ssid:
            return str(ss.get("name") or "").strip() or str(ssid)
    return str(ssid)


def _customer_banner(cust: dict | None, customer_id: int) -> str:
    if not cust:
        return f"Customer **#{customer_id}**"
    if cust.get("type") == "individual" and cust.get("individual"):
        ind = cust["individual"]
        nm = ind.get("name") or ""
        nid = ind.get("national_id") or "—"
        return f"**{nm}** · ID: **{nid}** · customer #{customer_id}"
    if cust.get("corporate"):
        co = cust["corporate"]
        nm = co.get("trading_name") or co.get("legal_name") or ""
        reg = co.get("reg_number") or co.get("tin") or "—"
        return f"**{nm}** · Reg/TIN: **{reg}** · customer #{customer_id}"
    return f"Customer **#{customer_id}**"


def render_loan_applications_ui(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    customers_available: bool,
    search_customers_by_name: Callable[..., list[dict[str, Any]]],
    get_customer: Callable[[int], dict | None] | None,
    get_display_name: Callable[[int], str],
    create_individual: Callable[..., int] | None,
    create_corporate_with_entities: Callable[..., dict[str, Any]] | None,
    get_consumer_schemes: Callable[[], list[dict[str, Any]]],
    list_sectors: Callable[[], list[dict[str, Any]]],
    list_subsectors: Callable[..., list[dict[str, Any]]],
    list_agents: Callable[..., list[dict[str, Any]]],
    get_loan: Callable[[int], dict | None] | None,
) -> None:
    render_sub_sub_header("Loan pipeline")
    if not loan_management_available:
        st.error(f"Loan management module is not available. ({loan_management_error})")
        return

    try:
        from loan_management import (
            SKIP_PIPELINE_BUTTON_CODES,
            STATUS_PROSPECT,
            STATUS_SUPERSEDED,
            create_loan_application,
            effective_business_facility_subtypes,
            effective_loan_application_statuses,
            generate_agent_commission_invoices,
            get_agent_commission_invoice_pdf_bytes,
            get_loan_application,
            is_terminal_application_status,
            link_loan_to_application,
            list_agent_commission_invoices,
            mark_agent_commission_invoice_paid,
            recognise_agent_commission_eom,
            list_loan_applications,
            soft_delete_loan_application,
            status_label_for_code,
            supersede_loan_application,
            update_application_status,
            update_loan_application,
        )
    except ImportError as e:
        st.error(f"Loan applications API is unavailable: {e}")
        return

    flash = st.session_state.pop("loan_apps_flash", None)
    if flash:
        st.success(flash)

    if not customers_available or not get_customer:
        st.error("Customer module is required to create loan applications. Check database connection.")
        return

    _tab_opts = [
        "Find or Create Customer",
        "Loan Application",
        "Update status",
        "Link booked loan",
        "Generate invoices",
        "Commission payment",
    ]
    _pending_tab = st.session_state.pop(_SK_INNER_TAB_INTENT, None)
    if _pending_tab in _tab_opts:
        st.session_state["loan_applications_inner_tab"] = _pending_tab
    st.session_state.setdefault("loan_applications_inner_tab", _tab_opts[0])
    if st.session_state.get("loan_applications_inner_tab") not in _tab_opts:
        st.session_state["loan_applications_inner_tab"] = _tab_opts[0]
    active_tab = st.radio(
        "Loan application area",
        _tab_opts,
        key="loan_applications_inner_tab",
        horizontal=True,
        label_visibility="collapsed",
    )

    # ----- Tab 1: Find or Create Customer -----
    if active_tab == "Find or Create Customer":
        c1, c2, c3, c4 = st.columns([4.8, 1.2, 1.5, 1.5], gap="small")
        with c1:
            fq = st.text_input(
                "Search by name or customer #",
                key="la_find_q",
                placeholder="Type part of the name or numeric id",
                label_visibility="collapsed",
            )
        qstrip = (fq or "").strip()
        with c2:
            if st.button("Search", key="la_find_go", type="primary", use_container_width=True):
                def _do_search() -> list[dict[str, Any]]:
                    return search_customers_by_name(qstrip, limit=100, status="active")

                st.session_state["la_find_results"] = run_with_spinner(
                    "Searching customers…",
                    _do_search,
                )
                st.session_state["la_find_searched"] = True
                st.rerun()
        with c3:
            if st.button(
                "Add Individual",
                key="la_nav_add_individual",
                help="Create customer in-line",
                use_container_width=True,
            ):
                st.session_state["la_create_customer_mode"] = "individual"
        with c4:
            if st.button(
                "Add Corporate",
                key="la_nav_add_corporate",
                help="Create customer in-line",
                use_container_width=True,
            ):
                st.session_state["la_create_customer_mode"] = "corporate"
        res = st.session_state.get("la_find_results")
        searched = bool(st.session_state.get("la_find_searched"))
        if searched and res is not None and qstrip:
            if not res:
                st.info("No matching customers — try another term or create a new customer above.")
        # Only show rows when user explicitly searched with a non-empty term.
        if searched and qstrip and res:
            labels = [
                f"#{r['id']} — {r.get('display_name', '')} ({r.get('type', '')})" for r in res
            ]
            pick_ix = st.selectbox(
                "Matching customers",
                range(len(labels)),
                format_func=lambda i: labels[i],
                key="la_find_pick",
            )
            if st.button("Use selected customer", key="la_find_use", type="primary"):
                st.session_state[_SK_CUSTOMER] = int(res[pick_ix]["id"])
                st.session_state.pop("la_find_results", None)
                st.session_state.pop("la_find_searched", None)
                _queue_inner_tab_switch("Loan Application")
                st.rerun()

        create_mode = st.session_state.get("la_create_customer_mode")
        if create_mode == "individual":
            st.markdown("**Create Individual**")
            if not create_individual:
                st.error("Customer creation API unavailable for individuals.")
            else:
                with st.form("la_inline_create_individual"):
                    i1, i2, i3 = st.columns(3, gap="small")
                    with i1:
                        iname = st.text_input("Full name *", key="la_ci_name")
                    with i2:
                        inid = st.text_input(
                            "National ID *",
                            key="la_ci_national_id",
                            help=NATIONAL_ID_FORMAT_HELP,
                            placeholder="e.g. 1234567A12",
                        )
                    with i3:
                        iemp = st.text_input("Employer details", key="la_ci_employer")
                    i4, i5, i6 = st.columns(3, gap="small")
                    with i4:
                        iph1 = st.text_input("Phone 1", key="la_ci_phone1")
                    with i5:
                        iph2 = st.text_input("Phone 2", key="la_ci_phone2")
                    with i6:
                        iem1 = st.text_input("Email 1", key="la_ci_email1")
                    i7, i8, i9 = st.columns(3, gap="small")
                    with i7:
                        iem2 = st.text_input("Email 2", key="la_ci_email2")
                    sectors = list_sectors() or []
                    subsectors = list_subsectors() or []
                    with i8:
                        sec_names = ["(None)"] + [str(s.get("name") or "") for s in sectors]
                        sec_ix = st.selectbox(
                            "Sector",
                            range(len(sec_names)),
                            format_func=lambda i: sec_names[i],
                            key="la_ci_sector",
                        )
                        sec_id = None if sec_ix == 0 else int(sectors[int(sec_ix) - 1]["id"])
                    with i9:
                        ss_rows = [s for s in subsectors if sec_id and int(s["sector_id"]) == sec_id]
                        ss_names = ["(None)"] + [str(s.get("name") or "") for s in ss_rows]
                        ss_ix = st.selectbox(
                            "Subsector",
                            range(len(ss_names)),
                            format_func=lambda i: ss_names[i],
                            key="la_ci_subsector",
                        )
                        ss_id = None if ss_ix == 0 else int(ss_rows[int(ss_ix) - 1]["id"])
                    ca, cb = st.columns(2, gap="small")
                    with ca:
                        c_submit = st.form_submit_button("Create individual", type="primary")
                    with cb:
                        c_cancel = st.form_submit_button("Cancel")
                    if c_cancel:
                        st.session_state.pop("la_create_customer_mode", None)
                        st.rerun()
                    if c_submit:
                        if not (iname or "").strip():
                            st.error("Full name is required.")
                        elif not is_valid_national_id_format(inid):
                            st.error(f"National ID must match: {NATIONAL_ID_FORMAT_HELP}.")
                        else:
                            def _create_individual_now() -> int:
                                return create_individual(
                                    name=iname.strip(),
                                    national_id=normalize_national_id_input(inid),
                                    employer_details=(iemp or "").strip() or None,
                                    phone1=(iph1 or "").strip() or None,
                                    phone2=(iph2 or "").strip() or None,
                                    email1=(iem1 or "").strip() or None,
                                    email2=(iem2 or "").strip() or None,
                                    sector_id=sec_id,
                                    subsector_id=ss_id,
                                )
                            try:
                                new_id = run_with_spinner("Creating individual…", _create_individual_now)
                                st.session_state[_SK_CUSTOMER] = int(new_id)
                                st.session_state.pop("la_create_customer_mode", None)
                                _queue_inner_tab_switch("Loan Application")
                                st.session_state["loan_apps_flash"] = f"Customer **#{new_id}** created."
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
        elif create_mode == "corporate":
            st.markdown("**Create Corporate**")
            if not create_corporate_with_entities:
                st.error("Customer creation API unavailable for corporates.")
            else:
                cs1, cs2 = st.columns(2, gap="small")
                with cs1:
                    la_cc_n_sig = int(
                        st.number_input(
                            "Signatory rows",
                            min_value=1,
                            max_value=15,
                            value=max(1, min(15, int(st.session_state.get("la_cc_n_sig", 1)))),
                            step=1,
                            key="la_cc_n_sig_num",
                        )
                    )
                    st.session_state["la_cc_n_sig"] = la_cc_n_sig
                with cs2:
                    la_cc_n_dir = int(
                        st.number_input(
                            "Director rows",
                            min_value=1,
                            max_value=15,
                            value=max(1, min(15, int(st.session_state.get("la_cc_n_dir", 1)))),
                            step=1,
                            key="la_cc_n_dir_num",
                        )
                    )
                    st.session_state["la_cc_n_dir"] = la_cc_n_dir
                st.caption(
                    "Each signatory or director uses one row: name, national ID, phone. "
                    "Set signatory and director row counts above."
                )
                with st.form("la_inline_create_corporate"):
                    c1a, c1b, c1c = st.columns(3, gap="small")
                    with c1a:
                        legal = st.text_input("Legal name *", key="la_cc_legal")
                    with c1b:
                        trading = st.text_input("Trading name", key="la_cc_trading")
                    with c1c:
                        reg_no = st.text_input("Registration number", key="la_cc_reg")
                    c2a, c2b, c2c = st.columns(3, gap="small")
                    with c2a:
                        tin = st.text_input("TIN", key="la_cc_tin")
                    c_sectors = list_sectors() or []
                    c_subsectors = list_subsectors() or []
                    with c2b:
                        csec_names = ["(None)"] + [str(s.get("name") or "") for s in c_sectors]
                        csec_ix = st.selectbox(
                            "Sector",
                            range(len(csec_names)),
                            format_func=lambda i: csec_names[i],
                            key="la_cc_sector",
                        )
                        csec_id = None if csec_ix == 0 else int(c_sectors[int(csec_ix) - 1]["id"])
                    with c2c:
                        css_rows = [s for s in c_subsectors if csec_id and int(s["sector_id"]) == csec_id]
                        css_names = ["(None)"] + [str(s.get("name") or "") for s in css_rows]
                        css_ix = st.selectbox(
                            "Subsector",
                            range(len(css_names)),
                            format_func=lambda i: css_names[i],
                            key="la_cc_subsector",
                        )
                        css_id = None if css_ix == 0 else int(css_rows[int(css_ix) - 1]["id"])

                    st.markdown("**Signatories**")
                    n_sig = int(st.session_state.get("la_cc_n_sig", 1))
                    hs1, hs2, hs3 = st.columns(3, gap="small")
                    hs1.caption("Name")
                    hs2.caption("National ID")
                    hs3.caption("Phone")
                    for si in range(n_sig):
                        srow = st.columns(3, gap="small")
                        with srow[0]:
                            st.text_input(
                                "sig_nm",
                                key=f"la_cc_sig_nm_{si}",
                                label_visibility="collapsed",
                                placeholder="Full name",
                            )
                        with srow[1]:
                            st.text_input(
                                "sig_nid",
                                key=f"la_cc_sig_nid_{si}",
                                help=NATIONAL_ID_FORMAT_HELP if si == 0 else None,
                                label_visibility="collapsed",
                                placeholder="ID number",
                            )
                        with srow[2]:
                            st.text_input(
                                "sig_ph",
                                key=f"la_cc_sig_ph_{si}",
                                label_visibility="collapsed",
                                placeholder="Phone",
                            )
                    st.markdown("**Directors**")
                    n_dir = int(st.session_state.get("la_cc_n_dir", 1))
                    hd1, hd2, hd3 = st.columns(3, gap="small")
                    hd1.caption("Name")
                    hd2.caption("National ID")
                    hd3.caption("Phone")
                    for di in range(n_dir):
                        drow = st.columns(3, gap="small")
                        with drow[0]:
                            st.text_input(
                                "dir_nm",
                                key=f"la_cc_dir_nm_{di}",
                                label_visibility="collapsed",
                                placeholder="Full name",
                            )
                        with drow[1]:
                            st.text_input(
                                "dir_nid",
                                key=f"la_cc_dir_nid_{di}",
                                help=NATIONAL_ID_FORMAT_HELP if di == 0 else None,
                                label_visibility="collapsed",
                                placeholder="ID number",
                            )
                        with drow[2]:
                            st.text_input(
                                "dir_ph",
                                key=f"la_cc_dir_ph_{di}",
                                label_visibility="collapsed",
                                placeholder="Phone",
                            )
                    c5a, c5b = st.columns(2, gap="small")
                    with c5a:
                        corp_submit = st.form_submit_button("Create corporate", type="primary")
                    with c5b:
                        corp_cancel = st.form_submit_button("Cancel")
                    if corp_cancel:
                        st.session_state.pop("la_create_customer_mode", None)
                        st.session_state["la_cc_n_sig"] = 1
                        st.session_state["la_cc_n_dir"] = 1
                        st.session_state.pop("la_cc_n_sig_num", None)
                        st.session_state.pop("la_cc_n_dir_num", None)
                        st.rerun()
                    if corp_submit:
                        errs: list[str] = []
                        if not (legal or "").strip():
                            errs.append("Legal name is required.")
                        contact_persons_list: list[dict[str, Any]] = []
                        for si in range(n_sig):
                            snm = (st.session_state.get(f"la_cc_sig_nm_{si}") or "").strip()
                            snid = st.session_state.get(f"la_cc_sig_nid_{si}")
                            sph = (st.session_state.get(f"la_cc_sig_ph_{si}") or "").strip()
                            if not snm:
                                continue
                            if not is_valid_national_id_format(snid):
                                errs.append(
                                    f"Signatory row {si + 1}: national ID must match {NATIONAL_ID_FORMAT_HELP}."
                                )
                                continue
                            contact_persons_list.append(
                                {
                                    "full_name": snm,
                                    "national_id": normalize_national_id_input(snid),
                                    "designation": "Signatory",
                                    "phone1": sph or None,
                                }
                            )
                        directors_list: list[dict[str, Any]] = []
                        for di in range(n_dir):
                            dnm = (st.session_state.get(f"la_cc_dir_nm_{di}") or "").strip()
                            dnid = st.session_state.get(f"la_cc_dir_nid_{di}")
                            dph = (st.session_state.get(f"la_cc_dir_ph_{di}") or "").strip()
                            if not dnm:
                                continue
                            if not is_valid_national_id_format(dnid):
                                errs.append(
                                    f"Director row {di + 1}: national ID must match {NATIONAL_ID_FORMAT_HELP}."
                                )
                                continue
                            directors_list.append(
                                {
                                    "full_name": dnm,
                                    "national_id": normalize_national_id_input(dnid),
                                    "phone1": dph or None,
                                }
                            )
                        if errs:
                            for msg in errs:
                                st.error(msg)
                        else:
                            def _create_corporate_now() -> dict[str, Any]:
                                kw: dict[str, Any] = dict(
                                    legal_name=legal.strip(),
                                    trading_name=(trading or "").strip() or None,
                                    reg_number=(reg_no or "").strip() or None,
                                    tin=(tin or "").strip() or None,
                                    sector_id=csec_id,
                                    subsector_id=css_id,
                                )
                                if contact_persons_list:
                                    kw["contact_persons"] = contact_persons_list
                                if directors_list:
                                    kw["directors"] = directors_list
                                return create_corporate_with_entities(**kw)

                            try:
                                created = run_with_spinner("Creating corporate…", _create_corporate_now)
                                new_id = int(created["customer_id"])
                                st.session_state[_SK_CUSTOMER] = new_id
                                st.session_state.pop("la_create_customer_mode", None)
                                st.session_state["la_cc_n_sig"] = 1
                                st.session_state["la_cc_n_dir"] = 1
                                st.session_state.pop("la_cc_n_sig_num", None)
                                st.session_state.pop("la_cc_n_dir_num", None)
                                _queue_inner_tab_switch("Loan Application")
                                st.session_state["loan_apps_flash"] = f"Customer **#{new_id}** created."
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))

    # ----- Tab 2: Loan Application -----
    if active_tab == "Loan Application":
        wip_loan = st.session_state.get(_SK_CUSTOMER)
        if wip_loan is None:
            st.info("Select or create a customer in **Find or Create Customer** first.")
        else:
            wip_cid = int(wip_loan)
            cust_row = get_customer(wip_cid) or {}
            try:
                from core.config_manager import get_system_config

                pipe_cfg = get_system_config()
            except Exception:
                pipe_cfg = {}
            bf_facility_opts = effective_business_facility_subtypes(pipe_cfg)
            try:
                cust_display = (get_display_name(int(wip_cid)) or "").strip()
            except Exception:
                cust_display = ""
            if not cust_display:
                cust_display = _customer_banner(cust_row, wip_cid).replace("**", "")
            top_a, top_b, top_c = st.columns(3, gap="small")
            with top_a:
                st.text_input("Customer", value=cust_display, disabled=True, key="la_file_customer_display")
            with top_b:
                st.text_input("Customer ID", value=str(wip_cid), disabled=True, key="la_file_customer_id_display")
            with top_c:
                facility = st.radio(
                    "Facility type",
                    ["Consumer", "Business Loan"],
                    horizontal=True,
                    key="la_file_facility_type",
                )

            schemes = get_consumer_schemes() or []
            sectors_catalog = list_sectors() or []

            sel_product_code: str | None = None
            meta: dict[str, Any] = {"facility_type": facility}

            ag_rows = list_agents("active") or list_agents(None) or []
            ag_labels = ["— Select agent —"] + [f"{a.get('name', '')} (#{a['id']})" for a in ag_rows]
            ag_ids = [None] + [int(a["id"]) for a in ag_rows]

            if facility == "Consumer":
                cust_sid = cust_row.get("sector_id")
                cust_ssid = cust_row.get("subsector_id")
                subs_for_cust = list_subsectors(int(cust_sid)) if cust_sid else []
                st.caption(
                    "Sector (from customer): "
                    f"**{_sector_name(sectors_catalog, cust_sid)}** · "
                    f"Subsector: **{_subsector_name(subs_for_cust, cust_ssid)}**"
                )
                meta["sector_id"] = cust_sid
                meta["subsector_id"] = cust_ssid
                if not schemes:
                    st.warning(
                        "No **consumer schemes** — add active consumer-loan products with complete default rates under "
                        "**System configurations → Products**."
                    )
                mid_a, mid_b, mid_c = st.columns(3, gap="small")
                with mid_a:
                    if schemes:
                        sch_labels = [str(s.get("name") or "") for s in schemes]
                        scheme_ix = st.selectbox(
                            "Scheme *",
                            range(len(sch_labels)),
                            format_func=lambda i: sch_labels[i],
                            key="la_file_scheme_ix",
                            help="Rates come from each product’s consumer-loan defaults.",
                        )
                        sch = schemes[int(scheme_ix)]
                        sel_product_code = str(sch.get("product_code") or "").strip() or None
                        meta["consumer_scheme_label"] = sch.get("name")
                        meta["consumer_scheme_product_code"] = sel_product_code
                    else:
                        scheme_ix = 0
                        st.caption("— No schemes —")
                with mid_b:
                    la_principal = st.number_input(
                        "Amount requested (net proceeds) *",
                        min_value=0.0,
                        value=0.0,
                        step=100.0,
                        format="%.2f",
                        key="la_file_principal",
                        help="Net proceeds to the borrower; Loan Capture defaults **Net Proceeds** to this amount.",
                    )
                with mid_c:
                    ai = st.selectbox(
                        "Agent *",
                        range(len(ag_labels)),
                        format_func=lambda i: ag_labels[i],
                        key="la_file_agent",
                    )
                    sel_agent_id = ag_ids[int(ai)]
                st.caption("Rates, term, and currency are set at **Loan Capture**.")
            else:
                mid_a, mid_b, mid_c = st.columns(3, gap="small")
                with mid_a:
                    sub_ix = st.selectbox(
                        "Business facility *",
                        range(len(bf_facility_opts)),
                        format_func=lambda i: bf_facility_opts[i],
                        key="la_file_business_subtype",
                    )
                    bus_subtype = bf_facility_opts[int(sub_ix)]
                    meta["business_facility_subtype"] = bus_subtype
                with mid_b:
                    sec_names = ["— Select sector —"] + [str(s.get("name") or "") for s in sectors_catalog]
                    sec_ids = [None] + [int(s["id"]) for s in sectors_catalog]
                    s_ix = st.selectbox(
                        "Sector *",
                        range(len(sec_names)),
                        format_func=lambda i: sec_names[i],
                        key="la_file_bus_sector_ix",
                    )
                    bus_sector_id = sec_ids[int(s_ix)]
                    meta["sector_id"] = bus_sector_id
                with mid_c:
                    subs_opts = list_subsectors(bus_sector_id) if bus_sector_id else []
                    sub_names = ["— Subsector (optional) —"] + [str(x.get("name") or "") for x in subs_opts]
                    sub_ids = [None] + [int(x["id"]) for x in subs_opts]
                    if len(sub_names) <= 1:
                        meta["subsector_id"] = None
                        st.caption("Pick a sector to choose a subsector when configured.")
                    else:
                        u_ix = st.selectbox(
                            "Subsector",
                            range(len(sub_names)),
                            format_func=lambda i: sub_names[i],
                            key="la_file_bus_sub_ix",
                        )
                        meta["subsector_id"] = sub_ids[int(u_ix)]

                bot_a, bot_b, bot_c = st.columns(3, gap="small")
                with bot_a:
                    la_principal = st.number_input(
                        "Amount requested (net proceeds) *",
                        min_value=0.0,
                        value=0.0,
                        step=100.0,
                        format="%.2f",
                        key="la_file_principal",
                        help="Net proceeds to the borrower; Loan Capture defaults **Net Proceeds** to this amount.",
                    )
                with bot_b:
                    ai = st.selectbox(
                        "Agent *",
                        range(len(ag_labels)),
                        format_func=lambda i: ag_labels[i],
                        key="la_file_agent",
                    )
                    sel_agent_id = ag_ids[int(ai)]
                with bot_c:
                    st.caption("Rates, term, and currency are set at **Loan Capture**.")

            if st.button("Save loan application", key="la_file_save", type="primary"):
                errs: list[str] = []
                if la_principal <= 0:
                    errs.append("Enter an amount requested (net proceeds) greater than zero.")
                if not sel_agent_id:
                    errs.append("Select an agent.")
                if facility == "Consumer":
                    if not schemes:
                        errs.append("Configure at least one consumer scheme (product).")
                    elif not sel_product_code:
                        errs.append("Select a valid scheme.")
                else:
                    if not meta.get("sector_id"):
                        errs.append("Select a sector for business facilities.")
                if errs:
                    for e in errs:
                        st.error(e)
                else:
                    try:
                        snap = _national_id_snapshot(cust_row)
                        aid = create_loan_application(
                            customer_id=wip_cid,
                            agent_id=sel_agent_id,
                            national_id=snap,
                            requested_principal=float(la_principal),
                            product_code=sel_product_code,
                            metadata=meta,
                            status=STATUS_PROSPECT,
                            created_by="loan_applications_ui",
                        )
                        del st.session_state[_SK_CUSTOMER]
                        st.session_state["loan_apps_flash"] = f"Created loan application **#{aid}**."
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

    # ----- Tab 3: Update status -----
    if active_tab == "Update status":
        try:
            from core.config_manager import get_system_config

            cfg_pipe: dict[str, Any] = get_system_config()
        except Exception:
            cfg_pipe = {}

        render_sub_sub_header("Update application status")
        st.caption(
            "Use **Jump to Loan Capture** to book a loan; when the loan is saved and linked, status becomes **Disbursed** "
            "(BOOKED). Pipeline buttons and business facilities are under **System configurations → Loan pipeline**."
        )

        _prev_m = st.session_state.get("_la_us_mode")
        col_find_lbl, col_mode, col_q, col_go = st.columns([1.35, 2.15, 4.6, 1.3], gap="small")
        with col_find_lbl:
            st.markdown("**Find application**")
        with col_mode:
            sm = st.radio(
                "Mode",
                ["Application ID", "Customer name"],
                horizontal=True,
                label_visibility="collapsed",
                key="la_us_mode",
            )
        with col_q:
            if sm == "Application ID":
                _def_aid = int(st.session_state.get("la_us_focus") or 1)
                inp_id = st.number_input(
                    "Application ID",
                    min_value=1,
                    value=_def_aid,
                    key="la_us_app_num",
                    label_visibility="collapsed",
                    placeholder="Application ID",
                )
            else:
                st.text_input(
                    "Customer name",
                    key="la_us_cust_q",
                    label_visibility="collapsed",
                    placeholder="Customer name (partial match)",
                )
        load_clicked = False
        search_clicked = False
        with col_go:
            if sm == "Application ID":
                load_clicked = st.button("Load", key="la_us_load_app", type="primary", use_container_width=True)
            else:
                search_clicked = st.button("Search", key="la_us_search_name", type="primary", use_container_width=True)

        if _prev_m is not None and _prev_m != sm:
            st.session_state.pop("la_us_focus", None)
            st.session_state.pop("la_us_hits", None)
        st.session_state["_la_us_mode"] = sm

        app_rec: dict[str, Any] | None = None

        if sm == "Application ID":
            if load_clicked:
                row = get_loan_application(int(inp_id))
                if row:
                    st.session_state["la_us_focus"] = int(inp_id)
                    st.rerun()
                else:
                    st.session_state.pop("la_us_focus", None)
                    st.error("Application not found.")

            fid = st.session_state.get("la_us_focus")
            if fid:
                app_rec = get_loan_application(int(fid))
        else:
            q = (st.session_state.get("la_us_cust_q") or "").strip()
            if search_clicked:
                hits_s: list[dict[str, Any]] = []
                if q:
                    for c in search_customers_by_name(q, limit=80, status="active"):
                        cid = int(c["id"])
                        hits_s.extend(
                            list_loan_applications(customer_id=cid, limit=80, include_superseded=False)
                        )
                st.session_state["la_us_hits"] = hits_s
                st.rerun()

            hits = list(st.session_state.get("la_us_hits") or [])
            if hits:
                labels = [
                    f"#{a['id']} · {a.get('reference_number') or ''} · customer {a.get('customer_id')}"
                    for a in hits
                ]
                pick_ix = st.selectbox(
                    "Choose application",
                    range(len(labels)),
                    format_func=lambda i: labels[i],
                    key="la_us_pick",
                )
                app_rec = get_loan_application(int(hits[int(pick_ix)]["id"]))
            else:
                st.caption("Search returns applications linked to matching active customers.")

        if app_rec:
            cid = app_rec.get("customer_id")
            cust_nm = ""
            try:
                if cid is not None:
                    cust_nm = (get_display_name(int(cid)) or "").strip()
            except Exception:
                cust_nm = ""
            lab = status_label_for_code(app_rec.get("status"), cfg_pipe)
            st.markdown(f"**{app_rec.get('reference_number')}** · Application **#{app_rec['id']}**")
            st.info(
                f"**Current status:** {lab}  \n"
                f"`{app_rec.get('status')}` · Customer: **{cust_nm or '—'}** (#{cid}) · "
                f"loan_id={app_rec.get('loan_id')}"
            )

            defs = [
                r
                for r in effective_loan_application_statuses(cfg_pipe)
                if str(r.get("code") or "").strip().upper() not in SKIP_PIPELINE_BUTTON_CODES
            ]
            has_loan = bool(app_rec.get("loan_id"))
            st_now = (app_rec.get("status") or "").upper()
            superseded_already = st_now == STATUS_SUPERSEDED
            term = is_terminal_application_status(app_rec.get("status"), cfg_pipe)

            btn_cols = st.columns(len(defs), gap="small")
            for bi, row in enumerate(defs):
                code = str(row.get("code") or "").strip().upper()
                label = str(row.get("label") or code).strip() or code
                friendly = str(row.get("display_label") or "").strip() or label
                action = str(row.get("action") or "").strip().lower()
                safe_k = "".join(ch if ch.isalnum() else "_" for ch in code)[:36] or str(bi)

                dis = False
                if action == "soft_delete":
                    dis = has_loan or superseded_already
                elif action == "supersede":
                    dis = has_loan or superseded_already
                else:
                    dis = term or superseded_already

                with btn_cols[bi]:
                    if st.button(
                        label,
                        key=f"la_us_cap_{safe_k}_{bi}",
                        disabled=dis,
                        use_container_width=True,
                    ):
                        try:
                            aid = int(app_rec["id"])
                            if action == "soft_delete":
                                soft_delete_loan_application(
                                    aid,
                                    deleted_by="loan_applications_ui",
                                )
                                st.session_state.pop("la_us_focus", None)
                                st.session_state.pop("la_us_hits", None)
                                st.session_state["loan_apps_flash"] = "Application soft-deleted."
                            elif action == "supersede":
                                nid = supersede_loan_application(aid, created_by="loan_applications_ui")
                                st.session_state["loan_apps_flash"] = (
                                    f"Superseded; new application **#{nid}**."
                                )
                            else:
                                update_application_status(aid, code)
                                st.session_state["loan_apps_flash"] = (
                                    f"Status recorded as **{friendly}** (`{code}`)."
                                )
                            st.rerun()
                        except Exception as ex:
                            st.session_state["_la_us_err"] = str(ex)

            err = st.session_state.pop("_la_us_err", None)
            if err:
                st.error(err)

            if st.button("Jump to Loan Capture", key="la_jump_capture"):
                _jblk = _loan_capture_jump_blocker()
                if _jblk:
                    st.error(_jblk)
                else:
                    queue_main_nav_after_widgets(
                        "Loan management",
                        loan_mgmt_subnav="Loan Capture",
                    )
                    st.session_state["capture_prefill_application_id"] = int(app_rec["id"])
                    st.session_state["loan_apps_flash"] = (
                        f"Switched to Loan Capture — reference application **#{app_rec['id']}** "
                        "when you submit for approval."
                    )
                    st.rerun()
        else:
            st.info("Load or select an application to view status and update it.")

    # ----- Tab: Link booked loan -----
    if active_tab == "Link booked loan":
        render_sub_sub_header("Link booked loan")
        st.caption(
            "After **Approve Loans** created the loan, run this to set `source_application_id` on the loan "
            "and the commission accrual stub when an agent is on the loan."
        )
        lk1, lk2, lk3 = st.columns([1, 1, 2], gap="small")
        with lk1:
            link_loan = st.number_input("Loan id", min_value=1, value=1, key="la_link_loan")
        with lk2:
            link_app = st.number_input("Application id", min_value=1, value=1, key="la_link_app")
        with lk3:
            skip_comm = st.checkbox("Skip commission accrual row", key="la_skip_comm")
        if st.button("Link loan to application", key="la_link_btn", type="primary"):
            try:
                if not get_loan:
                    st.error("Loan lookup is unavailable.")
                else:
                    lr = get_loan(int(link_loan))
                    if not lr:
                        st.error("Loan not found.")
                    else:
                        link_loan_to_application(
                            int(link_loan),
                            int(link_app),
                            skip_commission_accrual=skip_comm,
                        )
                        st.success("Linked successfully.")
            except Exception as e:
                st.error(str(e))

    # ----- Tab: Generate invoices -----
    if active_tab == "Generate invoices":
        render_sub_sub_header("Generate agent commission invoices")
        st.caption(
            "Pick a date range (loan disbursement date) and one or more agents. "
            "The system groups uninvoiced commission accruals into one invoice per agent."
        )
        g1, g2, g3 = st.columns([1.2, 1.2, 3.2], gap="small")
        with g1:
            inv_from = st.date_input(
                "From",
                value=st.session_state.get("la_inv_from", date.today().replace(day=1)),
                key="la_inv_from",
            )
        with g2:
            inv_to = st.date_input(
                "To",
                value=st.session_state.get("la_inv_to", date.today()),
                key="la_inv_to",
            )
        ag_rows_inv = list_agents("active") or list_agents(None) or []
        ag_label_by_id = {int(a["id"]): f"{a.get('name', '')} (#{a['id']})" for a in ag_rows_inv}
        selected_agents = st.multiselect(
            "Agents",
            options=sorted(ag_label_by_id.keys()),
            format_func=lambda aid: ag_label_by_id.get(int(aid), str(aid)),
            key="la_inv_agents",
        )
        if st.button("Generate invoices", key="la_inv_generate", type="primary"):
            if inv_to < inv_from:
                st.error("To date must be on or after From date.")
            elif not selected_agents:
                st.error("Select at least one agent.")
            else:
                try:
                    created_ids = generate_agent_commission_invoices(
                        period_start=inv_from,
                        period_end=inv_to,
                        agent_ids=[int(x) for x in selected_agents],
                        created_by="loan_applications_ui",
                    )
                    if created_ids:
                        st.success(f"Created {len(created_ids)} invoice(s): {', '.join(str(x) for x in created_ids)}.")
                    else:
                        st.info("No eligible commission accruals found for the selected period/agents.")
                except Exception as e:
                    st.error(str(e))
        try:
            issued = list_agent_commission_invoices(status="ISSUED", limit=150)
        except Exception as e:
            st.warning(f"Could not load issued invoices: {e}")
            issued = []
        if issued:
            st.markdown("**Issued invoices (latest)**")
            st.dataframe(
                [
                    {
                        "Invoice ID": int(r["id"]),
                        "Invoice #": r.get("invoice_number"),
                        "Agent": r.get("agent_name") or f"#{r.get('agent_id')}",
                        "Period": f"{r.get('period_start')} → {r.get('period_end')}",
                        "Total": float(r.get("total_commission") or 0),
                        "Status": r.get("status"),
                    }
                    for r in issued
                ],
                width="stretch",
                hide_index=True,
            )
            _pdf_labels = {
                int(r["id"]): (
                    f"#{r['id']} · {r.get('invoice_number') or '—'} · "
                    + (str(r.get("agent_name")).strip() or f"Agent #{r.get('agent_id')}")
                )
                for r in issued
            }
            _pdf_pick = st.selectbox(
                "Invoice PDF",
                options=sorted(_pdf_labels.keys(), reverse=True),
                format_func=lambda i: _pdf_labels[int(i)],
                key="la_inv_pdf_pick",
                help="Print or save: open the downloaded PDF and use your viewer’s print/save.",
            )
            _pdf_bytes = get_agent_commission_invoice_pdf_bytes(int(_pdf_pick))
            _fn = (
                "agent_commission_"
                + "".join(
                    c if c.isalnum() or c in "-_" else "-"
                    for c in str(
                        next((x.get("invoice_number") for x in issued if int(x["id"]) == int(_pdf_pick)), "")
                        or _pdf_pick
                    )
                )
                + ".pdf"
            )
            if _pdf_bytes:
                st.download_button(
                    label="Download invoice PDF",
                    data=_pdf_bytes,
                    file_name=_fn,
                    mime="application/pdf",
                    key="la_inv_pdf_dl",
                    use_container_width=True,
                )
            else:
                st.caption("PDF could not be generated (missing invoice or ReportLab unavailable).")
        else:
            st.info("No issued invoices yet.")

    # ----- Tab: Commission payment -----
    if active_tab == "Commission payment":
        render_sub_sub_header("Commission payment and EOM recognition")
        st.caption(
            "Mark invoices as paid to settle accrued commission and post GL. "
            "Commission amortisation posts automatically at EOM; manual run below is for catch-up/replay."
        )
        p1, p2 = st.columns([2.8, 1.2], gap="small")
        with p1:
            inv_status = st.selectbox(
                "Invoice status",
                ["ISSUED", "PAID", "(All)"],
                key="la_pay_status",
            )
        with p2:
            inv_limit = st.number_input("Limit", min_value=20, max_value=500, value=200, step=10, key="la_pay_limit")
        status_filter = None if inv_status == "(All)" else inv_status
        try:
            invoices = list_agent_commission_invoices(status=status_filter, limit=int(inv_limit))
        except Exception as e:
            invoices = []
            st.error(str(e))
        if invoices:
            inv_labels = []
            for r in invoices:
                _agent_lbl = r.get("agent_name") or f"Agent #{r.get('agent_id')}"
                inv_labels.append(
                    f"#{r['id']} · {r.get('invoice_number')} · {_agent_lbl} · "
                    f"{float(r.get('total_commission') or 0):,.2f} · {r.get('status')}"
                )
            pick_ix = st.selectbox(
                "Select invoice",
                range(len(inv_labels)),
                format_func=lambda i: inv_labels[i],
                key="la_pay_pick",
            )
            inv = invoices[int(pick_ix)]
            st.markdown(
                f"**Invoice:** {inv.get('invoice_number')} · **Agent:** {inv.get('agent_name') or inv.get('agent_id')}  \n"
                f"**Period:** {inv.get('period_start')} → {inv.get('period_end')} · "
                f"**Total:** {float(inv.get('total_commission') or 0):,.2f} · "
                f"**Status:** {inv.get('status')}"
            )
            _pay_pdf = get_agent_commission_invoice_pdf_bytes(int(inv["id"]))
            _pay_fn = (
                "agent_commission_"
                + "".join(
                    c if c.isalnum() or c in "-_" else "-"
                    for c in str(inv.get("invoice_number") or inv["id"])
                )
                + ".pdf"
            )
            if _pay_pdf:
                st.download_button(
                    label="Download invoice PDF",
                    data=_pay_pdf,
                    file_name=_pay_fn,
                    mime="application/pdf",
                    key=f"la_pay_pdf_dl_{inv['id']}",
                    use_container_width=True,
                    help="Open the PDF to print or save; includes agent TIN/contact and loan summary.",
                )
            else:
                st.caption("PDF could not be generated for this invoice.")
            if str(inv.get("status") or "").upper() != "PAID":
                pay_date = st.date_input("Payment date", value=date.today(), key="la_pay_date")
                if st.button("Mark invoice as paid", key="la_pay_mark", type="primary"):
                    try:
                        mark_agent_commission_invoice_paid(
                            int(inv["id"]),
                            payment_date=pay_date,
                            created_by="loan_applications_ui",
                        )
                        st.success("Invoice marked as paid and commission payment GL posted.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
        else:
            st.info("No invoices found for the selected filter.")
        st.markdown("---")
        e1, e2 = st.columns([1.2, 3], gap="small")
        with e1:
            eom_date = st.date_input("EOM as-of date", value=date.today(), key="la_comm_eom_date")
        with e2:
            st.caption("Posts COMMISSION_AMORTISATION for unrecognised accruals up to the selected date.")
        if st.button("Run EOM commission recognition", key="la_comm_eom_run", type="primary"):
            try:
                cnt = recognise_agent_commission_eom(
                    as_of_date=eom_date,
                    created_by="loan_applications_ui",
                )
                st.success(f"Posted recognition journals: {cnt}.")
            except Exception as e:
                st.error(str(e))
