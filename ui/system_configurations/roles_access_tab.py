"""System config: dynamic roles and human-oriented permission assignments."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable

import streamlit as st

from auth.permission_catalog import all_permission_records
from dal import get_conn
from rbac.repository import RbacRepository
from rbac.service import rbac_tables_ready
from style import render_sub_sub_header

# Product-style areas (like Creditor loans UI): sidebar + in-screen activities together.
# Order is the order of sections on this tab. Predicates are mutually exclusive for current catalog keys.
_RBAC_UI_AREAS: tuple[tuple[str, Callable[[str], bool]], ...] = (
    ("Customers", lambda pk: pk == "nav.customers" or pk.startswith("customers.")),
    (
        "Loan pipeline",
        lambda pk: pk == "nav.loan_applications" or pk.startswith("loan_applications."),
    ),
    (
        "Loan management",
        lambda pk: pk == "nav.loan_management" or pk.startswith("loan_management."),
    ),
    (
        "Creditor loans",
        lambda pk: pk == "nav.creditor_loans" or pk.startswith("creditor_loans."),
    ),
    ("Portfolio reports", lambda pk: pk == "nav.portfolio_reports" or pk.startswith("portfolio_reports.")),
    ("Teller", lambda pk: pk == "nav.teller" or pk.startswith("teller.")),
    ("Reamortisation", lambda pk: pk == "nav.reamortisation" or pk.startswith("reamortisation.")),
    ("Statements", lambda pk: pk == "nav.statements" or pk.startswith("statements.")),
    (
        "Accounting",
        lambda pk: pk == "nav.accounting" or pk.startswith("accounting."),
    ),
    (
        "Journals",
        lambda pk: pk == "nav.journals" or pk.startswith("journals."),
    ),
    ("Notifications", lambda pk: pk == "nav.notifications" or pk.startswith("notifications.")),
    (
        "Document management",
        lambda pk: pk == "nav.document_management" or pk.startswith("document_management."),
    ),
    ("End of day", lambda pk: pk == "nav.end_of_day" or pk.startswith("eod.")),
    (
        "System configurations",
        lambda pk: pk == "nav.system_configurations" or pk == "dashboard.admin",
    ),
    ("Subscription", lambda pk: pk == "nav.subscription" or pk.startswith("subscription.")),
    (
        "Security & reserved",
        lambda pk: pk.startswith("reserved.") or pk.startswith("security."),
    ),
)

_FALLBACK_CATEGORY_ORDER = (
    "Navigation",
    "Loan management",
    "Creditor loans",
    "Reamortisation",
    "Statements",
    "Document Management",
    "End of day",
    "System configurations",
    "Subscription",
    "Portfolio reports",
    "Customers",
    "Accounting",
    "Journals",
    "Teller",
    "Security",
)

# Second-level headings within a product area (operational split).
_SUBGROUP_BY_PK: dict[str, tuple[int, str]] = {
    "nav.reamortisation": (0, "Sidebar — open Reamortisation"),
    "reamortisation.approve_modifications": (1, "1 · Approve modification"),
    "reamortisation.direct_principal": (2, "2 · Direct principal"),
    "reamortisation.general_workspace": (3, "3 · Loan modification, recast & unapplied"),
    "nav.statements": (0, "Sidebar — open Statements"),
    "statements.debtor_loans": (1, "1 · Debtor (customer) loan statements"),
    "statements.creditor_loans": (2, "2 · Creditor loan statements"),
    "statements.gl": (3, "3 · General ledger"),
    "nav.document_management": (0, "Sidebar — open Document management"),
    "document_management.view": (1, "1 · View & download"),
    "document_management.edit": (2, "2 · Configure classes/categories & upload"),
    "nav.system_configurations": (0, "Sidebar — open System configurations"),
    "dashboard.admin": (1, "1 · Admin Dashboard (main menu)"),
    "nav.end_of_day": (0, "Sidebar — open End of day"),
    "eod.advance_system_date": (1, "1 · EOD advance (run / advance system date)"),
    "eod.fix_issues": (2, "2 · Fix EOD issues"),
    "nav.subscription": (0, "Sidebar — open Subscription"),
    "subscription.tenant_account": (1, "1 · Tenant / client (organisation account)"),
    "subscription.vendor_console": (2, "2 · Vendor console"),
    "subscription.platform_admin": (3, "3 · Platform superadmin"),
    "nav.teller": (0, "Sidebar — open Teller"),
    "teller.single_receipt": (1, "1 · Single receipt"),
    "teller.batch_and_reverse": (2, "2 · Batch processing & reverse receipts"),
    "teller.scheduled_receipts": (3, "3 · Scheduled receipts"),
    "nav.portfolio_reports": (0, "Sidebar — open Portfolio reports"),
    "portfolio_reports.view_reports": (1, "1 · Reports (view-only analyses)"),
    "portfolio_reports.data_exports": (2, "2 · Data exports"),
    "nav.customers": (0, "Sidebar — open Customers"),
    "nav.loan_applications": (0, "Sidebar — open Loan pipeline"),
    "customers.approve": (1, "1 · Approvals"),
    "customers.view_only": (2, "2 · View only (read-oriented)"),
    "customers.workspace": (3, "3 · Capture, agents & batch"),
    "nav.loan_management": (0, "Sidebar — open Loan management"),
    "loan_management.schedules_repayments": (1, "1 · Schedules & repayments (view)"),
    "loan_management.approve_loans": (2, "2 · Approve loans"),
    "loan_management.batch_capture": (3, "3 · Batch loan capture (migration)"),
    "nav.accounting": (0, "Sidebar — open Accounting"),
    "accounting.chart_templates_mapping": (1, "1 · Chart, templates & receipt mapping"),
    "accounting.financial_reports": (2, "2 · Financial reports"),
    "accounting.bank_reconciliation": (3, "3 · Bank reconciliation"),
    "accounting.supervise": (4, "Accounting — supervise"),
    "nav.journals": (0, "Sidebar — open Journals"),
    "journals.manual": (1, "1 · Manual journal"),
    "journals.balance_adjustment": (2, "2 · Balance adjustment"),
    "journals.approvals": (3, "3 · Journal approvals"),
    "nav.creditor_loans": (0, "Sidebar — open Creditor loans"),
    "creditor_loans.view": (1, "1 · View"),
    "creditor_loans.capture": (2, "2 · Capture"),
    "creditor_loans.receipts": (3, "3 · Receipts"),
    "creditor_loans.counterparties": (4, "4 · Counterparties"),
    "creditor_loans.writeoff": (5, "5 · Write-off"),
    "nav.notifications": (0, "Sidebar — open Notifications"),
    "notifications.send": (1, "1 · Send notification"),
    "notifications.history": (2, "2 · History"),
    "notifications.templates": (3, "3 · Templates"),
}


def _subgroup_order_and_label(pk: str) -> tuple[int, str | None]:
    meta = _SUBGROUP_BY_PK.get(str(pk))
    if not meta:
        return (99, None)
    return meta


def _ui_area_title_for_permission_key(pk: str) -> tuple[int, str]:
    for i, (title, pred) in enumerate(_RBAC_UI_AREAS):
        if pred(pk):
            return (i, title)
    return (900, "Other")


def _ui_area_title_for_row(row: dict) -> tuple[int, str]:
    pk = str(row.get("permission_key") or "")
    idx, title = _ui_area_title_for_permission_key(pk)
    if idx >= 900:
        cat = str(row.get("category") or "Other")
        try:
            sub = _FALLBACK_CATEGORY_ORDER.index(cat)
        except ValueError:
            sub = 50
        return (900 + sub, cat)
    return (idx, title)


def _perm_widget_id(role_id: int, permission_key: str) -> str:
    safe = permission_key.replace(".", "_").replace(" ", "_")
    return f"rbac_perm_{role_id}_{safe}"


def _row_area_sort_key(row: dict) -> tuple[int, str]:
    pk = str(row.get("permission_key") or "")
    nav_first = 0 if pk.startswith("nav.") else 1
    return (nav_first, str(row.get("label") or ""))


def _row_matches_filter(row: dict, q: str) -> bool:
    if not q:
        return True
    _, area_title = _ui_area_title_for_row(row)
    blob = " ".join(
        str(row.get(k) or "")
        for k in ("permission_key", "label", "category", "summary", "grants_md")
    )
    blob = f"{blob} {area_title}".lower()
    return q in blob


def _area_help_caption(rows: list[dict]) -> str | None:
    pks = [str(r.get("permission_key") or "") for r in rows]
    has_nav = any(p.startswith("nav.") for p in pks)
    has_feature = any(not p.startswith("nav.") for p in pks if p)
    if has_nav and has_feature:
        return (
            "Sidebar entry plus **in-screen** activities for this product area "
            "(different roles can mix sidebar vs feature ticks)."
        )
    if has_nav:
        return "Opens this product area from the sidebar."
    return None


def _sorted_area_titles(perms_by_area: dict[str, list[dict]]) -> list[str]:
    def area_sort_key(title: str) -> tuple[int, str]:
        rows = perms_by_area[title]
        idx = min(_ui_area_title_for_row(r)[0] for r in rows)
        return (idx, title.lower())

    return sorted(perms_by_area.keys(), key=area_sort_key)


def render_roles_access_tab(*, get_current_user: Callable[..., dict | None]) -> None:
    user = get_current_user() or {}
    actor_role = str(user.get("role") or "").strip().upper()
    if actor_role not in ("ADMIN", "SUPERADMIN"):
        st.warning("This tab is only available to organisation administrators.")
        return
    actor_is_superadmin = actor_role == "SUPERADMIN"

    if not rbac_tables_ready():
        st.error(
            "RBAC tables are not installed. Apply migration: "
            "`python scripts/run_migration_78.py` (schema `78_rbac_dynamic_roles.sql`)."
        )
        return

    st.markdown("### Roles and access")
    top_l, top_r = st.columns([2.6, 1.1], gap="small", vertical_alignment="top")
    with top_l:
        st.caption(
            "**Row 1:** role · **Row 2:** filter / presets / **Save**. Then tick activities below. "
            "**Super administrator** is not listed. Only a superadmin may edit the **Administrator** role, "
            "copy its permissions, or assign **Admin Dashboard**."
        )
    with top_r:
        with st.expander("DB mapping", expanded=False):
            st.markdown(
                """
Each **activity** (tick row) is one row in **`rbac_permissions`** (`permission_key`, label, category, …).

- **Navigation** rows usually gate a **sidebar section**; other rows gate **features** inside an area.

**Save** writes **`rbac_role_permissions`** (role ↔ permission). **`users.role`** is a `rbac_roles.role_key`; the app loads keys from **`rbac_role_permissions`**. **`rbac_roles`** lists role names (built-in and custom).
                """
            )

    conn = get_conn()
    try:
        repo = RbacRepository(conn)
        roles = repo.list_roles()
    finally:
        conn.close()

    roles_editable = [r for r in roles if str(r.get("role_key") or "").strip().upper() != "SUPERADMIN"]
    role_labels = {r["role_key"]: f"{r['display_name']}  (`{r['role_key']}`)" for r in roles_editable}
    role_keys_ordered = [r["role_key"] for r in roles_editable]

    if not role_keys_ordered:
        st.warning("No roles found.")
        return

    row1_a, row1_b, row1_c = st.columns([3.4, 0.55, 1.05], gap="small", vertical_alignment="bottom")
    with row1_a:
        sel_key = st.selectbox(
            "Role",
            role_keys_ordered,
            format_func=lambda k: role_labels.get(k, k),
            key="rbac_edit_role_select",
            help="Super administrator is not listed; that role is not edited here.",
        )
    sel = next((r for r in roles_editable if r["role_key"] == sel_key), None)
    if not sel:
        st.stop()

    rid = int(sel["id"])
    is_system = bool(sel.get("is_system"))
    admin_permissions_read_only = sel_key.strip().upper() == "ADMIN" and not actor_is_superadmin

    conn = get_conn()
    try:
        repo = RbacRepository(conn)
        perms_db = repo.list_permissions()
        current = repo.get_permission_keys_for_role_key(sel_key)
    finally:
        conn.close()

    n_on = len(current)
    with row1_b:
        st.caption(f"**{n_on}** on")
    with row1_c:
        if not is_system:
            if st.button(
                "Delete role",
                key="rbac_del_role",
                help="Custom roles only; ensure no users still use this role.",
            ):
                try:
                    c3 = get_conn()
                    try:
                        r3 = RbacRepository(c3)
                        if r3.delete_role_if_custom(rid):
                            st.success("Role deleted.")
                            st.rerun()
                        st.error("Could not delete role (system roles or missing row).")
                    finally:
                        c3.close()
                except Exception as e:
                    st.error(str(e))

    if admin_permissions_read_only:
        st.caption("**Administrator** is view-only for you; a superadmin must edit it.")

    perms_by_area: dict[str, list[dict]] = defaultdict(list)
    for row in perms_db:
        _, title = _ui_area_title_for_row(row)
        perms_by_area[title].append(row)
    for title in perms_by_area:
        perms_by_area[title].sort(key=_row_area_sort_key)

    if actor_is_superadmin:
        r2_a, r2_b, r2_c, r2_d, r2_e = st.columns(
            [2.35, 1.0, 1.0, 1.05, 1.15], gap="small", vertical_alignment="bottom"
        )
    else:
        r2_a, r2_b, r2_c, r2_d = st.columns([2.5, 1.05, 1.05, 1.15], gap="small", vertical_alignment="bottom")
        r2_e = None
    with r2_a:
        filt = st.text_input(
            "Filter",
            placeholder="Filter by name or id…",
            key="rbac_activity_filter",
        )
    q = (filt or "").strip().lower()

    with r2_b:
        if st.button(
            "Preset → Loan officer",
            key="rbac_pre_lo",
            disabled=admin_permissions_read_only,
            help="Replace ticks with built-in Loan officer.",
        ):
            try:
                c4 = get_conn()
                try:
                    r4 = RbacRepository(c4)
                    r4.clone_permissions_from_role(
                        rid, "LOAN_OFFICER", actor_is_superadmin=actor_is_superadmin
                    )
                finally:
                    c4.close()
                st.success("Replaced from Loan officer.")
                st.rerun()
            except PermissionError as e:
                st.error(str(e))
            except Exception as e:
                st.error(str(e))
    with r2_c:
        if st.button(
            "Preset → Administrator",
            key="rbac_pre_ad",
            disabled=admin_permissions_read_only or not actor_is_superadmin,
            help="Replace ticks with built-in Administrator (superadmin only).",
        ):
            try:
                c5 = get_conn()
                try:
                    r5 = RbacRepository(c5)
                    r5.clone_permissions_from_role(rid, "ADMIN", actor_is_superadmin=actor_is_superadmin)
                finally:
                    c5.close()
                st.success("Replaced from Administrator.")
                st.rerun()
            except PermissionError as e:
                st.error(str(e))
            except Exception as e:
                st.error(str(e))
    with r2_d:
        if st.button(
            "Save",
            type="primary",
            key="rbac_save",
            disabled=admin_permissions_read_only,
            help="Save ticks for this role.",
        ):
            new_sel: set[str] = set()
            for row in perms_db:
                pk = str(row["permission_key"])
                restricted = bool(row.get("grant_restricted_to_superadmin"))
                wid = _perm_widget_id(rid, pk)
                rendered = _row_matches_filter(row, q)
                if not rendered:
                    if restricted and not actor_is_superadmin:
                        if pk in current:
                            new_sel.add(pk)
                    elif wid in st.session_state:
                        if bool(st.session_state[wid]):
                            new_sel.add(pk)
                    elif pk in current:
                        new_sel.add(pk)
                    continue
                if restricted and not actor_is_superadmin:
                    if pk in current:
                        new_sel.add(pk)
                else:
                    if st.session_state.get(wid, False):
                        new_sel.add(pk)
            try:
                c6 = get_conn()
                try:
                    r6 = RbacRepository(c6)
                    r6.replace_role_permissions(rid, new_sel, actor_is_superadmin=actor_is_superadmin)
                finally:
                    c6.close()
                st.success("Saved.")
                st.rerun()
            except PermissionError as e:
                st.error(str(e))
            except Exception as e:
                st.error(str(e))
    if r2_e is not None:
        with r2_e:
            if st.button(
                "Sync catalog",
                key="rbac_sync_cat",
                help="Upsert rbac_permissions from code (superadmin).",
            ):
                try:
                    c7 = get_conn()
                    try:
                        r7 = RbacRepository(c7)
                        r7.sync_permissions_from_catalog(all_permission_records())
                    finally:
                        c7.close()
                    st.success("Catalog synced.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    st.markdown("##### Activities by product area")
    st.caption(
        "**Creditor loans** lists sidebar access plus numbered sub-areas. "
        "**Accounting** and **Journals** are separate: journals has manual / balance adjustment / approvals."
    )

    for area_title in _sorted_area_titles(perms_by_area):
        rows = [r for r in perms_by_area[area_title] if _row_matches_filter(r, q)]
        if not rows:
            continue
        render_sub_sub_header(area_title)
        cap = _area_help_caption(rows)
        if cap:
            st.caption(cap)
        rows_sorted = sorted(
            rows,
            key=lambda r: (
                _subgroup_order_and_label(str(r.get("permission_key") or ""))[0],
                _row_area_sort_key(r),
            ),
        )
        last_sub: str | None = None
        for row in rows_sorted:
            pk = str(row["permission_key"])
            _so, sub_lbl = _subgroup_order_and_label(pk)
            if sub_lbl and sub_lbl != last_sub:
                last_sub = sub_lbl
                st.markdown(f"**{sub_lbl}**")
            restricted = bool(row.get("grant_restricted_to_superadmin"))
            risk = str(row.get("risk_tag") or "standard")
            lock_note = (
                " — *Superadmin only*" if restricted and not actor_is_superadmin else ""
            )
            wid = _perm_widget_id(rid, pk)
            _ro = admin_permissions_read_only or (restricted and not actor_is_superadmin)
            cb_col, lab_col, det_col = st.columns([0.55, 5.2, 1.25], gap="small", vertical_alignment="center")
            with cb_col:
                if _ro:
                    st.checkbox(
                        "on",
                        value=pk in current,
                        disabled=True,
                        key=wid + ("_locked" if restricted and not actor_is_superadmin else "_admin_ro"),
                        label_visibility="collapsed",
                    )
                else:
                    st.checkbox(
                        "on",
                        value=pk in current,
                        key=wid,
                        label_visibility="collapsed",
                    )
            with lab_col:
                summ = (row.get("summary") or "").strip()
                one = f"**{row['label']}**{lock_note}"
                if summ:
                    short = summ[:140] + ("…" if len(summ) > 140 else "")
                    one += f" — *{short}*"
                st.markdown(one)
            with det_col:
                with st.popover("Details"):
                    st.markdown(row.get("grants_md") or "_No extra detail._")
                    st.caption(f"`{pk}` · {risk}")

    with st.expander("Create a new custom role", expanded=False):
        st.caption("New roles start with no activities until you assign ticks above (after selecting the new role).")
        c_a, c_b, c_c = st.columns([2, 2, 1], gap="small")
        with c_a:
            new_rk = st.text_input("Role key (e.g. ANALYST)", key="rbac_new_key", max_chars=64)
        with c_b:
            new_dn = st.text_input("Display name", key="rbac_new_name", max_chars=120)
        with c_c:
            st.write("")
            st.write("")
            if st.button("Create role", key="rbac_create"):
                if not new_rk.strip() or not new_dn.strip():
                    st.error("Role key and display name are required.")
                elif not actor_is_superadmin and new_rk.strip().upper().replace(" ", "_") in (
                    "ADMIN",
                    "SUPERADMIN",
                ):
                    st.error("Only a super administrator may create a role reserved for Administrator or Super administrator.")
                else:
                    try:
                        c2 = get_conn()
                        try:
                            r2 = RbacRepository(c2)
                            r2.create_role(new_rk, new_dn, is_system=False)
                        finally:
                            c2.close()
                        st.success(f"Created role {new_rk.strip().upper().replace(' ', '_')}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not create role: {e}")
