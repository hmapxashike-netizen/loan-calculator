"""System config: dynamic roles and human-oriented permission assignments."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable

import streamlit as st

from auth.permission_catalog import all_permission_records
from dal import get_conn
from rbac.repository import RbacRepository
from rbac.service import rbac_tables_ready


def _perm_widget_id(role_id: int, permission_key: str) -> str:
    safe = permission_key.replace(".", "_").replace(" ", "_")
    return f"rbac_perm_{role_id}_{safe}"


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
    st.caption(
        "Permissions control sidebar sections and dashboards. Each capability includes a short "
        "summary and details below. The technical id is for support and audit (not needed day-to-day)."
    )

    conn = get_conn()
    try:
        repo = RbacRepository(conn)
        roles = repo.list_roles()
    finally:
        conn.close()

    role_labels = {r["role_key"]: f"{r['display_name']} (`{r['role_key']}`)" for r in roles}
    role_keys_ordered = [r["role_key"] for r in roles]

    st.divider()
    st.markdown("**New role**")
    c_a, c_b, c_c = st.columns([2, 2, 1], gap="small")
    with c_a:
        new_rk = st.text_input("Role key (letters/numbers, e.g. ANALYST)", key="rbac_new_key", max_chars=64)
    with c_b:
        new_dn = st.text_input("Display name", key="rbac_new_name", max_chars=120)
    with c_c:
        st.write("")
        st.write("")
        if st.button("Create role", key="rbac_create"):
            if not new_rk.strip() or not new_dn.strip():
                st.error("Role key and display name are required.")
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

    st.divider()
    if not role_keys_ordered:
        st.warning("No roles found.")
        return

    sel_key = st.selectbox(
        "Edit role",
        role_keys_ordered,
        format_func=lambda k: role_labels.get(k, k),
        key="rbac_edit_role_select",
    )
    sel = next((r for r in roles if r["role_key"] == sel_key), None)
    if not sel:
        st.stop()

    rid = int(sel["id"])
    is_system = bool(sel.get("is_system"))

    if not is_system:
        if st.button("Delete this custom role (no users should still use it)", key="rbac_del_role"):
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

    conn = get_conn()
    try:
        repo = RbacRepository(conn)
        perms_db = repo.list_permissions()
        current = repo.get_permission_keys_for_role_key(sel_key)
    finally:
        conn.close()

    perms_by_cat: dict[str, list[dict]] = defaultdict(list)
    for row in perms_db:
        perms_by_cat[str(row["category"])].append(row)
    for cat in perms_by_cat:
        perms_by_cat[cat].sort(key=lambda x: str(x["label"]))

    preset_c1, preset_c2 = st.columns(2)
    with preset_c1:
        if st.button("Preset: same as Loan Officer", key="rbac_pre_lo"):
            try:
                c4 = get_conn()
                try:
                    r4 = RbacRepository(c4)
                    r4.clone_permissions_from_role(
                        rid, "LOAN_OFFICER", actor_is_superadmin=actor_is_superadmin
                    )
                finally:
                    c4.close()
                st.success("Permissions replaced from Loan Officer.")
                st.rerun()
            except PermissionError as e:
                st.error(str(e))
            except Exception as e:
                st.error(str(e))
    with preset_c2:
        if st.button("Preset: same as Administrator", key="rbac_pre_ad"):
            try:
                c5 = get_conn()
                try:
                    r5 = RbacRepository(c5)
                    r5.clone_permissions_from_role(rid, "ADMIN", actor_is_superadmin=actor_is_superadmin)
                finally:
                    c5.close()
                st.success("Permissions replaced from Administrator.")
                st.rerun()
            except PermissionError as e:
                st.error(str(e))
            except Exception as e:
                st.error(str(e))

    st.caption("Adjust access below, then **Save permissions for this role**.")

    for cat in sorted(perms_by_cat.keys()):
        with st.expander(cat, expanded=(cat in ("Navigation", "Dashboards"))):
            for row in perms_by_cat[cat]:
                pk = str(row["permission_key"])
                restricted = bool(row.get("grant_restricted_to_superadmin"))
                risk = str(row.get("risk_tag") or "standard")
                lock_note = (
                    " — *Superadmin only to assign*" if restricted and not actor_is_superadmin else ""
                )
                wid = _perm_widget_id(rid, pk)
                if restricted and not actor_is_superadmin:
                    st.checkbox(
                        f"**{row['label']}**{lock_note}",
                        value=pk in current,
                        disabled=True,
                        key=wid + "_locked",
                    )
                else:
                    st.checkbox(
                        f"**{row['label']}**{lock_note}",
                        value=pk in current,
                        key=wid,
                    )
                st.caption(row.get("summary") or "")
                with st.expander("What this grants", expanded=False):
                    st.markdown(row.get("grants_md") or "")
                    st.caption(f"Technical id: `{pk}` · Risk: {risk}")
                st.divider()

    if st.button("Save permissions for this role", type="primary", key="rbac_save"):
        new_sel: set[str] = set()
        for row in perms_db:
            pk = str(row["permission_key"])
            restricted = bool(row.get("grant_restricted_to_superadmin"))
            wid = _perm_widget_id(rid, pk)
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
            st.success("Permissions saved.")
            st.rerun()
        except PermissionError as e:
            st.error(str(e))
        except Exception as e:
            st.error(str(e))

    if actor_is_superadmin and st.button(
        "Sync permission definitions from product catalog", key="rbac_sync_cat"
    ):
        try:
            c7 = get_conn()
            try:
                r7 = RbacRepository(c7)
                r7.sync_permissions_from_catalog(all_permission_records())
            finally:
                c7.close()
            st.success("Catalog synced to database.")
            st.rerun()
        except Exception as e:
            st.error(str(e))
