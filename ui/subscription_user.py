"""Tenant subscription status and proof-of-payment uploads."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO

import streamlit as st

from subscription import repository as sub_repo
from db.tenant_registry import (
    get_stored_tenant_company,
    get_stored_tenant_schema,
    list_active_tenants,
    remember_tenant_context,
)
from subscription.subscription_utils import extend_period, get_billing_end_date
from subscription.access import refresh_subscription_access_snapshot
from style import BRAND_GREEN, inject_style_block

_SUB_VEND_PENDING = "sub_vend_pending_action"


def _format_pop_period_end(v) -> str:
    """Format ``period_end_applied_to`` for display."""
    if v is None:
        return "-"
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if hasattr(v, "date"):
        return v.date().isoformat()
    return str(v)


def _vendor_pending() -> dict | None:
    raw = st.session_state.get(_SUB_VEND_PENDING)
    return raw if isinstance(raw, dict) else None


def _vendor_set_pending(kind: str) -> None:
    st.session_state[_SUB_VEND_PENDING] = {"kind": kind}


def _vendor_clear_pending() -> None:
    st.session_state.pop(_SUB_VEND_PENDING, None)


def _inject_grace_green_css() -> None:
    """Green actions for grace row (scoped via ``st.container(key='farnda_sub_grace_row')``)."""
    inject_style_block(
        f"""
.st-key-farnda_sub_grace_row button[kind="primary"],
.st-key-farnda_sub_grace_row button[kind="secondary"],
.st-key-farnda_sub_grace_row button[data-testid="stBaseButton-primary"],
.st-key-farnda_sub_grace_row button[data-testid="stBaseButton-secondary"] {{
  background-color: {BRAND_GREEN} !important;
  color: #ffffff !important;
  border: 1px solid #449a32 !important;
}}
.st-key-farnda_sub_grace_row button[kind="primary"]:hover,
.st-key-farnda_sub_grace_row button[kind="secondary"]:hover,
.st-key-farnda_sub_grace_row button[data-testid="stBaseButton-primary"]:hover,
.st-key-farnda_sub_grace_row button[data-testid="stBaseButton-secondary"]:hover {{
  background-color: #4aad3a !important;
  border-color: #3d8f30 !important;
}}
"""
    )


def _vendor_execute_pending(user: dict, tenant_schema: str) -> None:
    p = _vendor_pending()
    if not p:
        return
    kind = p["kind"]

    if kind == "advance":
        row = sub_repo.get_tenant_subscription_row(tenant_schema)
        if not row:
            st.error("No subscription row.")
            return
        _ps, pe, _term = sub_repo.row_dates(row)
        if pe is None:
            st.error("Period end not set.")
            return
        cy = st.session_state.get("sub_vend_cycle", "Monthly")
        if cy not in ("Monthly", "Quarterly"):
            cy = "Monthly"
        nxt = extend_period(pe, cy)
        nstart = pe + timedelta(days=1)
        sub_repo.update_tenant_subscription(
            tenant_schema,
            period_start=nstart,
            period_end=nxt,
            clear_termination=True,
        )
        refresh_subscription_access_snapshot(user)
        st.success(f"New period end: {nxt}")

    elif kind == "reset_anchor":
        anchor = st.session_state.get("sub_vend_anchor")
        if not isinstance(anchor, date):
            st.error("Invalid anchor date.")
            return
        new_cycle = st.session_state.get("sub_vend_cycle", "Monthly")
        cy = "Quarterly" if new_cycle == "Quarterly" else "Monthly"
        end_d = get_billing_end_date(anchor, cy)
        sub_repo.update_tenant_subscription(
            tenant_schema,
            period_start=anchor,
            period_end=end_d,
            clear_termination=True,
        )
        refresh_subscription_access_snapshot(user)
        st.success(f"Period end set to {end_d}")

    elif kind == "clear_term":
        sub_repo.set_tenant_access_terminated(tenant_schema, terminated=False)
        sub_repo.update_tenant_subscription(tenant_schema, clear_termination=True)
        refresh_subscription_access_snapshot(user)
        st.success("Termination cleared.")

    elif kind == "apply_tier":
        new_tier = st.session_state.get("sub_vend_tier", "Basic")
        new_cycle = st.session_state.get("sub_vend_cycle", "Monthly")
        sub_repo.update_tenant_subscription(
            tenant_schema,
            tier_name=new_tier,
            billing_cycle=new_cycle,
            clear_termination=True,
        )
        refresh_subscription_access_snapshot(user)
        st.success("Tier / cycle updated.")

    elif kind == "grace_apply":
        grace_pick = st.session_state.get("sub_vend_grace_date")
        if not isinstance(grace_pick, date):
            st.error("Invalid grace date.")
            return
        sub_repo.update_tenant_subscription(tenant_schema, grace_access_until=grace_pick)
        refresh_subscription_access_snapshot(user)
        st.success("Grace saved.")

    elif kind == "grace_clear":
        sub_repo.update_tenant_subscription(tenant_schema, grace_access_until=None)
        refresh_subscription_access_snapshot(user)
        st.success("Grace cleared.")


# Mixed-case, case-sensitive: avoids lazy all-caps or all-lowercase slips; user must type deliberately.
_VENDOR_CONFIRM_PHRASE = "cOnFiRm AcTiOn"


def _render_confirm_strip(
    *,
    kinds: tuple[str, ...],
    message_by_kind: dict[str, str],
    user: dict,
    tenant_schema: str,
    key_suffix: str,
) -> None:
    p = _vendor_pending()
    if not p or p.get("kind") not in kinds:
        return
    kind = str(p["kind"])
    msg = message_by_kind.get(kind, "Proceed?")
    confirm_key = f"sub_vend_type_confirm_{key_suffix}_{kind}"

    st.caption(msg)
    c0, c1, c2 = st.columns([2.2, 0.55, 0.55], gap="small", vertical_alignment="bottom")
    with c0:
        st.text_input(
            "Phrase",
            key=confirm_key,
            placeholder=_VENDOR_CONFIRM_PHRASE,
            label_visibility="collapsed",
            autocomplete="off",
            help="Case-sensitive; must match placeholder exactly.",
        )
    phrase_ok = (st.session_state.get(confirm_key) or "").strip() == _VENDOR_CONFIRM_PHRASE
    with c1:
        if st.button(
            "OK",
            type="primary",
            key=f"sub_vend_cf_yes_{key_suffix}_{kind}",
            disabled=not phrase_ok,
            use_container_width=True,
        ):
            if (st.session_state.get(confirm_key) or "").strip() != _VENDOR_CONFIRM_PHRASE:
                st.error("Phrase mismatch.")
                return
            try:
                _vendor_execute_pending(user, tenant_schema)
            except Exception as e:
                st.error(str(e))
            else:
                _vendor_clear_pending()
                st.session_state.pop(confirm_key, None)
                st.rerun()
    with c2:
        if st.button(
            "Cancel",
            key=f"sub_vend_cf_no_{key_suffix}_{kind}",
            use_container_width=True,
        ):
            _vendor_clear_pending()
            st.session_state.pop(confirm_key, None)
            st.rerun()


def _render_subscription_summary(row: dict, *, key_prefix: str) -> tuple[str, str, date | None]:
    """Metrics + period line. Returns (tier, cycle, period_end) for POP context."""
    ps, pe, _term = sub_repo.row_dates(row)
    tier = str(row.get("tier_name") or "Basic")
    cycle = str(row.get("billing_cycle") or "Monthly")
    if cycle not in ("Monthly", "Quarterly"):
        cycle = "Monthly"

    today = date.today()

    c1, c2, c3 = st.columns([1, 1, 1], gap="small")
    with c1:
        st.metric("Tier", tier)
    with c2:
        st.metric("Billing cycle", cycle)
    with c3:
        if pe is None:
            st.metric("Days to expiry", "—")
        elif today > pe:
            st.metric("Days overdue", (today - pe).days)
        else:
            st.metric("Days to expiry", (pe - today).days)

    st.write(f"**Period:** {ps or '-'} → **{pe or '-'}**")
    return tier, cycle, pe


def _pop_row_label(m: dict, *, vendor: bool) -> str:
    base = f"{m['uploaded_at']} | {m['file_name']} | period {_format_pop_period_end(m.get('period_end_applied_to'))}"
    if vendor:
        return f"{base} | {m.get('uploaded_by') or '-'}"
    return base


def _render_pop_list_click_preview(
    tenant_schema: str,
    meta: list,
    *,
    key_prefix: str,
    vendor: bool,
    company: str | None = None,
) -> None:
    """
    One button per upload: first click selects and opens preview; second click on the same
    row closes preview. Another row switches selection and opens its preview.
    """
    st.caption("Click an upload to open preview; click the same row again to close preview.")
    sel_key = f"{key_prefix}_pop_selected_id"
    open_key = f"{key_prefix}_pop_preview_open"

    for m in meta:
        mid = int(m["id"])
        label = _pop_row_label(m, vendor=vendor)
        cur_sel = st.session_state.get(sel_key)
        is_open = bool(st.session_state.get(open_key))
        highlighted = cur_sel == mid and is_open
        if st.button(
            label,
            key=f"{key_prefix}_pop_row_{mid}",
            use_container_width=True,
            type="primary" if highlighted else "secondary",
        ):
            if cur_sel == mid and is_open:
                st.session_state[open_key] = False
            else:
                st.session_state[sel_key] = mid
                st.session_state[open_key] = True
            st.rerun()

    sid = st.session_state.get(sel_key)
    if sid is None:
        return
    row = next((x for x in meta if int(x["id"]) == int(sid)), None)
    if row is None:
        return

    if vendor and company is not None:
        st.info(
            f"**Uploaded by:** {row.get('uploaded_by') or '-'} | "
            f"**Invoice / period end:** {_format_pop_period_end(row.get('period_end_applied_to'))} | "
            f"**Tenant:** {company} (`{tenant_schema}`)"
        )

    doc = sub_repo.get_pop_upload_content(tenant_schema, int(sid))
    if not doc or doc.get("file_content") is None:
        st.warning("Could not load file bytes.")
        return
    raw = doc["file_content"]
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    mime = str(doc.get("mime_type") or "")

    if st.session_state.get(open_key):
        if mime.startswith("image/"):
            st.image(BytesIO(raw), caption=str(doc.get("file_name") or "POP"))
        elif mime == "application/pdf" or (doc.get("file_name") or "").lower().endswith(".pdf"):
            st.caption("PDF - use Download to open in your viewer.")

    st.download_button(
        "Download",
        data=raw,
        file_name=str(doc.get("file_name") or "pop"),
        mime=mime or "application/octet-stream",
        key=f"{key_prefix}_pop_dl_{sid}",
    )


def _render_pop_upload_and_list(
    tenant_schema: str,
    user: dict,
    *,
    row: dict,
    key_prefix: str,
) -> None:
    st.markdown("**Proof of payment**")
    ps, pe, _term = sub_repo.row_dates(row)
    st.caption(
        "Tag each upload with the **billing period end date** the invoice covers "
        f"(your current subscription window: **{ps or '-'}** -> **{pe or '-'}**)."
    )
    pc1, pc2 = st.columns([1.15, 1], gap="small", vertical_alignment="bottom")
    with pc1:
        use_current_pe = st.checkbox(
            "POP is for current subscription period end",
            value=True,
            key=f"{key_prefix}_pop_use_current_period",
        )
    with pc2:
        if use_current_pe:
            st.markdown(f"**Period end:** {_format_pop_period_end(pe)}")
            invoice_period_end: date | None = pe
        else:
            invoice_period_end = st.date_input(
                "Billing period end (on invoice)",
                value=pe or date.today(),
                key=f"{key_prefix}_pop_invoice_period_end",
            )

    up = st.file_uploader(
        "Upload POP (PDF / image)",
        type=["pdf", "png", "jpg", "jpeg"],
        key=f"{key_prefix}_pop_upload",
    )
    confirm_upl = st.checkbox(
        "I confirm I want to save this file as proof of payment",
        key=f"{key_prefix}_pop_upload_confirm",
    )
    if up is not None and st.button("Save upload", type="primary", key=f"{key_prefix}_pop_save"):
        if not confirm_upl:
            st.error("Check the confirmation box before saving.")
            return
        raw = up.getvalue()
        try:
            sub_repo.insert_pop_upload(
                tenant_schema,
                uploaded_by=str(user.get("email") or user.get("id") or ""),
                file_name=up.name,
                mime_type=up.type or "application/octet-stream",
                file_size=len(raw),
                file_content=raw,
                period_end_applied_to=invoice_period_end,
            )
            st.session_state.pop(f"{key_prefix}_pop_upload_confirm", None)
            st.success("Upload saved.")
            st.rerun()
        except Exception as e:
            st.error(str(e))

    try:
        meta = sub_repo.list_pop_uploads_metadata(tenant_schema, limit=20)
    except Exception as e:
        st.error(str(e))
        return

    if meta:
        st.markdown("**Recent uploads**")
        _render_pop_list_click_preview(
            tenant_schema,
            meta,
            key_prefix=key_prefix,
            vendor=False,
        )


def _render_pop_readonly(tenant_schema: str, *, key_prefix: str) -> None:
    """Vendor: list submitted POPs, preview images, download all types."""
    st.markdown("**Submitted proof of payment**")
    company = get_stored_tenant_company() or "-"
    st.caption(
        f"**Tenant:** {company} | **Schema:** `{tenant_schema}` - "
        "each row shows who uploaded the file and which billing period end it applies to."
    )
    st.caption("Images preview in-app; PDFs via Download.")
    try:
        meta = sub_repo.list_pop_uploads_metadata(tenant_schema, limit=20)
    except Exception as e:
        st.error(str(e))
        return

    if not meta:
        st.info("No POP files submitted yet.")
        return

    _render_pop_list_click_preview(
        tenant_schema,
        meta,
        key_prefix=key_prefix,
        vendor=True,
        company=company,
    )


def _render_vendor_tenant_switcher() -> None:
    """Let platform vendor staff bind session tenant without full admin navigation."""
    try:
        tenants = list_active_tenants()
    except Exception as e:
        st.warning(f"Could not load tenant list: {e}")
        return
    if not tenants:
        st.warning("No active rows in public.tenants.")
        return
    names = [str(t["company_name"]) for t in tenants]
    key = "farnda_vendor_working_tenant_company"
    cur = get_stored_tenant_company()
    if key not in st.session_state:
        st.session_state[key] = cur if cur in names else names[0]
        if cur not in names:
            try:
                remember_tenant_context(st.session_state[key])
                st.rerun()
            except Exception as e:
                st.session_state["_farnda_tenant_bind_message"] = str(e)

    def _sync_tenant() -> None:
        picked = st.session_state.get(key)
        if not picked or picked not in names:
            return
        try:
            remember_tenant_context(picked)
        except Exception as e:
            st.session_state["_farnda_tenant_bind_message"] = str(e)
        else:
            st.rerun()

    st.selectbox(
        "Working tenant",
        options=names,
        key=key,
        on_change=_sync_tenant,
    )


def _render_vendor_subscription_tab(tenant_schema: str, user: dict, row: dict) -> None:
    """
    Term settings, grace period, POP read-only (no summary metrics - those sit on Organisation tab).
    Grace does not change period_end or tier; it temporarily lifts delinquency enforcement.
    """
    if (user.get("role") or "") not in ("VENDOR", "SUPERADMIN"):
        st.error("Vendor subscription tools are restricted to vendor or super-administrator users.")
        st.stop()
    _ps, pe, _term = sub_repo.row_dates(row)
    tier = str(row.get("tier_name") or "Basic")
    cycle = str(row.get("billing_cycle") or "Monthly")
    if cycle not in ("Monthly", "Quarterly"):
        cycle = "Monthly"

    st.markdown("**Term settings**")
    today = date.today()
    ac1, ac2 = st.columns(2, gap="small")
    with ac1:
        try:
            vts = sub_repo.list_vendor_tiers()
            tier_opts = [str(x["tier_name"]) for x in vts] or ["Basic", "Premium"]
        except Exception:
            tier_opts = ["Basic", "Premium"]
        st.selectbox(
            "Tier",
            tier_opts,
            index=tier_opts.index(tier) if tier in tier_opts else 0,
            key="sub_vend_tier",
        )
    with ac2:
        st.selectbox(
            "Cycle",
            ["Monthly", "Quarterly"],
            index=0 if cycle == "Monthly" else 1,
            key="sub_vend_cycle",
        )
    if st.button("Apply tier / cycle", key="sub_vend_apply_tier"):
        _vendor_set_pending("apply_tier")
        st.rerun()
    _render_confirm_strip(
        kinds=("apply_tier",),
        message_by_kind={
            "apply_tier": "Apply tier/cycle for tenant?",
        },
        user=user,
        tenant_schema=tenant_schema,
        key_suffix="tier",
    )

    st.divider()
    pr1, pr2, pr3, pr4 = st.columns([1.05, 1.25, 1.05, 1.15], gap="small", vertical_alignment="bottom")
    with pr1:
        if st.button(
            "Next period",
            key="sub_vend_advance",
            disabled=pe is None,
            use_container_width=True,
            help="Advance to the next billing window from the current period end.",
        ):
            _vendor_set_pending("advance")
            st.rerun()
    with pr2:
        st.caption("Anchor")
        st.date_input(
            "anchor",
            value=today,
            key="sub_vend_anchor",
            label_visibility="collapsed",
        )
    with pr3:
        if st.button(
            "Reset from anchor",
            key="sub_vend_reset_anchor",
            use_container_width=True,
            help="Rebuild period from the anchor date using the selected cycle.",
        ):
            _vendor_set_pending("reset_anchor")
            st.rerun()
    with pr4:
        if st.button(
            "Clear termination",
            key="sub_vend_clear_term",
            use_container_width=True,
            help="Clear access termination after payment.",
        ):
            _vendor_set_pending("clear_term")
            st.rerun()

    _render_confirm_strip(
        kinds=("advance", "reset_anchor", "clear_term"),
        message_by_kind={
            "advance": "Advance to next billing period?",
            "reset_anchor": "Reset period from anchor?",
            "clear_term": "Clear access termination?",
        },
        user=user,
        tenant_schema=tenant_schema,
        key_suffix="period",
    )

    st.divider()
    st.markdown("**Grace access**")
    st.caption(
        "Inclusive end date: enforcement treats access as current through that day. "
        "Does not change billing period or tier."
    )
    current_grace = sub_repo.grace_access_until_date(row)
    if current_grace is not None:
        st.caption(f"Active grace until: **{current_grace.isoformat()}**")

    _inject_grace_green_css()
    with st.container(key="farnda_sub_grace_row"):
        g1, g2, g3 = st.columns([1.4, 2.3, 2.3], gap="small", vertical_alignment="bottom")
        with g1:
            st.caption("Grace until (inclusive)")
            st.date_input(
                "grace_until",
                value=current_grace or today,
                key="sub_vend_grace_date",
                label_visibility="collapsed",
            )
        with g2:
            if st.button("Apply grace", type="primary", key="sub_vend_set_grace", use_container_width=True):
                _vendor_set_pending("grace_apply")
                st.rerun()
        with g3:
            if st.button("Clear grace", type="secondary", key="sub_vend_clear_grace", use_container_width=True):
                _vendor_set_pending("grace_clear")
                st.rerun()

    _render_confirm_strip(
        kinds=("grace_apply", "grace_clear"),
        message_by_kind={
            "grace_apply": "Grant grace to selected date?",
            "grace_clear": "Clear grace now?",
        },
        user=user,
        tenant_schema=tenant_schema,
        key_suffix="grace",
    )

    st.divider()
    _render_pop_readonly(tenant_schema, key_prefix="sub_vend")


def _render_org_subscription_tab(tenant_schema: str, user: dict, row: dict, *, key_prefix: str) -> None:
    _render_subscription_summary(row, key_prefix=key_prefix)
    st.divider()
    _render_pop_upload_and_list(tenant_schema, user, row=row, key_prefix=key_prefix)


def render_subscription_user_ui(*, get_current_user) -> None:
    user = get_current_user() or {}
    role = user.get("role") or ""
    tenant_schema = get_stored_tenant_schema()
    if not tenant_schema:
        st.error("No tenant context. Subscription cannot be loaded.")
        return

    if role not in ("ADMIN", "LOAN_OFFICER", "VENDOR", "SUPERADMIN"):
        st.warning("This page is not available for your role.")
        return

    try:
        from rbac.subfeature_access import (
            subscription_can_platform_admin,
            subscription_can_tenant_account,
            subscription_can_vendor_console,
        )
    except Exception:

        def subscription_can_tenant_account(user=None) -> bool:  # type: ignore[misc]
            return True

        def subscription_can_vendor_console(user=None) -> bool:  # type: ignore[misc]
            return True

        def subscription_can_platform_admin(user=None) -> bool:  # type: ignore[misc]
            return True

    try:
        row = sub_repo.get_tenant_subscription_row(tenant_schema)
    except Exception as e:
        st.error(f"Subscription load failed: {e}")
        return

    if not row:
        st.warning("No subscription row yet.")
        return

    can_tenant = subscription_can_tenant_account(user)
    can_vendor = subscription_can_vendor_console(user)
    can_platform = subscription_can_platform_admin(user)

    if role == "VENDOR":
        if not can_vendor:
            st.warning("You do not have permission for vendor subscription tools.")
            return
        st.caption("Platform vendor access: you manage subscription terms for the **working tenant** below.")
        _render_vendor_tenant_switcher()
        st.divider()
        _render_vendor_subscription_tab(tenant_schema, user, row)
        return

    if role == "SUPERADMIN":
        show_vendor_ui = can_vendor or can_platform
        show_org_ui = can_tenant
        if show_vendor_ui and show_org_ui:
            tab_vendor, tab_org = st.tabs(["Vendor", "Organisation account"])
            with tab_vendor:
                st.caption("Choose **working tenant**, then use vendor tools (same as vendor role).")
                _render_vendor_tenant_switcher()
                st.divider()
                _render_vendor_subscription_tab(tenant_schema, user, row)
            with tab_org:
                _render_org_subscription_tab(tenant_schema, user, row, key_prefix="sub_org")
        elif show_vendor_ui:
            st.caption("Choose **working tenant**, then use vendor tools.")
            _render_vendor_tenant_switcher()
            st.divider()
            _render_vendor_subscription_tab(tenant_schema, user, row)
        elif show_org_ui:
            _render_org_subscription_tab(tenant_schema, user, row, key_prefix="sub_org")
        else:
            st.warning("No subscription permissions are enabled for your role.")
        return

    if role == "LOAN_OFFICER":
        if not can_tenant:
            st.warning("You do not have permission to view the organisation subscription.")
            return
        _render_org_subscription_tab(tenant_schema, user, row, key_prefix="sub_org")
        return

    if not can_tenant:
        st.warning("You do not have permission to view the organisation subscription.")
        return
    _render_org_subscription_tab(tenant_schema, user, row, key_prefix="sub_org")
