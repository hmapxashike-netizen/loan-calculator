from __future__ import annotations

import logging
import os
import base64
from html import escape as html_escape
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
import importlib.util

import streamlit as st
import pandas as pd
from streamlit_option_menu import option_menu

from middleware import get_current_user, clear_current_user, require_login
from auth.ui import auth_page, render_totp_recovery_regeneration_sidebar
from dal import get_conn, UserRepository, SecurityAuditLogRepository
from auth.service import AuthService
from style import (
    inject_farnda_global_styles_once,
    inject_style_block,
    render_main_page_title,
    render_sub_header,
    render_sub_sub_header,
)
from ui.components import inject_tertiary_hyperlink_css_once


def _configure_logging_from_env() -> None:
    """
    Ensure INFO-level logs are visible in Streamlit terminal output when requested.

    Streamlit apps often run with WARNING-level defaults; our EOD timing and
    incremental stats are emitted via logger.info(...).
    """
    lvl_raw = (os.environ.get("PYTHONLOGLEVEL") or "").strip().upper()
    if not lvl_raw:
        return
    level = getattr(logging, lvl_raw, None)
    if not isinstance(level, int):
        return

    root = logging.getLogger()
    root.setLevel(level)
    # Avoid duplicate handlers on hot reload.
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    else:
        for h in root.handlers:
            try:
                h.setLevel(level)
            except Exception:
                pass


_configure_logging_from_env()


def _privileged_roles_touch(old_role: str, new_role: str) -> bool:
    """True if either side is a platform / tenant-admin role only SUPERADMIN may assign or change."""
    pr = frozenset({"VENDOR", "SUPERADMIN", "ADMIN"})
    return old_role in pr or new_role in pr


def _user_role_edit_allowed(actor_role: str | None, old_role: str, new_role: str) -> tuple[bool, str]:
    if not _privileged_roles_touch(old_role, new_role):
        return True, ""
    if actor_role == "SUPERADMIN":
        return True, ""
    return (
        False,
        "Only a super administrator can assign, remove, or change administrator, vendor, or super-admin roles.",
    )


def _assignable_roles_for_ui(
    actor_role: str | None = None,
    *,
    current_user_role: str | None = None,
) -> list[str]:
    """
    Roles shown in admin user pickers. Non-superadmin users never get ADMIN / VENDOR / SUPERADMIN
    in the list, except when managing an existing user who already has one of those (so the
    selectbox can display their current role).
    """
    ar = (actor_role or "").strip().upper()
    try:
        from rbac.service import list_assignable_role_keys, rbac_tables_ready

        if rbac_tables_ready():
            keys = list_assignable_role_keys()
            if not keys:
                keys = []
        else:
            keys = []
    except Exception:
        keys = []
    if not keys:
        keys = ["ADMIN", "LOAN_OFFICER", "BORROWER", "SUPERADMIN", "VENDOR"]
    if ar == "SUPERADMIN":
        return sorted(keys, key=str)
    blocked = frozenset({"SUPERADMIN", "VENDOR", "ADMIN"})
    allowed = [k for k in keys if k not in blocked]
    cur = (current_user_role or "").strip().upper()
    if cur and cur in blocked and cur not in allowed:
        allowed.append(cur)
    return sorted(set(allowed), key=str)


def _load_loan_ui_module():
    """
    Load app.py by file path so the module name is not the bare string ``app``.
    That avoids clashes with another installed/local package named ``app`` (can
    surface as KeyError: 'app' during import on some environments).
    """
    path = Path(__file__).resolve().parent / "app.py"
    spec = importlib.util.spec_from_file_location("farnda_cred_loan_ui", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load loan UI module from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


loan_app = _load_loan_ui_module()


def _dataframe_first_col_left_config(df: pd.DataFrame) -> dict | None:
    """Streamlit dataframe: left-align first column only (headers follow grid defaults)."""
    if df.shape[1] < 1:
        return None
    return {str(df.columns[0]): {"alignment": "left"}}


def _audit_log_ts_cell(v: object) -> str:
    """String for audit ``created_at`` (matches Agents / View & Manage table timestamps)."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (ValueError, TypeError):
        pass
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OSError):
            pass
    s = str(v).strip()
    return s[:19] if len(s) >= 19 else s


def _audit_log_id_cell(v: object) -> str:
    """String audit row id (numeric display like Agents table ID column)."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (ValueError, TypeError):
        pass
    try:
        return str(int(v))
    except (TypeError, ValueError):
        return str(v)


def _recent_login_audit_pdf_bytes(df_display: pd.DataFrame, *, title: str = "Recent login activity") -> bytes | None:
    """PDF table export for audit login grid (landscape; truncates long agent strings)."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import landscape, letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    except ImportError:
        return None
    if df_display.empty:
        return None
    df_pdf = df_display.fillna("").astype(str).copy()
    ua_col = next((c for c in df_pdf.columns if str(c).strip().lower() == "user agent"), None)
    if ua_col:
        df_pdf[ua_col] = df_pdf[ua_col].str.slice(0, 140)
    buf = BytesIO()
    page = landscape(letter)
    doc = SimpleDocTemplate(
        buf,
        pagesize=page,
        rightMargin=36,
        leftMargin=36,
        topMargin=42,
        bottomMargin=42,
    )
    styles = getSampleStyleSheet()
    story = [Paragraph(title, styles["Title"]), Spacer(1, 14)]
    table_data = [df_pdf.columns.tolist()] + df_pdf.values.tolist()
    ncols = max(len(df_pdf.columns), 1)
    avail_w = page[0] - 72
    col_w = avail_w / ncols
    t = Table(table_data, colWidths=[col_w] * ncols, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("FONTSIZE", (0, 1), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                ("TOPPADDING", (0, 0), (-1, 0), 6),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
            ]
        )
    )
    story.append(t)
    doc.build(story)
    buf.seek(0)
    return buf.getvalue()


def render_footer() -> None:
    st.markdown("---")
    st.markdown(
        "<div style='text-align:center; color:#64748B; font-size:1.0625rem; padding-bottom:0.5rem;'>"
        "Copyright Farnda Solutions 2026. All rights reserved."
        "</div>",
        unsafe_allow_html=True,
    )


def _logo_path() -> Path:
    base_dir = Path(__file__).resolve().parent
    for file_name in ("FarndaCred logo with.svg", "FarndaCred logo with.png"):
        candidate = base_dir / file_name
        if candidate.exists():
            return candidate
    return base_dir / "FarndaCred logo with.png"


def render_sidebar_branding() -> None:
    """Compact logo only (no slogan); sticky at top of sidebar. Slogan stays on login page."""
    logo_path = _logo_path()
    if logo_path.exists():
        mime = "image/svg+xml" if logo_path.suffix.lower() == ".svg" else "image/png"
        logo_b64 = base64.b64encode(logo_path.read_bytes()).decode("ascii")
        st.sidebar.markdown(
            f"""
            <div class="farnda-sidebar-sticky-head">
              <div class="farnda-sidebar-logo-wrap">
                <img src="data:{mime};base64,{logo_b64}" alt="FarndaCred" class="farnda-sidebar-logo-img" />
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.sidebar.markdown(
            """
            <div class="farnda-sidebar-sticky-head">
              <p class="farnda-sidebar-wordmark-fallback">FarndaCred</p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _format_sidebar_name(full_name: str) -> str:
    parts = [part.strip() for part in full_name.split() if part.strip()]
    if not parts:
        return "User"
    if len(parts) == 1:
        return parts[0]
    initials = "".join(f"{part[0].upper()}." for part in parts[:-1] if part)
    surname = parts[-1]
    return f"{initials} {surname}".strip()


def render_sidebar_user_meta(user: dict, system_date) -> None:
    display_name = _format_sidebar_name(user.get("full_name", "User"))
    st.sidebar.markdown(
        f"""
        <div class="farnda-user-card" style="background:#F8FAFC; padding:0.85rem; margin-top:0;">
            <div style="font-size:0.875rem; color:#64748B; margin-bottom:0.25rem; font-weight:600;">Logged in as</div>
            <div style="font-weight:700; color:#002147; margin-bottom:0.35rem; font-size:0.98rem;">{html_escape(display_name)}</div>
            <div class="farnda-system-date" style="font-size:1.2rem; font-weight:700;">System date: {system_date.isoformat()}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


_OPTION_MENU_ICON_BY_SECTION: dict[str, str] = {
    "Home": "house",
    "Admin Dashboard": "shield-lock",
    "Officer Dashboard": "briefcase",
    "Customers": "people",
    "Loan management": "cash-coin",
    "Loan Capture": "clipboard-plus",
    "Portfolio reports": "bar-chart-line",
    "Teller": "bank",
    "Reamortisation": "arrow-repeat",
    "Statements": "file-earmark-text",
    "Accounting": "journal-bookmark",
    "Journals": "journal-text",
    "Notifications": "bell",
    "Document Management": "folder",
    "End of day": "moon-stars",
    "System configurations": "gear",
    "Subscription": "credit-card",
    "View Schedule": "calendar3",
    "Loan Calculators": "calculator",
    "Update Loans": "arrow-clockwise",
    "Interest In Suspense": "hourglass-split",
    "Approve Loans": "check-circle",
}


def _section_option_menu_icon(section: str) -> str:
    return _OPTION_MENU_ICON_BY_SECTION.get(section, "grid")


def _apply_sidebar_option_menu_iframe_height(n_items: int) -> None:
    """
    streamlit-option-menu renders inside stIFrame with a default height that can clip
    the last items. Re-apply each run (Streamlit does not persist prior st.html nodes).
    """
    # Uppercase labels often wrap to two lines; last row (e.g. Subscription) was getting clipped.
    row_px = 84
    chrome = 420
    tail_pad = 72
    px = max(640, min(2800, n_items * row_px + chrome + tail_pad))
    vh_floor = "max(520px, calc(100dvh - 200px))"
    inject_style_block(
        f"""
        [data-testid="stSidebar"] iframe[data-testid="stIFrame"] {{
          height: max({px}px, {vh_floor}) !important;
          min-height: max({px}px, {vh_floor}) !important;
          max-height: none !important;
        }}
        [data-testid="stSidebar"] [data-testid="stElementContainer"]:has(iframe[data-testid="stIFrame"]) {{
          overflow: visible !important;
          min-height: max({px}px, {vh_floor}) !important;
        }}
        """
    )


def render_sidebar_option_menu(menu_keys: list[str], current_choice: str) -> str:
    """
    Render sidebar navigation via streamlit-option-menu.
    This bypasses Streamlit built-in radio/page nav internals for full styling control.
    """
    if current_choice not in menu_keys:
        current_choice = menu_keys[0]
    display_options = [section.upper() for section in menu_keys]
    display_to_section = {section.upper(): section for section in menu_keys}
    with st.sidebar:
        selected = option_menu(
            menu_title=None,
            options=display_options,
            icons=[_section_option_menu_icon(section) for section in menu_keys],
            default_index=menu_keys.index(current_choice),
            orientation="vertical",
            key="farnda_main_option_menu",
            styles={
                "container": {
                    "padding": "0 !important",
                    "margin": "1.2rem 0 0 0 !important",
                    "background-color": "transparent",
                },
                "icon": {
                    "color": "#002147",
                    "font-size": "0.9rem",
                },
                "nav-link": {
                    "font-size": "0.86rem",
                    "font-weight": "500",
                    "text-align": "left",
                    "margin": "0",
                    "padding": "0.5rem 0.55rem",
                    "border-radius": "0",
                    "line-height": "1.25",
                    "min-height": "2.5rem",
                    "border-bottom": "1px solid rgba(0, 33, 71, 0.14)",
                    "white-space": "normal",
                    "overflow-wrap": "anywhere",
                },
                "nav-link:hover": {
                    "background-color": "rgba(0, 33, 71, 0.05)",
                    "color": "#0f172a",
                },
                "nav-link-selected": {
                    "background-color": "#dbeafe",
                    "color": "#1e3a8a",
                    "font-weight": "600",
                },
            },
        )
    section = display_to_section.get(selected, current_choice)
    return section if section in menu_keys else current_choice


def borrower_home():
    render_main_page_title("Home")
    st.write("Borrower self-service pages can go here (loan applications, statements, etc.).")


def officer_home():
    render_main_page_title("Officer Dashboard")
    st.write("Loan officer workspace. Use the 'FarndaCred App' entry for full calculators.")


def admin_home():
    render_main_page_title("Admin Dashboard")
    st.session_state.setdefault("admin_users_panel", None)

    inject_tertiary_hyperlink_css_once()
    if msg := st.session_state.pop("admin_ok_msg", None):
        st.success(msg)
    if msg := st.session_state.pop("admin_info_msg", None):
        st.info(msg)

    tab_users, tab_audit = st.tabs(["User Management", "Security Audit Log"])

    with tab_users:
        # Admin User Management: align text inputs and Role select height; keep checkboxes on same baseline.
        st.markdown(
            """
            <style>
              [data-testid="stMain"] div[data-testid="stSelectbox"] [data-baseweb="select"] > div,
              [data-testid="stMain"] div[data-testid="stTextInput"] input {
                min-height: 2.75rem !important;
                box-sizing: border-box !important;
              }
              [data-testid="stMain"] div[data-testid="stSelectbox"] [data-baseweb="select"] > div {
                padding-top: 2px !important;
                padding-bottom: 2px !important;
              }
              [data-testid="stMain"] div[data-testid="stCheckbox"] label {
                min-height: 2.75rem !important;
                display: flex !important;
                align-items: center !important;
                margin-bottom: 0 !important;
              }
            </style>
            """,
            unsafe_allow_html=True,
        )

        render_sub_header("Users")
        try:
            conn = get_conn()
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return

        try:
            users_repo = UserRepository(conn)
            users = users_repo.list_users()
        except Exception as e:
            st.error(f"Could not load users: {e}")
            return
        finally:
            conn.close()

        if not users:
            st.info("No users found.")
        else:
            df = pd.DataFrame(
                [
                    {
                        "id": u.id,
                        "email": u.email,
                        "full_name": u.full_name,
                        "role": u.role,
                        "is_active": u.is_active,
                        "failed_attempts": u.failed_login_attempts,
                        "locked_until": u.locked_until,
                        "last_login": u.last_login,
                        "created_at": u.created_at,
                    }
                    for u in users
                ]
            )
            df_users = df.drop(columns=["password_hash"], errors="ignore")
            st.dataframe(
                df_users,
                width="stretch",
                hide_index=True,
                column_config=_dataframe_first_col_left_config(df_users),
            )

        open_m, open_c, _ = st.columns([1.25, 1.15, 4.5], gap=None, vertical_alignment="center")
        with open_m:
            if st.button(
                "Manage Selected User",
                disabled=not users,
                key="adm_open_manage",
                type="tertiary",
                help="Open user update panel",
            ):
                st.session_state["admin_users_panel"] = "manage"
                st.rerun()
        with open_c:
            if st.button(
                "Create New User",
                key="adm_open_create",
                type="tertiary",
                help="Open create user panel",
            ):
                st.session_state["admin_users_panel"] = "create"
                st.rerun()

        panel = st.session_state.get("admin_users_panel")

        if panel == "manage" and users:
            hdr_l, hdr_r = st.columns([8, 1], gap="small", vertical_alignment="center")
            with hdr_l:
                render_sub_sub_header("Manage Selected User")
            with hdr_r:
                if st.button("✕", key="adm_close_manage", help="Close"):
                    st.session_state["admin_users_panel"] = None
                    st.rerun()

            email_choices = [u.email for u in users]
            sel_col, role_col = st.columns([2, 3], gap="small", vertical_alignment="bottom")
            with sel_col:
                selected_email = st.selectbox("Select User", email_choices, key="adm_pick_user")
            selected_user = next(u for u in users if u.email == selected_email)
            with role_col:
                cu = get_current_user()
                _manage_roles = _assignable_roles_for_ui(
                    (cu or {}).get("role"),
                    current_user_role=str(selected_user.role),
                )
                _mr_idx = (
                    _manage_roles.index(selected_user.role)
                    if selected_user.role in _manage_roles
                    else 0
                )
                new_role = st.selectbox(
                    "Role",
                    _manage_roles,
                    index=_mr_idx,
                    key=f"adm_role_{selected_user.id}",
                )

            col2, col3, col4 = st.columns(3, gap="small", vertical_alignment="bottom")
            with col2:
                is_active = st.checkbox(
                    "Active",
                    value=selected_user.is_active,
                    key=f"adm_active_{selected_user.id}",
                )
            with col3:
                reset_pw = st.checkbox(
                    "Generate Temp Password",
                    value=False,
                    key=f"adm_resetpw_{selected_user.id}",
                )
            with col4:
                unlock = st.checkbox(
                    "Unlock Account",
                    value=False,
                    key=f"adm_unlock_{selected_user.id}",
                )

            if st.button("Apply Changes", type="primary", key=f"adm_apply_{selected_user.id}"):
                cu = get_current_user()
                ok_role, role_err = _user_role_edit_allowed(
                    (cu or {}).get("role"),
                    str(selected_user.role),
                    str(new_role),
                )
                if not ok_role:
                    st.error(role_err)
                else:
                    try:
                        conn_apply = get_conn()
                        temp_password = None
                        changed = False
                        try:
                            repo = UserRepository(conn_apply)
                            if selected_user.role != new_role:
                                repo.update_role(selected_user.id, new_role)
                                changed = True
                            if selected_user.is_active != is_active:
                                repo.set_active(selected_user.id, is_active)
                                changed = True
                            if reset_pw:
                                import secrets

                                temp_password = secrets.token_urlsafe(12)
                                auth = AuthService(conn_apply)
                                pw_hash = auth.hash_password(temp_password)
                                repo.update_password(selected_user.id, pw_hash)
                                changed = True
                            if unlock:
                                repo.unlock_account(selected_user.id)
                                changed = True
                        finally:
                            conn_apply.close()
                        if changed:
                            # Persist via session state so messages survive st.rerun() (same pattern as Create User).
                            st.session_state["admin_ok_msg"] = "Changes applied."
                            if temp_password:
                                st.session_state["admin_info_msg"] = (
                                    f"Temporary password for {selected_email}: `{temp_password}`"
                                )
                            st.rerun()
                        else:
                            st.info("No changes to apply.")
                    except Exception as e:
                        st.error(f"Failed to apply changes: {e}")

        elif panel == "create":
            hdr_l, hdr_r = st.columns([8, 1], gap="small", vertical_alignment="center")
            with hdr_l:
                render_sub_sub_header("Create New User")
            with hdr_r:
                if st.button("✕", key="adm_close_create", help="Close"):
                    st.session_state["admin_users_panel"] = None
                    st.rerun()

            r1c1, r1c2 = st.columns(2, gap="small", vertical_alignment="bottom")
            with r1c1:
                new_email = st.text_input("Email", key="adm_new_email")
            with r1c2:
                new_full_name = st.text_input("Full Name", key="adm_new_full_name")
            r2c1, r2c2 = st.columns(2, gap="small", vertical_alignment="bottom")
            with r2c1:
                cu = get_current_user()
                _ar_new = _assignable_roles_for_ui((cu or {}).get("role"))
                _ar_idx = _ar_new.index("BORROWER") if "BORROWER" in _ar_new else 0
                new_role = st.selectbox(
                    "Role",
                    _ar_new,
                    index=_ar_idx,
                    key="adm_new_role",
                )
            with r2c2:
                new_password = st.text_input(
                    "Initial Password (Blank = Auto-Generate)",
                    type="password",
                    key="adm_new_password",
                )

            if st.button("Create User", type="primary", key="adm_create_user"):
                cu = get_current_user()
                ok_cr, cr_err = _user_role_edit_allowed((cu or {}).get("role"), "BORROWER", str(new_role))
                if not ok_cr:
                    st.error(cr_err)
                else:
                    try:
                        conn_c = get_conn()
                        try:
                            users_repo = UserRepository(conn_c)
                            if users_repo.get_by_email(new_email):
                                st.error("A user with that email already exists.")
                            else:
                                import secrets

                                raw_password = new_password or secrets.token_urlsafe(12)
                                auth = AuthService(conn_c)
                                pw_hash = auth.hash_password(raw_password)
                                user = users_repo.create_user(
                                    email=new_email,
                                    password_hash=pw_hash,
                                    full_name=new_full_name,
                                    role=new_role,
                                )
                                st.session_state["admin_ok_msg"] = f"User {user.email} created."
                                if not new_password:
                                    st.session_state["admin_info_msg"] = (
                                        f"Temporary password for {user.email}: `{raw_password}`"
                                    )
                                st.session_state["admin_users_panel"] = None
                                st.rerun()
                        finally:
                            conn_c.close()
                    except Exception as e:
                        st.error(f"Failed to create user: {e}")

    with tab_audit:
        render_sub_sub_header("Recent login activity")
        _to_d = date.today()
        _from_d = _to_d - timedelta(days=30)
        # One row: From + control | To + control | CSV | PDF (wider) | flexible space — all top-aligned
        _r = st.columns([1.95, 1.95, 1.0, 1.75, 3.35], gap="small", vertical_alignment="top")
        with _r[0]:
            _fl, _fi = st.columns([0.5, 1.5], gap="xsmall", vertical_alignment="top")
            with _fl:
                st.markdown(
                    '<p style="margin:0;padding-top:0.15rem;font-weight:600;">From</p>',
                    unsafe_allow_html=True,
                )
            with _fi:
                d_from = st.date_input(
                    "From",
                    value=_from_d,
                    key="adm_audit_date_from",
                    format="DD/MM/YYYY",
                    label_visibility="collapsed",
                )
        with _r[1]:
            _tl, _ti = st.columns([0.45, 1.55], gap="xsmall", vertical_alignment="top")
            with _tl:
                st.markdown(
                    '<p style="margin:0;padding-top:0.15rem;font-weight:600;">To</p>',
                    unsafe_allow_html=True,
                )
            with _ti:
                d_to = st.date_input(
                    "To",
                    value=_to_d,
                    key="adm_audit_date_to",
                    format="DD/MM/YYYY",
                    label_visibility="collapsed",
                )

        rows: list = []
        _err_audit = None
        if d_from > d_to:
            _err_audit = "From date must be on or before To date."
        else:
            try:
                conn = get_conn()
                try:
                    repo = SecurityAuditLogRepository(conn)
                    rows = repo.list_between(d_from, d_to, limit=5000)
                finally:
                    conn.close()
            except Exception as e:
                _err_audit = f"Database error: {e}"

        _stem = f"recent_login_activity_{d_from.isoformat()}_{d_to.isoformat()}"
        _csv_bytes = b" "
        _pdf_bytes: bytes | None = None
        _can_export = not _err_audit and bool(rows)
        df_show = None
        header_list: list[str] = []

        if _can_export:
            df = pd.DataFrame(rows)
            _audit_order = [
                "id",
                "created_at",
                "email_used",
                "success",
                "ip_address",
                "user_agent",
                "event_type",
                "user_id",
            ]
            cols_show = [c for c in _audit_order if c in df.columns]
            df_show = df[cols_show].copy()
            if "id" in df_show.columns:
                df_show["id"] = df_show["id"].map(_audit_log_id_cell)
            if "created_at" in df_show.columns:
                df_show["created_at"] = df_show["created_at"].map(_audit_log_ts_cell)
            if "success" in df_show.columns:
                df_show["success"] = df_show["success"].map(
                    lambda x: "Yes" if x is True else ("No" if x is False else "")
                )
            _audit_headers = {
                "id": "ID",
                "created_at": "When",
                "email_used": "Email",
                "success": "Success",
                "ip_address": "IP",
                "user_agent": "User Agent",
                "event_type": "Event",
                "user_id": "User ID",
            }
            header_list = [_audit_headers[c] for c in cols_show]
            df_export = df_show.copy()
            df_export.columns = header_list
            _csv_buf = BytesIO()
            df_export.to_csv(_csv_buf, index=False, encoding="utf-8-sig", lineterminator="\n")
            _csv_bytes = _csv_buf.getvalue()
            _pdf_bytes = _recent_login_audit_pdf_bytes(df_export, title="Recent login activity")

        with _r[2]:
            st.download_button(
                "Download CSV",
                data=_csv_bytes,
                file_name=f"{_stem}.csv",
                mime="text/csv; charset=utf-8",
                key="adm_audit_login_csv",
                type="tertiary",
                disabled=not _can_export,
                use_container_width=True,
            )
        with _r[3]:
            st.download_button(
                "Download PDF",
                data=_pdf_bytes if _pdf_bytes else b" ",
                file_name=f"{_stem}.pdf",
                mime="application/pdf",
                key="adm_audit_login_pdf",
                type="tertiary",
                disabled=not _can_export or _pdf_bytes is None,
                use_container_width=True,
            )
        with _r[4]:
            st.empty()

        if _err_audit:
            st.error(_err_audit)
        elif not rows:
            st.info("No audit events in this date range.")
        else:
            _df_audit_display = df_show.copy()
            _df_audit_display.columns = header_list
            st.dataframe(
                _df_audit_display,
                width="stretch",
                hide_index=True,
                column_config=_dataframe_first_col_left_config(_df_audit_display),
            )


def loan_management_app():
    """
    Wrapper that calls the existing app.main() to render the legacy/back-office UI.
    This is only reachable for LOAN_OFFICER and ADMIN via role-based menu.
    """
    loan_app.main()


def _build_menu_for_role_legacy(role: str) -> dict[str, callable]:
    """Fallback when RBAC tables are missing or return no permissions."""
    if role == "BORROWER":
        return {"Home": borrower_home}

    loan_sections = loan_app.get_loan_app_sections()

    if role == "VENDOR":
        return {
            "Subscription": lambda: loan_app.render_loan_app_section("Subscription"),
        }

    if role == "LOAN_OFFICER":
        allowed = [s for s in loan_sections if s != "System configurations"]
        menu: dict[str, callable] = {"Officer Dashboard": officer_home}
        for section in allowed:
            menu[section] = lambda section_name=section: loan_app.render_loan_app_section(section_name)
        return menu

    if role == "ADMIN":
        menu = {"Admin Dashboard": admin_home}
        for section in loan_sections:
            menu[section] = lambda section_name=section: loan_app.render_loan_app_section(section_name)
        return menu

    if role == "SUPERADMIN":
        menu = {"Admin Dashboard": admin_home}
        for section in loan_sections:
            menu[section] = lambda section_name=section: loan_app.render_loan_app_section(section_name)
        return menu

    return {}


def _build_menu_from_permission_keys(role: str, keys: frozenset[str]) -> dict[str, callable]:
    from auth.permission_catalog import (
        PERMISSION_DASHBOARD_ADMIN,
        PERMISSION_DASHBOARD_OFFICER,
        nav_permission_key_for_section,
    )

    loan_sections = loan_app.get_loan_app_sections()
    menu: dict[str, callable] = {}

    if PERMISSION_DASHBOARD_OFFICER in keys:
        menu["Officer Dashboard"] = officer_home
    elif PERMISSION_DASHBOARD_ADMIN in keys:
        menu["Admin Dashboard"] = admin_home

    for section in loan_sections:
        pk = nav_permission_key_for_section(section)
        if pk and pk in keys:
            menu[section] = lambda section_name=section: loan_app.render_loan_app_section(section_name)

    if not menu and role == "VENDOR":
        sub_k = nav_permission_key_for_section("Subscription")
        if sub_k and sub_k in keys:
            menu["Subscription"] = lambda: loan_app.render_loan_app_section("Subscription")

    return menu


def build_menu_for_role(role: str) -> dict[str, callable]:
    if role == "BORROWER":
        return {"Home": borrower_home}

    try:
        from rbac.service import get_permission_keys_for_role_key, rbac_tables_ready

        if not rbac_tables_ready():
            return _build_menu_for_role_legacy(role)
        k = get_permission_keys_for_role_key(role)
        if not k:
            return _build_menu_for_role_legacy(role)
        return _build_menu_from_permission_keys(role, k)
    except Exception:
        return _build_menu_for_role_legacy(role)


def main():
    st.set_page_config(
        page_title="FarndaCred – Secure",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_farnda_global_styles_once()

    user = get_current_user()
    if user is None:
        # Not logged in: only show auth page (login/register)
        auth_page()
        render_footer()
        return

    enroll_codes = st.session_state.get("_farnda_enrollment_recovery_codes")
    if (
        isinstance(enroll_codes, list)
        and enroll_codes
        and (user.get("role") or "") in ("SUPERADMIN", "VENDOR")
    ):
        render_main_page_title("Save your recovery codes")
        st.warning(
            "**Copy the codes below** and store them somewhere safe (password manager or printout). "
            "They are not shown again after you continue. "
            "If the menu on the left is hidden, click the **>** control at the top-left to open the sidebar."
        )
        st.text_area(
            "Recovery codes (one per line)",
            value="\n".join(enroll_codes),
            height=240,
            key="farnda_enrollment_codes_gate",
        )
        if st.button(
            "I have saved these codes — continue to the app",
            type="primary",
            key="farnda_enrollment_codes_dismiss",
        ):
            st.session_state.pop("_farnda_enrollment_recovery_codes", None)
            st.rerun()
        render_footer()
        st.stop()

    # Logged in: show role-filtered sidebar
    try:
        from eod.system_business_date import get_effective_date

        system_date = get_effective_date()
    except ImportError:
        system_date = datetime.now().date()

    menu = build_menu_for_role(user["role"])
    if user["role"] in ("ADMIN", "LOAN_OFFICER", "VENDOR", "SUPERADMIN"):
        from subscription.access import (
            filter_menu_for_subscription,
            refresh_subscription_access_snapshot,
        )

        snap = refresh_subscription_access_snapshot(user)
        if snap.terminated and not snap.enforcement_skipped:
            st.error(
                "Your organisation's subscription has been terminated. Contact your administrator."
            )
            st.stop()
        menu = filter_menu_for_subscription(menu, snap, role=user["role"])

    fz = st.session_state.get("subscription_frozen_effective_date")
    if fz is not None and isinstance(fz, date):
        system_date = min(system_date, fz)

    if not menu:
        st.error("No pages available for your role.")
        return

    render_sidebar_branding()
    nav_key = "farnda_main_nav_choice"
    menu_keys = list(menu.keys())
    current_choice = st.session_state.get(nav_key)
    if current_choice not in menu:
        current_choice = menu_keys[0]
        st.session_state[nav_key] = current_choice
    _apply_sidebar_option_menu_iframe_height(len(menu_keys))
    choice = render_sidebar_option_menu(menu_keys=menu_keys, current_choice=current_choice)
    st.session_state[nav_key] = choice
    render_sidebar_user_meta(user=user, system_date=system_date)
    render_totp_recovery_regeneration_sidebar(user)
    if msg := st.session_state.pop("_farnda_tenant_bind_message", None):
        st.sidebar.warning(str(msg))

    # Global guard to ensure we never render a page without a user
    require_login()
    if user["role"] in ("ADMIN", "LOAN_OFFICER", "SUPERADMIN"):
        from subscription.access import get_subscription_snapshot, render_subscription_banners

        render_subscription_banners(get_subscription_snapshot())
    page_fn = menu[choice]
    page_fn()
    if st.sidebar.button("Log out", key="sidebar_logout", type="primary", use_container_width=True):
        clear_current_user()
        st.rerun()
    render_footer()


if __name__ == "__main__":
    main()

