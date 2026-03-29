from __future__ import annotations

import base64
from datetime import datetime
from pathlib import Path
import importlib.util

import streamlit as st
import pandas as pd

from middleware import get_current_user, clear_current_user, require_login
from auth_ui import auth_page
from dal import get_conn, UserRepository, SecurityAuditLogRepository
from auth_service import AuthService


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


def render_footer() -> None:
    st.markdown("---")
    st.markdown(
        "<div style='text-align:center; color:#64748B; font-size:0.85rem; padding-bottom:0.5rem;'>"
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
    logo_path = _logo_path()
    if logo_path.exists():
        mime = "image/svg+xml" if logo_path.suffix.lower() == ".svg" else "image/png"
        logo_b64 = base64.b64encode(logo_path.read_bytes()).decode("ascii")
        st.sidebar.markdown(
            f"""
            <div style="background:#E5E7EB; border-radius:10px; padding:0.55rem; margin-bottom:0.35rem;">
                <img src="data:{mime};base64,{logo_b64}" style="width:100%; height:auto; display:block;" />
            </div>
            <div style="font-size:0.82rem; color:#374151; margin-top:0.15rem; text-align:center;">
                Calculated Value, Unmatched Trust
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.sidebar.markdown(
            "<div style='font-size:0.82rem; color:#374151; text-align:center;'>Calculated Value, Unmatched Trust</div>",
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


def render_sidebar_user_meta(user: dict, system_date, calendar_date) -> None:
    display_name = _format_sidebar_name(user.get("full_name", "User"))
    role = user.get("role", "USER")
    st.sidebar.markdown(
        f"""
        <div style="background:#F3F4F6; border:1px solid #D1D5DB; border-radius:10px; padding:0.7rem; margin-top:0.3rem;">
            <div style="font-size:0.75rem; color:#6B7280; margin-bottom:0.2rem;">Logged in as</div>
            <div style="font-weight:600; color:#111827; margin-bottom:0.4rem;">{display_name} ({role})</div>
            <div style="font-size:1.02rem; color:#16A34A; font-weight:700;">System date: {system_date.isoformat()}</div>
            <div style="font-size:0.85rem; color:#374151;">Calendar date: {calendar_date.isoformat()}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def borrower_home():
    st.header("Borrower Home")
    st.write("Borrower self-service pages can go here (loan applications, statements, etc.).")


def officer_home():
    st.header("Loan Officer Dashboard")
    st.write("Loan officer workspace. Use the 'FarndaCred App' entry for full calculators.")


def admin_home():
    st.header("Admin Dashboard")

    tab_users, tab_audit = st.tabs(["User Management", "Security Audit Log"])

    with tab_users:
        st.subheader("Users")
        try:
            conn = get_conn()
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return

        users_repo = UserRepository(conn)

        try:
            users = users_repo.list_users()
        except Exception as e:
            conn.close()
            st.error(f"Could not load users: {e}")
            return

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
            st.dataframe(df.drop(columns=["password_hash"], errors="ignore"), width="stretch")

            st.markdown("### Manage selected user")
            email_choices = [u.email for u in users]
            selected_email = st.selectbox("Select user", email_choices) if email_choices else None

            if selected_email:
                selected_user = next(u for u in users if u.email == selected_email)
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    new_role = st.selectbox(
                        "Role",
                        ["ADMIN", "LOAN_OFFICER", "BORROWER"],
                        index=["ADMIN", "LOAN_OFFICER", "BORROWER"].index(selected_user.role),
                        key=f"adm_role_{selected_user.id}",
                    )
                with col2:
                    is_active = st.checkbox(
                        "Active",
                        value=selected_user.is_active,
                        key=f"adm_active_{selected_user.id}",
                    )
                with col3:
                    reset_pw = st.checkbox(
                        "Generate temp password",
                        value=False,
                        key=f"adm_resetpw_{selected_user.id}",
                    )
                with col4:
                    unlock = st.checkbox(
                        "Unlock account",
                        value=False,
                        key=f"adm_unlock_{selected_user.id}",
                    )

                if st.button("Apply changes", type="primary", key=f"adm_apply_{selected_user.id}"):
                    try:
                        if selected_user.role != new_role:
                            users_repo.update_role(selected_user.id, new_role)
                        if selected_user.is_active != is_active:
                            users_repo.set_active(selected_user.id, is_active)
                        temp_password = None
                        if reset_pw:
                            import secrets

                            temp_password = secrets.token_urlsafe(12)
                            auth = AuthService(conn)
                            pw_hash = auth.hash_password(temp_password)
                            users_repo.update_password(selected_user.id, pw_hash)
                        if unlock:
                            users_repo.unlock_account(selected_user.id)
                        conn.close()
                        st.success("Changes applied.")
                        if temp_password:
                            st.info(f"Temporary password for {selected_email}: `{temp_password}`")
                        st.rerun()
                    except Exception as e:
                        conn.close()
                        st.error(f"Failed to apply changes: {e}")

        st.markdown("---")
        st.subheader("Create new user")
        new_email = st.text_input("Email", key="adm_new_email")
        new_full_name = st.text_input("Full name", key="adm_new_full_name")
        new_role = st.selectbox(
            "Role for new user",
            ["BORROWER", "LOAN_OFFICER", "ADMIN"],
            index=0,
            key="adm_new_role",
        )
        new_password = st.text_input("Initial password (leave blank to auto-generate)", type="password")

        if st.button("Create user", type="primary", key="adm_create_user"):
            try:
                conn = get_conn()
                users_repo = UserRepository(conn)
                if users_repo.get_by_email(new_email):
                    st.error("A user with that email already exists.")
                else:
                    import secrets

                    raw_password = new_password or secrets.token_urlsafe(12)
                    auth = AuthService(conn)
                    pw_hash = auth.hash_password(raw_password)
                    user = users_repo.create_user(
                        email=new_email,
                        password_hash=pw_hash,
                        full_name=new_full_name,
                        role=new_role,
                    )
                    conn.close()
                    st.success(f"User {user.email} created.")
                    if not new_password:
                        st.info(f"Temporary password for {user.email}: `{raw_password}`")
            except Exception as e:
                st.error(f"Failed to create user: {e}")

    with tab_audit:
        st.subheader("Recent login activity")
        try:
            conn = get_conn()
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return

        repo = SecurityAuditLogRepository(conn)
        try:
            rows = repo.list_recent(limit=200)
        except Exception as e:
            conn.close()
            st.error(f"Could not load audit log: {e}")
            return

        conn.close()

        if not rows:
            st.info("No audit events yet.")
        else:
            df = pd.DataFrame(rows)
            st.dataframe(df, width="stretch")


def loan_management_app():
    """
    Wrapper that calls the existing app.main() to render the legacy/back-office UI.
    This is only reachable for LOAN_OFFICER and ADMIN via role-based menu.
    """
    loan_app.main()


def build_menu_for_role(role: str) -> dict[str, callable]:
    if role == "BORROWER":
        return {"Home": borrower_home}

    loan_sections = loan_app.get_loan_app_sections()

    if role == "LOAN_OFFICER":
        allowed = [s for s in loan_sections if s != "System configurations"]
        menu: dict[str, callable] = {"Officer Dashboard": officer_home}
        for section in allowed:
            menu[section] = lambda section_name=section: loan_app.render_loan_app_section(section_name)
        return menu

    if role == "ADMIN":
        menu: dict[str, callable] = {"Admin Dashboard": admin_home}
        for section in loan_sections:
            menu[section] = lambda section_name=section: loan_app.render_loan_app_section(section_name)
        return menu

    return {}


def main():
    st.set_page_config(page_title="FarndaCred – Secure", layout="wide")

    user = get_current_user()
    if user is None:
        # Not logged in: only show auth page (login/register)
        auth_page()
        render_footer()
        return

    # Logged in: show role-filtered sidebar
    try:
        from system_business_date import get_effective_date
        system_date = get_effective_date()
    except ImportError:
        system_date = datetime.now().date()
    now = datetime.now().date()

    menu = build_menu_for_role(user["role"])
    if not menu:
        st.error("No pages available for your role.")
        return

    render_sidebar_branding()
    st.sidebar.divider()
    st.sidebar.markdown(
        "<div style='font-weight:700; font-size:125%; margin-bottom:0.15rem;'>Navigation</div>",
        unsafe_allow_html=True,
    )
    choice = st.sidebar.radio("Navigation", list(menu.keys()), label_visibility="collapsed")
    st.sidebar.divider()
    render_sidebar_user_meta(user=user, system_date=system_date, calendar_date=now)

    # Global guard to ensure we never render a page without a user
    require_login()
    page_fn = menu[choice]
    page_fn()
    st.sidebar.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
    st.sidebar.divider()
    if st.sidebar.button("Log out", key="sidebar_logout"):
        clear_current_user()
        st.rerun()
    render_footer()


if __name__ == "__main__":
    main()

