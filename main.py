from __future__ import annotations

from datetime import datetime

import streamlit as st
import pandas as pd

from middleware import get_current_user, clear_current_user, require_login
from auth_ui import auth_page
from dal import get_conn, UserRepository, SecurityAuditLogRepository
from auth_service import AuthService

import app as loan_app  # reuse existing Streamlit loan UI


def borrower_home():
    st.title("Borrower Home")
    st.write("Borrower self-service pages can go here (loan applications, statements, etc.).")


def officer_home():
    st.title("Loan Officer Dashboard")
    st.write("Loan officer workspace. Use the 'Loan Management App' entry for full calculators.")


def admin_home():
    st.title("Admin Dashboard")

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


def build_menu_for_role(role: str):
    """
    Returns an ordered mapping: label -> callable for pages allowed for this role.
    """
    if role == "BORROWER":
        return {
            "Home": borrower_home,
        }
    if role == "LOAN_OFFICER":
        return {
            "Officer Dashboard": officer_home,
            "Loan Management App": loan_management_app,
        }
    if role == "ADMIN":
        return {
            "Admin Dashboard": admin_home,
            "Loan Management App": loan_management_app,
        }
    return {}


def main():
    st.set_page_config(page_title="LMS – Secure", layout="wide")

    user = get_current_user()
    if user is None:
        # Not logged in: only show auth page (login/register)
        auth_page()
        return

    # Logged in: show role-filtered sidebar
    st.sidebar.write(f"Logged in as **{user['full_name']}** ({user['role']})")
    try:
        from system_business_date import get_effective_date
        system_date = get_effective_date()
    except ImportError:
        system_date = datetime.now().date()
    now = datetime.now()
    st.sidebar.caption(f"**System date:** {system_date.isoformat()}")
    st.sidebar.caption(f"**Calendar date:** {now.strftime('%Y-%m-%d %H:%M:%S')}")
    if st.sidebar.button("Log out"):
        clear_current_user()
        st.rerun() 

    menu = build_menu_for_role(user["role"])
    if not menu:
        st.error("No pages available for your role.")
        return

    choice = st.sidebar.radio("Navigation", list(menu.keys()))

    # Global guard to ensure we never render a page without a user
    require_login()
    page_fn = menu[choice]
    page_fn()


if __name__ == "__main__":
    main()

