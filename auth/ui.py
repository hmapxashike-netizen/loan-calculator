from __future__ import annotations

from pathlib import Path

import streamlit as st

from dal import get_conn, UserRepository
from auth.service import AuthService
from middleware import set_current_user, clear_current_user
from style import render_sub_sub_header

from ui.streamlit_feedback import run_with_spinner


def _logo_path() -> Path:
    """Same asset names as ``main._logo_path``: files usually live at repo root, not under ``auth/``."""
    auth_dir = Path(__file__).resolve().parent
    project_root = auth_dir.parent
    for base in (project_root, auth_dir):
        for file_name in ("FarndaCred logo with.svg", "FarndaCred logo with.png"):
            candidate = base / file_name
            if candidate.exists():
                return candidate
    return project_root / "FarndaCred logo with.png"


def login_form():
    render_sub_sub_header("Login")
    email = st.text_input("Email", key="login_email")
    password = st.text_input("Password", type="password", key="login_password")

    if st.button("Login", type="primary"):
        if not email or not password:
            st.error("Please enter both email and password.")
            return

        try:
            conn = get_conn()
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return

        try:
            auth = AuthService(conn)
            user, status = run_with_spinner(
                "Signing in…",
                lambda: auth.authenticate(email=email, password=password, ip=None, user_agent=None),
            )
        finally:
            conn.close()

        if status == "locked":
            st.error("Your account is locked due to too many failed attempts. Please try again later or contact an administrator.")
            return

        if status != "ok" or not user:
            st.error("Invalid email or password.")
            return

        if not user.is_active:
            st.error("Your account is inactive. Please contact an administrator.")
            return

        set_current_user(user)
        try:
            from db.tenant_registry import bind_default_tenant_context_safely

            bind_default_tenant_context_safely()
        except Exception:
            pass
        st.success(f"Welcome, {user.full_name}!")
        st.rerun()


def registration_form():
    render_sub_sub_header("Register (Borrower)")
    email = st.text_input("Email", key="reg_email")
    full_name = st.text_input("Full name", key="reg_full_name")
    password = st.text_input("Password", type="password", key="reg_password")

    if st.button("Create account"):
        if not (email and full_name and password):
            st.error("All fields are required.")
            return

        try:
            conn = get_conn()
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return

        try:
            users = UserRepository(conn)
            if users.get_by_email(email):
                # Generic error to avoid enumeration
                st.error("Unable to create account.")
                return

            auth = AuthService(conn)

            def _hash_and_create():
                pw_hash = auth.hash_password(password)
                return users.create_user(
                    email=email,
                    password_hash=pw_hash,
                    full_name=full_name,
                    role="BORROWER",
                )

            run_with_spinner("Creating account…", _hash_and_create)
        finally:
            conn.close()

        st.success("Account created. You can now log in.")


def auth_page():
    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
        logo_path = _logo_path()
        if logo_path.exists():
            logo_left, logo_mid, logo_right = st.columns([1, 2, 1])
            with logo_mid:
                st.image(str(logo_path), width=320)
        else:
            st.markdown(
                '<p class="farnda-auth-wordmark-fallback">FarndaCred</p>',
                unsafe_allow_html=True,
            )

        if "current_user" in st.session_state:
            u = st.session_state["current_user"]
            st.info(f"Logged in as {u['email']} ({u['role']})")
            if st.button("Log out"):
                clear_current_user()
                st.rerun()
            return

        st.markdown(
            '<p class="farnda-auth-slogan">Calculated Value, Unmatched Trust</p>',
            unsafe_allow_html=True,
        )

        tab_login, tab_register = st.tabs(["Login", "Register"])
        with tab_login:
            login_form()
        with tab_register:
            registration_form()

