from __future__ import annotations

import streamlit as st

from dal import get_conn, UserRepository
from auth_service import AuthService
from middleware import set_current_user, clear_current_user


def login_form():
    st.subheader("Login")
    # Use a centered, narrower column so fields are not full-width
    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
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
            user, status = auth.authenticate(email=email, password=password, ip=None, user_agent=None)
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
        st.success(f"Welcome, {user.full_name}!")
        st.rerun()


def registration_form():
    st.subheader("Register (Borrower)")
    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
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
            pw_hash = auth.hash_password(password)
            user = users.create_user(
                email=email,
                password_hash=pw_hash,
                full_name=full_name,
                role="BORROWER",
            )
        finally:
            conn.close()

        st.success("Account created. You can now log in.")


def auth_page():
    st.title("Loan Management System")

    if "current_user" in st.session_state:
        u = st.session_state["current_user"]
        st.info(f"Logged in as {u['email']} ({u['role']})")
        if st.button("Log out"):
            clear_current_user()
            st.rerun()
        return

    tab_login, tab_register = st.tabs(["Login", "Register"])
    with tab_login:
        login_form()
    with tab_register:
        registration_form()

