from __future__ import annotations

import streamlit as st

from middleware import get_current_user, clear_current_user, require_login
from auth_ui import auth_page
from pages.borrower_dashboard import borrower_dashboard
from pages.loan_officer_dashboard import loan_officer_dashboard
from pages.admin_dashboard import admin_dashboard
from pages.reports import reports_page


def build_menu_for_role(role: str):
    """
    Map of sidebar labels to page callables, per role.
    """
    if role == "BORROWER":
        return {
            "My Dashboard": borrower_dashboard,
        }
    if role == "LOAN_OFFICER":
        return {
            "Officer Dashboard": loan_officer_dashboard,
            "Reports": reports_page,
        }
    if role == "ADMIN":
        return {
            "Admin Dashboard": admin_dashboard,
            "Officer Dashboard": loan_officer_dashboard,
            "Borrower Dashboard": borrower_dashboard,
            "Reports": reports_page,
        }
    return {}


def main():
    st.set_page_config(page_title="Loan Management System", layout="wide")

    user = get_current_user()
    if user is None:
        # Not logged in: only show auth screen
        auth_page()
        return

    # Logged in: show role-filtered menu
    with st.sidebar:
        st.markdown(f"**{user['full_name']}**<br/><span style='font-size: 0.85rem;'>{user['email']}</span>", unsafe_allow_html=True)
        st.write(f"Role: `{user['role']}`")
        if st.button("Log out"):
            clear_current_user()
            st.experimental_rerun()

        st.divider()
        menu = build_menu_for_role(user["role"])
        if not menu:
            st.error("No pages available for your role.")
            return
        choice = st.radio("Navigation", list(menu.keys()))

    # Global safeguard: never render a page without a user
    require_login()
    page_fn = menu[choice]
    page_fn()


if __name__ == "__main__":
    main()

