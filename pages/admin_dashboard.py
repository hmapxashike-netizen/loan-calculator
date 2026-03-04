import streamlit as st

from middleware import require_role


@require_role("ADMIN")
def admin_dashboard():
    st.title("Admin Dashboard")
    st.write("User management and system settings will appear here.")

