import streamlit as st

from middleware import require_role


@require_role("LOAN_OFFICER", "ADMIN")
def reports_page():
    st.title("Reports")
    st.write("Portfolio and risk reports will appear here.")

