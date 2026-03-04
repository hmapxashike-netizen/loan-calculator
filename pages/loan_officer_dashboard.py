import streamlit as st

from middleware import require_role


@require_role("LOAN_OFFICER", "ADMIN")
def loan_officer_dashboard():
    st.title("Loan Officer Dashboard")
    st.write("Loan approvals and portfolio view will appear here.")

