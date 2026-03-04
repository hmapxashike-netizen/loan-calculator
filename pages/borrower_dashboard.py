import streamlit as st

from middleware import require_role


@require_role("BORROWER")
def borrower_dashboard():
    st.title("Borrower Dashboard")
    st.write("Your loans and applications will appear here.")

