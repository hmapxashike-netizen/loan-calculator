import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
from config import get_database_url

def render_suspense_ui():
    st.header("Interest in Suspense Management")
    st.markdown("Manually flag or unflag loans for Interest in Suspense (Non-Accrual Status).")
    
    conn = psycopg2.connect(get_database_url(), cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        with conn.cursor() as cur:
            # Get all active loans
            cur.execute("""
                SELECT l.id,
                       l.status,
                       l.principal,
                       l.customer_id,
                       l.interest_in_suspense
                FROM loans l
                ORDER BY l.id DESC
            """)
            loans = cur.fetchall()
            
            if not loans:
                st.info("No active loans found.")
                return
            
            # Format options for search
            options = []
            loan_map = {}
            for row in loans:
                label = (
                    f"Loan {row['id']} - Customer {row['customer_id']} "
                    f"- Status: {row['status']} - Suspense: {'YES' if row['interest_in_suspense'] else 'NO'}"
                )
                options.append(label)
                loan_map[label] = row
            
            selected = st.selectbox("Search and select a loan", options)
            
            if selected:
                loan = loan_map[selected]
                st.subheader(f"Loan ID: {loan['id']}")
                st.write(f"**Customer ID:** {loan['customer_id']}")
                st.write(f"**Principal:** ${loan['principal']:,.2f}")
                st.write(f"**Current Status:** {loan['status']}")
                st.write(f"**Interest in Suspense Flag:** {'YES' if loan['interest_in_suspense'] else 'NO'}")
                
                new_flag = st.radio("Mark Interest in Suspense?", [True, False], index=0 if loan['interest_in_suspense'] else 1)
                
                if new_flag != loan['interest_in_suspense']:
                    if st.button("Update Flag"):
                        cur.execute("UPDATE loans SET interest_in_suspense = %s WHERE id = %s", (new_flag, loan['id']))
                        conn.commit()
                        st.success(f"Successfully updated Interest in Suspense flag to {'YES' if new_flag else 'NO'} for Loan {loan['id']}.")
                        
                        # Tell them about the catch-up journal
                        if new_flag == True:
                            st.warning("Note: Moving a loan to Suspense manually requires posting a catch-up journal (`REVERSAL_REGULAR_INTEREST_ACCRUAL`) for already recognized income. Please ensure this is posted in the Journals module.")
                        
                        st.rerun()
                else:
                    st.info("No changes to make.")
                    
    finally:
        conn.close()
