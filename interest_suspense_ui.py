import psycopg2
import psycopg2.extras
import pandas as pd
import streamlit as st

from config import get_database_url
from display_formatting import format_display_currency


def _format_loan_status(status: object) -> str:
    if status is None or str(status).strip() == "":
        return "—"
    return str(status).replace("_", " ").strip().title()


def render_suspense_ui():
    st.header("Interest in Suspense Management")
    
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
            
            _s_lab, _s_dd, _s_sp = st.columns([2, 6, 4], gap="small", vertical_alignment="center")
            with _s_lab:
                st.markdown(
                    '<p style="margin:0;padding-top:0.45rem;font-weight:600;color:#31333F;">Search and select a loan</p>',
                    unsafe_allow_html=True,
                )
            with _s_dd:
                selected = st.selectbox(
                    "Search and select a loan",
                    options,
                    label_visibility="collapsed",
                    key="iis_loan_pick",
                )
            with _s_sp:
                st.empty()

            if selected:
                loan = loan_map[selected]
                summary_df = pd.DataFrame(
                    [
                        {
                            "Loan ID": str(int(loan["id"])),
                            "Customer ID": str(int(loan["customer_id"])),
                            "Principal": format_display_currency(loan["principal"]),
                            "Current Status": _format_loan_status(loan.get("status")),
                            "Interest in Suspense Flag": "YES"
                            if loan["interest_in_suspense"]
                            else "NO",
                        }
                    ]
                )
                _cn = [str(c) for c in summary_df.columns]
                _cc = {_cn[0]: {"alignment": "left"}}
                for _c in _cn[1:]:
                    _cc[_c] = {"alignment": "center"}
                st.dataframe(
                    summary_df,
                    width="stretch",
                    hide_index=True,
                    height=88,
                    column_config=_cc,
                )

                new_flag = st.radio(
                    "Mark Interest In Suspense?",
                    [True, False],
                    index=0 if loan["interest_in_suspense"] else 1,
                    format_func=lambda x: "YES" if x else "NO",
                )
                
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
