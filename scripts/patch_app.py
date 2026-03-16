import re

new_ui = '''def accounting_ui():
    """
    Database-backed Accounting Module.
    """
    from accounting_service import AccountingService
    import pandas as pd
    from datetime import datetime
    import streamlit as st

    svc = AccountingService()

    st.markdown(
        "<div style='background-color: #0F766E; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Accounting Module</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    tab_coa, tab_templates, tab_reports = st.tabs(
        ["Chart of Accounts", "Transaction Templates", "Financial Reports"]
    )

    # 1. Chart of Accounts
    with tab_coa:
        st.subheader("Chart of Accounts")
        if not svc.is_coa_initialized():
            st.warning("Chart of Accounts is not initialized.")
            if st.button("Initialize Default Chart of Accounts"):
                svc.initialize_default_coa()
                st.success("Default Chart of Accounts initialized!")
                st.rerun()
        
        accounts = svc.list_accounts()
        if accounts:
            df_accounts = pd.DataFrame([{
                "Code": a["code"],
                "Name": a["name"],
                "Category": a["category"],
                "System Tag": a["system_tag"] or "",
                "Parent Code": a["parent_code"] or ""
            } for a in accounts])
            st.dataframe(df_accounts, use_container_width=True, hide_index=True)
        
        st.divider()
        st.subheader("Add Custom Account")
        with st.form("add_account_form"):
            code = st.text_input("Account Code (e.g. A100003)")
            name = st.text_input("Account Name")
            category = st.selectbox("Category", ["ASSET", "LIABILITY", "EQUITY", "INCOME", "EXPENSE"])
            system_tag = st.text_input("System Tag (Optional)")
            submitted = st.form_submit_button("Create Account")
            if submitted:
                if code and name:
                    svc.create_account(code, name, category, system_tag if system_tag else None)
                    st.success("Account created!")
                    st.rerun()
                else:
                    st.error("Code and Name are required.")

    # 2. Transaction Templates
    with tab_templates:
        st.subheader("Transaction Templates (Journal Links)")
        templates = svc.list_all_transaction_templates()
        if templates:
            df_templates = pd.DataFrame([{
                "Event Type": t["event_type"],
                "System Tag": t["system_tag"],
                "Direction": t["direction"],
                "Description": t["description"]
            } for t in templates])
            st.dataframe(df_templates, use_container_width=True, hide_index=True)
        else:
            st.info("No transaction templates defined.")
            
        st.divider()
        st.subheader("Link New Journal")
        with st.form("add_template_form"):
            evt = st.text_input("Event Type (e.g., LOAN_DISBURSEMENT)")
            tag = st.text_input("System Tag (e.g., loan_principal)")
            direction = st.selectbox("Direction", ["DEBIT", "CREDIT"])
            desc = st.text_input("Description")
            submitted2 = st.form_submit_button("Add Link")
            if submitted2 and evt and tag:
                svc.link_journal(evt, tag, direction, desc)
                st.success("Journal link added!")
                st.rerun()

    # 3. Reports
    with tab_reports:
        st.subheader("Financial Reports")
        rep_tb, rep_pl, rep_bs, rep_eq, rep_cf = st.tabs([
            "Trial Balance", "Profit & Loss", "Balance Sheet", "Statement of Equity", "Cash Flow"
        ])
        
        as_of = st.date_input("As of Date", value=datetime.today().date())
        start_d = st.date_input("Start Date (for P&L, Equity, Cash Flow)", value=datetime.today().date().replace(day=1), key="global_start")
        
        with rep_tb:
            st.markdown("### Trial Balance")
            tb = svc.get_trial_balance(as_of)
            if tb:
                df_tb = pd.DataFrame([{
                    "Code": r["code"], "Name": r["name"], "Category": r["category"],
                    "Debit": float(r["debit"]), "Credit": float(r["credit"])
                } for r in tb])
                st.dataframe(df_tb, use_container_width=True, hide_index=True)
                st.write(f"**Total Debits:** {df_tb['Debit'].sum():.2f} | **Total Credits:** {df_tb['Credit'].sum():.2f}")
            else:
                st.info("No data.")
                
        with rep_pl:
            st.markdown("### Profit and Loss")
            if st.button("Generate P&L"):
                pl = svc.get_profit_and_loss(start_d, as_of)
                if pl:
                    df_pl = pd.DataFrame([{
                        "Code": r["code"], "Name": r["name"], "Category": r["category"],
                        "Balance": float(r["credit"] - r["debit"]) if r["category"] == "INCOME" else float(r["debit"] - r["credit"])
                    } for r in pl])
                    st.dataframe(df_pl, use_container_width=True)
                else:
                    st.info("No data.")

        with rep_bs:
            st.markdown("### Balance Sheet")
            if st.button("Generate Balance Sheet"):
                bs = svc.get_balance_sheet(as_of)
                if bs:
                    df_bs = pd.DataFrame([{
                        "Code": r["code"], "Name": r["name"], "Category": r["category"],
                        "Balance": float(r["debit"] - r["credit"]) if r["category"] == "ASSET" else float(r["credit"] - r["debit"])
                    } for r in bs])
                    st.dataframe(df_bs, use_container_width=True)
                else:
                    st.info("No data.")

        with rep_eq:
            st.markdown("### Statement of Changes in Equity")
            if st.button("Generate Statement of Equity"):
                eq = svc.get_statement_of_changes_in_equity(start_d, as_of)
                if eq:
                    df_eq = pd.DataFrame([{
                        "Code": r["code"], "Name": r["name"], "Category": r["category"],
                        "Balance": float(r["credit"] - r["debit"])
                    } for r in eq])
                    st.dataframe(df_eq, use_container_width=True)
                else:
                    st.info("No data.")

        with rep_cf:
            st.markdown("### Statement of Cash Flows (Indirect)")
            if st.button("Generate Cash Flow"):
                cf = svc.get_cash_flow_statement(start_d, as_of)
                st.json(cf)
'''

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

pattern = re.compile(r"def accounting_ui\(\):.*?((?=\n(?:def|class) )|\Z)", re.DOTALL)
new_content = pattern.sub(new_ui.replace("\\", "\\\\"), content)

with open("app.py", "w", encoding="utf-8") as f:
    f.write(new_content)
print("app.py patched successfully.")
