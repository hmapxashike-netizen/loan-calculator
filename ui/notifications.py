"""Notifications UI (Streamlit). Session keys: notification_history, hist_* ."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st



from style import render_main_header, render_sub_header, render_sub_sub_header

def render_notifications_ui(
    *,
    customers_available: bool,
    list_customers,
    get_display_name,
) -> None:
    tab_send, tab_templates, tab_history = st.tabs(
        [
            "Send Notification",
            "Templates",
            "History",
        ]
    )

    with tab_send:
        render_sub_sub_header("Send a Notification")
        with st.form("send_notification_form"):
            recipient_type = st.radio(
                "Send to",
                ["Specific Customer", "All Active Customers", "Custom Phone/Email"],
                horizontal=True,
            )

            customer_search = None
            if recipient_type == "Specific Customer":
                if customers_available:
                    cust_list = list_customers()
                    if cust_list:
                        cust_options = {
                            c["id"]: f"{get_display_name(int(c['id']))} (ID: {c['id']})"
                            for c in cust_list
                            if c.get("id") is not None
                        }
                        customer_id = st.selectbox(
                            "Select Customer",
                            options=list(cust_options.keys()),
                            format_func=lambda x: cust_options[x],
                        )
                    else:
                        st.warning("No customers found.")
                else:
                    st.error("Customers module is unavailable.")
            elif recipient_type == "Custom Phone/Email":
                custom_contact = st.text_input("Enter Email or Phone Number")

            st.divider()
            notification_type = st.selectbox("Notification Method", ["SMS", "Email", "In-App/Push"])
            template_used = st.selectbox(
                "Use Template (Optional)",
                ["None", "Payment Reminder", "Payment Overdue", "Account Update", "Loan Approved"],
            )

            subject = ""
            if notification_type == "Email":
                subject = st.text_input("Subject")

            message_body = st.text_area("Message Body", height=150)

            submitted = st.form_submit_button("Send Notification", type="primary")
            if submitted:
                if not message_body.strip():
                    st.error("Message body cannot be empty.")
                else:
                    st.success("Notification queued for delivery successfully!")

                    if "notification_history" not in st.session_state:
                        st.session_state["notification_history"] = []

                    target = ""
                    if recipient_type == "Specific Customer" and "customer_id" in locals():
                        target = f"Customer ID: {customer_id}"
                    elif recipient_type == "Custom Phone/Email":
                        target = custom_contact
                    else:
                        target = "All Active Customers"

                    st.session_state["notification_history"].insert(
                        0,
                        {
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "type": notification_type,
                            "recipient": target,
                            "status": "Sent",
                            "message": message_body[:50] + "..."
                            if len(message_body) > 50
                            else message_body,
                        },
                    )

    with tab_templates:
        render_sub_sub_header("Manage Templates")
        st.info(
            "Here you can define and edit standard templates to use for bulk or automated notifications."
        )

        with st.expander("Create New Template"):
            with st.form("new_template_form"):
                new_tpl_name = st.text_input("Template Name", placeholder="e.g. Loan Disbursement SMS")
                new_tpl_type = st.selectbox("Template Type", ["SMS", "Email", "In-App"])
                new_tpl_body = st.text_area("Template Content (use {variables} for dynamic fields)")
                if st.form_submit_button("Save Template"):
                    st.success(f"Template '{new_tpl_name}' saved.")

        st.markdown("### Existing Templates")
        mock_templates = pd.DataFrame(
            [
                {
                    "Template Name": "Payment Reminder",
                    "Type": "SMS",
                    "Last Updated": "2024-01-15",
                    "Content Preview": "Dear {name}, your payment of {amount} is due...",
                },
                {
                    "Template Name": "Payment Overdue",
                    "Type": "Email",
                    "Last Updated": "2024-02-10",
                    "Content Preview": "Notice: Your account is currently in arrears...",
                },
                {
                    "Template Name": "Loan Approved",
                    "Type": "SMS",
                    "Last Updated": "2023-11-20",
                    "Content Preview": "Congratulations {name}, your loan application...",
                },
            ]
        )
        st.dataframe(mock_templates, hide_index=True, use_container_width=True)

    with tab_history:
        render_sub_sub_header("Notification History")

        col1, col2 = st.columns(2)
        with col1:
            filter_type = st.selectbox("Filter by Type", ["All", "SMS", "Email", "In-App"], key="hist_filter_type")
        with col2:
            filter_status = st.selectbox(
                "Filter by Status",
                ["All", "Sent", "Failed", "Pending"],
                key="hist_filter_status",
            )

        history = st.session_state.get("notification_history", [])

        if not history:
            st.info("No notifications have been sent during this session.")
            mock_history = pd.DataFrame(
                [
                    {
                        "timestamp": "2024-03-01 09:15:00",
                        "type": "SMS",
                        "recipient": "Customer ID: 12",
                        "status": "Sent",
                        "message": "Your loan has been disbursed...!",
                    },
                    {
                        "timestamp": "2024-03-01 08:30:22",
                        "type": "Email",
                        "recipient": "Customer ID: 45",
                        "status": "Failed",
                        "message": "Statement for February 2024",
                    },
                    {
                        "timestamp": "2024-02-28 14:05:10",
                        "type": "SMS",
                        "recipient": "All Active Customers",
                        "status": "Sent",
                        "message": "Notice: Our offices will be closed...",
                    },
                ]
            )
            st.dataframe(mock_history, hide_index=True, use_container_width=True)
        else:
            df_history = pd.DataFrame(history)

            if filter_type != "All":
                df_history = df_history[df_history["type"] == filter_type]
            if filter_status != "All":
                df_history = df_history[df_history["status"] == filter_status]

            st.dataframe(df_history, hide_index=True, use_container_width=True)
