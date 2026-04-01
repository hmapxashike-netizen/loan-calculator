"""Loan calculators: consumer, term, bullet, customised repayments (Streamlit UI)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from loans import (
    add_months,
    days_in_month,
    is_last_day_of_month,
    parse_schedule_dates_from_table,
    recompute_customised_from_payments,
    repayment_dates,
)


def render_consumer_loan_ui(
    *,
    get_consumer_schemes,
    get_system_config,
    get_system_date,
    get_global_loan_settings,
    compute_consumer_schedule,
    format_schedule_df,
    schedule_export_downloads,
) -> None:
        schemes = get_consumer_schemes()
        scheme_names = [s["name"] for s in schemes]
        cfg = get_system_config()
        default_additional_rate_pct = cfg.get("consumer_default_additional_rate_pct", 0.0)

        st.subheader("Consumer Loan Parameters")
        # Currency selection with system default + override
        accepted_currencies = cfg.get(
            "accepted_currencies", [cfg.get("base_currency", "USD")]
        )
        loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
        default_ccy = loan_curr_cfg.get("consumer_loan", cfg.get("base_currency", "USD"))
        if default_ccy not in accepted_currencies:
            accepted_currencies = [default_ccy, *accepted_currencies]
        glob = get_global_loan_settings()
        scheme_options = scheme_names + ["Other"]
        p_col1, p_col2 = st.columns(2)
        with p_col1:
            currency = st.selectbox(
                "Currency",
                accepted_currencies,
                index=accepted_currencies.index(default_ccy)
                if default_ccy in accepted_currencies
                else 0,
                key="cl_currency",
            )
            principal_input_choice = st.radio(
                "What are you entering?",
                ["Net proceeds", "Principal (total loan amount)"],
                key="cl_principal_input",
            )
            input_total_facility = principal_input_choice == "Principal (total loan amount)"
            loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
            loan_required = st.number_input(
                loan_input_label,
                min_value=0.0,
                value=140.0,
                step=10.0,
                format="%.2f",
                key="cl_principal",
            )
            loan_term = st.number_input(
                "Term (Months)",
                min_value=1,
                max_value=60,
                value=6,
                step=1,
                key="cl_term",
            )
        with p_col2:
            st.caption("Schemes and default rates are managed in **System configurations**.")
            scheme = st.selectbox("Loan Scheme", scheme_options, key="cl_scheme")
            disbursement_input = st.date_input("Disbursement date", get_system_date(), key="cl_start")
            disbursement_date = datetime.combine(disbursement_input, datetime.min.time())
            default_first_rep = add_months(disbursement_date, 1).date()
            first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="cl_first_rep")
            first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
            use_anniversary = st.radio(
                "Repayments on",
                ["Anniversary date (same day each month)", "Last day of each month"],
                key="cl_timing",
            ).startswith("Anniversary")
        if not use_anniversary and not is_last_day_of_month(first_repayment_date):
            st.error("When repayments are on last day of month, First Repayment Date must be the last day of that month.")

        # Future disbursement: prompt for additional rate when disbursement_date > next month
        today_normalized = datetime.combine(get_system_date(), datetime.min.time()).replace(hour=0, minute=0, second=0, microsecond=0)
        next_month_limit = add_months(today_normalized, 1)
        additional_buffer_rate = 0.0

        if disbursement_date > next_month_limit:
            st.warning("Future date detected: additional interest rate applies per extra month.")
            additional_rate_pct = st.number_input(
                "Additional Monthly Rate (%) per extra month",
                min_value=0.0,
                max_value=100.0,
                value=float(default_additional_rate_pct),
                step=0.1,
                help="Rate applied for each month the disbursement date is beyond next month (0 is acceptable).",
                key="cl_add_rate",
            )
            months_excess = max(
                0,
                (disbursement_date.year - next_month_limit.year) * 12
                + (disbursement_date.month - next_month_limit.month),
            )
            additional_buffer_rate = (additional_rate_pct / 100.0) * months_excess

        # Base rates: from selected scheme or manual entry for Other
        if scheme != "Other":
            sch = next((s for s in schemes if s["name"] == scheme), None)
            base_rate = (sch["interest_rate_pct"] / 100.0) if sch else 0.07
            admin_fee = (sch["admin_fee_pct"] / 100.0) if sch else 0.07
        else:
            o_col1, o_col2 = st.columns(2)
            with o_col1:
                interest_rate_percent = st.number_input(
                    "Interest rate (%)",
                    min_value=0.0,
                    max_value=100.0,
                    value=0.0,
                    step=0.1,
                    key="cl_other_rate",
                )
            with o_col2:
                admin_fee_percent = st.number_input(
                    "Administration fee (%)",
                    min_value=0.0,
                    max_value=100.0,
                    value=0.0,
                    step=0.1,
                    key="cl_other_admin",
                )
            has_error = False
            if interest_rate_percent <= 0.0:
                st.error("Please enter an interest rate greater than 0% for the 'Other' scheme.")
                has_error = True
            if admin_fee_percent <= 0.0:
                st.error("Please enter an administration fee greater than 0% for the 'Other' scheme.")
                has_error = True
            if has_error:
                return
            base_rate = interest_rate_percent / 100.0
            admin_fee = admin_fee_percent / 100.0

        flat_rate = glob.get("interest_method") == "Flat rate"
        if not use_anniversary and not is_last_day_of_month(first_repayment_date):
            return
        details, df_schedule = compute_consumer_schedule(
            loan_required, loan_term, disbursement_date, base_rate, admin_fee, input_total_facility,
            glob.get("rate_basis", "Per month"), flat_rate, scheme=scheme,
            additional_monthly_rate=additional_buffer_rate,
            first_repayment_date=first_repayment_date, use_anniversary=use_anniversary,
        )
        details["currency"] = currency
        total_facility = details["principal"]
        amount_required_display = details["disbursed_amount"]
        total_monthly_rate = details["monthly_rate"]
        monthly_installment = details["installment"]
        end_date = details["end_date"]
        first_repayment_date = details["first_repayment_date"]

        st.markdown(
            "<div style='background-color: #16A34A; color: white; padding: 8px 12px; "
            "font-weight: bold; font-size: 1.1rem;'>Consumer Loan Calculator</div>",
            unsafe_allow_html=True,
        )
        st.markdown("<br>", unsafe_allow_html=True)

        calc_css = """
        <style>
        .calc-desc { font-size: 0.85rem; color: #64748B; margin-top: 2px; margin-bottom: 8px; }
        .calc-value-red { color: #DC2626; font-weight: bold; }
        </style>
        """
        st.markdown(calc_css, unsafe_allow_html=True)

        st.markdown(f"**a. Scheme:** {scheme}")
        st.markdown(f"**b. Net proceeds:** {amount_required_display:,.2f} US Dollars")
        st.markdown(f"**c. Interest Rate (% per month):** {total_monthly_rate * 100:.2f}%")
        st.markdown(
            f"<p class='calc-desc'>"
            f"{total_monthly_rate * 100:.2f}% per month accrued from day to day on principal balance"
            "</p>",
            unsafe_allow_html=True,
        )
        st.markdown(f"**d. Administration Fees (%):** {admin_fee * 100:.2f}%")
        st.markdown(
            f"<p class='calc-desc'>"
            f"{admin_fee * 100:.2f}% once-off on total loan amount"
            "</p>",
            unsafe_allow_html=True,
        )
        st.markdown(f"**e. Principal (total loan amount):** {total_facility:,.2f} US Dollars")
        st.markdown(
            f"<span class='calc-value-red'><strong>f. Monthly Instalment:</strong> US${monthly_installment:,.2f}</span>",
            unsafe_allow_html=True,
        )
        st.markdown(f"**g. Disbursement date:** {disbursement_date.strftime('%d-%b-%Y')}")
        st.markdown(f"**h. Loan Term (months):** {loan_term}")
        st.markdown(f"**j. First Repayment Date:** {first_repayment_date.strftime('%d-%b-%Y')}")
        st.markdown(f"**k. No. of Repayments:** {loan_term} times")
        st.markdown(f"**i. End Date:** {end_date.strftime('%d-%b-%Y')}")

        with st.expander("Notes", expanded=False):
            st.markdown(
                "<div style='background-color: #F1F5F9; padding: 12px 16px; border-radius: 4px;'>"
                "<strong>Notes</strong><br>"
                "1. Select Scheme (a.). If the loan does not fall under a Scheme, select \"Other\"<br>"
                "2. Enter net proceeds in (b) or principal (total loan amount)<br>"
                "3. If you selected \"Other\", enter interest rate (c.) and administration fee (d.)<br>"
                "4. Enter the Loan Term in months (h.)<br>"
                "5. Monthly repayment (f.) assumes every month has 30 days<br>"
                "6. Default rates and schemes are in **System configurations**"
                "</div>",
                unsafe_allow_html=True,
            )
        with st.expander("Repayment schedule and downloads", expanded=False):
            st.dataframe(format_schedule_df(df_schedule), width="stretch", hide_index=True, height=240)
            schedule_export_downloads(df_schedule, file_stem="consumer_loan_schedule", key_prefix="dl_sched_consumer")

        # 6. Save button - DB-ready structure (from shared engine)
        loan_record = {**details, "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
        for k in ("disbursement_date", "start_date", "end_date", "first_repayment_date"):
            if k in loan_record and hasattr(loan_record[k], "isoformat"):
                loan_record[k] = loan_record[k].isoformat()

        with st.expander("Save to system", expanded=False):
            if st.button("Save Loan Record to System", type="primary", key="cl_save"):
                # TODO: Replace with db.insert(loan_record) when DB is ready
                st.success(f"Loan for ${loan_required:,.2f} has been prepared for database sync.")
                with st.expander("Preview record (for DB insertion)"):
                    st.json(loan_record)




def render_term_loan_ui(
    *,
    get_global_loan_settings,
    get_system_config,
    get_system_date,
    loan_management_available: bool,
    list_products,
    get_product_config_from_db,
    get_product_rate_basis,
    compute_term_schedule,
    format_schedule_df,
    schedule_export_downloads,
) -> None:
        glob = get_global_loan_settings()
        cfg = get_system_config()
        # Optional product selector for calculator defaults (safe fallback to system defaults).
        rate_basis = glob.get("rate_basis", "Per month")
        product_cfg: dict[str, Any] = {}
        product_opts = []
        try:
            all_products = list_products(active_only=True) if loan_management_available else []
            product_opts = [
                p for p in (all_products or [])
                if str(p.get("loan_type") or "").strip().lower() == "term_loan"
            ]
        except Exception:
            product_opts = []

        st.subheader("Term Loan Parameters")
        if product_opts:
            prod_labels = ["System defaults"] + [f"{p.get('code')} - {p.get('name')}" for p in product_opts]
            selected_label = st.selectbox("Product (optional)", prod_labels, key="term_product_pick")
            if selected_label != "System defaults":
                picked = product_opts[prod_labels.index(selected_label) - 1]
                product_code = str(picked.get("code") or "").strip()
                if product_code:
                    product_cfg = get_product_config_from_db(product_code) or {}
                    rate_basis = get_product_rate_basis(product_cfg, fallback=rate_basis)

        # Currency selection with system default + override
        accepted_currencies = cfg.get(
            "accepted_currencies", [cfg.get("base_currency", "USD")]
        )
        loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
        default_ccy = loan_curr_cfg.get("term_loan", cfg.get("base_currency", "USD"))
        if default_ccy not in accepted_currencies:
            accepted_currencies = [default_ccy, *accepted_currencies]
        p_col1, p_col2 = st.columns(2)
        with p_col1:
            currency = st.selectbox(
                "Currency",
                accepted_currencies,
                index=accepted_currencies.index(default_ccy)
                if default_ccy in accepted_currencies
                else 0,
                key="term_currency",
            )
            principal_input_choice = st.radio(
                "What are you entering?",
                ["Net proceeds", "Principal (total loan amount)"],
                key="term_principal_input",
            )
            input_total_facility = principal_input_choice == "Principal (total loan amount)"
            loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
            loan_required = st.number_input(
                loan_input_label,
                min_value=0.0,
                value=1000.0,
                step=100.0,
                format="%.2f",
                key="term_principal",
            )
            loan_term = st.number_input(
                "Term (Months)",
                min_value=1,
                max_value=120,
                value=24,
                step=1,
                key="term_months",
            )
        with p_col2:
            disbursement_input = st.date_input("Disbursement date", get_system_date(), key="term_disb")
            disbursement_date = datetime.combine(disbursement_input, datetime.min.time())

        # Term loan: defaults from selected product (if any), else system config; always safe.
        dr_sys = cfg.get("default_rates", {}).get("term_loan", {}) or {}
        dr_prod = (product_cfg.get("default_rates") or {}).get("term_loan") or {}
        dr = {**dr_sys, **dr_prod}
        default_interest = float(dr.get("interest_pct") or 7.0)
        default_drawdown = float(dr.get("drawdown_pct") or 2.5)
        default_arrangement = float(dr.get("arrangement_pct") or 2.5)
        rate_label = "Interest rate (% per annum)" if rate_basis == "Per annum" else "Interest rate (% per month)"
        fee_col1, fee_col2 = st.columns(2)
        with fee_col1:
            rate_pct = st.number_input(rate_label, 0.0, 100.0, default_interest, step=0.1, key="term_rate")
            drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, default_drawdown, step=0.1, key="term_drawdown") / 100.0
        with fee_col2:
            arrangement_fee_pct = st.number_input("Arrangement fee (%)", 0.0, 100.0, default_arrangement, step=0.1, key="term_arrangement") / 100.0
        total_fee = drawdown_fee_pct + arrangement_fee_pct
        if rate_pct <= 0:
            st.error("Please enter an interest rate greater than 0%.")
            return
        if total_fee < 0:
            st.error("Total of Drawdown and Arrangement fees cannot be negative.")
            return

        if input_total_facility:
            total_facility = loan_required
        else:
            total_facility = loan_required / (1.0 - total_fee)
        # Rate basis: per annum -> annual_rate = rate_pct/100; per month -> annual_rate = (rate_pct/100)*12
        annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
        flat_rate = glob.get("interest_method") == "Flat rate"

        # Grace period + repayment timing
        g_col1, g_col2 = st.columns(2)
        with g_col1:
            st.markdown("**Grace Period**")
            grace_type = st.radio(
                "Grace period type",
                ["No grace period", "Principal moratorium", "Principal and interest moratorium"],
                key="term_grace",
            )
            moratorium_months = 0
            if "Principal moratorium" in grace_type:
                moratorium_months = st.number_input("Moratorium length (months)", 1, 60, 3, key="term_moratorium_p")
            elif "Principal and interest" in grace_type:
                moratorium_months = st.number_input("Moratorium length (months)", 1, 60, 3, key="term_moratorium_pi")
        with g_col2:
            default_first_rep = add_months(disbursement_date, 1).date()
            first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="term_first_rep")
            first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
            st.markdown("**Repayment Timing**")
            use_anniversary = st.radio(
                "Repayments on",
                ["Anniversary date (same day each month)", "Last day of each month"],
                key="term_timing",
            ).startswith("Anniversary")

        today_norm = datetime.combine(get_system_date(), datetime.min.time()).replace(hour=0, minute=0, second=0, microsecond=0)
        next_month_limit = add_months(today_norm, 1)

        if grace_type == "No grace period" and first_repayment_date > next_month_limit:
            st.error("No grace period: First Repayment Date must not be greater than next month.")
            return

        if "Principal" in grace_type and moratorium_months >= loan_term:
            st.error("Moratorium length must be less than loan term.")
            return

        if not use_anniversary and not is_last_day_of_month(first_repayment_date):
            last_day = days_in_month(first_repayment_date.year, first_repayment_date.month)
            example = datetime(first_repayment_date.year, first_repayment_date.month, last_day).strftime("%d-%b-%Y")
            st.error(
                "When repayments are on the **last day of each month**, the First Repayment Date must be the last day of its month. "
                f"For {first_repayment_date.strftime('%B %Y')} the last day is **{example}**."
            )
            return

        details, df_schedule = compute_term_schedule(
            loan_required, loan_term, disbursement_date, rate_pct, drawdown_fee_pct, arrangement_fee_pct,
            input_total_facility, grace_type, moratorium_months, first_repayment_date, use_anniversary,
            rate_basis, flat_rate,
        )
        details["currency"] = currency
        installment = details["installment"]
        end_date = details["end_date"]

        # Display
        st.markdown(
            "<div style='background-color: #16A34A; color: white; padding: 8px 12px; "
            "font-weight: bold; font-size: 1.1rem;'>Term Loan Calculator (Actual/360)</div>",
            unsafe_allow_html=True,
        )
        st.markdown("<br>", unsafe_allow_html=True)

        calc_css = """
        <style>
        .calc-desc { font-size: 0.85rem; color: #64748B; margin-top: 2px; margin-bottom: 8px; }
        .calc-value-red { color: #DC2626; font-weight: bold; }
        </style>
        """
        st.markdown(calc_css, unsafe_allow_html=True)

        net_proceeds_to_display = float(details.get("disbursed_amount", loan_required) or 0.0)
        st.markdown(f"**a. Net proceeds:** {net_proceeds_to_display:,.2f} US Dollars")
        st.markdown(f"**b. Interest Rate (annual, Actual/360):** {details['annual_rate'] * 100:.2f}%")
        st.markdown(
            "<p class='calc-desc'>Interest accrued on actual days / 360 basis</p>",
            unsafe_allow_html=True,
        )
        st.markdown(f"**c. Drawdown fee (%):** {drawdown_fee_pct * 100:.2f}% | **Arrangement fee (%):** {arrangement_fee_pct * 100:.2f}%")
        total_fee = drawdown_fee_pct + arrangement_fee_pct
        st.markdown(
            f"<p class='calc-desc'>Total {total_fee * 100:.2f}% once-off on total facility</p>",
            unsafe_allow_html=True,
        )
        st.markdown(f"**d. Principal (total loan amount):** {details['principal']:,.2f} US Dollars")
        st.markdown(
            f"<span class='calc-value-red'><strong>e. Installment (from first P&I period):</strong> US${installment:,.2f}</span>",
            unsafe_allow_html=True,
        )
        st.markdown(f"**f. Disbursement Date:** {disbursement_date.strftime('%d-%b-%Y')}")
        st.markdown(f"**g. Loan Term (months):** {loan_term}")
        st.markdown(f"**h. First Repayment Date:** {first_repayment_date.strftime('%d-%b-%Y')}")
        st.markdown(f"**i. Grace period:** {grace_type}")
        st.markdown(f"**j. End Date:** {end_date.strftime('%d-%b-%Y')}")

        with st.expander("Repayment schedule and downloads", expanded=False):
            st.dataframe(format_schedule_df(df_schedule), width="stretch", hide_index=True, height=240)
            schedule_export_downloads(df_schedule, file_stem="term_loan_schedule", key_prefix="dl_sched_term")

        loan_record = {**details, "loan_type": "term_loan", "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
        for k in ("disbursement_date", "start_date", "end_date", "first_repayment_date"):
            if k in loan_record and hasattr(loan_record[k], "isoformat"):
                loan_record[k] = loan_record[k].isoformat()

        with st.expander("Save to system", expanded=False):
            if st.button("Save Term Loan Record to System", type="primary", key="term_save"):
                st.success(f"Term loan for ${loan_required:,.2f} has been prepared for database sync.")
                with st.expander("Preview record (for DB insertion)"):
                    st.json(loan_record)




def render_bullet_loan_ui(
    *,
    get_global_loan_settings,
    get_system_config,
    get_system_date,
    compute_bullet_schedule,
    format_schedule_df,
    schedule_export_downloads,
) -> None:
        glob = get_global_loan_settings()
        cfg = get_system_config()
        st.subheader("Bullet Loan Parameters")
        # Currency selection with system default + override
        accepted_currencies = cfg.get(
            "accepted_currencies", [cfg.get("base_currency", "USD")]
        )
        loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
        default_ccy = loan_curr_cfg.get("bullet_loan", cfg.get("base_currency", "USD"))
        if default_ccy not in accepted_currencies:
            accepted_currencies = [default_ccy, *accepted_currencies]
        p_col1, p_col2 = st.columns(2)
        with p_col1:
            currency = st.selectbox(
                "Currency",
                accepted_currencies,
                index=accepted_currencies.index(default_ccy)
                if default_ccy in accepted_currencies
                else 0,
                key="bullet_currency",
            )
            principal_input_choice = st.radio(
                "What are you entering?",
                ["Net proceeds", "Principal (total loan amount)"],
                key="bullet_principal_input",
            )
            input_total_facility = principal_input_choice == "Principal (total loan amount)"
            loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
            loan_required = st.number_input(
                loan_input_label,
                min_value=0.0,
                value=1000.0,
                step=100.0,
                format="%.2f",
                key="bullet_principal",
            )
            loan_term = st.number_input(
                "Term (Months)",
                min_value=1,
                max_value=120,
                value=12,
                step=1,
                key="bullet_term",
            )
        with p_col2:
            bullet_type = st.radio(
                "Bullet type",
                ["Straight bullet (no interim payments)", "Bullet with interest payments"],
                key="bullet_type",
            )
            disbursement_input = st.date_input("Disbursement date", get_system_date(), key="bullet_disb")
            disbursement_date = datetime.combine(disbursement_input, datetime.min.time())

        dr = cfg.get("default_rates", {}).get("bullet_loan", {})
        rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
        f_col1, f_col2 = st.columns(2)
        with f_col1:
            rate_pct = st.number_input(rate_label, min_value=0.0, max_value=100.0, value=float(dr.get("interest_pct", 7.0)), step=0.1, key="bullet_rate")
            drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="bullet_drawdown") / 100.0
        with f_col2:
            arrangement_fee_pct = st.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct", 2.5)), step=0.1, key="bullet_arrangement") / 100.0
        total_fee = drawdown_fee_pct + arrangement_fee_pct
        if rate_pct <= 0:
            st.error("Please enter an interest rate greater than 0%.")
            return
        if total_fee < 0:
            st.error("Total of Drawdown and Arrangement fees cannot be negative.")
            return

        flat_rate = glob.get("interest_method") == "Flat rate"
        first_repayment_date = None
        use_anniversary = True
        if "with interest" in bullet_type:
            t_col1, t_col2 = st.columns(2)
            with t_col1:
                default_first_rep = add_months(disbursement_date, 1).date()
                first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="bullet_first_rep")
                first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
            with t_col2:
                use_anniversary = st.radio(
                    "Interest payments on",
                    ["Anniversary date (same day each month)", "Last day of each month"],
                    key="bullet_timing",
                ).startswith("Anniversary")
            if not use_anniversary and not is_last_day_of_month(first_repayment_date):
                last_day = days_in_month(first_repayment_date.year, first_repayment_date.month)
                example = datetime(first_repayment_date.year, first_repayment_date.month, last_day).strftime("%d-%b-%Y")
                st.error(
                    "When interest payments are on the **last day of each month**, the First Repayment Date must be the last day of its month. "
                    f"For {first_repayment_date.strftime('%B %Y')} the last day is **{example}**."
                )
                return

        details, df_schedule = compute_bullet_schedule(
            loan_required, loan_term, disbursement_date, rate_pct, drawdown_fee_pct, arrangement_fee_pct,
            input_total_facility, bullet_type, first_repayment_date, use_anniversary,
            glob.get("rate_basis", "Per month"), flat_rate,
        )
        details["currency"] = currency

        # Display
        st.markdown(
            "<div style='background-color: #16A34A; color: white; padding: 8px 12px; "
            "font-weight: bold; font-size: 1.1rem;'>Bullet Loan Calculator (Actual/360)</div>",
            unsafe_allow_html=True,
        )
        st.markdown("<br>", unsafe_allow_html=True)

        calc_css = """
        <style>
        .calc-desc { font-size: 0.85rem; color: #64748B; margin-top: 2px; margin-bottom: 8px; }
        .calc-value-red { color: #DC2626; font-weight: bold; }
        </style>
        """
        st.markdown(calc_css, unsafe_allow_html=True)

        net_proceeds_to_display = float(details.get("disbursed_amount", loan_required) or 0.0)
        st.markdown(f"**a. Net proceeds:** {net_proceeds_to_display:,.2f} US Dollars")
        st.markdown(f"**b. Interest Rate (annual, Actual/360):** {details['annual_rate'] * 100:.2f}%")
        st.markdown("<p class='calc-desc'>Interest on actual days / 360 basis</p>", unsafe_allow_html=True)
        st.markdown(f"**c. Drawdown fee (%):** {drawdown_fee_pct * 100:.2f}% | **Arrangement fee (%):** {arrangement_fee_pct * 100:.2f}%")
        st.markdown(f"<p class='calc-desc'>Total {total_fee * 100:.2f}% once-off on total facility</p>", unsafe_allow_html=True)
        st.markdown(f"**d. Principal (total loan amount):** {details['principal']:,.2f} US Dollars")
        st.markdown(
            f"<span class='calc-value-red'><strong>e. Total payment at maturity:</strong> US${details['total_payment']:,.2f}</span>",
            unsafe_allow_html=True,
        )
        st.markdown(f"**f. Disbursement Date:** {disbursement_date.strftime('%d-%b-%Y')}")
        st.markdown(f"**g. Term (months):** {loan_term}")
        st.markdown(f"**h. End date:** {details['end_date'].strftime('%d-%b-%Y')}")
        if details.get("first_repayment_date") is not None:
            st.markdown(f"**i. First interest payment:** {details['first_repayment_date'].strftime('%d-%b-%Y')}")

        with st.expander("Repayment schedule and downloads", expanded=False):
            st.dataframe(format_schedule_df(df_schedule), width="stretch", hide_index=True, height=240)
            schedule_export_downloads(df_schedule, file_stem="bullet_loan_schedule", key_prefix="dl_sched_bullet")

        loan_record = {**details, "loan_type": "bullet_loan", "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
        for k in ("disbursement_date", "end_date", "first_repayment_date"):
            if k in loan_record and loan_record[k] is not None and hasattr(loan_record[k], "isoformat"):
                loan_record[k] = loan_record[k].isoformat()

        with st.expander("Save to system", expanded=False):
            if st.button("Save Bullet Loan Record to System", type="primary", key="bullet_save"):
                st.success(f"Bullet loan for ${net_proceeds_to_display:,.2f} has been prepared for database sync.")
                with st.expander("Preview record (for DB insertion)"):
                    st.json(loan_record)




def render_customised_repayments_ui(
    *,
    get_global_loan_settings,
    get_system_config,
    get_system_date,
    format_schedule_df,
    schedule_export_downloads,
    money_df_column_config,
    schedule_editor_disabled_amounts,
    first_repayment_from_customised_table,
) -> None:
        glob = get_global_loan_settings()
        cfg = get_system_config()
        flat_rate = glob.get("interest_method") == "Flat rate"

        st.subheader("Customised Repayments Parameters")
        # Currency selection with system default + override
        accepted_currencies = cfg.get(
            "accepted_currencies", [cfg.get("base_currency", "USD")]
        )
        loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
        default_ccy = loan_curr_cfg.get(
            "customised_repayments", cfg.get("base_currency", "USD")
        )
        if default_ccy not in accepted_currencies:
            accepted_currencies = [default_ccy, *accepted_currencies]
        p_col1, p_col2 = st.columns(2)
        with p_col1:
            currency = st.selectbox(
                "Currency",
                accepted_currencies,
                index=accepted_currencies.index(default_ccy)
                if default_ccy in accepted_currencies
                else 0,
                key="cust_currency",
            )
            principal_input_choice = st.radio(
                "What are you entering?",
                ["Net proceeds", "Principal (total loan amount)"],
                key="cust_principal_input",
            )
            input_total_facility = principal_input_choice == "Principal (total loan amount)"
            loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
            loan_required = st.number_input(
                loan_input_label,
                min_value=0.0,
                value=1000.0,
                step=100.0,
                format="%.2f",
                key="cust_principal",
            )
            loan_term = st.number_input(
                "Term (Months)",
                min_value=1,
                max_value=120,
                value=12,
                step=1,
                key="cust_term",
            )
        with p_col2:
            disbursement_input = st.date_input("Disbursement date", get_system_date(), key="cust_start")
            disbursement_date = datetime.combine(disbursement_input, datetime.min.time())
            irregular_calc = st.checkbox("Irregular", value=False, key="cust_irregular", help="Allow editing dates and adding rows; schedule recomputes from table.")
            use_anniversary = st.radio(
                "Repayments on",
                ["Anniversary date (same day each month)", "Last day of each month"],
                key="cust_timing",
            ).startswith("Anniversary")
        default_first_rep = add_months(disbursement_date, 1).date()
        if not use_anniversary:
            default_first_rep = default_first_rep.replace(day=days_in_month(default_first_rep.year, default_first_rep.month))
        existing_cust = st.session_state.get("customised_repayments_df")
        first_rep_calc = first_repayment_from_customised_table(existing_cust) if existing_cust is not None and len(existing_cust) > 1 else None
        first_rep_display_calc = (first_rep_calc.date() if first_rep_calc else default_first_rep)
        st.date_input("First repayment date (from table)", first_rep_display_calc, key="cust_first_rep", disabled=True, help="From first row with non-zero payment.")
        first_repayment_date = datetime.combine(first_rep_display_calc, datetime.min.time())
        dr = cfg.get("default_rates", {}).get("customised_repayments", {})
        rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
        f_col1, f_col2 = st.columns(2)
        with f_col1:
            rate_pct = st.number_input(rate_label, 0.0, 100.0, float(dr.get("interest_pct", 7.0)), step=0.1, key="cust_rate")
            drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="cust_drawdown") / 100.0
        with f_col2:
            arrangement_fee_pct = st.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct", 2.5)), step=0.1, key="cust_arrangement") / 100.0
        total_fee = drawdown_fee_pct + arrangement_fee_pct
        if total_fee < 0:
            st.error("Total of Drawdown and Arrangement fees cannot be negative.")
            return

        if input_total_facility:
            total_facility = loan_required
        else:
            total_facility = loan_required / (1.0 - total_fee)
        annual_rate = (rate_pct / 100.0) * 12.0 if glob.get("rate_basis") == "Per month" else (rate_pct / 100.0)

        session_key = "customised_repayments_df"
        params_key = (round(total_facility, 2), loan_term, disbursement_date.strftime("%Y-%m-%d"), irregular_calc)
        if session_key not in st.session_state or st.session_state.get("customised_params") != params_key:
            st.session_state["customised_params"] = params_key
            schedule_dates_init = repayment_dates(disbursement_date, first_repayment_date, int(loan_term), use_anniversary)
            rows = [{"Period": 0, "Date": disbursement_date.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": round(total_facility, 2), "Total Outstanding": round(total_facility, 2)}]
            for i, dt in enumerate(schedule_dates_init, 1):
                rows.append({"Period": i, "Date": dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0})
            st.session_state[session_key] = pd.DataFrame(rows)

        df = st.session_state[session_key].copy()
        schedule_dates = parse_schedule_dates_from_table(df, start_date=disbursement_date)
        df = recompute_customised_from_payments(df, total_facility, schedule_dates, annual_rate, flat_rate, disbursement_date)
        st.session_state[session_key] = df

        st.markdown(
            "<div style='background-color: #16A34A; color: white; padding: 8px 12px; font-weight: bold; font-size: 1.1rem;'>Customised Repayments (Actual/360)</div>",
            unsafe_allow_html=True,
        )
        if irregular_calc:
            if st.button("Add row", key="cust_add_row"):
                last_df = st.session_state[session_key]
                if len(last_df) > 0:
                    try:
                        last_date_str = str(last_df.at[len(last_df) - 1, "Date"]).strip()[:32]
                        last_dt = datetime.combine(datetime.strptime(last_date_str, "%d-%b-%Y").date(), datetime.min.time())
                    except (ValueError, TypeError):
                        last_dt = add_months(disbursement_date, len(last_df))
                    next_dt = add_months(last_dt, 1)
                    if not use_anniversary:
                        next_dt = next_dt.replace(day=days_in_month(next_dt.year, next_dt.month))
                    new_row = {"Period": len(last_df), "Date": next_dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0}
                    st.session_state[session_key] = pd.concat([last_df, pd.DataFrame([new_row])], ignore_index=True)
                    st.rerun()
            st.caption("Irregular: edit **Date** and **Payment**; add rows with the button above. Schedule recomputes from table dates.")
        else:
            st.caption("Edit the **Payment** column; interest and balances update automatically. Save only when the loan is fully cleared (Total Outstanding = $0).")
        date_editable_calc = irregular_calc
        with st.expander("Repayment editor and schedule", expanded=False):
            edited = st.data_editor(
                df,
                column_config=money_df_column_config(
                    df,
                    overrides={
                        "Period": st.column_config.NumberColumn(disabled=True),
                        "Date": st.column_config.TextColumn(
                            disabled=not date_editable_calc,
                            help="Format: DD-Mon-YYYY" if date_editable_calc else None,
                        ),
                    },
                    column_disabled=schedule_editor_disabled_amounts,
                ),
                width="stretch",
                hide_index=True,
                key="cust_editor",
                height=260,
            )
            if not edited.equals(df):
                schedule_dates_edit = parse_schedule_dates_from_table(edited, start_date=disbursement_date)
                df_updated = recompute_customised_from_payments(edited, total_facility, schedule_dates_edit, annual_rate, flat_rate, disbursement_date)
                st.session_state[session_key] = df_updated
                st.rerun()

        final_total_outstanding = float(df.at[len(df) - 1, "Total Outstanding"]) if len(df) > 1 and "Total Outstanding" in df.columns else total_facility
        if abs(final_total_outstanding) < 0.01:
            st.success("Loan cleared. You may save this record.")
        else:
            st.warning(f"Total outstanding at end: **${final_total_outstanding:,.2f}**. Adjust payments so Total Outstanding is $0 to save.")

        can_save = abs(final_total_outstanding) < 0.01
        with st.expander("Save to system", expanded=False):
            if st.button("Save Customised Repayments to System", type="primary", key="cust_save", disabled=not can_save):
                if can_save:
                    st.success(f"Customised loan for ${loan_required:,.2f} has been prepared for database sync.")
                    with st.expander("Preview record (for DB insertion)"):
                        st.json({
                            "loan_type": "customised_repayments",
                            "timestamp": datetime.now().isoformat(),
                            "principal": float(total_facility),
                            "disbursed_amount": float(loan_required),
                            "term": int(loan_term),
                            "annual_rate": float(annual_rate),
                            "drawdown_fee": float(drawdown_fee_pct),
                            "arrangement_fee": float(arrangement_fee_pct),
                            "disbursement_date": disbursement_date.isoformat(),
                            "currency": currency,
                            "schedule": df.to_dict(orient="records"),
                        })

