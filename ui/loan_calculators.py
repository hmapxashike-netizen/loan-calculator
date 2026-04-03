"""Loan calculators: consumer, term, bullet, customised repayments (Streamlit UI)."""

from __future__ import annotations

from datetime import datetime
import pandas as pd
import streamlit as st


from style import render_main_header, render_sub_header, render_sub_sub_header

from ui.components import inject_tertiary_hyperlink_css_once, render_centered_html_table

from loans import (
    add_months,
    days_in_month,
    is_last_day_of_month,
    parse_schedule_dates_from_table,
    recompute_customised_from_payments,
    repayment_dates,
    schedule_dataframe_to_csv_bytes,
    schedule_dataframe_to_excel_bytes,
)

_LABEL_AMOUNT_BASIS = "Amount Basis"
_LABEL_NET_PROCEEDS = "Net Proceeds"
_LABEL_PRINCIPAL_TOTAL = "Principal (Total Loan Amount)"
_AMOUNT_BASIS_OPTIONS = [_LABEL_NET_PROCEEDS, _LABEL_PRINCIPAL_TOTAL]
_LABEL_DISBURSEMENT_DATE = "Disbursement Date"
_LABEL_REPAYMENTS_ON = "Repayments On"


def _render_calc_schedule_table(format_schedule_df, df_schedule: pd.DataFrame) -> None:
    render_sub_sub_header("Repayment schedule")
    styled = format_schedule_df(df_schedule)
    render_centered_html_table(styled, [str(c) for c in styled.columns])


def _schedule_export_bytes_pair(df: pd.DataFrame) -> tuple[bytes, bytes]:
    return (
        schedule_dataframe_to_csv_bytes(df, amount_decimals=2),
        schedule_dataframe_to_excel_bytes(df, amount_decimals=2),
    )


def render_consumer_loan_ui(
    *,
    get_consumer_schemes,
    get_system_config,
    get_system_date,
    get_global_loan_settings,
    compute_consumer_schedule,
    format_schedule_df,
) -> None:
        schemes = get_consumer_schemes()
        scheme_names = [s["name"] for s in schemes]
        cfg = get_system_config()
        default_additional_rate_pct = cfg.get("consumer_default_additional_rate_pct", 0.0)

        render_sub_sub_header("Consumer Loan Parameters")
        accepted_currencies = cfg.get(
            "accepted_currencies", [cfg.get("base_currency", "USD")]
        )
        loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
        default_ccy = loan_curr_cfg.get("consumer_loan", cfg.get("base_currency", "USD"))
        if default_ccy not in accepted_currencies:
            accepted_currencies = [default_ccy, *accepted_currencies]
        glob = get_global_loan_settings()
        scheme_options = scheme_names + ["Other"]
        r1 = st.columns(4, gap=None)
        with r1[0]:
            currency = st.selectbox(
                "Currency",
                accepted_currencies,
                index=accepted_currencies.index(default_ccy)
                if default_ccy in accepted_currencies
                else 0,
                key="cl_currency",
            )
        with r1[1]:
            _basis = st.selectbox(
                _LABEL_AMOUNT_BASIS,
                _AMOUNT_BASIS_OPTIONS,
                key="cl_amount_basis",
            )
            input_total_facility = _basis == _LABEL_PRINCIPAL_TOTAL
        with r1[2]:
            loan_input_label = _LABEL_PRINCIPAL_TOTAL if input_total_facility else _LABEL_NET_PROCEEDS
            loan_required = st.number_input(
                loan_input_label,
                min_value=0.0,
                value=140.0,
                step=10.0,
                format="%.2f",
                key="cl_principal",
            )
        with r1[3]:
            loan_term = st.number_input(
                "Term (Months)",
                min_value=1,
                max_value=60,
                value=6,
                step=1,
                key="cl_term",
            )
        r2 = st.columns(4, gap=None)
        with r2[0]:
            scheme = st.selectbox("Loan Scheme", scheme_options, key="cl_scheme")
        with r2[1]:
            disbursement_input = st.date_input(_LABEL_DISBURSEMENT_DATE, get_system_date(), key="cl_start")
            disbursement_date = datetime.combine(disbursement_input, datetime.min.time())
        with r2[2]:
            default_first_rep = add_months(disbursement_date, 1).date()
            first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="cl_first_rep")
            first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
        with r2[3]:
            _rep = st.selectbox(
                _LABEL_REPAYMENTS_ON,
                ["Anniversary date (same day each month)", "Last day of each month"],
                key="cl_repay_on",
            )
            use_anniversary = _rep.startswith("Anniversary")
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
            o4 = st.columns(4, gap=None)
            with o4[0]:
                interest_rate_percent = st.number_input(
                    "Interest rate (%)",
                    min_value=0.0,
                    max_value=100.0,
                    value=0.0,
                    step=0.1,
                    key="cl_other_rate",
                )
            with o4[1]:
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

        _render_calc_schedule_table(format_schedule_df, df_schedule)

        loan_record = {**details, "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
        for k in ("disbursement_date", "start_date", "end_date", "first_repayment_date"):
            if k in loan_record and hasattr(loan_record[k], "isoformat"):
                loan_record[k] = loan_record[k].isoformat()

        inject_tertiary_hyperlink_css_once()
        _csv_b, _xlsx_b = _schedule_export_bytes_pair(df_schedule)
        _ac1, _ac2, _ac3, _ = st.columns([1, 1, 1, 4], gap=None, vertical_alignment="center")
        with _ac1:
            st.download_button(
                "Download CSV",
                data=_csv_b,
                file_name="consumer_loan_schedule.csv",
                mime="text/csv",
                key="dl_sched_consumer_csv",
                type="tertiary",
                help="UTF-8 with BOM; amounts rounded to 2dp for readability.",
            )
        with _ac2:
            st.download_button(
                "Download Excel",
                data=_xlsx_b,
                file_name="consumer_loan_schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_sched_consumer_xlsx",
                type="tertiary",
                help="Native Excel numbers (.xlsx); no text warnings.",
            )
        with _ac3:
            if st.button("Save record to DB", type="tertiary", key="cl_save"):
                st.success(f"Loan for ${loan_required:,.2f} has been prepared for database sync.")
                with st.expander("Preview record (for DB insertion)"):
                    st.json(loan_record)




def render_term_loan_ui(
    *,
    get_global_loan_settings,
    get_system_config,
    get_system_date,
    compute_term_schedule,
    format_schedule_df,
) -> None:
        glob = get_global_loan_settings()
        cfg = get_system_config()
        rate_basis = glob.get("rate_basis", "Per month")

        render_sub_sub_header("Term Loan Parameters")
        # Currency selection with system default + override
        accepted_currencies = cfg.get(
            "accepted_currencies", [cfg.get("base_currency", "USD")]
        )
        loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
        default_ccy = loan_curr_cfg.get("term_loan", cfg.get("base_currency", "USD"))
        if default_ccy not in accepted_currencies:
            accepted_currencies = [default_ccy, *accepted_currencies]
        t1 = st.columns(4, gap=None)
        with t1[0]:
            currency = st.selectbox(
                "Currency",
                accepted_currencies,
                index=accepted_currencies.index(default_ccy)
                if default_ccy in accepted_currencies
                else 0,
                key="term_currency",
            )
        with t1[1]:
            _tb = st.selectbox(
                _LABEL_AMOUNT_BASIS,
                _AMOUNT_BASIS_OPTIONS,
                key="term_amount_basis",
            )
            input_total_facility = _tb == _LABEL_PRINCIPAL_TOTAL
        with t1[2]:
            loan_input_label = _LABEL_PRINCIPAL_TOTAL if input_total_facility else _LABEL_NET_PROCEEDS
            loan_required = st.number_input(
                loan_input_label,
                min_value=0.0,
                value=1000.0,
                step=100.0,
                format="%.2f",
                key="term_principal",
            )
        with t1[3]:
            loan_term = st.number_input(
                "Term (Months)",
                min_value=1,
                max_value=120,
                value=24,
                step=1,
                key="term_months",
            )

        dr_sys = cfg.get("default_rates", {}).get("term_loan", {}) or {}
        dr = dict(dr_sys)
        default_interest = float(dr.get("interest_pct") or 7.0)
        default_drawdown = float(dr.get("drawdown_pct") or 2.5)
        default_arrangement = float(dr.get("arrangement_pct") or 2.5)
        rate_label = "Interest rate (% per annum)" if rate_basis == "Per annum" else "Interest rate (% per month)"
        t2 = st.columns(4, gap=None)
        with t2[0]:
            disbursement_input = st.date_input(_LABEL_DISBURSEMENT_DATE, get_system_date(), key="term_disb")
            disbursement_date = datetime.combine(disbursement_input, datetime.min.time())
        with t2[1]:
            rate_pct = st.number_input(rate_label, 0.0, 100.0, default_interest, step=0.1, key="term_rate")
        with t2[2]:
            drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, default_drawdown, step=0.1, key="term_drawdown") / 100.0
        with t2[3]:
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

        t3 = st.columns(4, gap=None)
        with t3[0]:
            grace_type = st.selectbox(
                "Grace period",
                ["No grace period", "Principal moratorium", "Principal and interest moratorium"],
                key="term_grace_sel",
            )
        moratorium_months = 0
        with t3[1]:
            if "Principal moratorium" in grace_type:
                moratorium_months = st.number_input("Moratorium (months)", 1, 60, 3, key="term_moratorium_p")
            elif "Principal and interest" in grace_type:
                moratorium_months = st.number_input("Moratorium (months)", 1, 60, 3, key="term_moratorium_pi")
        with t3[2]:
            default_first_rep = add_months(disbursement_date, 1).date()
            first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="term_first_rep")
            first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
        with t3[3]:
            _tr = st.selectbox(
                _LABEL_REPAYMENTS_ON,
                ["Anniversary date (same day each month)", "Last day of each month"],
                key="term_repay_on",
            )
            use_anniversary = _tr.startswith("Anniversary")

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

        _render_calc_schedule_table(format_schedule_df, df_schedule)

        loan_record = {**details, "loan_type": "term_loan", "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
        for k in ("disbursement_date", "start_date", "end_date", "first_repayment_date"):
            if k in loan_record and hasattr(loan_record[k], "isoformat"):
                loan_record[k] = loan_record[k].isoformat()

        inject_tertiary_hyperlink_css_once()
        _csv_b, _xlsx_b = _schedule_export_bytes_pair(df_schedule)
        _ac1, _ac2, _ac3, _ = st.columns([1, 1, 1, 4], gap=None, vertical_alignment="center")
        with _ac1:
            st.download_button(
                "Download CSV",
                data=_csv_b,
                file_name="term_loan_schedule.csv",
                mime="text/csv",
                key="dl_sched_term_csv",
                type="tertiary",
                help="UTF-8 with BOM; amounts rounded to 2dp for readability.",
            )
        with _ac2:
            st.download_button(
                "Download Excel",
                data=_xlsx_b,
                file_name="term_loan_schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_sched_term_xlsx",
                type="tertiary",
                help="Native Excel numbers (.xlsx); no text warnings.",
            )
        with _ac3:
            if st.button("Save record to DB", type="tertiary", key="term_save"):
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
) -> None:
        glob = get_global_loan_settings()
        cfg = get_system_config()
        render_sub_sub_header("Bullet Loan Parameters")
        # Currency selection with system default + override
        accepted_currencies = cfg.get(
            "accepted_currencies", [cfg.get("base_currency", "USD")]
        )
        loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
        default_ccy = loan_curr_cfg.get("bullet_loan", cfg.get("base_currency", "USD"))
        if default_ccy not in accepted_currencies:
            accepted_currencies = [default_ccy, *accepted_currencies]
        b1 = st.columns(4, gap=None)
        with b1[0]:
            currency = st.selectbox(
                "Currency",
                accepted_currencies,
                index=accepted_currencies.index(default_ccy)
                if default_ccy in accepted_currencies
                else 0,
                key="bullet_currency",
            )
        with b1[1]:
            _bb = st.selectbox(
                _LABEL_AMOUNT_BASIS,
                _AMOUNT_BASIS_OPTIONS,
                key="bullet_amount_basis",
            )
            input_total_facility = _bb == _LABEL_PRINCIPAL_TOTAL
        with b1[2]:
            loan_input_label = _LABEL_PRINCIPAL_TOTAL if input_total_facility else _LABEL_NET_PROCEEDS
            loan_required = st.number_input(
                loan_input_label,
                min_value=0.0,
                value=1000.0,
                step=100.0,
                format="%.2f",
                key="bullet_principal",
            )
        with b1[3]:
            loan_term = st.number_input(
                "Term (Months)",
                min_value=1,
                max_value=120,
                value=12,
                step=1,
                key="bullet_term",
            )
        b2 = st.columns(4, gap=None)
        with b2[0]:
            bullet_type = st.selectbox(
                "Bullet type",
                ["Straight bullet (no interim payments)", "Bullet with interest payments"],
                key="bullet_type_sel",
            )
        with b2[1]:
            disbursement_input = st.date_input(_LABEL_DISBURSEMENT_DATE, get_system_date(), key="bullet_disb")
            disbursement_date = datetime.combine(disbursement_input, datetime.min.time())

        dr = cfg.get("default_rates", {}).get("bullet_loan", {})
        rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
        b3 = st.columns(4, gap=None)
        with b3[0]:
            rate_pct = st.number_input(rate_label, min_value=0.0, max_value=100.0, value=float(dr.get("interest_pct", 7.0)), step=0.1, key="bullet_rate")
        with b3[1]:
            drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="bullet_drawdown") / 100.0
        with b3[2]:
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
            b4 = st.columns(4, gap=None)
            with b4[0]:
                default_first_rep = add_months(disbursement_date, 1).date()
                first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="bullet_first_rep")
                first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
            with b4[1]:
                _bt = st.selectbox(
                    "Interest payments on",
                    ["Anniversary date (same day each month)", "Last day of each month"],
                    key="bullet_timing_sel",
                )
                use_anniversary = _bt.startswith("Anniversary")
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
        net_proceeds_to_display = float(details.get("disbursed_amount", loan_required) or 0.0)

        _render_calc_schedule_table(format_schedule_df, df_schedule)

        loan_record = {**details, "loan_type": "bullet_loan", "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
        for k in ("disbursement_date", "end_date", "first_repayment_date"):
            if k in loan_record and loan_record[k] is not None and hasattr(loan_record[k], "isoformat"):
                loan_record[k] = loan_record[k].isoformat()

        inject_tertiary_hyperlink_css_once()
        _csv_b, _xlsx_b = _schedule_export_bytes_pair(df_schedule)
        _ac1, _ac2, _ac3, _ = st.columns([1, 1, 1, 4], gap=None, vertical_alignment="center")
        with _ac1:
            st.download_button(
                "Download CSV",
                data=_csv_b,
                file_name="bullet_loan_schedule.csv",
                mime="text/csv",
                key="dl_sched_bullet_csv",
                type="tertiary",
                help="UTF-8 with BOM; amounts rounded to 2dp for readability.",
            )
        with _ac2:
            st.download_button(
                "Download Excel",
                data=_xlsx_b,
                file_name="bullet_loan_schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_sched_bullet_xlsx",
                type="tertiary",
                help="Native Excel numbers (.xlsx); no text warnings.",
            )
        with _ac3:
            if st.button("Save record to DB", type="tertiary", key="bullet_save"):
                st.success(f"Bullet loan for ${net_proceeds_to_display:,.2f} has been prepared for database sync.")
                with st.expander("Preview record (for DB insertion)"):
                    st.json(loan_record)




def render_customised_repayments_ui(
    *,
    get_global_loan_settings,
    get_system_config,
    get_system_date,
    format_schedule_df,
    money_df_column_config,
    schedule_editor_disabled_amounts,
    first_repayment_from_customised_table,
) -> None:
        glob = get_global_loan_settings()
        cfg = get_system_config()
        flat_rate = glob.get("interest_method") == "Flat rate"

        render_sub_sub_header("Customised Repayments Parameters")
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
        c1 = st.columns(4, gap=None)
        with c1[0]:
            currency = st.selectbox(
                "Currency",
                accepted_currencies,
                index=accepted_currencies.index(default_ccy)
                if default_ccy in accepted_currencies
                else 0,
                key="cust_currency",
            )
        with c1[1]:
            _cb = st.selectbox(
                _LABEL_AMOUNT_BASIS,
                _AMOUNT_BASIS_OPTIONS,
                key="cust_amount_basis",
            )
            input_total_facility = _cb == _LABEL_PRINCIPAL_TOTAL
        with c1[2]:
            loan_input_label = _LABEL_PRINCIPAL_TOTAL if input_total_facility else _LABEL_NET_PROCEEDS
            loan_required = st.number_input(
                loan_input_label,
                min_value=0.0,
                value=1000.0,
                step=100.0,
                format="%.2f",
                key="cust_principal",
            )
        with c1[3]:
            loan_term = st.number_input(
                "Term (Months)",
                min_value=1,
                max_value=120,
                value=12,
                step=1,
                key="cust_term",
            )
        c2 = st.columns(4, gap=None)
        with c2[0]:
            disbursement_input = st.date_input(_LABEL_DISBURSEMENT_DATE, get_system_date(), key="cust_start")
            disbursement_date = datetime.combine(disbursement_input, datetime.min.time())
        with c2[1]:
            irregular_calc = st.checkbox("Irregular", value=False, key="cust_irregular", help="Allow editing dates and adding rows; schedule recomputes from table.")
        with c2[2]:
            _cr = st.selectbox(
                _LABEL_REPAYMENTS_ON,
                ["Anniversary date (same day each month)", "Last day of each month"],
                key="cust_repay_on",
            )
            use_anniversary = _cr.startswith("Anniversary")
        default_first_rep = add_months(disbursement_date, 1).date()
        if not use_anniversary:
            default_first_rep = default_first_rep.replace(day=days_in_month(default_first_rep.year, default_first_rep.month))
        existing_cust = st.session_state.get("customised_repayments_df")
        first_rep_calc = first_repayment_from_customised_table(existing_cust) if existing_cust is not None and len(existing_cust) > 1 else None
        first_rep_display_calc = (first_rep_calc.date() if first_rep_calc else default_first_rep)
        with c2[3]:
            st.date_input("First repayment (from table)", first_rep_display_calc, key="cust_first_rep", disabled=True, help="From first row with non-zero payment.")
        first_repayment_date = datetime.combine(first_rep_display_calc, datetime.min.time())
        dr = cfg.get("default_rates", {}).get("customised_repayments", {})
        rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
        c3 = st.columns(4, gap=None)
        with c3[0]:
            rate_pct = st.number_input(rate_label, 0.0, 100.0, float(dr.get("interest_pct", 7.0)), step=0.1, key="cust_rate")
        with c3[1]:
            drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="cust_drawdown") / 100.0
        with c3[2]:
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

        render_sub_sub_header("Repayment schedule")
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
            height=320,
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
        inject_tertiary_hyperlink_css_once()
        _csv_b, _xlsx_b = _schedule_export_bytes_pair(df)
        _ac1, _ac2, _ac3, _ = st.columns([1, 1, 1, 4], gap=None, vertical_alignment="center")
        with _ac1:
            st.download_button(
                "Download CSV",
                data=_csv_b,
                file_name="customised_loan_schedule.csv",
                mime="text/csv",
                key="dl_sched_cust_csv",
                type="tertiary",
                help="UTF-8 with BOM; amounts rounded to 2dp for readability.",
            )
        with _ac2:
            st.download_button(
                "Download Excel",
                data=_xlsx_b,
                file_name="customised_loan_schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_sched_cust_xlsx",
                type="tertiary",
                help="Native Excel numbers (.xlsx); no text warnings.",
            )
        with _ac3:
            if st.button("Save record to DB", type="tertiary", key="cust_save", disabled=not can_save):
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

