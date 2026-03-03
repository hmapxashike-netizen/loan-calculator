import streamlit as st
from datetime import datetime, timedelta
import numpy_financial as npf


def _add_months(dt: datetime, months: int) -> datetime:
    month = dt.month - 1 + months
    year = dt.year + month // 12
    month = month % 12 + 1

    days_in_month = [
        31,
        29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28,
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    ]
    day = min(dt.day, days_in_month[month - 1])

    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond)


def calculate_loan(
    scheme: str,
    loan_required: float,
    loan_term: int,
    start_date: datetime,
    interest_rate_override: float | None = None,
    admin_fee_override: float | None = None,
):
    scheme_upper = scheme.upper()

    # Interest rate logic
    if interest_rate_override is not None:
        interest_rate = interest_rate_override
    else:
        if scheme_upper in ("SSB", "TPC"):
            interest_rate = 0.07
        else:
            interest_rate = 0.0

    # Admin fee logic
    if admin_fee_override is not None:
        admin_fee = admin_fee_override
    else:
        if scheme_upper == "SSB":
            admin_fee = 0.07
        elif scheme_upper == "TPC":
            admin_fee = 0.05
        else:
            admin_fee = 0.0

    total_facility = loan_required / (1.0 - admin_fee)

    # PMT: (interest_rate, loan_term, -total_facility)
    monthly_installment = float(npf.pmt(interest_rate, loan_term, -total_facility))

    end_date = _add_months(start_date, loan_term) - timedelta(days=1)

    return {
        "principal_debt": float(loan_required),
        "monthly_installment": monthly_installment,
        "end_date": end_date,
        "interest_rate": float(interest_rate),
        "admin_fee": float(admin_fee),
        "total_facility": float(total_facility),
    }


def main():
    st.markdown("### LINCOLN CAPITAL (PRIVATE) LIMITED")
    st.title("Consumer Loan Calculator")

    # Sidebar inputs
    st.sidebar.header("Loan Inputs")
    scheme = st.sidebar.selectbox("Scheme", ["SSB", "TPC", "Other"])
    loan_required = st.sidebar.number_input(
        "Amount",
        min_value=0.0,
        value=140.0,
        step=10.0,
        format="%.2f",
    )
    loan_term = st.sidebar.number_input(
        "Term (months)",
        min_value=1,
        value=6,
        step=1,
    )
    start_date_input = st.sidebar.date_input("Start Date", datetime.today().date())
    start_date = datetime.combine(start_date_input, datetime.min.time())

    # Additional inputs when scheme is 'Other'
    interest_rate_override = None
    admin_fee_override = None
    if scheme == "Other":
        interest_rate_percent = st.sidebar.number_input(
            "Interest rate (%)",
            min_value=0.0,
            max_value=100.0,
            value=0.0,
            step=0.1,
        )
        admin_fee_percent = st.sidebar.number_input(
            "Administration fee (%)",
            min_value=0.0,
            max_value=100.0,
            value=0.0,
            step=0.1,
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
        interest_rate_override = interest_rate_percent / 100.0
        admin_fee_override = admin_fee_percent / 100.0

    # Compute loan
    result = calculate_loan(
        scheme,
        loan_required,
        int(loan_term),
        start_date,
        interest_rate_override=interest_rate_override,
        admin_fee_override=admin_fee_override,
    )

    # Main area outputs
    st.subheader("Loan Summary")

    # Make Monthly Instalment stand out
    st.metric(
        label="Monthly Instalment",
        value=f"{result['monthly_installment']:,.2f}",
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.write("**Principal Debt**")
        st.write(f"{result['principal_debt']:,.2f}")

    with col2:
        st.write("**Interest Rate (p.a.)**")
        st.write(f"{result['interest_rate'] * 100:.2f}%")

    with col3:
        st.write("**Administration Fee**")
        st.write(f"{result['admin_fee'] * 100:.2f}%")

    with col4:
        st.write("**End Date**")
        st.write(result["end_date"].date().strftime("%Y-%m-%d"))

    st.write("**Total Facility**")
    st.write(f"{result['total_facility']:,.2f}")


if __name__ == "__main__":
    main()