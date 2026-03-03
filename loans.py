"""
Loans module: all loan schedule computations (consumer, term, bullet, customised).
Actual/360 day basis where applicable. No UI; use from app.py or other entry points.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import pandas as pd
import numpy_financial as npf


# --- Date / schedule helpers ---

def days_in_month(year: int, month: int) -> int:
    if month == 2:
        return 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
    return [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]


def add_months(dt: datetime, months: int) -> datetime:
    month = dt.month - 1 + months
    year = dt.year + month // 12
    month = month % 12 + 1
    days_in = [
        31,
        29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28,
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    ]
    day = min(dt.day, days_in[month - 1])
    return datetime(year, month, day, dt.hour, dt.minute, dt.second, dt.microsecond)


def is_last_day_of_month(d: datetime) -> bool:
    """True if d is the last calendar day of its month."""
    return d.day == days_in_month(d.year, d.month)


def _get_next_repayment_date(
    prev_date: datetime,
    first_repayment_date: datetime,
    period_num: int,
    use_anniversary: bool,
) -> datetime:
    """Get repayment date for period (1-based). prev_date is disbursement for period 1."""
    if period_num == 1:
        return first_repayment_date
    if use_anniversary:
        target_day = first_repayment_date.day
        new_month = prev_date.month + 1
        new_year = prev_date.year
        if new_month > 12:
            new_month -= 12
            new_year += 1
        max_day = days_in_month(new_year, new_month)
        day = min(target_day, max_day)
        return datetime(new_year, new_month, day)
    else:
        new_month = prev_date.month + 1
        new_year = prev_date.year
        if new_month > 12:
            new_month -= 12
            new_year += 1
        day = days_in_month(new_year, new_month)
        return datetime(new_year, new_month, day)


def repayment_dates(
    disbursement_date: datetime,
    first_repayment_date: datetime,
    num_periods: int,
    use_anniversary: bool,
) -> list[datetime]:
    dates = []
    prev = disbursement_date
    for i in range(1, num_periods + 1):
        d = _get_next_repayment_date(prev, first_repayment_date, i, use_anniversary)
        dates.append(d)
        prev = d
    return dates


# --- Penalty interest (for future arrears logic) ---

def effective_penalty_rate(
    penalty_rate_pct: float,
    penalty_quotation: str,
    regular_annual_rate: float | None = None,
) -> float:
    """
    Return effective penalty rate (decimal) based on quotation.
    Absolute Rate: penalty_rate_pct/100 is the penalty rate.
    Margin: penalty = regular_annual_rate + (penalty_rate_pct/100).
    """
    p = penalty_rate_pct / 100.0
    if penalty_quotation == "Margin" and regular_annual_rate is not None:
        return regular_annual_rate + p
    return p


# --- Display formatting ---

SCHEDULE_AMOUNT_COLUMNS = [
    "Payment", "Principal", "Interest",
    "Principal Balance", "Total Outstanding", "Monthly Installment"
]


def format_schedule_display(df: pd.DataFrame):
    """Format amount columns to 2 decimal places for schedule display."""
    cols = [c for c in SCHEDULE_AMOUNT_COLUMNS if c in df.columns]
    if not cols:
        return df.style
    return df.style.format({c: "{:.2f}" for c in cols})


# --- Consumer loan (30/360 style monthly) ---

def get_amortization_schedule(
    total_facility: float,
    monthly_rate: float,
    term: int,
    start_date: datetime,
    installment: float,
    flat_rate: bool = False,
) -> pd.DataFrame:
    schedule = []
    schedule.append({
        "Period": 0,
        "Date": start_date.strftime("%d-%b-%Y"),
        "Monthly Installment": 0.0,
        "Principal": 0.0,
        "Interest": 0.0,
        "Principal Balance": round(total_facility, 2),
        "Total Outstanding": round(total_facility, 2),
    })
    remaining_balance = total_facility

    for i in range(1, term + 1):
        if flat_rate:
            interest_payment = total_facility * monthly_rate
        else:
            interest_payment = remaining_balance * monthly_rate
        principal_payment = installment - interest_payment
        remaining_balance -= principal_payment
        bal = round(max(0, remaining_balance), 2)
        schedule.append({
            "Period": i,
            "Date": add_months(start_date, i).strftime("%d-%b-%Y"),
            "Monthly Installment": round(installment, 2),
            "Principal": round(principal_payment, 2),
            "Interest": round(interest_payment, 2),
            "Principal Balance": bal,
            "Total Outstanding": bal,
        })
    return pd.DataFrame(schedule)


# --- Term loan (Actual/360) ---

def get_term_loan_amortization_schedule(
    total_facility: float,
    annual_rate: float,
    disbursement_date: datetime,
    repayment_dates_list: list[datetime],
    grace_type: str,
    moratorium_months: int,
    flat_rate: bool = False,
) -> tuple[pd.DataFrame, float]:
    """
    Actual/360 day basis. Returns (schedule_df, total_installment).
    grace_type: 'none' | 'principal' | 'principal_and_interest'
    """
    schedule = []
    schedule.append({
        "Period": 0,
        "Date": disbursement_date.strftime("%d-%b-%Y"),
        "Monthly Installment": 0.0,
        "Principal": 0.0,
        "Interest": 0.0,
        "Principal Balance": round(total_facility, 2),
        "Total Outstanding": round(total_facility, 2),
    })
    balance = total_facility
    principal_balance = total_facility
    prev_date = disbursement_date
    num_periods = len(repayment_dates_list)
    principal_for_flat = total_facility

    if grace_type == "principal_and_interest":
        for i in range(moratorium_months):
            end_date = repayment_dates_list[i]
            days = (end_date - prev_date).days
            interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
            balance += interest
            schedule.append({
                "Period": i + 1,
                "Date": end_date.strftime("%d-%b-%Y"),
                "Monthly Installment": 0.0,
                "Principal": 0.0,
                "Interest": round(interest, 2),
                "Principal Balance": round(principal_balance, 2),
                "Total Outstanding": round(balance, 2),
            })
            prev_date = end_date
        remaining = num_periods - moratorium_months
        if remaining > 0:
            pmt = float(npf.pmt(annual_rate / 12, remaining, -balance))
            for i in range(moratorium_months, num_periods):
                end_date = repayment_dates_list[i]
                days = (end_date - prev_date).days
                interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
                principal = pmt - interest
                balance -= principal
                principal_balance -= principal
                pb, to = round(max(0, principal_balance), 2), round(max(0, balance), 2)
                schedule.append({
                    "Period": i + 1,
                    "Date": end_date.strftime("%d-%b-%Y"),
                    "Monthly Installment": round(pmt, 2),
                    "Principal": round(principal, 2),
                    "Interest": round(interest, 2),
                    "Principal Balance": pb,
                    "Total Outstanding": to,
                })
                prev_date = end_date
            return pd.DataFrame(schedule), pmt
    elif grace_type == "principal":
        principal_start_idx = moratorium_months
        for i in range(principal_start_idx):
            end_date = repayment_dates_list[i]
            days = (end_date - prev_date).days
            interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
            schedule.append({
                "Period": i + 1,
                "Date": end_date.strftime("%d-%b-%Y"),
                "Monthly Installment": round(interest, 2),
                "Principal": 0.0,
                "Interest": round(interest, 2),
                "Principal Balance": round(balance, 2),
                "Total Outstanding": round(balance, 2),
            })
            prev_date = end_date
        remaining = num_periods - principal_start_idx
        pmt = float(npf.pmt(annual_rate / 12, remaining, -balance))
        for i in range(principal_start_idx, num_periods):
            end_date = repayment_dates_list[i]
            days = (end_date - prev_date).days
            interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
            principal = pmt - interest
            balance -= principal
            principal_balance -= principal
            pb, to = round(max(0, principal_balance), 2), round(max(0, balance), 2)
            schedule.append({
                "Period": i + 1,
                "Date": end_date.strftime("%d-%b-%Y"),
                "Monthly Installment": round(pmt, 2),
                "Principal": round(principal, 2),
                "Interest": round(interest, 2),
                "Principal Balance": pb,
                "Total Outstanding": to,
            })
            prev_date = end_date
        return pd.DataFrame(schedule), pmt
    else:
        pmt = float(npf.pmt(annual_rate / 12, num_periods, -balance))
        for i in range(num_periods):
            end_date = repayment_dates_list[i]
            days = (end_date - prev_date).days
            interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
            principal = pmt - interest
            balance -= principal
            principal_balance -= principal
            pb, to = round(max(0, principal_balance), 2), round(max(0, balance), 2)
            schedule.append({
                "Period": i + 1,
                "Date": end_date.strftime("%d-%b-%Y"),
                "Monthly Installment": round(pmt, 2),
                "Principal": round(principal, 2),
                "Interest": round(interest, 2),
                "Principal Balance": pb,
                "Total Outstanding": to,
            })
            prev_date = end_date
        return pd.DataFrame(schedule), pmt


# --- Bullet loan (Actual/360) ---

def get_bullet_schedule(
    total_facility: float,
    annual_rate: float,
    disbursement_date: datetime,
    maturity_date: datetime,
    bullet_type: str,
    repayment_dates_list: list[datetime] | None,
    flat_rate: bool = False,
) -> pd.DataFrame:
    """
    Actual/360. bullet_type: 'straight' (no interim) or 'with_interest' (interest on dates, principal at maturity).
    """
    schedule = []
    schedule.append({
        "Period": 0,
        "Date": disbursement_date.strftime("%d-%b-%Y"),
        "Payment": 0.0,
        "Principal": 0.0,
        "Interest": 0.0,
        "Principal Balance": round(total_facility, 2),
        "Total Outstanding": round(total_facility, 2),
    })
    balance = total_facility
    principal_balance = total_facility
    prev_date = disbursement_date
    principal_for_flat = total_facility

    if bullet_type == "straight":
        days = (maturity_date - disbursement_date).days
        interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
        schedule.append({
            "Period": 1,
            "Date": maturity_date.strftime("%d-%b-%Y"),
            "Payment": round(balance + interest, 2),
            "Principal": round(balance, 2),
            "Interest": round(interest, 2),
            "Principal Balance": 0.0,
            "Total Outstanding": 0.0,
        })
        return pd.DataFrame(schedule)

    dates = repayment_dates_list or []
    for i, end_date in enumerate(dates):
        days = (end_date - prev_date).days
        interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
        is_last = i == len(dates) - 1
        if is_last:
            payment = balance + interest
            principal = balance
            balance = 0.0
            principal_balance = 0.0
        else:
            payment = interest
            principal = 0.0
        schedule.append({
            "Period": i + 1,
            "Date": end_date.strftime("%d-%b-%Y"),
            "Payment": round(payment, 2),
            "Principal": round(principal, 2),
            "Interest": round(interest, 2),
            "Principal Balance": round(principal_balance, 2),
            "Total Outstanding": round(balance, 2),
        })
        prev_date = end_date
    return pd.DataFrame(schedule)


# --- Customised repayments: recompute schedule from editable payments ---

def parse_schedule_dates_from_table(df: pd.DataFrame, date_fmt: str = "%d-%b-%Y", start_date: datetime | None = None) -> list[datetime]:
    """
    Parse 'Date' column from schedule dataframe (rows 1..n, row 0 is start).
    Returns list of datetimes, one per repayment row. On parse failure uses start_date + index months as fallback.
    """
    out: list[datetime] = []
    fallback = start_date or datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    for idx in range(1, len(df)):
        try:
            raw = df.at[idx, "Date"]
            if pd.isna(raw):
                raise ValueError("missing")
            s = str(raw).strip()[:32]
            out.append(datetime.combine(datetime.strptime(s, date_fmt).date(), datetime.min.time()))
        except (ValueError, TypeError):
            if out:
                out.append(add_months(out[-1], 1))
            else:
                out.append(add_months(fallback, 1))
    return out


def recompute_customised_from_payments(
    template_df: pd.DataFrame,
    total_facility: float,
    repayment_dates_list: list[datetime],
    annual_rate: float,
    flat_rate: bool,
    start_date: datetime,
) -> pd.DataFrame:
    """
    Given a template dataframe with editable Payment column, recompute Interest, Principal,
    Principal Balance, Total Outstanding. Total Outstanding = prior + interest - payment;
    Principal Balance = facility - cumulative principal.
    """
    out = template_df.copy()
    if "Remaining Balance" in out.columns:
        if "Principal Balance" not in out.columns:
            out["Principal Balance"] = out["Remaining Balance"].copy()
        if "Total Outstanding" not in out.columns:
            out["Total Outstanding"] = out["Remaining Balance"].copy()
        out = out.drop(columns=["Remaining Balance"])
    if "Principal Balance" not in out.columns:
        out["Principal Balance"] = total_facility
    if "Total Outstanding" not in out.columns:
        out["Total Outstanding"] = total_facility
    prev_date = start_date
    total_outstanding = total_facility
    principal_balance = total_facility
    cum_principal = 0.0
    principal_for_flat = total_facility
    for idx in range(1, len(out)):
        end_date = repayment_dates_list[idx - 1]
        days = (end_date - prev_date).days
        interest = (principal_for_flat if flat_rate else total_outstanding) * annual_rate * (days / 360)
        payment = float(out.at[idx, "Payment"]) if pd.notna(out.at[idx, "Payment"]) else 0.0
        total_outstanding = total_outstanding + interest - payment
        principal = min(max(0.0, payment - interest), principal_balance)
        cum_principal += principal
        principal_balance = total_facility - cum_principal
        out.at[idx, "Interest"] = round(interest, 2)
        out.at[idx, "Principal"] = round(principal, 2)
        out.at[idx, "Principal Balance"] = round(max(0, principal_balance), 2)
        out.at[idx, "Total Outstanding"] = round(max(0, total_outstanding), 2)
        prev_date = end_date
    return out
