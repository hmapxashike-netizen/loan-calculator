"""
Loans module: all loan schedule computations (consumer, term, bullet, customised).
Actual/360 day basis where applicable. No UI; use from app.py or other entry points.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import pandas as pd
import numpy_financial as npf

from decimal_utils import as_10dp


def _q10(v: float) -> float:
    """Quantize to 10dp for schedule amounts (matches project standard)."""
    return float(as_10dp(v))


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
    # Use digit grouping for readability (e.g., 1,234.56).
    return df.style.format({c: "{:,.2f}" for c in cols})


def prepare_schedule_export_dataframe(df: pd.DataFrame, *, amount_decimals: int = 2) -> pd.DataFrame:
    """
    Coerce schedule columns to plain numeric types for CSV/Excel export.

    - Amounts: full precision via ``as_10dp`` then rounded to ``amount_decimals`` for human-readable
      downloads (matches on-screen schedule formatting; DB storage remains 10dp elsewhere).
    - Period → integer; Date-like columns → plain strings.
    """
    out = df.copy()
    ad = max(0, min(28, int(amount_decimals)))
    for c in out.columns:
        name = str(c).strip()
        lower = name.lower()
        if lower == "period":
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(np.int64)
        elif lower == "date" or lower.endswith(" date") or lower in (
            "value_date",
            "due_date",
            "payment_date",
            "first_repayment_date",
        ):
            out[c] = out[c].map(lambda x: "" if pd.isna(x) else str(x).strip())
        else:
            num = pd.to_numeric(out[c], errors="coerce")

            def _cell(v: object) -> float:
                if pd.isna(v):
                    return 0.0
                try:
                    f = float(v)
                except (TypeError, ValueError):
                    return 0.0
                if not np.isfinite(f):
                    return 0.0
                q = float(as_10dp(f))
                return round(q, ad) if ad else q

            out[c] = num.map(_cell)

    return out


def schedule_dataframe_to_csv_bytes(df: pd.DataFrame, *, amount_decimals: int = 2) -> bytes:
    """
    CSV bytes for Excel: UTF-8 BOM, fixed decimals (default 2), no scientific notation in cells.

    Prefer :func:`schedule_dataframe_to_excel_bytes` if Excel still shows “number stored as text”.
    """
    prep = prepare_schedule_export_dataframe(df, amount_decimals=amount_decimals)
    buf = BytesIO()
    fmt = f"%.{max(0, min(28, int(amount_decimals)))}f"
    prep.to_csv(
        buf,
        index=False,
        encoding="utf-8-sig",
        float_format=fmt,
        lineterminator="\n",
    )
    return buf.getvalue()


def schedule_dataframe_to_excel_bytes(df: pd.DataFrame, *, amount_decimals: int = 2) -> bytes:
    """
    Excel workbook bytes: real numeric cell types (avoids green “text” triangles in Excel).

    Uses openpyxl via pandas; amounts rounded like CSV export.
    """
    prep = prepare_schedule_export_dataframe(df, amount_decimals=amount_decimals)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        prep.to_excel(writer, index=False, sheet_name="Schedule")
    buf.seek(0)
    return buf.getvalue()


# --- Consumer loan (30/360 style monthly) ---

def get_amortization_schedule(
    total_facility: float,
    monthly_rate: float,
    term: int,
    start_date: datetime,
    installment: float,
    flat_rate: bool = False,
    schedule_dates: list[datetime] | None = None,
) -> pd.DataFrame:
    """
    Consumer loan amortization (30/360 style). If schedule_dates is provided,
    those dates are used for the Date column; otherwise add_months(start_date, i).
    """
    schedule = []
    schedule.append({
        "Period": 0,
        "Date": start_date.strftime("%d-%b-%Y"),
        "Monthly Installment": 0.0,
        "Principal": 0.0,
        "Interest": 0.0,
        "Principal Balance": _q10(total_facility),
        "Total Outstanding": _q10(total_facility),
    })
    remaining_balance = total_facility

    for i in range(1, term + 1):
        if flat_rate:
            interest_payment = total_facility * monthly_rate
        else:
            interest_payment = remaining_balance * monthly_rate
        principal_payment = installment - interest_payment
        remaining_balance -= principal_payment
        bal = _q10(max(0, remaining_balance))
        date_str = (
            schedule_dates[i - 1].strftime("%d-%b-%Y")
            if schedule_dates and i <= len(schedule_dates)
            else add_months(start_date, i).strftime("%d-%b-%Y")
        )
        schedule.append({
            "Period": i,
            "Date": date_str,
            "Monthly Installment": _q10(installment),
            "Principal": _q10(principal_payment),
            "Interest": _q10(interest_payment),
            "Principal Balance": bal,
            "Total Outstanding": bal,
        })
    return pd.DataFrame(schedule)


# --- Term loan (Actual/360) ---


def _solve_level_payment_actual_360(
    opening_balance: float,
    annual_rate: float,
    periods_days: list[int],
    *,
    flat_rate: bool,
    principal_for_flat: float,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> float:
    """
    Solve for a constant instalment that amortises opening_balance to ~0
    using Actual/360 and the provided day-counts per period.

    This preserves the existing interest formula:
        interest_i = basis_i * annual_rate * (days_i / 360)
    where basis_i is either outstanding balance (reducing) or original
    principal (flat), but ensures heterogeneous period lengths (e.g. stubs)
    are reflected in the payment size.
    """
    n = len(periods_days)
    if n == 0 or opening_balance <= 0:
        return 0.0
    if annual_rate <= 0:
        # Pure principal amortisation
        return opening_balance / n

    # Lower bound: must at least cover max period interest to amortise.
    basis0 = principal_for_flat if flat_rate else opening_balance
    max_days = max(periods_days)
    max_interest = basis0 * annual_rate * (max_days / 360.0)
    low = float(max_interest) * 1.001

    # Upper bound: generous multiple of principal to guarantee convergence.
    high = low + opening_balance * 5.0

    def _ending_balance(pmt: float) -> float:
        """
        Internal helper for bisection: simulate the loan using the given payment.

        IMPORTANT: This deliberately allows the balance to go negative in the
        final periods so that the root we solve for corresponds to exhausting
        the balance exactly on the last scheduled repayment date, rather than
        potentially finishing earlier. Clamping to zero inside this function
        would change the shape of the function and can lead to solutions that
        fully repay in an earlier period (with subsequent zero instalments),
        which is not desired here.
        """
        bal = float(opening_balance)
        for days in periods_days:
            basis = principal_for_flat if flat_rate else bal
            interest = basis * annual_rate * (days / 360.0)
            principal = pmt - interest
            bal -= principal
        return bal

    # If the lower bound already over-amortises (very low rate / short term),
    # accept it to avoid unnecessary iterations.
    bal_low = _ending_balance(low)
    if bal_low <= 0:
        return low

    for _ in range(max_iter):
        mid = 0.5 * (low + high)
        bal_mid = _ending_balance(mid)
        if abs(bal_mid) < tol:
            return mid
        if bal_mid > 0:
            # Payment too low → positive remaining balance
            low = mid
        else:
            # Payment too high → negative remaining balance
            high = mid
    return 0.5 * (low + high)


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
        "Principal Balance": _q10(total_facility),
        "Total Outstanding": _q10(total_facility),
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
                "Interest": _q10(interest),
                "Principal Balance": _q10(principal_balance),
                "Total Outstanding": _q10(balance),
            })
            prev_date = end_date
        remaining = num_periods - moratorium_months
        if remaining > 0:
            # Solve constant instalment using actual days for remaining periods.
            periods_days: list[int] = []
            tmp_prev = prev_date
            for idx in range(moratorium_months, num_periods):
                end_date = repayment_dates_list[idx]
                periods_days.append((end_date - tmp_prev).days)
                tmp_prev = end_date
            pmt = _solve_level_payment_actual_360(
                opening_balance=balance,
                annual_rate=annual_rate,
                periods_days=periods_days,
                flat_rate=flat_rate,
                principal_for_flat=principal_for_flat,
            )
            for i in range(moratorium_months, num_periods):
                end_date = repayment_dates_list[i]
                days = (end_date - prev_date).days
                interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
                principal = max(0.0, pmt - interest)
                payment = pmt
                is_last = (i == num_periods - 1)
                if is_last:
                    # Force full payoff on the final instalment using any tiny residual balance.
                    principal = balance
                    payment = interest + principal
                balance -= principal
                principal_balance -= principal
                pb, to = _q10(max(0, principal_balance)), _q10(max(0, balance))
                schedule.append({
                    "Period": i + 1,
                    "Date": end_date.strftime("%d-%b-%Y"),
                    "Monthly Installment": _q10(payment),
                    "Principal": _q10(principal),
                    "Interest": _q10(interest),
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
                "Monthly Installment": _q10(interest),
                "Principal": 0.0,
                "Interest": _q10(interest),
                "Principal Balance": _q10(balance),
                "Total Outstanding": _q10(balance),
            })
            prev_date = end_date
        remaining = num_periods - principal_start_idx
        # Solve constant instalment for the principal-paying periods only.
        periods_days = []
        tmp_prev = prev_date
        for idx in range(principal_start_idx, num_periods):
            end_date = repayment_dates_list[idx]
            periods_days.append((end_date - tmp_prev).days)
            tmp_prev = end_date
        pmt = _solve_level_payment_actual_360(
            opening_balance=balance,
            annual_rate=annual_rate,
            periods_days=periods_days,
            flat_rate=flat_rate,
            principal_for_flat=principal_for_flat,
        )
        for i in range(principal_start_idx, num_periods):
            end_date = repayment_dates_list[i]
            days = (end_date - prev_date).days
            interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
            principal = max(0.0, pmt - interest)
            payment = pmt
            is_last = (i == num_periods - 1)
            if is_last:
                principal = balance
                payment = interest + principal
            balance -= principal
            principal_balance -= principal
            pb, to = _q10(max(0, principal_balance)), _q10(max(0, balance))
            schedule.append({
                "Period": i + 1,
                "Date": end_date.strftime("%d-%b-%Y"),
                "Monthly Installment": _q10(payment),
                "Principal": _q10(principal),
                "Interest": _q10(interest),
                "Principal Balance": pb,
                "Total Outstanding": to,
            })
            prev_date = end_date
        return pd.DataFrame(schedule), pmt
    else:
        # No grace: solve constant instalment across all periods using actual days.
        periods_days = []
        tmp_prev = prev_date
        for idx in range(num_periods):
            end_date = repayment_dates_list[idx]
            periods_days.append((end_date - tmp_prev).days)
            tmp_prev = end_date
        pmt = _solve_level_payment_actual_360(
            opening_balance=balance,
            annual_rate=annual_rate,
            periods_days=periods_days,
            flat_rate=flat_rate,
            principal_for_flat=principal_for_flat,
        )
        for i in range(num_periods):
            end_date = repayment_dates_list[i]
            days = (end_date - prev_date).days
            interest = (principal_for_flat if flat_rate else balance) * annual_rate * (days / 360)
            principal = max(0.0, pmt - interest)
            payment = pmt
            is_last = (i == num_periods - 1)
            if is_last:
                principal = balance
                payment = interest + principal
            balance -= principal
            principal_balance -= principal
            pb, to = _q10(max(0, principal_balance)), _q10(max(0, balance))
            schedule.append({
                "Period": i + 1,
                "Date": end_date.strftime("%d-%b-%Y"),
                "Monthly Installment": _q10(payment),
                "Principal": _q10(principal),
                "Interest": _q10(interest),
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
        "Principal Balance": _q10(total_facility),
        "Total Outstanding": _q10(total_facility),
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
            "Payment": _q10(balance + interest),
            "Principal": _q10(balance),
            "Interest": _q10(interest),
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
            "Payment": _q10(payment),
            "Principal": _q10(principal),
            "Interest": _q10(interest),
            "Principal Balance": _q10(principal_balance),
            "Total Outstanding": _q10(balance),
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
        out.at[idx, "Interest"] = _q10(interest)
        out.at[idx, "Principal"] = _q10(principal)
        out.at[idx, "Principal Balance"] = _q10(max(0, principal_balance))
        out.at[idx, "Total Outstanding"] = _q10(max(0, total_outstanding))
        prev_date = end_date
    return out
