"""
Reamortisation engine: Loan Modification (new terms/agreement) and Loan Recast (prepayment → new instalment).
Uses loans.py for schedule generation and loan_management for persistence.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from loans import (
    add_months,
    days_in_month,
    get_amortization_schedule,
    get_term_loan_amortization_schedule,
    get_bullet_schedule,
    is_last_day_of_month,
    repayment_dates,
)
from decimal_utils import as_10dp

from loan_management import (
    get_loan,
    get_latest_schedule_version,
    get_schedule_lines,
    get_loan_daily_state_balances,
    save_new_schedule_version,
    update_loan_details,
    _connection,
    _date_conv,
)
from loan_management.loan_records import update_loan_restructure_flags, update_loan_safe_details

try:
    from psycopg2.extras import RealDictCursor
except ImportError:
    RealDictCursor = None


def _parse_schedule_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(str(s).strip()[:32], "%d-%b-%Y").date()
    except (ValueError, TypeError):
        return None


def _last_due_date_from_loan(loan: dict) -> date | None:
    """Last due date from loan.end_date or last schedule line."""
    end = loan.get("end_date")
    if end:
        return _date_conv(end)
    lines = get_schedule_lines(loan["id"])
    if not lines:
        return None
    last = max((_parse_schedule_date(l.get("Date")) for l in lines if l.get("Date")), default=None)
    return last


def _recast_due_dates_template(loan_id: int, recast_date: date) -> list[datetime]:
    """
    Reuse existing contractual due dates after recast_date from current schedule version.
    This preserves timing cadence (month-end or anniversary day).
    """
    try:
        version = get_latest_schedule_version(loan_id)
        lines = get_schedule_lines(loan_id, version)
    except Exception:
        return []
    out: list[datetime] = []
    seen: set[date] = set()
    for l in lines:
        p = int(l.get("Period") or 0)
        if p <= 0:
            continue
        d = _parse_schedule_date(l.get("Date"))
        if d is None or d <= recast_date or d in seen:
            continue
        seen.add(d)
        out.append(datetime.combine(d, datetime.min.time()))
    out.sort()
    return out


def _next_due_by_cadence(prev_due: datetime, *, anchor_day: int, month_end: bool) -> datetime:
    nxt = add_months(prev_due, 1)
    if month_end:
        return datetime(nxt.year, nxt.month, days_in_month(nxt.year, nxt.month))
    day = min(anchor_day, days_in_month(nxt.year, nxt.month))
    return datetime(nxt.year, nxt.month, day)


def _solve_equal_payment_amount(
    *,
    principal: float,
    annual_rate: float,
    start_dt: datetime,
    due_dates: list[datetime],
) -> float:
    """
    Solve a single constant instalment amount that amortizes to ~0 at last due date
    using actual/360 accrual and 10dp math.
    """

    def _end_balance(payment: float) -> float:
        bal = float(as_10dp(principal))
        prev = start_dt
        for d in due_dates:
            days = (d - prev).days
            intr = float(as_10dp(bal * annual_rate * (days / 360.0)))
            bal = float(as_10dp(bal + intr - payment))
            prev = d
        return float(as_10dp(bal))

    low = 0.0
    high = float(as_10dp(principal))
    while _end_balance(high) > 0:
        high = float(as_10dp(high * 2.0))
        if high > 1e12:
            raise ValueError("Unable to solve equal instalment for recast schedule.")

    for _ in range(160):
        mid = float(as_10dp((low + high) / 2.0))
        e = _end_balance(mid)
        if abs(e) <= 1e-10:
            return mid
        if e > 0:
            low = mid
        else:
            high = mid
    return float(as_10dp((low + high) / 2.0))


def _build_equal_payment_schedule_rows(
    *,
    recast_date: date,
    principal: float,
    annual_rate: float,
    due_dates: list[datetime],
    payment: float,
) -> list[dict[str, Any]]:
    start_dt = datetime.combine(recast_date, datetime.min.time())
    schedule: list[dict[str, Any]] = [
        {
            "Period": 0,
            "Date": start_dt.strftime("%d-%b-%Y"),
            "Monthly Installment": 0.0,
            "Principal": 0.0,
            "Interest": 0.0,
            "Principal Balance": _qsched(principal),
            "Total Outstanding": _qsched(principal),
        }
    ]

    bal = float(as_10dp(principal))
    prev = start_dt
    for i, d in enumerate(due_dates, start=1):
        days = (d - prev).days
        intr = float(as_10dp(bal * annual_rate * (days / 360.0)))
        prin = float(as_10dp(payment - intr))
        bal = float(as_10dp(bal - prin))
        if i == len(due_dates) and abs(bal) <= 1e-10:
            bal = 0.0
        schedule.append(
            {
                "Period": i,
                "Date": d.strftime("%d-%b-%Y"),
                "Monthly Installment": _qsched(payment),
                "Principal": _qsched(prin),
                "Interest": _qsched(intr),
                "Principal Balance": _qsched(max(0.0, bal)),
                "Total Outstanding": _qsched(max(0.0, bal)),
            }
        )
        prev = d
    return schedule


def get_loan_for_modification(loan_id: int, as_of: date | None = None) -> dict | None:
    """
    Return loan plus current schedule and balance info for modification UI.
    Keys: loan, schedule_version, schedule_lines, balances, last_due_date.
    """
    if as_of is None:
        from eod.system_business_date import get_effective_date
        as_of = get_effective_date()
    loan = get_loan(loan_id)
    if not loan:
        return None
    version = get_latest_schedule_version(loan_id)
    lines = get_schedule_lines(loan_id, version)
    balances = get_loan_daily_state_balances(loan_id, as_of)
    last_due = _last_due_date_from_loan(loan)
    return {
        "loan": loan,
        "schedule_version": version,
        "schedule_lines": lines,
        "balances": balances,
        "last_due_date": last_due,
    }


def _build_modification_schedule(
    loan_id: int,
    restructure_date: date,
    new_loan_type: str,
    new_params: dict[str, Any],
    outstanding_interest_treatment: str,
) -> tuple[pd.DataFrame, float, float | None]:
    """
    Build the proposed schedule for a loan modification (no persistence).
    Returns (schedule_df, new_principal_balance, new_installment or None).
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    balances = get_loan_daily_state_balances(loan_id, restructure_date)
    principal_balance = float(loan.get("principal") or loan.get("disbursed_amount") or 0)
    if balances:
        principal_balance = balances.get("principal_not_due", 0) + balances.get("principal_arrears", 0)
    if principal_balance <= 0:
        principal_balance = float(loan.get("principal") or 0)

    if outstanding_interest_treatment == "capitalise" and balances:
        interest_total = (
            balances.get("interest_accrued_balance", 0)
            + balances.get("interest_arrears_balance", 0)
            + balances.get("default_interest_balance", 0)
            + balances.get("penalty_interest_balance", 0)
        )
        principal_balance += interest_total

    term = int(new_params.get("term") or 12)
    annual_rate = float(new_params.get("annual_rate") or 0) / 100.0 if new_params.get("annual_rate") is not None else 0.0
    start_dt = datetime.combine(restructure_date, datetime.min.time())
    installment: float | None = None

    if new_loan_type == "consumer_loan":
        monthly_rate = float(new_params.get("monthly_rate") or 0) / 100.0
        flat_rate = (new_params.get("flat_rate") or "reducing").lower() == "flat"
        inst = float(new_params.get("installment") or 0)
        if inst <= 0 and (monthly_rate or term):
            import numpy_financial as npf
            inst = float(npf.pmt(monthly_rate or 0.01, term, -principal_balance))
        df = get_amortization_schedule(
            total_facility=principal_balance,
            monthly_rate=monthly_rate or 0.01,
            term=term,
            start_date=start_dt,
            installment=inst or principal_balance / max(term, 1),
            flat_rate=flat_rate,
        )
        installment = inst or (principal_balance / max(term, 1))
    elif new_loan_type == "term_loan":
        grace_type = new_params.get("grace_type") or "none"
        moratorium_months = int(new_params.get("moratorium_months") or 0)
        first_repayment_date = new_params.get("first_repayment_date")
        if hasattr(first_repayment_date, "date"):
            first_repayment_date = datetime.combine(first_repayment_date.date() if first_repayment_date else restructure_date, datetime.min.time())
        else:
            first_repayment_date = add_months(start_dt, 1)
        dates_list = repayment_dates(start_dt, first_repayment_date, term, use_anniversary=True)
        df, installment = get_term_loan_amortization_schedule(
            total_facility=principal_balance,
            annual_rate=annual_rate,
            disbursement_date=start_dt,
            repayment_dates_list=dates_list,
            grace_type=grace_type,
            moratorium_months=moratorium_months,
            flat_rate=False,
        )
    elif new_loan_type == "bullet_loan":
        end_dt = new_params.get("end_date") or add_months(start_dt, term)
        if hasattr(end_dt, "date"):
            end_dt = datetime.combine(end_dt.date(), datetime.min.time())
        dates_list = repayment_dates(start_dt, end_dt, term, use_anniversary=True) if term else []
        df = get_bullet_schedule(
            total_facility=principal_balance,
            annual_rate=annual_rate,
            disbursement_date=start_dt,
            maturity_date=end_dt,
            bullet_type=new_params.get("bullet_type") or "with_interest",
            repayment_dates_list=dates_list,
            flat_rate=False,
        )
    else:
        first_repayment_date = add_months(start_dt, 1)
        dates_list = repayment_dates(start_dt, first_repayment_date, term, use_anniversary=True)
        df, installment = get_term_loan_amortization_schedule(
            total_facility=principal_balance,
            annual_rate=annual_rate,
            disbursement_date=start_dt,
            repayment_dates_list=dates_list,
            grace_type="none",
            moratorium_months=0,
            flat_rate=False,
        )
    return df, principal_balance, installment


def preview_loan_modification(
    loan_id: int,
    restructure_date: date,
    new_loan_type: str,
    new_params: dict[str, Any],
    outstanding_interest_treatment: str,
) -> dict[str, Any]:
    """
    Preview the proposed schedule for a loan modification (no DB changes).
    Returns dict with keys: schedule_df, new_principal, new_installment, term, new_loan_type.
    """
    df, new_principal, new_installment = _build_modification_schedule(
        loan_id, restructure_date, new_loan_type, new_params, outstanding_interest_treatment
    )
    return {
        "schedule_df": df,
        "new_principal": new_principal,
        "new_installment": new_installment,
        "term": int(new_params.get("term") or 12),
        "new_loan_type": new_loan_type,
    }


def execute_loan_modification(
    loan_id: int,
    restructure_date: date,
    new_loan_type: str,
    new_params: dict[str, Any],
    outstanding_interest_treatment: str,
    *,
    notes: str | None = None,
) -> int:
    """
    Apply a loan modification: amortise current balance (and capitalised interest if chosen)
    over new terms. new_loan_type: consumer_loan, term_loan, bullet_loan, customised_repayments.
    new_params must contain the fields required by the chosen type (term, annual_rate, etc.).
    outstanding_interest_treatment: 'capitalise' | 'write_off'.
    Returns new schedule version.
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    prev_version = get_latest_schedule_version(loan_id)
    df, principal_balance, installment = _build_modification_schedule(
        loan_id, restructure_date, new_loan_type, new_params, outstanding_interest_treatment
    )
    term = int(new_params.get("term") or 12)
    new_version = prev_version + 1

    save_new_schedule_version(loan_id, df, new_version)
    update_loan_details(
        loan_id,
        principal=round(principal_balance, 2),
        term=term,
        annual_rate=new_params.get("annual_rate"),
        installment=round(installment, 2) if installment is not None else None,
        loan_type=new_loan_type,
    )
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_modifications (
                    loan_id, modification_date, previous_schedule_version, new_schedule_version,
                    outstanding_interest_treatment, new_loan_type, new_term, new_annual_rate,
                    new_principal, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    loan_id,
                    restructure_date,
                    prev_version,
                    new_version,
                    outstanding_interest_treatment,
                    new_loan_type,
                    term,
                    new_params.get("annual_rate"),
                    round(principal_balance, 2),
                    notes,
                ),
            )
    update_loan_restructure_flags(loan_id, remodified_in_place=True)
    return new_version


def _build_recast_schedule(
    loan_id: int,
    recast_date: date,
    new_principal_balance: float,
) -> tuple[pd.DataFrame, float]:
    """
    Build the proposed recast schedule (no persistence).
    Returns (schedule_df, new_installment).
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    due_dates = _recast_due_dates_template(loan_id, recast_date)
    if not due_dates:
        last_due = _last_due_date_from_loan(loan)
        if not last_due:
            raise ValueError(f"Cannot determine maturity for loan {loan_id}.")
        if recast_date > last_due:
            raise ValueError("Recast date cannot be after the last due date.")
        # Fallback only when schedule lines are unavailable; keep prior behavior.
        start_dt = datetime.combine(recast_date, datetime.min.time())
        end_dt = datetime.combine(last_due, datetime.min.time())
        remaining_months = max(1, (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month))
        first_repayment_date = add_months(start_dt, 1)
        due_dates = repayment_dates(start_dt, first_repayment_date, remaining_months, use_anniversary=True)
    if not due_dates:
        raise ValueError(f"Cannot determine maturity for loan {loan_id}.")
    if new_principal_balance <= 0:
        raise ValueError("New principal balance must be positive.")

    annual_rate = _normalize_recast_annual_rate(loan.get("annual_rate"))
    start_dt = datetime.combine(recast_date, datetime.min.time())
    new_installment = _solve_equal_payment_amount(
        principal=new_principal_balance,
        annual_rate=annual_rate,
        start_dt=start_dt,
        due_dates=due_dates,
    )
    rows = _build_equal_payment_schedule_rows(
        recast_date=recast_date,
        principal=new_principal_balance,
        annual_rate=annual_rate,
        due_dates=due_dates,
        payment=new_installment,
    )
    return pd.DataFrame(rows), new_installment


def _qsched(v: float) -> float:
    return float(as_10dp(v))


def _normalize_recast_annual_rate(raw_rate: Any) -> float:
    """
    Normalize persisted annual_rate to decimal for schedule math.

    Canonical storage for new data is percent (e.g. 12 for 12%), but existing
    rows may still hold decimal form (e.g. 0.12 or 1.2). To prevent 100x drift,
    treat values ``<= 2`` as decimal and larger values as percent.
    """
    r = float(raw_rate or 0.0)
    if r <= 0:
        return 0.0
    return float(as_10dp(r if r <= 2.0 else (r / 100.0)))


def _build_recast_schedule_maintain_instalment(
    loan_id: int,
    recast_date: date,
    new_principal_balance: float,
    fixed_instalment: float,
    balancing_position: str = "final_installment",
) -> tuple[pd.DataFrame, float]:
    """
    Keep contractual instalment amount and place the single balancing figure either on
    the next immediate instalment or final instalment.
    Uses actual/360 between schedule dates, same anniversary logic as maintain_term.
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    due_dates = _recast_due_dates_template(loan_id, recast_date)
    if not due_dates:
        last_due = _last_due_date_from_loan(loan)
        if not last_due:
            raise ValueError(f"Cannot determine maturity for loan {loan_id}.")
        if recast_date > last_due:
            raise ValueError("Recast date cannot be after the last due date.")
        # Fallback only when schedule lines are unavailable; keep prior behavior.
        start_dt = datetime.combine(recast_date, datetime.min.time())
        end_dt = datetime.combine(last_due, datetime.min.time())
        remaining_months = max(1, (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month))
        first_repayment_date = add_months(start_dt, 1)
        due_dates = repayment_dates(start_dt, first_repayment_date, remaining_months, use_anniversary=True)
    if not due_dates:
        raise ValueError(f"Cannot determine maturity for loan {loan_id}.")
    if new_principal_balance <= 0:
        raise ValueError("New principal balance must be positive.")
    if fixed_instalment <= 0:
        raise ValueError("Fixed instalment must be positive for maintain_instalment.")
    if balancing_position != "final_installment":
        raise ValueError("Only 'final_installment' balancing is supported for maintain_instalment.")

    start_dt = datetime.combine(recast_date, datetime.min.time())
    annual_rate = _normalize_recast_annual_rate(loan.get("annual_rate"))

    def _build_rows(
        *,
        first_payment_override: float | None = None,
        balance_final_on_last_due: bool = False,
    ) -> list[dict[str, Any]]:
        schedule: list[dict[str, Any]] = [
            {
                "Period": 0,
                "Date": start_dt.strftime("%d-%b-%Y"),
                "Monthly Installment": 0.0,
                "Principal": 0.0,
                "Interest": 0.0,
                "Principal Balance": _qsched(new_principal_balance),
                "Total Outstanding": _qsched(new_principal_balance),
            }
        ]
        balance = float(new_principal_balance)
        principal_balance = float(new_principal_balance)
        prev_date = start_dt
        pmt = float(fixed_instalment)
        period_no = 0

        for i, end_date in enumerate(due_dates):
            period_no += 1
            days = (end_date - prev_date).days
            interest = float(as_10dp(balance * annual_rate * (days / 360.0)))
            is_last_due = i == len(due_dates) - 1
            target_payment = float(as_10dp(first_payment_override)) if (i == 0 and first_payment_override is not None) else pmt
            if balance_final_on_last_due and is_last_due:
                principal = max(0.0, float(as_10dp(balance)))
                payment = float(as_10dp(interest + principal))
            else:
                principal = max(0.0, float(as_10dp(min(balance, max(0.0, target_payment - interest)))))
                payment = float(as_10dp(interest + principal))
            balance = max(0.0, float(as_10dp(balance - principal)))
            principal_balance = max(0.0, float(as_10dp(principal_balance - principal)))
            schedule.append(
                {
                    "Period": period_no,
                    "Date": end_date.strftime("%d-%b-%Y"),
                    "Monthly Installment": _qsched(payment),
                    "Principal": _qsched(principal),
                    "Interest": _qsched(interest),
                    "Principal Balance": _qsched(principal_balance),
                    "Total Outstanding": _qsched(balance),
                }
            )
            prev_date = end_date

        # Keep charging fixed instalment on every due date until cleared (term may extend).
        guard = 0
        month_end = all(is_last_day_of_month(d) for d in due_dates)
        anchor_day = due_dates[0].day
        while balance > 1e-10 and guard < 600:
            guard += 1
            period_no += 1
            next_due = _next_due_by_cadence(prev_date, anchor_day=anchor_day, month_end=month_end)
            days = (next_due - prev_date).days
            interest = float(as_10dp(balance * annual_rate * (days / 360.0)))
            principal = max(0.0, float(as_10dp(min(balance, max(0.0, pmt - interest)))))
            payment = float(as_10dp(interest + principal))
            if principal <= 1e-10:
                raise ValueError("Fixed instalment is too low to amortize this balance under current rate.")
            balance = max(0.0, float(as_10dp(balance - principal)))
            principal_balance = max(0.0, float(as_10dp(principal_balance - principal)))
            if balance <= 1e-10:
                principal = principal + balance
                balance = 0.0
                principal_balance = 0.0
                payment = float(as_10dp(interest + principal))
            schedule.append(
                {
                    "Period": period_no,
                    "Date": next_due.strftime("%d-%b-%Y"),
                    "Monthly Installment": _qsched(payment),
                    "Principal": _qsched(principal),
                    "Interest": _qsched(interest),
                    "Principal Balance": _qsched(principal_balance),
                    "Total Outstanding": _qsched(balance),
                }
            )
            prev_date = next_due
        return schedule

    pmt = float(fixed_instalment)
    schedule_rows = _build_rows(balance_final_on_last_due=True)
    return pd.DataFrame(schedule_rows), pmt


def _build_recast_schedule_prepay_upcoming_installments(
    loan_id: int,
    recast_date: date,
    new_principal_balance: float,
    fixed_instalment: float,
    prepayment_amount: float,
) -> tuple[pd.DataFrame, float]:
    """
    Scenario 3: prepayment of upcoming instalments.

    - Keep original maturity date/rate basis.
    - Use recast-reduced principal as opening.
    - Keep contractual instalment as the due amount basis.
    - Apply prepayment pool to upcoming dues (interest first, then principal) so
      fully covered periods show zero due and first uncovered period becomes the
      balancing transition amount.
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    due_dates = _recast_due_dates_template(loan_id, recast_date)
    if not due_dates:
        last_due = _last_due_date_from_loan(loan)
        if not last_due:
            raise ValueError(f"Cannot determine maturity for loan {loan_id}.")
        if recast_date > last_due:
            raise ValueError("Recast date cannot be after the last due date.")
        start_dt = datetime.combine(recast_date, datetime.min.time())
        end_dt = datetime.combine(last_due, datetime.min.time())
        remaining_months = max(1, (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month))
        first_repayment_date = add_months(start_dt, 1)
        due_dates = repayment_dates(start_dt, first_repayment_date, remaining_months, use_anniversary=True)
    if not due_dates:
        raise ValueError(f"Cannot determine maturity for loan {loan_id}.")
    if new_principal_balance <= 0:
        raise ValueError("New principal balance must be positive.")
    if fixed_instalment <= 0:
        raise ValueError("Fixed instalment must be positive for prepay_upcoming_installments.")

    annual_rate = _normalize_recast_annual_rate(loan.get("annual_rate"))
    pmt = float(as_10dp(fixed_instalment))
    pool = max(0.0, float(as_10dp(prepayment_amount)))

    # Apply pool from earliest dues: many full clears (0), then at most one lower due, then strict contractual dues.
    due_plan: list[float] = []
    for _ in due_dates:
        if pool <= 1e-10:
            due_plan.append(pmt)
            continue
        if pool >= pmt - 1e-10:
            due_plan.append(0.0)
            pool = float(as_10dp(pool - pmt))
            continue
        due_plan.append(float(as_10dp(pmt - pool)))
        pool = 0.0
    df = _build_schedule_from_due_plan(
        recast_date=recast_date,
        new_principal_balance=new_principal_balance,
        annual_rate=annual_rate,
        due_dates=due_dates,
        due_plan=due_plan,
    )
    return df, pmt


def _build_schedule_from_due_plan(
    *,
    recast_date: date,
    new_principal_balance: float,
    annual_rate: float,
    due_dates: list[datetime],
    due_plan: list[float],
) -> pd.DataFrame:
    start_dt = datetime.combine(recast_date, datetime.min.time())
    schedule: list[dict[str, Any]] = [
        {
            "Period": 0,
            "Date": start_dt.strftime("%d-%b-%Y"),
            "Monthly Installment": 0.0,
            "Principal": 0.0,
            "Interest": 0.0,
            "Principal Balance": _qsched(new_principal_balance),
            "Total Outstanding": _qsched(new_principal_balance),
        }
    ]
    balance = float(as_10dp(new_principal_balance))
    principal_balance = float(as_10dp(new_principal_balance))
    prev_date = start_dt
    for i, end_date in enumerate(due_dates, start=1):
        due = float(as_10dp(due_plan[i - 1]))
        days = (end_date - prev_date).days
        interest = float(as_10dp(balance * annual_rate * (days / 360.0)))
        raw_principal = float(as_10dp(due - interest))
        if raw_principal >= 0.0:
            principal = min(balance, raw_principal)
            balance = float(as_10dp(balance - principal))
        else:
            principal = 0.0
            balance = float(as_10dp(balance + abs(raw_principal)))
        principal_balance = float(as_10dp(max(0.0, balance)))
        schedule.append(
            {
                "Period": i,
                "Date": end_date.strftime("%d-%b-%Y"),
                "Monthly Installment": _qsched(due),
                "Principal": _qsched(principal),
                "Interest": _qsched(interest),
                "Principal Balance": _qsched(principal_balance),
                "Total Outstanding": _qsched(max(0.0, balance)),
            }
        )
        prev_date = end_date
        if balance <= 1e-10:
            for j, tail_date in enumerate(due_dates[i:], start=i + 1):
                schedule.append(
                    {
                        "Period": j,
                        "Date": tail_date.strftime("%d-%b-%Y"),
                        "Monthly Installment": 0.0,
                        "Principal": 0.0,
                        "Interest": 0.0,
                        "Principal Balance": 0.0,
                        "Total Outstanding": 0.0,
                    }
                )
            break
    return pd.DataFrame(schedule)


def build_recast_schedule_for_mode(
    loan_id: int,
    recast_date: date,
    new_principal_balance: float,
    mode: str,
    balancing_position: str = "final_installment",
    prepayment_amount: float | None = None,
) -> tuple[pd.DataFrame, float]:
    """Dispatch maintain_term vs maintain_instalment recast schedule builders."""
    if mode == "maintain_instalment":
        loan = get_loan(loan_id)
        if not loan:
            raise ValueError(f"Loan {loan_id} not found.")
        inst = float(loan.get("installment") or 0)
        if inst <= 0:
            raise ValueError("Loan has no positive installment; use maintain_term instead.")
        df, pmt = _build_recast_schedule_maintain_instalment(
            loan_id,
            recast_date,
            new_principal_balance,
            inst,
            balancing_position=balancing_position,
        )
        if prepayment_amount is not None and float(prepayment_amount) > 0:
            annual_rate = _normalize_recast_annual_rate(loan.get("annual_rate"))
            due_dates: list[datetime] = []
            for s in df.iloc[1:]["Date"].tolist():
                d = _parse_schedule_date(str(s))
                if d is not None:
                    due_dates.append(datetime.combine(d, datetime.min.time()))
            if due_dates:
                pool = max(0.0, float(as_10dp(float(prepayment_amount))))
                contractual = float(as_10dp(inst))
                due_plan = [contractual for _ in due_dates]
                for i in range(len(due_plan) - 1, -1, -1):
                    if pool <= 1e-10:
                        break
                    if pool >= contractual - 1e-10:
                        due_plan[i] = 0.0
                        pool = float(as_10dp(pool - contractual))
                    else:
                        due_plan[i] = float(as_10dp(contractual - pool))
                        pool = 0.0
                df = _build_schedule_from_due_plan(
                    recast_date=recast_date,
                    new_principal_balance=new_principal_balance,
                    annual_rate=annual_rate,
                    due_dates=due_dates,
                    due_plan=due_plan,
                )
        return df, pmt
    if mode == "prepay_upcoming_installments":
        loan = get_loan(loan_id)
        if not loan:
            raise ValueError(f"Loan {loan_id} not found.")
        inst = float(loan.get("installment") or 0)
        if inst <= 0:
            raise ValueError("Loan has no positive installment; cannot run prepay upcoming installments mode.")
        if prepayment_amount is None or float(prepayment_amount) <= 0:
            raise ValueError("prepayment_amount must be positive for prepay_upcoming_installments mode.")
        return _build_recast_schedule_prepay_upcoming_installments(
            loan_id=loan_id,
            recast_date=recast_date,
            new_principal_balance=new_principal_balance,
            fixed_instalment=inst,
            prepayment_amount=float(prepayment_amount),
        )
    return _build_recast_schedule(loan_id, recast_date, new_principal_balance)


def preview_loan_recast(
    loan_id: int,
    recast_date: date,
    new_principal_balance: float,
) -> dict[str, Any]:
    """
    Preview the proposed recast schedule (no DB changes).
    Returns dict with keys: schedule_df, new_installment.
    """
    df, new_installment = _build_recast_schedule(loan_id, recast_date, new_principal_balance)
    return {"schedule_df": df, "new_installment": new_installment}


def execute_loan_recast(
    loan_id: int,
    recast_date: date,
    new_principal_balance: float,
    *,
    trigger_repayment_id: int | None = None,
    notes: str | None = None,
) -> float:
    """
    Re-amortise the loan from recast_date to original maturity with new_principal_balance.
    Same loan type and rate; only instalment changes. Returns new instalment.
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    df, new_installment = _build_recast_schedule(loan_id, recast_date, new_principal_balance)
    prev_version = get_latest_schedule_version(loan_id)
    new_version = prev_version + 1

    save_new_schedule_version(loan_id, df, new_version)
    update_loan_details(loan_id, principal=round(new_principal_balance, 2), installment=round(new_installment, 2))
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_recasts (
                    loan_id, recast_date, previous_schedule_version, new_schedule_version,
                    new_installment, trigger_repayment_id, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (loan_id, recast_date, prev_version, new_version, round(new_installment, 2), trigger_repayment_id, notes),
            )
    return new_installment


def apply_loan_modification_from_approval_schedule(
    loan_id: int,
    restructure_date: date,
    schedule_df: pd.DataFrame,
    loan_details: dict[str, Any],
    new_loan_type: str,
    *,
    outstanding_interest_treatment: str = "capitalise",
    restructure_fee_amount: float = 0.0,
    notes: str | None = None,
) -> int:
    """
    Apply an approver-approved modification: new schedule version + loan header updates from capture-shaped details.
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    prev_version = get_latest_schedule_version(loan_id)
    new_version = prev_version + 1
    save_new_schedule_version(loan_id, schedule_df, new_version)

    term = int(loan_details.get("term") or 12)
    principal = float(as_10dp(loan_details.get("principal") or 0))
    disbursed = float(as_10dp(loan_details.get("disbursed_amount") if loan_details.get("disbursed_amount") is not None else principal))

    lt_db = new_loan_type
    if " " in lt_db:
        lt_db = {
            "Consumer Loan": "consumer_loan",
            "Term Loan": "term_loan",
            "Bullet Loan": "bullet_loan",
            "Customised Repayments": "customised_repayments",
        }.get(new_loan_type, new_loan_type.replace(" ", "_").lower())

    ud: dict[str, Any] = {
        "principal": round(principal, 2),
        "disbursed_amount": round(disbursed, 2),
        "term": term,
        "loan_type": lt_db,
    }
    if loan_details.get("annual_rate") is not None:
        ud["annual_rate"] = float(loan_details["annual_rate"])
    if loan_details.get("monthly_rate") is not None:
        ud["monthly_rate"] = float(loan_details["monthly_rate"])
    if loan_details.get("installment") is not None:
        ud["installment"] = round(float(loan_details["installment"]), 2)
    if loan_details.get("total_payment") is not None:
        ud["total_payment"] = float(as_10dp(loan_details["total_payment"]))
    if loan_details.get("end_date") is not None:
        ud["end_date"] = _date_conv(loan_details["end_date"])
    if loan_details.get("first_repayment_date") is not None:
        ud["first_repayment_date"] = _date_conv(loan_details["first_repayment_date"])
    if loan_details.get("product_code"):
        ud["product_code"] = str(loan_details["product_code"]).strip()
    if loan_details.get("bullet_type"):
        ud["bullet_type"] = loan_details["bullet_type"]
    if loan_details.get("grace_type"):
        ud["grace_type"] = loan_details["grace_type"]
    if loan_details.get("moratorium_months") is not None:
        ud["moratorium_months"] = int(loan_details["moratorium_months"])

    update_loan_details(loan_id, **ud)

    safe_upd: dict[str, Any] = {}
    cs = loan_details.get("collateral_security_subtype_id")
    if cs is not None and str(cs).strip() != "":
        try:
            safe_upd["collateral_security_subtype_id"] = int(cs)
        except (TypeError, ValueError):
            pass
    elif loan_details.get("collateral_cleared"):
        safe_upd["collateral_security_subtype_id"] = None
        safe_upd["collateral_charge_amount"] = None
        safe_upd["collateral_valuation_amount"] = None
    if loan_details.get("collateral_charge_amount") is not None and not loan_details.get("collateral_cleared"):
        safe_upd["collateral_charge_amount"] = float(as_10dp(loan_details["collateral_charge_amount"]))
    if loan_details.get("collateral_valuation_amount") is not None and not loan_details.get("collateral_cleared"):
        safe_upd["collateral_valuation_amount"] = float(as_10dp(loan_details["collateral_valuation_amount"]))
    if safe_upd:
        update_loan_safe_details(loan_id, safe_upd)

    ar_note = loan_details.get("annual_rate")
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_modifications (
                    loan_id, modification_date, previous_schedule_version, new_schedule_version,
                    outstanding_interest_treatment, new_loan_type, new_term, new_annual_rate,
                    new_principal, restructure_fee_amount, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    loan_id,
                    restructure_date,
                    prev_version,
                    new_version,
                    outstanding_interest_treatment,
                    lt_db,
                    term,
                    ar_note,
                    round(principal, 2),
                    float(as_10dp(restructure_fee_amount or 0.0)),
                    notes,
                ),
            )
    update_loan_restructure_flags(loan_id, remodified_in_place=True)
    return new_version


def list_unapplied_funds(loan_id: int | None = None, status: str = "pending") -> list[dict]:
    """List unapplied funds entries (ledger-style: credits and debits), optionally for one loan."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if loan_id is not None:
                cur.execute(
                    "SELECT * FROM unapplied_funds WHERE loan_id = %s ORDER BY value_date, id",
                    (loan_id,),
                )
            else:
                cur.execute(
                    "SELECT * FROM unapplied_funds ORDER BY loan_id, value_date, id",
                    (),
                )
            return [dict(r) for r in cur.fetchall()]
