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
    get_amortization_schedule,
    get_term_loan_amortization_schedule,
    get_bullet_schedule,
    repayment_dates,
)
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


def get_loan_for_modification(loan_id: int, as_of: date | None = None) -> dict | None:
    """
    Return loan plus current schedule and balance info for modification UI.
    Keys: loan, schedule_version, schedule_lines, balances, last_due_date.
    """
    if as_of is None:
        as_of = date.today()
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
    last_due = _last_due_date_from_loan(loan)
    if not last_due:
        raise ValueError(f"Cannot determine maturity for loan {loan_id}.")
    if recast_date > last_due:
        raise ValueError("Recast date cannot be after the last due date.")
    if new_principal_balance <= 0:
        raise ValueError("New principal balance must be positive.")

    start_dt = datetime.combine(recast_date, datetime.min.time())
    end_dt = datetime.combine(last_due, datetime.min.time())
    remaining_months = max(1, (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month))
    annual_rate = float(loan.get("annual_rate") or 0)
    if annual_rate > 1:
        annual_rate = annual_rate / 100.0
    first_repayment_date = add_months(start_dt, 1)
    dates_list = repayment_dates(start_dt, first_repayment_date, remaining_months, use_anniversary=True)
    df, new_installment = get_term_loan_amortization_schedule(
        total_facility=new_principal_balance,
        annual_rate=annual_rate,
        disbursement_date=start_dt,
        repayment_dates_list=dates_list,
        grace_type="none",
        moratorium_months=0,
        flat_rate=False,
    )
    return df, new_installment


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
