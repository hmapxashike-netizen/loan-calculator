"""
Build creditor amortisation schedules using the same engines as debtor loans (loans.py).

``creditor_loan_types.behavior_json`` must include ``debtor_schedule_engine``:
``term_actual_360`` | ``consumer_30_360`` | ``bullet_actual_360`` | ``customised_actual_360``.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd

from loans import (
    add_months,
    consumer_level_payment,
    days_in_month,
    get_amortization_schedule,
    get_bullet_schedule,
    get_term_loan_amortization_schedule,
    parse_schedule_dates_from_table,
    repayment_dates,
    recompute_customised_from_payments,
)


def behavior_json_as_dict(behavior_json: Any) -> dict[str, Any]:
    if behavior_json is None:
        return {}
    if isinstance(behavior_json, dict):
        return dict(behavior_json)
    if isinstance(behavior_json, str):
        try:
            return json.loads(behavior_json)
        except json.JSONDecodeError:
            return {}
    return {}


def debtor_schedule_engine(behavior_json: Any) -> str:
    d = behavior_json_as_dict(behavior_json)
    eng = (d.get("debtor_schedule_engine") or "").strip().lower()
    if eng in ("term_actual_360", "consumer_30_360", "bullet_actual_360", "customised_actual_360"):
        return eng
    return "term_actual_360"


def build_creditor_schedule_dataframe(
    *,
    behavior_json: Any,
    principal: float,
    term_months: int,
    disbursement_date: datetime,
    rate_pct: float,
    rate_basis: str,
    flat_rate: bool,
    use_anniversary: bool,
    first_repayment_date: datetime | None,
    consumer_monthly_rate: float | None,
    bullet_type_label: str,
) -> pd.DataFrame:
    """
    ``rate_pct``: headline percent as in loan calculators (annual % if rate_basis Per annum,
    else per-month %). ``consumer_monthly_rate``: decimal monthly rate for consumer engine only.
    """
    engine = debtor_schedule_engine(behavior_json)
    if principal <= 0 or term_months <= 0:
        raise ValueError("principal and term must be positive.")

    if engine == "consumer_30_360":
        if consumer_monthly_rate is None or consumer_monthly_rate <= 0:
            raise ValueError("Consumer (30/360) requires a positive monthly rate (decimal).")
        first_rep = first_repayment_date or add_months(disbursement_date, 1)
        if not use_anniversary:
            first_rep = first_rep.replace(day=days_in_month(first_rep.year, first_rep.month))
        schedule_dates = repayment_dates(
            disbursement_date, first_rep, int(term_months), use_anniversary
        )
        inst = consumer_level_payment(float(principal), float(consumer_monthly_rate), int(term_months))
        return get_amortization_schedule(
            float(principal),
            float(consumer_monthly_rate),
            int(term_months),
            disbursement_date,
            inst,
            flat_rate=flat_rate,
            schedule_dates=schedule_dates,
        )

    if engine == "bullet_actual_360":
        annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
        end_date = add_months(disbursement_date, int(term_months))
        schedule_dates = None
        if "with" in bullet_type_label.lower() or "interest" in bullet_type_label.lower():
            first_rep = first_repayment_date or add_months(disbursement_date, 1)
            if not use_anniversary:
                first_rep = first_rep.replace(day=days_in_month(first_rep.year, first_rep.month))
            schedule_dates = repayment_dates(
                disbursement_date, first_rep, int(term_months), use_anniversary
            )
            end_date = schedule_dates[-1] if schedule_dates else end_date
        bullet_key = "straight" if "straight" in bullet_type_label.lower() else "with_interest"
        return get_bullet_schedule(
            float(principal),
            float(annual_rate),
            disbursement_date,
            end_date,
            bullet_key,
            schedule_dates,
            flat_rate=flat_rate,
        )

    if engine == "customised_actual_360":
        return initial_customised_creditor_schedule(
            principal=float(principal),
            term_months=int(term_months),
            disbursement_date=disbursement_date,
            rate_pct=float(rate_pct),
            rate_basis=str(rate_basis),
            flat_rate=flat_rate,
            use_anniversary=use_anniversary,
            first_repayment_date=first_repayment_date,
        )

    # term_actual_360 (default)
    first_rep = first_repayment_date or add_months(disbursement_date, 1)
    if not use_anniversary:
        first_rep = first_rep.replace(day=days_in_month(first_rep.year, first_rep.month))
    schedule_dates = repayment_dates(
        disbursement_date, first_rep, int(term_months), use_anniversary
    )
    annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
    sch, _inst = get_term_loan_amortization_schedule(
        float(principal),
        float(annual_rate),
        disbursement_date,
        schedule_dates,
        "none",
        0,
        flat_rate=flat_rate,
    )
    return sch


def initial_customised_creditor_schedule(
    *,
    principal: float,
    term_months: int,
    disbursement_date: datetime,
    rate_pct: float,
    rate_basis: str,
    flat_rate: bool,
    use_anniversary: bool,
    first_repayment_date: datetime | None,
) -> pd.DataFrame:
    """Zero-payment template; interest recomputed from Actual/360 on outstanding (see recompute_customised_from_payments)."""
    first_rep = first_repayment_date or add_months(disbursement_date, 1)
    if not use_anniversary:
        first_rep = first_rep.replace(day=days_in_month(first_rep.year, first_rep.month))
    schedule_dates_init = repayment_dates(
        disbursement_date, first_rep, int(term_months), use_anniversary
    )
    rows: list[dict[str, Any]] = [
        {
            "Period": 0,
            "Date": disbursement_date.strftime("%d-%b-%Y"),
            "Payment": 0.0,
            "Interest": 0.0,
            "Principal": 0.0,
            "Principal Balance": float(principal),
            "Total Outstanding": float(principal),
        }
    ]
    for i, dt in enumerate(schedule_dates_init, 1):
        rows.append(
            {
                "Period": i,
                "Date": dt.strftime("%d-%b-%Y"),
                "Payment": 0.0,
                "Interest": 0.0,
                "Principal": 0.0,
                "Principal Balance": 0.0,
                "Total Outstanding": 0.0,
            }
        )
    df = pd.DataFrame(rows)
    annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
    rd = parse_schedule_dates_from_table(df, start_date=disbursement_date)
    return recompute_customised_from_payments(
        df, float(principal), rd, float(annual_rate), flat_rate, disbursement_date
    )


def recompute_creditor_customised_from_editor(
    df: pd.DataFrame,
    *,
    principal: float,
    disbursement_date: datetime,
    rate_pct: float,
    rate_basis: str,
    flat_rate: bool,
) -> pd.DataFrame:
    annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
    rd = parse_schedule_dates_from_table(df, start_date=disbursement_date)
    return recompute_customised_from_payments(
        df.copy(), float(principal), rd, float(annual_rate), flat_rate, disbursement_date
    )
