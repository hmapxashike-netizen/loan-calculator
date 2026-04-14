"""
Generate test loan + schedule CSVs for batch import.

Schedule **Date** values are written as **YYYY-MM-DD** via ``schedule_date_to_iso_for_exchange`` (never
``str(...)[:10]`` on ``dd-Mon-yyyy`` — that truncated years). Ensure DB migration **76** has widened
``schedule_lines.\"Date\"`` so the app can persist full canonical dates.

Defaults: 3000 loans referencing CUST-0001..CUST-1000, disbursement dates between 2024-07-10 and
2024-09-10 (inclusive), penalty_rate_pct 10, all four loan types (Consumer / Term / Bullet /
Customised — customised schedules reuse one of the three amortisation engines for variety).
"""
import csv
import os
import random
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

import numpy_financial as npf
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from loans import (
    add_months,
    get_amortization_schedule,
    get_bullet_schedule,
    get_term_loan_amortization_schedule,
    repayment_dates,
)
from loan_management.schedules import schedule_date_to_iso_for_exchange

# products.loan_type (DB) -> capture / batch CSV loan_type label
_DB_LOAN_TYPE_TO_DISPLAY = {
    "consumer_loan": "Consumer Loan",
    "term_loan": "Term Loan",
    "bullet_loan": "Bullet Loan",
    "customised_repayments": "Customised Repayments",
}

DISB_START = date(2024, 7, 10)
DISB_END = date(2024, 9, 10)
DEFAULT_PENALTY_RATE_PCT = 10.0


def _product_codes_by_display_loan_type() -> dict[str, list[str]]:
    """Active product codes grouped by UI loan type (Consumer / Term / Bullet / Customised)."""
    try:
        from loan_management.product_catalog import list_products
    except Exception:
        return {}

    out: dict[str, list[str]] = defaultdict(list)
    for p in list_products(active_only=True) or []:
        code = (p.get("code") or "").strip()
        if not code:
            continue
        lt = (p.get("loan_type") or "").strip().lower()
        disp = _DB_LOAN_TYPE_TO_DISPLAY.get(lt)
        if disp:
            out[disp].append(code)
    return dict(out)


def _pick_product_code(codes_by_type: dict[str, list[str]], display_loan_type: str) -> str:
    codes = codes_by_type.get(display_loan_type) or []
    if codes:
        return random.choice(codes)
    return ""


def _random_disbursement_datetime() -> datetime:
    span_days = (DISB_END - DISB_START).days
    d = DISB_START + timedelta(days=random.randint(0, span_days))
    return datetime(d.year, d.month, d.day)


def format_2dp(val):
    return f"{float(val):.2f}"


def generate_loans(
    total_loans: int,
    num_customers: int,
    loans_file: str,
    schedule_file: str,
    *,
    penalty_rate_pct: float = DEFAULT_PENALTY_RATE_PCT,
) -> None:
    with open(loans_file, mode="w", newline="", encoding="utf-8") as fl, open(
        schedule_file, mode="w", newline="", encoding="utf-8"
    ) as fs:
        l_writer = csv.writer(fl)
        s_writer = csv.writer(fs)

        l_writer.writerow(
            [
                "import_key",
                "customer_ref",
                "customer_display_name",
                "customer_id",
                "loan_type",
                "product_code",
                "principal",
                "disbursed_amount",
                "term",
                "annual_rate",
                "monthly_rate",
                "drawdown_fee_amount",
                "arrangement_fee_amount",
                "admin_fee_amount",
                "drawdown_fee",
                "arrangement_fee",
                "admin_fee",
                "disbursement_date",
                "first_repayment_date",
                "end_date",
                "installment",
                "total_payment",
                "payment_timing",
                "penalty_rate_pct",
                "cash_gl_account_id",
                "loan_purpose_id",
                "agent_id",
                "relationship_manager_id",
            ]
        )
        s_writer.writerow(
            [
                "import_key",
                "Period",
                "Date",
                "Payment",
                "Principal",
                "Interest",
                "Principal Balance",
                "Total Outstanding",
            ]
        )

        codes_by_type = _product_codes_by_display_loan_type()
        warned_missing: set[str] = set()

        for loan_id_counter in range(1, total_loans + 1):
            cust_ref = f"CUST-{random.randint(1, num_customers):04d}"
            import_key = f"MIG-{loan_id_counter:04d}"

            base_disbursement_date = _random_disbursement_datetime()
            use_anniversary = random.choice([True, False])
            timing_str = "anniversary" if use_anniversary else "last_day_of_month"

            kind_roll = random.randint(0, 3)
            if kind_roll == 3:
                inner = random.choice([0, 1, 2])
                lt_label = "Customised Repayments"
            else:
                inner = kind_roll
                lt_label = ("Consumer Loan", "Term Loan", "Bullet Loan")[inner]

            pcode = _pick_product_code(codes_by_type, lt_label)
            if not pcode and lt_label not in warned_missing:
                print(
                    f"Note: No active DB product for {lt_label!r}; rows will have empty product_code until configured."
                )
                warned_missing.add(lt_label)

            penalty_cell = format_2dp(penalty_rate_pct)

            if inner == 0:
                term = random.randint(3, 36)
                loan_required = float(random.randint(1000, 10000))
                admin_fee = random.choice([0.0, 0.02, 0.05])
                base_rate = random.choice([0.03, 0.05, 0.08])

                total_facility = loan_required / (1.0 - admin_fee)
                disbursed_amount = loan_required
                admin_fee_amount = total_facility - disbursed_amount

                monthly_installment = float(npf.pmt(base_rate, term, -total_facility))

                first_rep = add_months(base_disbursement_date, 1)
                sched_dates = repayment_dates(base_disbursement_date, first_rep, term, use_anniversary)
                end_date = sched_dates[-1] if sched_dates else add_months(base_disbursement_date, term)

                df_sched = get_amortization_schedule(
                    total_facility,
                    base_rate,
                    term,
                    base_disbursement_date,
                    monthly_installment,
                    flat_rate=False,
                    schedule_dates=sched_dates,
                )

                l_writer.writerow(
                    [
                        import_key,
                        cust_ref,
                        "",
                        "",
                        lt_label,
                        pcode,
                        format_2dp(total_facility),
                        format_2dp(disbursed_amount),
                        term,
                        "",
                        format_2dp(base_rate),
                        "",
                        "",
                        format_2dp(admin_fee_amount),
                        "",
                        "",
                        format_2dp(admin_fee),
                        base_disbursement_date.date().isoformat(),
                        first_rep.date().isoformat(),
                        end_date.date().isoformat(),
                        format_2dp(monthly_installment),
                        format_2dp(monthly_installment * term),
                        timing_str,
                        penalty_cell,
                        "",
                        "",
                        "",
                        "",
                    ]
                )

            elif inner == 1:
                term = random.randint(6, 60)
                loan_required = float(random.randint(5000, 50000))
                dd_fee = random.choice([0.0, 0.01, 0.02])
                arr_fee = random.choice([0.0, 0.01, 0.02])
                annual_rate = random.choice([0.15, 0.20, 0.25, 0.35])
                total_fee = dd_fee + arr_fee

                total_facility = loan_required / (1.0 - total_fee)
                disbursed_amount = loan_required
                dd_fee_amt = total_facility * dd_fee
                arr_fee_amt = total_facility * arr_fee

                first_rep = add_months(base_disbursement_date, 1)
                sched_dates = repayment_dates(base_disbursement_date, first_rep, term, use_anniversary)
                end_date = sched_dates[-1] if sched_dates else add_months(base_disbursement_date, term)

                df_sched, installment = get_term_loan_amortization_schedule(
                    total_facility,
                    annual_rate,
                    base_disbursement_date,
                    sched_dates,
                    "none",
                    0,
                    flat_rate=False,
                )

                l_writer.writerow(
                    [
                        import_key,
                        cust_ref,
                        "",
                        "",
                        lt_label,
                        pcode,
                        format_2dp(total_facility),
                        format_2dp(disbursed_amount),
                        term,
                        format_2dp(annual_rate * 100),
                        "",
                        format_2dp(dd_fee_amt),
                        format_2dp(arr_fee_amt),
                        "",
                        format_2dp(dd_fee),
                        format_2dp(arr_fee),
                        "",
                        base_disbursement_date.date().isoformat(),
                        first_rep.date().isoformat(),
                        end_date.date().isoformat(),
                        format_2dp(installment),
                        format_2dp(installment * term),
                        timing_str,
                        penalty_cell,
                        "",
                        "",
                        "",
                        "",
                    ]
                )

            else:
                term = random.randint(1, 12)
                loan_required = float(random.randint(2000, 20000))
                dd_fee = random.choice([0.0, 0.01, 0.02])
                arr_fee = random.choice([0.0, 0.01, 0.02])
                annual_rate = random.choice([0.15, 0.20, 0.30])
                total_fee = dd_fee + arr_fee

                total_facility = loan_required / (1.0 - total_fee)
                disbursed_amount = loan_required
                dd_fee_amt = total_facility * dd_fee
                arr_fee_amt = total_facility * arr_fee

                b_type = random.choice(["straight", "with_interest"])
                first_rep = add_months(base_disbursement_date, 1)
                sched_dates = repayment_dates(base_disbursement_date, first_rep, term, use_anniversary)
                end_date = sched_dates[-1] if sched_dates else add_months(base_disbursement_date, term)

                df_sched = get_bullet_schedule(
                    total_facility,
                    annual_rate,
                    base_disbursement_date,
                    end_date,
                    b_type,
                    sched_dates,
                    flat_rate=False,
                )
                total_payment = float(df_sched["Payment"].sum())

                l_writer.writerow(
                    [
                        import_key,
                        cust_ref,
                        "",
                        "",
                        lt_label,
                        pcode,
                        format_2dp(total_facility),
                        format_2dp(disbursed_amount),
                        term,
                        format_2dp(annual_rate * 100),
                        "",
                        format_2dp(dd_fee_amt),
                        format_2dp(arr_fee_amt),
                        "",
                        format_2dp(dd_fee),
                        format_2dp(arr_fee),
                        "",
                        base_disbursement_date.date().isoformat(),
                        first_rep.date().isoformat(),
                        end_date.date().isoformat(),
                        "",
                        format_2dp(total_payment),
                        timing_str,
                        penalty_cell,
                        "",
                        "",
                        "",
                        "",
                    ]
                )

            for _, s_row in df_sched.iterrows():
                period = int(s_row.get("Period", 0))
                dt = schedule_date_to_iso_for_exchange(s_row.get("Date"))
                pay_raw = s_row.get("Payment")
                if pay_raw is None or pd.isna(pay_raw):
                    pay_raw = s_row.get("Monthly Installment", 0)
                payment = float(pay_raw or 0)
                prin = float(s_row.get("Principal", 0))
                inter = float(s_row.get("Interest", 0))
                p_bal = float(s_row.get("Principal Balance", 0))
                t_out = float(s_row.get("Total Outstanding", 0))

                s_writer.writerow(
                    [
                        import_key,
                        period,
                        dt,
                        format_2dp(payment),
                        format_2dp(prin),
                        format_2dp(inter),
                        format_2dp(p_bal),
                        format_2dp(t_out),
                    ]
                )

    print(f"Successfully generated {total_loans} loans in {loans_file} and schedules in {schedule_file}")


if __name__ == "__main__":
    generate_loans(3000, 1000, "test_loans.csv", "test_schedules.csv")
