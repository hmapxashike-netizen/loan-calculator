"""
Generate test loan + schedule CSVs for batch import.

Schedule **Date** values are written as **YYYY-MM-DD** via ``schedule_date_to_iso_for_exchange`` (never
``str(...)[:10]`` on ``dd-Mon-yyyy`` — that truncated years). Ensure DB migration **76** has widened
``schedule_lines.\"Date\"`` so the app can persist full canonical dates.
"""
import sys
import os
import csv
import random
from collections import defaultdict
from datetime import datetime

import numpy_financial as npf
import pandas as pd

# Add parent directory to path to import loan math
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from loans import (
    repayment_dates, get_amortization_schedule, get_term_loan_amortization_schedule,
    get_bullet_schedule, add_months,
)
from loan_management.schedules import schedule_date_to_iso_for_exchange

# products.loan_type (DB) -> capture / batch CSV loan_type label
_DB_LOAN_TYPE_TO_DISPLAY = {
    "consumer_loan": "Consumer Loan",
    "term_loan": "Term Loan",
    "bullet_loan": "Bullet Loan",
    "customised_repayments": "Customised Repayments",
}


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


def format_2dp(val):
    return f"{float(val):.2f}"

def generate_loans(num_clients, max_loans_per_client, loans_file, schedule_file):
    with open(loans_file, mode='w', newline='', encoding='utf-8') as fl, \
         open(schedule_file, mode='w', newline='', encoding='utf-8') as fs:
        
        l_writer = csv.writer(fl)
        s_writer = csv.writer(fs)
        
        # Write headers
        l_writer.writerow([
            "import_key", "customer_ref", "customer_display_name", "customer_id", "loan_type", "product_code", "principal", "disbursed_amount",
            "term", "annual_rate", "monthly_rate", "drawdown_fee_amount", "arrangement_fee_amount",
            "admin_fee_amount", "drawdown_fee", "arrangement_fee", "admin_fee", "disbursement_date",
            "first_repayment_date", "end_date", "installment", "total_payment", "payment_timing",
            "cash_gl_account_id", "loan_purpose_id", "agent_id", "relationship_manager_id"
        ])
        # Date column: always YYYY-MM-DD (never str(...)[:10] on dd-Mon-yyyy — that truncated the year).
        s_writer.writerow([
            "import_key", "Period", "Date", "Payment", "Principal", "Interest",
            "Principal Balance", "Total Outstanding"
        ])

        codes_by_type = _product_codes_by_display_loan_type()
        warned_missing: set[str] = set()

        loan_id_counter = 1
        for cid in range(1, num_clients + 1):
            cust_ref = f"CUST-{cid:04d}"
            num_loans = random.randint(1, max_loans_per_client)
            for _ in range(num_loans):
                loan_type_idx = random.randint(0, 2)
                import_key = f"MIG-{loan_id_counter:04d}"
                loan_id_counter += 1
                
                # Base parameters
                base_disbursement_date = datetime(2024, random.randint(1, 11), random.randint(1, 28))
                use_anniversary = random.choice([True, False])
                timing_str = "anniversary" if use_anniversary else "last_day_of_month"
                
                if loan_type_idx == 0:
                    # Consumer Loan
                    lt_label = "Consumer Loan"
                    pcode = _pick_product_code(codes_by_type, lt_label)
                    if not pcode and lt_label not in warned_missing:
                        print(
                            f"Note: No active DB product with loan_type consumer_loan; "
                            f"{lt_label} rows will have empty product_code until you add one."
                        )
                        warned_missing.add(lt_label)
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
                        total_facility, base_rate, term, base_disbursement_date, monthly_installment,
                        flat_rate=False, schedule_dates=sched_dates
                    )
                    
                    l_writer.writerow([
                        import_key, cust_ref, "", "", lt_label, pcode, format_2dp(total_facility), format_2dp(disbursed_amount),
                        term, "", format_2dp(base_rate), "", "", format_2dp(admin_fee_amount),
                        "", "", format_2dp(admin_fee), base_disbursement_date.isoformat(),
                        first_rep.isoformat(), end_date.isoformat(), format_2dp(monthly_installment),
                        format_2dp(monthly_installment * term), timing_str,
                        "", "", "", ""
                    ])
                    
                elif loan_type_idx == 1:
                    # Term Loan
                    lt_label = "Term Loan"
                    pcode = _pick_product_code(codes_by_type, lt_label)
                    if not pcode and lt_label not in warned_missing:
                        print(
                            f"Note: No active DB product with loan_type term_loan; "
                            f"{lt_label} rows will have empty product_code until you add one."
                        )
                        warned_missing.add(lt_label)
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
                        total_facility, annual_rate, base_disbursement_date, sched_dates,
                        "none", 0, flat_rate=False
                    )
                    
                    l_writer.writerow([
                        import_key, cust_ref, "", "", lt_label, pcode, format_2dp(total_facility), format_2dp(disbursed_amount),
                        term, format_2dp(annual_rate*100), "", format_2dp(dd_fee_amt), format_2dp(arr_fee_amt), "",
                        format_2dp(dd_fee), format_2dp(arr_fee), "", base_disbursement_date.isoformat(),
                        first_rep.isoformat(), end_date.isoformat(), format_2dp(installment),
                        format_2dp(installment * term), timing_str,
                        "", "", "", ""
                    ])
                    
                else:
                    # Bullet Loan
                    lt_label = "Bullet Loan"
                    pcode = _pick_product_code(codes_by_type, lt_label)
                    if not pcode and lt_label not in warned_missing:
                        print(
                            f"Note: No active DB product with loan_type bullet_loan; "
                            f"{lt_label} rows will have empty product_code until you add one."
                        )
                        warned_missing.add(lt_label)
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
                        total_facility, annual_rate, base_disbursement_date, end_date,
                        b_type, sched_dates, flat_rate=False
                    )
                    total_payment = df_sched["Payment"].sum()
                    
                    l_writer.writerow([
                        import_key, cust_ref, "", "", lt_label, pcode, format_2dp(total_facility), format_2dp(disbursed_amount),
                        term, format_2dp(annual_rate*100), "", format_2dp(dd_fee_amt), format_2dp(arr_fee_amt), "",
                        format_2dp(dd_fee), format_2dp(arr_fee), "", base_disbursement_date.isoformat(),
                        first_rep.isoformat(), end_date.isoformat(), "",
                        format_2dp(total_payment), timing_str,
                        "", "", "", ""
                    ])

                # Write schedule lines
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
                    
                    s_writer.writerow([
                        import_key, period, dt, format_2dp(payment), format_2dp(prin),
                        format_2dp(inter), format_2dp(p_bal), format_2dp(t_out)
                    ])

    print(f"Successfully generated {loan_id_counter - 1} loans in {loans_file} and schedules in {schedule_file}")

if __name__ == "__main__":
    generate_loans(100, 10, "test_loans.csv", "test_schedules.csv")
