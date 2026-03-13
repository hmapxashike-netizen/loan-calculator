"""
Regenerate existing schedule_lines at 10dp using loans.py (which now uses _q10).
Corrects version 1 (original) schedules only. Rescheduled loans (version 2+) keep their
existing values; run migration 29 first so schedule_lines columns support 10dp.

From project root:
  python scripts/correct_existing_schedules.py [--dry-run] [--loan-id ID]
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from loans import (
    add_months,
    get_amortization_schedule,
    get_term_loan_amortization_schedule,
    get_bullet_schedule,
    repayment_dates,
    parse_schedule_dates_from_table,
    recompute_customised_from_payments,
)
from loan_management import (
    get_loan,
    get_schedule_lines,
    replace_schedule_lines,
    _connection,
)


def _to_dt(d):
    """Convert date/datetime to datetime."""
    if d is None:
        return None
    if hasattr(d, "date"):
        return datetime.combine(d.date() if hasattr(d, "date") else d, datetime.min.time())
    return datetime.combine(d, datetime.min.time())


def _annual_rate(loan: dict) -> float:
    """Return annual_rate as decimal (DB stores 0.12 for 12% pa, 1.20 for 120% pa)."""
    return float(loan.get("annual_rate") or 0)


def _build_schedule_v1(loan: dict) -> pd.DataFrame | None:
    """Build schedule for version 1 from loan params. Returns None if params missing."""
    loan_type = (loan.get("loan_type") or "").lower().replace(" ", "_")
    principal = float(loan.get("principal") or loan.get("disbursed_amount") or 0)
    term = int(loan.get("term") or 0)
    disb = _to_dt(loan.get("disbursement_date") or loan.get("start_date"))
    first_rep = _to_dt(loan.get("first_repayment_date"))
    end_date = _to_dt(loan.get("end_date"))
    metadata = loan.get("metadata") or {}
    if isinstance(metadata, str):
        import json
        metadata = json.loads(metadata) if metadata else {}
    use_anniversary = (loan.get("payment_timing") or "anniversary").lower().startswith("anniversary")
    flat_rate = metadata.get("flat_rate") is True or str(metadata.get("interest_method", "")).lower().startswith("flat")

    if not disb or principal <= 0 or term <= 0:
        return None

    if loan_type == "consumer_loan":
        monthly_rate = float(loan.get("monthly_rate") or 0)
        installment = float(loan.get("installment") or 0)
        if installment <= 0:
            import numpy_financial as npf
            installment = float(npf.pmt(monthly_rate or 0.01, term, -principal))
        schedule_dates = None
        if first_rep:
            schedule_dates = repayment_dates(disb, first_rep, term, use_anniversary)
        return get_amortization_schedule(
            total_facility=principal,
            monthly_rate=monthly_rate or 0.01,
            term=term,
            start_date=disb,
            installment=installment,
            flat_rate=flat_rate,
            schedule_dates=schedule_dates,
        )

    if loan_type == "term_loan":
        annual_rate = _annual_rate(loan)
        grace_type = (loan.get("grace_type") or "No grace period").lower()
        if "principal and interest" in grace_type:
            grace_key = "principal_and_interest"
        elif "principal moratorium" in grace_type or "principal" in grace_type:
            grace_key = "principal"
        else:
            grace_key = "none"
        moratorium = int(loan.get("moratorium_months") or 0)
        if not first_rep:
            first_rep = add_months(disb, 1)
        dates_list = repayment_dates(disb, first_rep, term, use_anniversary)
        df, _ = get_term_loan_amortization_schedule(
            total_facility=principal,
            annual_rate=annual_rate,
            disbursement_date=disb,
            repayment_dates_list=dates_list,
            grace_type=grace_key,
            moratorium_months=moratorium,
            flat_rate=flat_rate,
        )
        return df

    if loan_type == "bullet_loan":
        annual_rate = _annual_rate(loan)
        bullet_type = (loan.get("bullet_type") or "with_interest").lower()
        bt = "straight" if "straight" in bullet_type else "with_interest"
        maturity = end_date or add_months(disb, term)
        schedule_dates = None
        if first_rep and "with_interest" in bt:
            schedule_dates = repayment_dates(disb, first_rep, term, use_anniversary)
            if schedule_dates:
                maturity = schedule_dates[-1]
        return get_bullet_schedule(
            total_facility=principal,
            annual_rate=annual_rate,
            disbursement_date=disb,
            maturity_date=maturity,
            bullet_type=bt,
            repayment_dates_list=schedule_dates,
            flat_rate=flat_rate,
        )

    if loan_type == "customised_repayments":
        lines = get_schedule_lines(loan["id"], schedule_version=1)
        if not lines:
            return None
        template = pd.DataFrame(lines)
        if "payment" in template.columns and "Payment" not in template.columns:
            template["Payment"] = template["payment"]
        if "Date" not in template.columns and "date" in template.columns:
            template["Date"] = template["date"]
        dates_list = parse_schedule_dates_from_table(template, start_date=disb)
        annual_rate = _annual_rate(loan)
        return recompute_customised_from_payments(
            template_df=template,
            total_facility=principal,
            repayment_dates_list=dates_list,
            annual_rate=annual_rate,
            flat_rate=flat_rate,
            start_date=disb,
        )

    return None


def main():
    parser = argparse.ArgumentParser(description="Correct existing schedules to 10dp")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done, do not write")
    parser.add_argument("--loan-id", type=int, help="Correct only this loan (default: all)")
    args = parser.parse_args()

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ls.id AS schedule_id, ls.loan_id, ls.version
                FROM loan_schedules ls
                WHERE ls.version = 1
                ORDER BY ls.loan_id
                """
            )
            rows = cur.fetchall()
            if args.loan_id:
                rows = [r for r in rows if r[1] == args.loan_id]

    ok = 0
    skip = 0
    err = 0
    for schedule_id, loan_id, version in rows:
        loan = get_loan(loan_id)
        if not loan:
            print(f"  Loan {loan_id} not found, skip")
            skip += 1
            continue
        df = _build_schedule_v1(loan)
        if df is None:
            print(f"  Loan {loan_id} ({loan.get('loan_type', '?')}): cannot build schedule, skip")
            skip += 1
            continue
        if args.dry_run:
            print(f"  Loan {loan_id}: would replace {len(df)} lines (dry-run)")
            ok += 1
            continue
        try:
            replace_schedule_lines(schedule_id, df)
            print(f"  Loan {loan_id}: replaced {len(df)} lines")
            ok += 1
        except Exception as e:
            print(f"  Loan {loan_id}: error {e}")
            err += 1

    print(f"\nDone: {ok} corrected, {skip} skipped, {err} errors")


if __name__ == "__main__":
    main()
