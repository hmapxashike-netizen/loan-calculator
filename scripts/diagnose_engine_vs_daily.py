"""
Check what get_engine_state_for_loan_date returns for key dates
vs what's saved in loan_daily_state.
"""
from datetime import date
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from eod import get_engine_state_for_loan_date
from loan_management import get_loan_daily_state_balances

loan_id = 10
dates = [date(2025, 10, 21), date(2025, 10, 22), date(2025, 10, 28)]

for d in dates:
    eng = get_engine_state_for_loan_date(loan_id, d)
    saved = get_loan_daily_state_balances(loan_id, d)
    print(f"\n=== {d} ===")
    print(f"  ENGINE state (accrual-only, no allocations):")
    if eng:
        for k in ['principal_arrears','interest_arrears_balance','penalty_interest_balance','default_interest_balance',
                  'penalty_interest_daily','default_interest_daily','regular_interest_daily','days_overdue']:
            print(f"    {k}: {eng.get(k)}")
    print(f"  SAVED daily_state:")
    if saved:
        for k in ['principal_arrears','interest_arrears_balance','penalty_interest_balance','default_interest_balance',
                  'penalty_interest_daily','default_interest_daily','regular_interest_daily','days_overdue']:
            print(f"    {k}: {saved.get(k)}")
    # What would pen_daily be computed as in allocate_repayment_waterfall?
    if eng and saved:
        eng_pen = float(eng.get("penalty_interest_daily", 0) or 0)
        eng_prin_arr = float(eng.get("principal_arrears", 0) or 0)
        new_principal_arrears = float(saved.get("principal_arrears", 0) or 0)
        new_interest_arrears = float(saved.get("interest_arrears_balance", 0) or 0)
        eng_int_arr = float(eng.get("interest_arrears_balance", 0) or 0)
        eng_def = float(eng.get("default_interest_daily", 0) or 0)
        pen_daily_calc = (new_principal_arrears * eng_pen / eng_prin_arr) if eng_prin_arr > 1e-6 else 0.0
        def_daily_calc = (new_interest_arrears * eng_def / eng_int_arr) if eng_int_arr > 1e-6 else 0.0
        days_ov = int(saved.get("days_overdue") or 0)
        arrears_total = float((saved.get("interest_arrears_balance") or 0)) + float((saved.get("default_interest_balance") or 0)) + float((saved.get("penalty_interest_balance") or 0)) + float((saved.get("principal_arrears") or 0))
        print(f"  WATERFALL would compute:")
        print(f"    arrears_total={arrears_total:.4f}  days_overdue={days_ov}")
        print(f"    pen_daily_if_ratio={pen_daily_calc:.6f}  (eng_pen={eng_pen:.4f}, eng_prin_arr={eng_prin_arr:.4f})")
        print(f"    def_daily_if_ratio={def_daily_calc:.6f}  (eng_def={eng_def:.4f}, eng_int_arr={eng_int_arr:.4f})")
        print(f"    condition 'zero_daily': {arrears_total <= 1e-6 or days_ov <= 0}")
