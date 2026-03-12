"""Check Oct 19-21 penalty/default daily states to find where the 1.78/3.15 extra comes from."""
from datetime import date, timedelta
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from loan_management import get_loan_daily_state_balances

loan_id = 10

print(f"{'date':<12} {'pen_bal':>10} {'pen_daily':>12} {'def_bal':>10} {'def_daily':>12} {'int_arr':>12} {'prin_arr':>12}")
for day in range(19, 24):
    d = date(2025, 10, day)
    s = get_loan_daily_state_balances(loan_id, d)
    if s:
        print(f"  {d}  {float(s.get('penalty_interest_balance') or 0):>10.4f}  "
              f"{float(s.get('penalty_interest_daily') or 0):>12.4f}  "
              f"{float(s.get('default_interest_balance') or 0):>10.4f}  "
              f"{float(s.get('default_interest_daily') or 0):>12.4f}  "
              f"{float(s.get('interest_arrears_balance') or 0):>12.4f}  "
              f"{float(s.get('principal_arrears') or 0):>12.4f}")
    else:
        print(f"  {d}  NO STATE")

# Also: for Oct 21 specifically, what was the balance EOD would have computed BEFORE the allocation?
# EOD computes: balance = yesterday_saved + daily_save - alloc(0 at that point)
s20 = get_loan_daily_state_balances(loan_id, date(2025, 10, 20))
s21 = get_loan_daily_state_balances(loan_id, date(2025, 10, 21))
if s20 and s21:
    pen20 = float(s20.get('penalty_interest_balance') or 0)
    def20 = float(s20.get('default_interest_balance') or 0)
    pen_daily21 = float(s21.get('penalty_interest_daily') or 0)
    def_daily21 = float(s21.get('default_interest_daily') or 0)
    print(f"\nOct 21 pre-alloc balance estimate (Oct20 + Oct21 daily):")
    print(f"  penalty: {pen20:.4f} + {pen_daily21:.4f} = {pen20+pen_daily21:.4f}  (alloc was 59.66)")
    print(f"  default: {def20:.4f} + {def_daily21:.4f} = {def20+def_daily21:.4f}  (alloc was 110.55)")
    print(f"  Over-alloc penalty: {59.66 - (pen20+pen_daily21):.4f}  (expect 0 if engine correct)")
    print(f"  Over-alloc default: {110.55 - (def20+def_daily21):.4f}  (expect 0 if engine correct)")
