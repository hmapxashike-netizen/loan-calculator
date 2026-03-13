import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loan_management import get_repayment_ids_for_loan_and_date, reallocate_repayment
from eod import run_eod_for_date, load_system_config_from_db

def fix_loan_15():
    sys_cfg = load_system_config_from_db()
    start_date = date(2025, 10, 21)
    end_date = date(2025, 11, 4)

    curr = start_date
    while curr <= end_date:
        print(f"Fixing date {curr}...")
        
        # 1. Run full EOD for this day (fixes accruals and unapplied funds applications)
        run_eod_for_date(curr, skip_reallocate_after_reversals=True)
        
        # 2. Reallocate any receipts on this day (uses the fixed logic)
        rids = get_repayment_ids_for_loan_and_date(15, curr)
        for rid in rids:
            print(f"  Reallocating receipt {rid}")
            reallocate_repayment(rid, system_config=sys_cfg)
            
        curr += timedelta(days=1)
        
    print("Done fixing loan 15.")

if __name__ == "__main__":
    fix_loan_15()

