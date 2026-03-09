"""
Repair repayment 17 for loan 10 (2025-10-28).

The receipt was incorrectly allocated to arrears when there were none.
This script reallocates using yesterday's state (27/10) so the full 788.33
goes to unapplied funds.

Run from project root:
  python scripts/repair_receipt_17.py
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    from loan_management import (
        reallocate_repayment,
        get_loan_daily_state_balances,
        get_unapplied_balance,
    )
    from datetime import date

    repayment_id = 17
    eff_date = date(2025, 10, 28)
    loan_id = 10

    print(f"Repairing repayment {repayment_id} (loan_id={loan_id}, eff_date={eff_date})")
    print("Before:")
    before = get_loan_daily_state_balances(loan_id, eff_date)
    if before:
        print(f"  interest_arrears_balance={before.get('interest_arrears_balance')}")
        print(f"  principal_arrears={before.get('principal_arrears')}")
    unapplied_before = get_unapplied_balance(loan_id, eff_date)
    print(f"  unapplied={unapplied_before}")

    print("Running reallocate_repayment(17, use_yesterday_state=True)...")
    reallocate_repayment(repayment_id, use_yesterday_state=True)
    print("Done.")

    print("After:")
    after = get_loan_daily_state_balances(loan_id, eff_date)
    if after:
        print(f"  interest_arrears_balance={after.get('interest_arrears_balance')}")
        print(f"  principal_arrears={after.get('principal_arrears')}")
    unapplied_after = get_unapplied_balance(loan_id, eff_date)
    print(f"  unapplied={unapplied_after}")

    if before and after:
        ia_b = before.get("interest_arrears_balance") or 0
        ia_a = after.get("interest_arrears_balance") or 0
        if ia_a == 0 and unapplied_after > 700:
            print("OK: Repair completed. Full amount in unapplied, no arrears.")
        else:
            print(f"Check: interest_arrears {ia_b} -> {ia_a}, unapplied {unapplied_before} -> {unapplied_after}")


if __name__ == "__main__":
    main()
