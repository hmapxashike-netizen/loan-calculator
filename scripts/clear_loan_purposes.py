"""
Delete all rows from public.loan_purposes and clear loans.loan_purpose_id.

Use when the UI list is empty but inserts fail with duplicate name, or to reset purposes.

From project root:
    python scripts/clear_loan_purposes.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loan_management import clear_all_loan_purposes


def main() -> None:
    loans_n, pur_n = clear_all_loan_purposes()
    print(f"Cleared loan_purposes: deleted {pur_n} row(s).")
    print(f"Updated loans: cleared loan_purpose_id on {loans_n} row(s).")


if __name__ == "__main__":
    main()
