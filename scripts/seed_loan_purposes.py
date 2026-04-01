"""
Idempotent seed for table ``loan_purposes`` (same data source as System configurations → Loan purposes).

- Creates nothing if migration 62 has not been applied (``ensure_loan_purpose_rows`` ensures schema).
- Skips any purpose whose name already exists (case-insensitive).

Edit default names in ``loan_purpose_seed.py``, then run from project root:

    python scripts/seed_loan_purposes.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loan_purpose_seed import DEFAULT_LOAN_PURPOSES, run_default_seed


def main() -> None:
    ins, sk = run_default_seed()
    print(f"Loan purposes: inserted {ins}, skipped (already in database) {sk}.")
    if DEFAULT_LOAN_PURPOSES:
        print(f"Seed list has {len(DEFAULT_LOAN_PURPOSES)} definition(s) in loan_purpose_seed.py.")


if __name__ == "__main__":
    main()
