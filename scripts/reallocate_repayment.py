"""
Re-allocate an already-saved receipt (correct allocation / waterfall).

Allocation runs at save receipt, not at EOD. This script reverses the existing
allocation and unapplied credits for the given repayment, then re-runs allocation
with current config (e.g. after fixing waterfall or logic).

Run from project root:  python scripts/reallocate_repayment.py 2
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/reallocate_repayment.py <repayment_id>")
        print("Example: python scripts/reallocate_repayment.py 2")
        sys.exit(1)
    try:
        repayment_id = int(sys.argv[1])
    except ValueError:
        print("repayment_id must be an integer.")
        sys.exit(1)

    from loan_management import reallocate_repayment

    reallocate_repayment(repayment_id)
    print(f"Repayment {repayment_id} re-allocated successfully.")


if __name__ == "__main__":
    main()
