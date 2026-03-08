"""
Correct receipts for a date: run EOD for that date, then re-allocate all receipts.

Use this when daily state or allocation was wrong. Order matters:
  1. EOD updates loan_daily_state (accruals, buckets) for the date.
  2. Reallocate uses that daily state to re-run allocation for each receipt.

Run from project root:
  python scripts/correct_receipts_for_date.py 2025-12-01
  python scripts/correct_receipts_for_date.py 01.12.2025
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _parse_date(s: str):
    from datetime import datetime
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid date: {s!r}. Use YYYY-MM-DD or DD.MM.YYYY.")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/correct_receipts_for_date.py <date>")
        print("Example: python scripts/correct_receipts_for_date.py 2025-12-01")
        print("")
        print("Procedure: 1) Run EOD for the date (fix daily state). 2) Re-allocate all receipts for that date.")
        sys.exit(1)

    value_date = _parse_date(sys.argv[1])

    # Step 1: EOD for this date so loan_daily_state is correct
    print(f"Step 1: Running EOD for {value_date} ...")
    from eod import run_eod_for_date
    result = run_eod_for_date(value_date)
    print(f"  EOD done (loans_processed={result.loans_processed}).")

    # Step 2: Re-allocate all receipts with value_date on this date
    print(f"Step 2: Re-allocating receipts for {value_date} ...")
    from loan_management import get_repayment_ids_for_value_date, reallocate_repayment

    ids = get_repayment_ids_for_value_date(value_date)
    if not ids:
        print(f"  No receipts with value_date {value_date} (status=posted).")
        print("Done.")
        return

    print(f"  Found {len(ids)} receipt(s): {ids}")
    for rid in ids:
        try:
            reallocate_repayment(rid)
            print(f"  Repayment {rid} re-allocated OK.")
        except Exception as e:
            print(f"  Repayment {rid} FAILED: {e}", file=sys.stderr)
            sys.exit(1)
    print("Done.")


if __name__ == "__main__":
    main()
