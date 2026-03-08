"""
Re-allocate all receipts (repayments) with value_date on a given date.

Uses current loan_daily_state and waterfall config. Run after EOD has been run
for that date so balances are correct.

Run from project root:
  python scripts/reallocate_receipts_for_date.py 2025-12-01
  python scripts/reallocate_receipts_for_date.py 01.12.2025
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
        print("Usage: python scripts/reallocate_receipts_for_date.py <date>")
        print("Example: python scripts/reallocate_receipts_for_date.py 2025-12-01")
        sys.exit(1)

    value_date = _parse_date(sys.argv[1])

    from loan_management import get_repayment_ids_for_value_date, reallocate_repayment

    ids = get_repayment_ids_for_value_date(value_date)
    if not ids:
        print(f"No receipts with value_date {value_date}.")
        return

    print(f"Re-allocating {len(ids)} receipt(s) for {value_date}: {ids}")
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
