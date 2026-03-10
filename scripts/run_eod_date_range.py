"""
Run EOD for every day in a date range (inclusive). Optionally reallocate every
receipt whose value_date falls on each day.

Same EOD logic as the app "Run EOD now" button; this script runs it for a range.
EOD is idempotent. Use --eod-only for "button behaviour" over many dates.

Run from project root:
  python scripts/run_eod_date_range.py 2025-10-08 2025-12-01 --eod-only   # EOD only (like the button)
  python scripts/run_eod_date_range.py 2025-10-08 2025-12-01             # EOD + reallocate receipts
  python scripts/run_eod_date_range.py 2025-10-08 2025-12-01 --quiet
"""
import argparse
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
    raise ValueError(f"Invalid date: {s!r}. Use YYYY-MM-DD, DD.MM.YYYY, or DD/MM/YYYY.")


def main():
    parser = argparse.ArgumentParser(
        description="Run EOD for each day in a date range (inclusive). Optionally reallocate receipts per day."
    )
    parser.add_argument(
        "start_date",
        help="Start date (e.g. 2025-10-08 or 08.10.2025)",
    )
    parser.add_argument(
        "end_date",
        help="End date (e.g. 2025-12-01 or 01.12.2025)",
    )
    parser.add_argument(
        "--eod-only",
        action="store_true",
        help="Run only EOD (same as the app button), do not reallocate receipts.",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Only print summary, not each date.",
    )
    args = parser.parse_args()

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)
    if start > end:
        start, end = end, start

    from datetime import timedelta
    from eod import run_eod_for_date
    if not args.eod_only:
        from loan_management import get_repayment_ids_for_value_date, reallocate_repayment

    current = start
    total_days = 0
    total_loans = 0
    total_reallocated = 0
    errors = []

    while current <= end:
        try:
            # When we reallocate all receipts ourselves, skip EOD's reallocate_after_reversals
            # to avoid double reallocate (EOD + this loop both calling reallocate for same receipt).
            skip_reallocate = not args.eod_only
            result = run_eod_for_date(current, skip_reallocate_after_reversals=skip_reallocate)
            total_days += 1
            total_loans += result.loans_processed
            if not args.quiet:
                print(f"EOD {current} OK (loans_processed={result.loans_processed})")

            if not args.eod_only:
                repayment_ids = get_repayment_ids_for_value_date(current)
                for rid in repayment_ids:
                    try:
                        reallocate_repayment(rid)
                        total_reallocated += 1
                        if not args.quiet:
                            print(f"  Reallocated receipt {rid}")
                    except Exception as e:
                        errors.append((current, f"realloc {rid}: {e}"))
                        if not args.quiet:
                            print(f"  Realloc {rid} FAILED: {e}", file=sys.stderr)
        except Exception as e:
            errors.append((current, str(e)))
            if not args.quiet:
                print(f"EOD {current} FAILED: {e}", file=sys.stderr)
        current += timedelta(days=1)

    summary = f"\nDone: {total_days} days, {total_loans} loan-days"
    if not args.eod_only:
        summary += f", {total_reallocated} receipts reallocated"
    print(summary + ".")
    if errors:
        print(f"Errors: {len(errors)}", file=sys.stderr)
        for d, msg in errors:
            print(f"  {d}: {msg}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
