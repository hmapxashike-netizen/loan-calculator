"""
Run EOD replay/backfill one date at a time over a range (inclusive).

This mirrors clicking the EOD replay/backfill button repeatedly:
``run_backfill_eod_for_date(d)`` for each calendar day.

Run from project root:
  python scripts/run_eod_replay_range.py 2024-01-18 2024-05-02
  python scripts/run_eod_replay_range.py 2024-01-18 2024-05-02 --dry-run
  python scripts/run_eod_replay_range.py 2024-01-18 2024-05-02 --stop-on-error
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import timedelta

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _parse_date(raw: str):
    from datetime import datetime

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid date: {raw!r}. Use YYYY-MM-DD, DD.MM.YYYY, or DD/MM/YYYY.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run replay/backfill EOD for each date in a range (inclusive)."
    )
    parser.add_argument("start_date", help="Start date (e.g. 2024-01-18)")
    parser.add_argument("end_date", help="End date (e.g. 2024-05-02)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned dates only; do not run replay.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop immediately on first replay error.",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Print summary only.",
    )
    args = parser.parse_args()

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)
    if start > end:
        start, end = end, start

    from services.eod_service import run_backfill_eod_for_date

    current = start
    runs_ok = 0
    loans_processed = 0
    errors: list[tuple[object, str]] = []

    while current <= end:
        if args.dry_run:
            if not args.quiet:
                print(f"DRY-RUN replay {current}")
            current += timedelta(days=1)
            continue
        try:
            res = run_backfill_eod_for_date(current)
            runs_ok += 1
            loans_processed += int(getattr(res, "loans_processed", 0) or 0)
            if not args.quiet:
                print(f"Replay {current} OK (loans_processed={getattr(res, 'loans_processed', 0)})")
        except Exception as ex:
            errors.append((current, str(ex)))
            if not args.quiet:
                print(f"Replay {current} FAILED: {ex}", file=sys.stderr)
            if args.stop_on_error:
                break
        current += timedelta(days=1)

    if args.dry_run:
        total_days = (end - start).days + 1
        print(f"Dry-run done: {total_days} date(s) planned.")
        return 0

    print(f"Done: {runs_ok} replay run(s) succeeded, loans_processed={loans_processed}.")
    if errors:
        print(f"Errors: {len(errors)}", file=sys.stderr)
        for d, msg in errors:
            print(f"  {d}: {msg}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
