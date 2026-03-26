"""
Recompute loan_daily_state for one loan for each day in [start_date, end_date] inclusive.

Uses the same engine step as "Recompute loan daily state" under Fix EOD issues, but for a
full date range in one run (legacy repair / after manual data fixes).

Run from project root (set DB env vars as for the app):

  python scripts/run_single_loan_eod_range.py 9 2025-06-17 2025-07-08

Optional:

  python scripts/run_single_loan_eod_range.py 9 17.06.2025 08.07.2025
"""
from __future__ import annotations

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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run single-loan EOD for each day in a date range (inclusive)."
    )
    parser.add_argument("loan_id", type=int, help="Loan ID (e.g. 9)")
    parser.add_argument("start_date", help="First day (e.g. 2025-06-17)")
    parser.add_argument("end_date", help="Last day (e.g. 2025-07-08)")
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Only print final success or failure.",
    )
    args = parser.parse_args()

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)
    if start > end:
        start, end = end, start

    from eod import run_single_loan_eod_date_range
    from loan_management import load_system_config_from_db

    cfg = load_system_config_from_db() or {}
    if not args.quiet:
        print(
            f"loan_id={args.loan_id} "
            f"from {start.isoformat()} through {end.isoformat()} (inclusive) …",
            flush=True,
        )

    ok, err = run_single_loan_eod_date_range(
        args.loan_id, start, end, sys_cfg=cfg
    )
    if ok:
        if not args.quiet:
            print("Done.", flush=True)
        else:
            print(
                f"OK loan_id={args.loan_id} {start.isoformat()}..{end.isoformat()}",
                flush=True,
            )
        return 0

    print(f"FAILED:loan_id={args.loan_id}: {err}", flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
