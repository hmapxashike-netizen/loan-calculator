"""
Print why the EOD engine would skip a loan on a date (early checks only).

  python scripts/diagnose_eod_loan.py 272 2024-01-01

Exit 0 if early checks pass (engine should attempt to write loan_daily_state); 1 if skipped.
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
    raise ValueError(f"Invalid date: {s!r}")


def main() -> int:
    p = argparse.ArgumentParser(description="Diagnose EOD skip reason for one loan + date.")
    p.add_argument("loan_id", type=int)
    p.add_argument("as_of_date", help="YYYY-MM-DD")
    args = p.parse_args()
    as_of = _parse_date(args.as_of_date)
    from eod.core import explain_single_loan_eod_skip_reason

    reason = explain_single_loan_eod_skip_reason(args.loan_id, as_of)
    if reason:
        print(reason, flush=True)
        return 1
    print(
        "Early checks passed: schedule/version at disbursement builds accrual periods. "
        "If full EOD still skips, check server logs for exceptions after this stage.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
