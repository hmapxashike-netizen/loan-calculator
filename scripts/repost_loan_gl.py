"""
Re-post deterministic GL journals for a loan over a date range.

Use after migration 49 (unapplied liquidation templates) so liquidation legs
use Dr unapplied_funds instead of stale bank-debit journals.

Examples:
  python scripts/repost_loan_gl.py --loan 1 --from 2025-09-01 --to 2025-12-31
  python scripts/repost_loan_gl.py --loan 1 --from 2025-01-01 --to 2026-03-28
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from loan_management import repost_gl_for_loan_date_range


def _parse_date(s: str) -> date:
    parts = s.strip().split("-")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM-DD, got {s!r}")
    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
    return date(y, m, d)


def main() -> None:
    p = argparse.ArgumentParser(description="Repost GL for one loan (teller receipts + unapplied liquidations).")
    p.add_argument("--loan", type=int, required=True, help="loan_id")
    p.add_argument("--from", dest="from_date", type=_parse_date, required=True, help="start date (YYYY-MM-DD)")
    p.add_argument("--to", dest="to_date", type=_parse_date, required=True, help="end date (YYYY-MM-DD)")
    p.add_argument("--created-by", default="repost_loan_gl", help="journal created_by label")
    args = p.parse_args()

    repost_gl_for_loan_date_range(
        args.loan,
        args.from_date,
        args.to_date,
        created_by=args.created_by,
    )
    print(
        f"GL repost complete: loan_id={args.loan} "
        f"{args.from_date.isoformat()} .. {args.to_date.isoformat()}"
    )


if __name__ == "__main__":
    main()
