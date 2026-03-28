#!/usr/bin/env python3
"""
Remove persisted loan_daily_state rows for the current system business date.

Policy: accrual for the system date must only be written by canonical date-advancing EOD.
Rows left by single-loan replay or manual runs should be deleted with this script.

Usage (project root, same DB env as the app):

  python scripts/delete_loan_daily_state_on_system_date.py
  python scripts/delete_loan_daily_state_on_system_date.py --execute --confirm
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

from config import get_database_url  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(
        description="Delete loan_daily_state rows where as_of_date = current system business date."
    )
    p.add_argument("--execute", action="store_true")
    p.add_argument("--confirm", action="store_true")
    args = p.parse_args()
    if args.execute and not args.confirm:
        print("Refusing: --execute requires --confirm", file=sys.stderr)
        return 2

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, current_system_date
            FROM system_business_config
            ORDER BY id
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row or not row.get("current_system_date"):
            print("No system_business_config.current_system_date found.", file=sys.stderr)
            return 1
        sys_d = row["current_system_date"]
        if hasattr(sys_d, "date"):
            sys_d = sys_d.date()

        cur.execute(
            "SELECT loan_id, as_of_date FROM loan_daily_state WHERE as_of_date = %s ORDER BY loan_id",
            (sys_d,),
        )
        hits = cur.fetchall()
        print(f"System business date: {sys_d.isoformat()}")
        print(f"loan_daily_state rows on that date: {len(hits)}")
        for h in hits[:50]:
            print(f"  loan_id={h['loan_id']}")
        if len(hits) > 50:
            print(f"  ... and {len(hits) - 50} more")

        if not args.execute:
            print("Dry-run. Pass --execute --confirm to delete these rows.")
            return 0

        cur.execute("DELETE FROM loan_daily_state WHERE as_of_date = %s", (sys_d,))
        n = cur.rowcount
        conn.commit()
        print(f"Deleted {n} row(s).")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
