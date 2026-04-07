"""
Repair schedule_lines.Date values that match dd-Mon-yyy (three-digit / truncated year).

Interprets the three characters after the second hyphen as the *prefix* of a four-digit year
(e.g. 202 -> 2024, 2025, …). For each loan, schedule lines are processed in Period order; the
smallest calendar date on or after the running anchor that matches the prefix is chosen. The
anchor starts at disbursement (or start) date so instalments progress forward across years.

Run after widening the column if needed: python scripts/run_migration_76.py

Usage:
  python scripts/repair_truncated_schedule_dates.py --dry-run
  python scripts/repair_truncated_schedule_dates.py
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url

_TRUNC_RE = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{3})$", re.I)


def _candidates_after_anchor(d_s: str, mon_s: str, ypref: str, anchor: date) -> list[date]:
    out: list[date] = []
    for y in range(1990, 2101):
        if not str(y).startswith(ypref):
            continue
        try:
            dt = datetime.strptime(f"{d_s}-{mon_s}-{y}", "%d-%b-%Y").date()
        except ValueError:
            continue
        if dt >= anchor:
            out.append(dt)
    return out


def _repair_row(d_str: str, anchor: date) -> tuple[date, str] | None:
    m = _TRUNC_RE.match(d_str.strip())
    if not m:
        return None
    d_s, mon_s, ypref = m.groups()
    cand = _candidates_after_anchor(d_s, mon_s, ypref, anchor)
    if not cand:
        return None
    best = min(cand)
    return best, best.strftime("%d-%b-%Y")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sl.id AS line_id, ls.loan_id, ls.version AS schedule_version,
                       sl."Period", sl."Date" AS d,
                       l.disbursement_date, l.start_date
                FROM schedule_lines sl
                JOIN loan_schedules ls ON ls.id = sl.loan_schedule_id
                JOIN loans l ON l.id = ls.loan_id
                WHERE sl."Date" IS NOT NULL
                  AND sl."Date" ~ '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{3}$'
                ORDER BY ls.loan_id, ls.version, sl."Period"
                """
            )
            rows = cur.fetchall()

        by_loan: dict[int, list[dict]] = {}
        for r in rows:
            by_loan.setdefault(int(r["loan_id"]), []).append(dict(r))

        updates: list[tuple[str, int]] = []
        skipped: list[str] = []

        for loan_id in sorted(by_loan.keys()):
            chunk = by_loan[loan_id]
            disb = chunk[0].get("disbursement_date") or chunk[0].get("start_date")
            if hasattr(disb, "date"):
                disb = disb.date()
            if not isinstance(disb, date):
                skipped.append(f"loan_id={loan_id}: no disbursement/start date")
                continue
            anchor = disb
            for r in sorted(chunk, key=lambda x: (int(x.get("schedule_version") or 1), int(x["Period"]))):
                d_raw = str(r["d"])
                if not _TRUNC_RE.match(d_raw.strip()):
                    continue
                got = _repair_row(d_raw, anchor)
                if not got:
                    skipped.append(f"loan_id={loan_id} period={r['Period']} date={d_raw!r}")
                    continue
                new_dt, new_s = got
                if new_s != d_raw.strip():
                    updates.append((new_s, int(r["line_id"])))
                anchor = new_dt

        print(f"Truncated lines scanned: {len(rows)} | updates: {len(updates)} | skip notes: {len(skipped)}")
        for u in updates[:25]:
            print(f"  line_id={u[1]} -> {u[0]!r}")
        if len(updates) > 25:
            print(f"  ... and {len(updates) - 25} more")
        for s in skipped[:20]:
            print(f"  SKIP {s}")
        if len(skipped) > 20:
            print(f"  ... and {len(skipped) - 20} more skips")

        if args.dry_run:
            print("Dry run: no database changes.")
            return

        if not updates:
            print("Nothing to update.")
            return

        with conn.cursor() as cur:
            for new_d, line_id in updates:
                cur.execute(
                    'UPDATE schedule_lines SET "Date" = %s WHERE id = %s',
                    (new_d, line_id),
                )
        conn.commit()
        print(f"Updated {len(updates)} schedule_lines row(s).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
