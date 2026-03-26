#!/usr/bin/env python3
"""
Restore loan_daily_state for a given date to the **opening** position = same buckets as
**prior calendar day's closing** (loan_daily_state for as_of_date - 1).

This does NOT delete repayments or allocations. Use after fixing receipts or when you need
the state row for `target_date` to match end-of-previous-day balances (e.g. after manual
fixes or deleted receipts where you will re-run EOD).

Optional:
  --recompute-net-unapplied  Recompute net_allocation / unallocated / credits for target
                             from current loan_repayments + unapplied_funds (via loan_management).
  --run-eod-through DATE     After copy, run run_single_loan_eod(loan_id, d) for each d from
                             target_date through this date (inclusive) to replay the engine.

System business date: This script does NOT advance the system calendar / business date.
Only full portfolio EOD (run_eod_for_date) does that. run_single_loan_eod only refreshes
loan_daily_state for the given loan and date.

Usage (project root, same DB as app: FARNDACRED_DATABASE_URL / LMS_DATABASE_URL / config):

  # Preview
  python scripts/restore_loan_opening_balance.py --loan-id 1 --target-date 2025-06-01

  # Apply (restore row 2025-06-01 from prior day closing) — use ONE line in PowerShell:
  python scripts/restore_loan_opening_balance.py --loan-id 1 --target-date 2025-06-01 --execute --confirm

  # Optional: recompute net/unapplied + replay single-loan EOD (still one line in PowerShell):
  python scripts/restore_loan_opening_balance.py --loan-id 1 --target-date 2025-06-01 --recompute-net-unapplied --run-eod-through 2025-06-05 --execute --confirm

Windows PowerShell: do not use ^ for line continuation (that is cmd.exe). Either put the
whole command on one line, or end a line with backtick ` and continue on the next line.
A broken line that starts with "--flag" can be parsed as an operator and cause errors.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

from config import get_database_url  # noqa: E402


def _parse_date(s: str) -> date:
    return date.fromisoformat(s.strip())


def _lds_copy_columns(cur) -> list[str]:
    """Columns to copy from prior day into target row (exclude keys and metadata)."""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'loan_daily_state'
          AND column_name NOT IN ('id', 'created_at', 'loan_id', 'as_of_date')
        ORDER BY ordinal_position
        """
    )
    return [r[0] for r in cur.fetchall()]


def _copy_prior_closing_to_target(
    cur,
    loan_id: int,
    target_date: date,
    cols: list[str],
) -> int:
    """
    UPSERT loan_daily_state(loan_id, target_date) from loan_daily_state(loan_id, target_date-1).
    Returns 1 if a row was written, 0 if prior row missing.
    """
    prev_date = target_date - timedelta(days=1)
    cur.execute(
        """
        SELECT 1 FROM loan_daily_state WHERE loan_id = %s AND as_of_date = %s
        """,
        (loan_id, prev_date),
    )
    if not cur.fetchone():
        return 0

    col_list = ", ".join(cols)
    select_list = ", ".join(f"src.{c}" for c in cols)
    set_list = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols)

    cur.execute(
        f"""
        INSERT INTO loan_daily_state (loan_id, as_of_date, {col_list})
        SELECT %s::int, %s::date, {select_list}
        FROM loan_daily_state AS src
        WHERE src.loan_id = %s AND src.as_of_date = %s
        ON CONFLICT (loan_id, as_of_date) DO UPDATE SET
        {set_list}
        """,
        (loan_id, target_date, loan_id, prev_date),
    )
    return 1


def main() -> int:
    p = argparse.ArgumentParser(description="Set loan_daily_state[target] = prior day closing (opening position).")
    p.add_argument("--loan-id", type=int, required=True)
    p.add_argument("--target-date", type=str, required=True, help="Row to overwrite (YYYY-MM-DD).")
    p.add_argument("--execute", action="store_true")
    p.add_argument("--confirm", action="store_true", help="Required with --execute.")
    p.add_argument(
        "--recompute-net-unapplied",
        action="store_true",
        help="After copy, set net_allocation, unallocated, credits from loan_management helpers.",
    )
    p.add_argument(
        "--run-eod-through",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="After copy, run run_single_loan_eod for each day from target-date through this date.",
    )
    args = p.parse_args()

    if args.execute and not args.confirm:
        print("Refusing: --execute requires --confirm", file=sys.stderr)
        return 2

    loan_id = args.loan_id
    target_date = _parse_date(args.target_date)
    prev_date = target_date - timedelta(days=1)

    eod_end: date | None = None
    if args.run_eod_through:
        eod_end = _parse_date(args.run_eod_through)
        if eod_end < target_date:
            print("--run-eod-through must be on or after --target-date", file=sys.stderr)
            return 2

    conn = None
    try:
        conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
        cur = conn.cursor()
        cols = _lds_copy_columns(cur)

        cur.execute(
            """
            SELECT as_of_date, principal_not_due, principal_arrears, interest_arrears_balance,
                   default_interest_balance, penalty_interest_balance, fees_charges_balance, total_exposure
            FROM loan_daily_state
            WHERE loan_id = %s AND as_of_date = %s
            """,
            (loan_id, prev_date),
        )
        prev_row = cur.fetchone()
        if not prev_row:
            print(
                f"No loan_daily_state row for loan_id={loan_id} as_of_date={prev_date} "
                f"(cannot copy opening from prior closing).",
                file=sys.stderr,
            )
            return 1

        print(f"Prior closing row ({prev_date}) preview: {dict(prev_row)}")
        print(f"Will set loan_daily_state for loan_id={loan_id} as_of_date={target_date} to match prior closing columns ({len(cols)} fields).")

        if not args.execute:
            print("Dry-run only. Pass --execute --confirm to apply.")
            conn.rollback()
            return 0

        n = _copy_prior_closing_to_target(cur, loan_id, target_date, cols)
        if n == 0:
            conn.rollback()
            return 1
        print(f"Copied prior ({prev_date}) -> target ({target_date}).")

        if args.recompute_net_unapplied:
            from loan_management import get_net_allocation_for_loan_date, get_unallocated_for_loan_date

            net = float(get_net_allocation_for_loan_date(loan_id, target_date, conn=conn))
            un = float(get_unallocated_for_loan_date(loan_id, target_date, conn=conn))
            # credits column may exist
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'loan_daily_state' AND column_name = 'credits'
                """
            )
            has_credits = cur.fetchone() is not None
            if has_credits:
                from loan_management import get_credits_for_loan_date

                cr = float(get_credits_for_loan_date(loan_id, target_date))
                cur.execute(
                    """
                    UPDATE loan_daily_state
                    SET net_allocation = %s, unallocated = %s, credits = %s
                    WHERE loan_id = %s AND as_of_date = %s
                    """,
                    (net, un, cr, loan_id, target_date),
                )
            else:
                cur.execute(
                    """
                    UPDATE loan_daily_state
                    SET net_allocation = %s, unallocated = %s
                    WHERE loan_id = %s AND as_of_date = %s
                    """,
                    (net, un, loan_id, target_date),
                )
            print(f"Recomputed net_allocation={net}, unallocated={un}" + (f", credits={cr}" if has_credits else "") + ".")

        conn.commit()
        print("Committed (copy" + (" + recompute" if args.recompute_net_unapplied else "") + ").")

        # EOD uses its own DB connections; run after our transaction commits so it does not get overwritten.
        if eod_end is not None:
            conn.close()
            conn = None
            from eod import run_single_loan_eod

            d = target_date
            while d <= eod_end:
                run_single_loan_eod(loan_id, d)
                print(f"run_single_loan_eod(loan_id={loan_id}, {d})")
                d += timedelta(days=1)
            print("EOD replay finished (each step commits in eod).")
        return 0
    except Exception as e:
        if conn is not None:
            conn.rollback()
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
