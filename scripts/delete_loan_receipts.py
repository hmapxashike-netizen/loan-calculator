#!/usr/bin/env python3
"""
Delete loan receipt(s) (loan_repayments) and dependent rows.

- loan_repayment_allocation: removed via ON DELETE CASCADE when the repayment row is deleted.
- unapplied_funds / ledger: repayment_id typically ON DELETE SET NULL (rows kept, FK nulled).
- Reversals: loan_repayments.original_repayment_id REFERENCES loan_repayments(id) ON DELETE RESTRICT,
  so any reversal row pointing at an original must be deleted before the original. This script expands
  the ID set to include dependent reversals and deletes in safe order (no incoming original_repayment_id
  from remaining set).

Accounting: journal_entries use event_id like REPAY-{id}-... Use --delete-journals to remove those
lines/entries after repayment deletion (optional).

**loan_daily_state** is NOT recalculated — re-run EOD / fix state after destructive deletes.

Usage (from project root, FARNDACRED_DATABASE_URL/LMS_DATABASE_URL or config as for the app):

  # Preview
  python scripts/delete_loan_receipts.py --repayment-id 1
  python scripts/delete_loan_receipts.py --repayment-id 1 --repayment-id 2

  # Actually delete (requires both flags)
  python scripts/delete_loan_receipts.py --repayment-id 1 --execute --confirm-delete

  # Also remove GL journals whose event_id starts with REPAY-{id}-
  python scripts/delete_loan_receipts.py --repayment-id 1 --execute --confirm-delete --delete-journals
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


def _expand_with_reversals(cur, initial: list[int]) -> set[int]:
    """Include any repayment whose original_repayment_id is in the set (recursive)."""
    ids: set[int] = set(initial)
    pending = set(initial)
    while pending:
        batch = list(pending)
        pending.clear()
        cur.execute(
            """
            SELECT id FROM loan_repayments
            WHERE original_repayment_id = ANY(%s::int[])
            """,
            (batch,),
        )
        for row in cur.fetchall():
            rid = int(row["id"])
            if rid not in ids:
                ids.add(rid)
                pending.add(rid)
    return ids


def _delete_repayments_in_fk_order(cur, ids: set[int]) -> list[int]:
    """
    Delete repayments in an order that respects ON DELETE RESTRICT on original_repayment_id:
    delete rows that are not referenced as original by any remaining row in the set first.
    Returns list of deleted ids in order.
    """
    remaining = set(ids)
    deleted_order: list[int] = []
    while remaining:
        cur.execute(
            """
            SELECT lr.id
            FROM loan_repayments lr
            WHERE lr.id = ANY(%s::int[])
              AND NOT EXISTS (
                SELECT 1 FROM loan_repayments x
                WHERE x.original_repayment_id = lr.id
                  AND x.id = ANY(%s::int[])
              )
            """,
            (list(remaining), list(remaining)),
        )
        batch = [int(r["id"]) for r in cur.fetchall()]
        if not batch:
            raise RuntimeError(
                "Could not find a safe delete batch (cycle in original_repayment_id?). "
                f"Remaining ids: {sorted(remaining)}"
            )
        cur.execute(
            "DELETE FROM loan_repayments WHERE id = ANY(%s::int[]) RETURNING id",
            (batch,),
        )
        for row in cur.fetchall():
            deleted_order.append(int(row["id"]))
        remaining -= set(batch)
    return deleted_order


def _delete_journals_for_repayments(cur, repayment_ids: list[int]) -> int:
    """Delete journal_entries (and journal_items via CASCADE) where event_id LIKE 'REPAY-{id}-%'."""
    total = 0
    for rid in repayment_ids:
        prefix = f"REPAY-{rid}-"
        cur.execute(
            """
            DELETE FROM journal_entries
            WHERE event_id IS NOT NULL AND event_id LIKE %s
            RETURNING id
            """,
            (prefix + "%",),
        )
        total += len(cur.fetchall())
    return total


def main() -> int:
    p = argparse.ArgumentParser(description="Delete loan receipt rows and cascaded allocations.")
    p.add_argument("--repayment-id", type=int, action="append", dest="repayment_ids", required=True)
    p.add_argument(
        "--execute",
        action="store_true",
        help="Perform deletes (default is dry-run only).",
    )
    p.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Required with --execute to acknowledge destructive action.",
    )
    p.add_argument(
        "--delete-journals",
        action="store_true",
        help="After deleting repayments, delete journal_entries where event_id LIKE 'REPAY-{id}-%%'.",
    )
    args = p.parse_args()

    if args.execute and not args.confirm_delete:
        print("Refusing: --execute requires --confirm-delete", file=sys.stderr)
        return 2

    initial = list(dict.fromkeys(args.repayment_ids))  # dedupe, preserve order

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, loan_id, amount, status, reference,
                   COALESCE(value_date, payment_date) AS eff_date,
                   original_repayment_id
            FROM loan_repayments
            WHERE id = ANY(%s::int[])
            """,
            (initial,),
        )
        found = {int(r["id"]): dict(r) for r in cur.fetchall()}
        missing = set(initial) - set(found.keys())
        if missing:
            print(
                f"ERROR: repayment id(s) not found: {sorted(missing)}. "
                "If you already deleted them in a previous run, that is expected.",
                file=sys.stderr,
            )
            conn.rollback()
            return 1

        expanded = _expand_with_reversals(cur, initial)
        cur.execute(
            """
            SELECT id, loan_id, amount, status, reference,
                   COALESCE(value_date, payment_date) AS eff_date,
                   original_repayment_id
            FROM loan_repayments
            WHERE id = ANY(%s::int[])
            """,
            (list(expanded),),
        )
        found = {int(r["id"]): dict(r) for r in cur.fetchall()}

        print("Rows to delete (loan_repayments):")
        for rid in sorted(expanded):
            r = found[rid]
            print(
                f"  id={rid} loan_id={r['loan_id']} amount={r['amount']} status={r.get('status')} "
                f"eff_date={r.get('eff_date')} original_repayment_id={r.get('original_repayment_id')} "
                f"ref={r.get('reference')}"
            )

        cur.execute(
            """
            SELECT repayment_id,
                   alloc_total, unallocated,
                   alloc_principal_arrears, alloc_interest_arrears,
                   alloc_default_interest, alloc_penalty_interest, alloc_fees_charges
            FROM loan_repayment_allocation
            WHERE repayment_id = ANY(%s::int[])
            """,
            (list(expanded),),
        )
        allocs = cur.fetchall()
        if allocs:
            print("\nloan_repayment_allocation rows (will CASCADE delete with repayment):")
            for a in allocs:
                print(f"  repayment_id={a['repayment_id']} alloc_total={a.get('alloc_total')} ...")

        if not args.execute:
            print("\nDry-run only. Pass --execute --confirm-delete to delete.")
            conn.rollback()
            return 0

        deleted = _delete_repayments_in_fk_order(cur, expanded)
        print(f"\nDeleted loan_repayments ids (order): {deleted}")

        if args.delete_journals:
            n = _delete_journals_for_repayments(cur, deleted)
            print(f"Deleted journal_entries rows (matched event_id): {n}")

        conn.commit()
        print("Committed.")
        print("OK: one invocation finished successfully (exit 0). Previewing the same id again will show 'not found'.")

        # loan_daily_state is NOT recalculated when receipts are removed — user must restore opening row(s).
        pairs = set()
        for rid in deleted:
            r = found.get(rid)
            if not r:
                continue
            lid = int(r["loan_id"])
            ed = r["eff_date"]
            if hasattr(ed, "date"):
                ed = ed.date()
            pairs.add((lid, ed))
        if pairs:
            print("\n--- IMPORTANT: fix loan_daily_state (not updated by this delete) ---")
            print(
                "For each loan + value date below, copy prior day's closing onto that date "
                "(opening position) and recompute net/unapplied:"
            )
            for lid, ed in sorted(pairs, key=lambda x: (x[0], x[1])):
                print(
                    f"\n  python scripts/restore_loan_opening_balance.py --loan-id {lid} "
                    f"--target-date {ed} --recompute-net-unapplied --execute --confirm"
                )
            print()
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
