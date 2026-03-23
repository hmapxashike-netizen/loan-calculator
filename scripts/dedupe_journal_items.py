"""
Deduplicate duplicate rows in `journal_items`.

Why this exists:
- You can have duplicate-looking GL lines in the UI even when `journal_entries`
  headers are not duplicated (i.e., no duplicate `(event_id, event_tag)`).
- This happens when the posting templates (or posting logic) inserts the
  same `journal_items` row multiple times for the same journal entry.

This script removes exact duplicates of `journal_items` within a single
`journal_entries` header by using a deterministic partition:
  (entry_id, account_id, debit, credit, memo)

Important:
- This deletes journal_items only (journal_entries are untouched).
- It is intended to be used only for EOD-related journals and typically only
  for older duplicate data.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date as Date

import psycopg2
from psycopg2.extras import RealDictCursor

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import get_database_url


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Deduplicate journal_items by (entry_id, account_id, debit, credit, memo)."
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without making changes.",
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        help="Required to perform destructive deletes (ignored in dry-run).",
    )
    p.add_argument(
        "--keep",
        choices=["latest", "earliest"],
        default="latest",
        help="Which duplicate item row to keep within each identical group.",
    )
    p.add_argument(
        "--event-id-prefix",
        default="EOD-",
        help="Only dedupe items whose journal_entries.event_id starts with this prefix. Default: EOD-",
    )
    p.add_argument(
        "--from-date",
        dest="from_date",
        help="Optional journal_entries.entry_date lower bound (YYYY-MM-DD).",
    )
    p.add_argument(
        "--to-date",
        dest="to_date",
        help="Optional journal_entries.entry_date upper bound (YYYY-MM-DD).",
    )
    p.add_argument(
        "--ignore-memo",
        action="store_true",
        help="Ignore `journal_items.memo` when identifying duplicate lines.",
    )
    p.add_argument(
        "--amount-decimals",
        type=int,
        default=10,
        help="Round debit/credit to this many decimals when matching duplicates. Default: 10.",
    )
    return p.parse_args()


def _parse_iso_date(value: str | None) -> Date | None:
    if not value:
        return None
    return Date.fromisoformat(value)


def _fetch_duplicate_item_ids(
    cur,
    *,
    event_id_prefix: str,
    keep: str,
    from_date: Date | None,
    to_date: Date | None,
    ignore_memo: bool,
    amount_decimals: int,
) -> list[str]:
    keep_direction = "DESC" if keep == "latest" else "ASC"

    where_clauses = [
        "je.event_id IS NOT NULL",
        "je.event_id LIKE %s",
    ]
    params: list[object] = [f"{event_id_prefix}%"]

    if from_date is not None:
        where_clauses.append("je.entry_date >= %s")
        params.append(from_date)
    if to_date is not None:
        where_clauses.append("je.entry_date <= %s")
        params.append(to_date)

    # Rank duplicate journal_items within each journal header (entry_id) and
    # within each identical posting group.
    #
    # `journal_items.debit/credit` are NUMERIC with high precision.
    # The UI typically displays fewer decimals, so strict matching can miss
    # duplicates that *look* identical. We round during matching.
    amount_decimals = max(0, int(amount_decimals))
    debit_expr = f"ROUND(ji.debit, {amount_decimals})"
    credit_expr = f"ROUND(ji.credit, {amount_decimals})"

    if ignore_memo:
        partition_memo_sql = ""
    else:
        partition_memo_sql = ",\n                    COALESCE(ji.memo, '')"

    sql = f"""
    WITH ranked AS (
        SELECT
            ji.id AS ji_id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    ji.entry_id,
                    ji.account_id,
                    {debit_expr},
                    {credit_expr}{partition_memo_sql}
                ORDER BY ji.id {keep_direction}
            ) AS rn
        FROM journal_items ji
        JOIN journal_entries je ON je.id = ji.entry_id
        WHERE {" AND ".join(where_clauses)}
    )
    SELECT ji_id
    FROM ranked
    WHERE rn > 1
    ORDER BY ji_id;
    """

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    return [r["ji_id"] for r in rows]


def main() -> None:
    args = _parse_args()
    from_date = _parse_iso_date(args.from_date)
    to_date = _parse_iso_date(args.to_date)

    if not args.dry_run and not args.confirm:
        raise SystemExit("Refusing to run destructive delete without --confirm. Use --dry-run first.")

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            duplicate_item_ids = _fetch_duplicate_item_ids(
                cur,
                event_id_prefix=args.event_id_prefix,
                keep=args.keep,
                from_date=from_date,
                to_date=to_date,
                ignore_memo=args.ignore_memo,
                amount_decimals=args.amount_decimals,
            )

            if not duplicate_item_ids:
                print("No duplicate journal_items found (nothing to delete).")
                return

            print(f"Duplicate journal_items to delete: {len(duplicate_item_ids)}")
            print(f"Keep mode: {args.keep} | event_id_prefix: {args.event_id_prefix}")
            if from_date is not None:
                print(f"From date: {from_date.isoformat()}")
            if to_date is not None:
                print(f"To date: {to_date.isoformat()}")
            print("Sample IDs:")
            for x in duplicate_item_ids[:10]:
                print(f"  - {x}")

            if args.dry_run:
                print("\nDRY RUN: no changes made.")
                return

            if not args.confirm:
                raise SystemExit("Internal error: reached destructive path without --confirm.")

            # Delete journal_items only. ids are UUID.
            try:
                cur.execute(
                    "DELETE FROM journal_items WHERE id = ANY(%s::uuid[])",
                    (duplicate_item_ids,),
                )
            except Exception:
                # Fallback if casting fails due to schema differences.
                cur.execute("DELETE FROM journal_items WHERE id = ANY(%s)", (duplicate_item_ids,))

        conn.commit()
        print("journal_items deduplication completed successfully.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

