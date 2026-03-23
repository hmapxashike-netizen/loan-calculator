"""
Deduplicate GL journal headers already persisted in the database.

Why this exists:
- EOD posts deterministic journals using `event_id` (e.g. `EOD-YYYY-MM-DD-LOAN-<id>-<EVENT_TYPE>`).
- Older schema versions may have allowed duplicates because `journal_entries` did not enforce uniqueness
  for `(event_id, event_tag)`.
- This script keeps one journal per `(event_id, event_tag)` and deletes the rest (plus their `journal_items`).

Safety:
- Default filter keeps scope limited to EOD journals (`event_id` starts with `EOD-`).
- Always run with `--dry-run` first.
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
    p = argparse.ArgumentParser(description="Deduplicate journal_entries by (event_id, event_tag).")
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
        help="Which journal row to keep in each duplicate group.",
    )
    p.add_argument(
        "--event-id-prefix",
        default="EOD-",
        help="Only dedupe journal rows whose event_id starts with this prefix. Default: EOD-",
    )
    p.add_argument(
        "--from-date",
        dest="from_date",
        help="Optional entry_date lower bound (YYYY-MM-DD).",
    )
    p.add_argument(
        "--to-date",
        dest="to_date",
        help="Optional entry_date upper bound (YYYY-MM-DD).",
    )
    return p.parse_args()


def _parse_iso_date(value: str | None) -> Date | None:
    if not value:
        return None
    return Date.fromisoformat(value)


def _fetch_duplicate_ids(
    cur,
    *,
    event_id_prefix: str,
    keep: str,
    from_date: Date | None,
    to_date: Date | None,
) -> list[str]:
    keep_direction = "DESC" if keep == "latest" else "ASC"

    where_clauses = [
        "je.event_id IS NOT NULL",
        "je.event_tag IS NOT NULL",
        "je.event_id LIKE %s",
    ]
    params: list[object] = [f"{event_id_prefix}%"]

    if from_date is not None:
        where_clauses.append("je.entry_date >= %s")
        params.append(from_date)
    if to_date is not None:
        where_clauses.append("je.entry_date <= %s")
        params.append(to_date)

    # Rank duplicates within each (event_id, event_tag) group.
    # Delete everything except rank=1.
    sql = f"""
    WITH ranked AS (
        SELECT
            je.id,
            je.event_id,
            je.event_tag,
            je.created_at,
            ROW_NUMBER() OVER (
                PARTITION BY je.event_id, je.event_tag
                ORDER BY je.created_at {keep_direction}, je.id
            ) AS rn
        FROM journal_entries je
        WHERE {" AND ".join(where_clauses)}
    )
    SELECT id
    FROM ranked
    WHERE rn > 1
    ORDER BY id;
    """

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    return [r["id"] for r in rows]


def main() -> None:
    args = _parse_args()
    from_date = _parse_iso_date(args.from_date)
    to_date = _parse_iso_date(args.to_date)

    if not args.dry_run and not args.confirm:
        raise SystemExit("Refusing to run destructive delete without --confirm. Use --dry-run first.")

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            duplicate_ids = _fetch_duplicate_ids(
                cur,
                event_id_prefix=args.event_id_prefix,
                keep=args.keep,
                from_date=from_date,
                to_date=to_date,
            )

            if not duplicate_ids:
                print("No duplicates found (nothing to delete).")
                return

            print(f"Duplicate journal entries to delete: {len(duplicate_ids)}")
            print(f"Keep mode: {args.keep} | event_id_prefix: {args.event_id_prefix}")
            if from_date is not None:
                print(f"From date: {from_date.isoformat()}")
            if to_date is not None:
                print(f"To date: {to_date.isoformat()}")
            print("Sample IDs:")
            for x in duplicate_ids[:10]:
                print(f"  - {x}")

            if args.dry_run:
                print("\nDRY RUN: no changes made.")
                return

            if not args.confirm:
                raise SystemExit("Internal error: reached destructive path without --confirm.")

            # Delete items first, then headers.
            # Prefer uuid[] casts (schema: ids are UUID), but fall back to no-cast
            # in case you have an older/altered schema.
            try:
                cur.execute(
                    "DELETE FROM journal_items WHERE entry_id = ANY(%s::uuid[])",
                    (duplicate_ids,),
                )
                cur.execute(
                    "DELETE FROM journal_entries WHERE id = ANY(%s::uuid[])",
                    (duplicate_ids,),
                )
            except Exception:
                cur.execute(
                    "DELETE FROM journal_items WHERE entry_id = ANY(%s)",
                    (duplicate_ids,),
                )
                cur.execute(
                    "DELETE FROM journal_entries WHERE id = ANY(%s)",
                    (duplicate_ids,),
                )

        conn.commit()
        print("Deduplication completed successfully.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

