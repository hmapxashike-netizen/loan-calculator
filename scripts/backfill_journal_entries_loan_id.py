"""
Backfill journal_entries.loan_id in small commits (safe for ~1M+ rows).

Requires migration 81 (column exists). Run after migration 82 (index) is optional;
index builds faster once loan_id is populated.

Usage (from project root):
  python scripts/backfill_journal_entries_loan_id.py

Optional env:
  FARNDACRED_LOAN_BACKFILL_BATCH=5000   (default 5000 rows touched per pattern pass)
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url

_BATCH = max(100, int(os.environ.get("FARNDACRED_LOAN_BACKFILL_BATCH", "5000")))
_MAX_LOOPS = int(os.environ.get("FARNDACRED_LOAN_BACKFILL_MAX_LOOPS", "500000"))


def _column_exists(cur) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_catalog = current_database()
          AND table_schema = ANY (current_schemas(false))
          AND table_name = 'journal_entries'
          AND column_name = 'loan_id'
        LIMIT 1
        """
    )
    return cur.fetchone() is not None


def main() -> None:
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            if not _column_exists(cur):
                print("journal_entries.loan_id is missing — run: python scripts/run_migration_81.py")
                sys.exit(2)
    finally:
        conn.close()

    # 1) Numeric event_id (e.g. LOAN_APPROVAL) — usually one fast UPDATE.
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE journal_entries je
                SET loan_id = (btrim(je.event_id))::integer
                WHERE je.loan_id IS NULL
                  AND je.event_id ~ '^[0-9]+$'
                  AND EXISTS (SELECT 1 FROM loans l WHERE l.id = (btrim(je.event_id))::integer)
                """
            )
            n1 = cur.rowcount
        conn.commit()
        print(f"Backfill pass 1 (numeric event_id): {n1} rows updated.")
    finally:
        conn.close()

    # 2) Reference contains LOAN-<digits> — batched to avoid one huge transaction.
    pat = r"""
        UPDATE journal_entries je
        SET loan_id = sub.lid
        FROM (
            SELECT je2.id,
                   lat.matched_id AS lid
            FROM journal_entries je2
            INNER JOIN LATERAL (
                SELECT l2.id AS matched_id
                FROM loans l2
                WHERE je2.loan_id IS NULL
                  AND COALESCE(je2.reference, '') ~ ('LOAN-' || l2.id::text || '($|[^0-9])')
                ORDER BY l2.id
                LIMIT 1
            ) lat ON TRUE
            WHERE je2.loan_id IS NULL
              AND COALESCE(je2.reference, '') LIKE '%%LOAN-%%'
            LIMIT %s
        ) sub
        WHERE je.id = sub.id
    """
    total2 = 0
    for _loop in range(_MAX_LOOPS):
        conn = psycopg2.connect(get_database_url())
        try:
            with conn.cursor() as cur:
                cur.execute(pat, (_BATCH,))
                n = cur.rowcount
            conn.commit()
            total2 += n
            print(f"Backfill pass 2 (reference LOAN-*): +{n} rows (cumulative {total2}).")
            if n == 0:
                break
        finally:
            conn.close()
    else:
        print("Backfill pass 2 stopped: FARNDACRED_LOAN_BACKFILL_MAX_LOOPS exceeded.")

    # 3) event_id contains LOAN-<id>- (composite ids) — batched.
    pat3 = r"""
        UPDATE journal_entries je
        SET loan_id = sub.lid
        FROM (
            SELECT je2.id,
                   lat.matched_id AS lid
            FROM journal_entries je2
            INNER JOIN LATERAL (
                SELECT l2.id AS matched_id
                FROM loans l2
                WHERE je2.loan_id IS NULL
                  AND strpos(COALESCE(je2.event_id, ''), 'LOAN-' || l2.id::text || '-') > 0
                ORDER BY l2.id
                LIMIT 1
            ) lat ON TRUE
            WHERE je2.loan_id IS NULL
              AND strpos(COALESCE(je2.event_id, ''), 'LOAN-') > 0
            LIMIT %s
        ) sub
        WHERE je.id = sub.id
    """
    total3 = 0
    for _loop in range(_MAX_LOOPS):
        conn = psycopg2.connect(get_database_url())
        try:
            with conn.cursor() as cur:
                cur.execute(pat3, (_BATCH,))
                n = cur.rowcount
            conn.commit()
            total3 += n
            print(f"Backfill pass 3 (event_id LOAN-*-): +{n} rows (cumulative {total3}).")
            if n == 0:
                break
        finally:
            conn.close()
    else:
        print("Backfill pass 3 stopped: FARNDACRED_LOAN_BACKFILL_MAX_LOOPS exceeded.")

    print("Backfill finished. Optionally run: python scripts/run_migration_82.py (if index not created yet).")


if __name__ == "__main__":
    main()
