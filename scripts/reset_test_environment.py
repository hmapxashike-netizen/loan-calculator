"""
Reset transactional FarndaCred data for testing.

This script removes loan/GL operational data and resets the system business date.
It keeps master/configuration data (users, products, chart of accounts, templates)
unless optional flags are passed.

Examples:
  python scripts/reset_test_environment.py --dry-run
  python scripts/reset_test_environment.py --confirm
  python scripts/reset_test_environment.py --confirm --system-date 2025-06-01
  python scripts/reset_test_environment.py --confirm --include-customers
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

import psycopg2

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import get_database_url


BASE_TRANSACTION_TABLES = [
    "eod_stage_events",
    "eod_runs",
    "financial_statement_snapshot_lines",
    "financial_statement_snapshots",
    "allocation_audit_log",
    "loan_daily_state",
    "loan_repayment_allocation",
    "unapplied_funds_ledger",
    "unapplied_funds",
    "loan_recasts",
    "loan_modifications",
    "loan_repayments",
    "schedule_lines",
    "loan_schedules",
    "loans",
    "journal_items",
    "journal_entries",
]

OPTIONAL_CUSTOMER_TABLES = [
    "corporate_shareholders",
    "corporate_directors",
    "corporate_contact_persons",
    "customer_addresses",
    "corporates",
    "individuals",
    "customers",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset FarndaCred transactional test data.")
    parser.add_argument(
        "--system-date",
        default="2025-01-01",
        help="System business date to set after reset (YYYY-MM-DD). Default: 2025-01-01",
    )
    parser.add_argument(
        "--include-customers",
        action="store_true",
        help="Also wipe customer records and related tables.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be truncated/reset without changing data.",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required to perform destructive reset (ignored in dry-run).",
    )
    return parser.parse_args()


def _validate_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid --system-date '{value}'. Use YYYY-MM-DD.") from exc


def _existing_tables(cur, table_names: list[str]) -> list[str]:
    """
    Return only objects that are real tables in Postgres.

    Using `to_regclass` alone will also match views, which then fail on
    `TRUNCATE TABLE`. We filter by pg_class.relkind.
    """
    existing: list[str] = []
    for tbl in table_names:
        cur.execute(
            """
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
            """,
            ("public", tbl),
        )
        row = cur.fetchone()
        # relkind:
        #  - r = ordinary table
        #  - p = partitioned table
        if row and row[0] in ("r", "p"):
            existing.append(tbl)
    return existing


def main() -> None:
    args = _parse_args()
    target_date = _validate_date(args.system_date)

    tables = list(BASE_TRANSACTION_TABLES)
    if args.include_customers:
        tables.extend(OPTIONAL_CUSTOMER_TABLES)

    if args.dry_run:
        print("DRY RUN: no data will be changed.")
    elif not args.confirm:
        print("Refusing to run destructive reset without --confirm.")
        print("Tip: run --dry-run first, then rerun with --confirm.")
        raise SystemExit(2)

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            existing = _existing_tables(cur, tables)
            missing = [t for t in tables if t not in existing]

            print("Tables targeted for truncate:")
            for t in existing:
                print(f"  - {t}")
            if missing:
                print("Tables not found (skipped):")
                for t in missing:
                    print(f"  - {t}")

            print(f"System business date target: {target_date.isoformat()}")

            if args.dry_run:
                return

            if existing:
                truncate_sql = "TRUNCATE TABLE " + ", ".join(existing) + " RESTART IDENTITY CASCADE"
                cur.execute(truncate_sql)

            cur.execute("SELECT to_regclass('public.system_business_config')")
            has_sys_cfg = cur.fetchone()[0] is not None
            if has_sys_cfg:
                cur.execute(
                    """
                    INSERT INTO system_business_config (id, current_system_date, eod_auto_run_time, is_auto_eod_enabled)
                    VALUES (1, %s, '23:00:00', FALSE)
                    ON CONFLICT (id) DO UPDATE
                    SET current_system_date = EXCLUDED.current_system_date,
                        updated_at = NOW()
                    """,
                    (target_date,),
                )
            conn.commit()
            print("Reset completed successfully.")
    except Exception as exc:
        conn.rollback()
        print(f"Reset failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    finally:
        conn.close()


if __name__ == "__main__":
    main()
