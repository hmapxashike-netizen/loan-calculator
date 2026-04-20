#!/usr/bin/env python3
"""
Remove **operational** data so you can start clean: debtor loans, schedules, repayments,
accrual/EOD state (including loan_daily_state / engine_resume), creditor-facility mirror data,
customer records, uploaded documents, **posted GL journals**, and financial statement snapshots.

Does **not** truncate (preserves configuration / masters):
  products, config, chart of accounts (accounts), users, RBAC, sectors/subsectors, loan_purposes,
  receipt_gl_mapping, disbursement_bank_options, creditor_loan_types (behaviour seeds),
  document_categories / document_classes, provision templates, etc.

There is **no Streamlit session table** in Postgres (sessions are in-memory). Optional
``--clear-security-audit`` clears ``security_audit_log`` (login history).

Usage:
  python scripts/wipe_fresh_start.py --dry-run
  python scripts/wipe_fresh_start.py --yes
  python scripts/wipe_fresh_start.py --yes --reset-business-date 2025-01-01
  python scripts/wipe_fresh_start.py --yes --include-agents --clear-security-audit

Legacy alias (same implementation):
  python scripts/wipe_customers_and_loans.py --yes
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


# Single TRUNCATE … CASCADE: Postgres resolves FK order. Only **real** public tables are applied.
TABLES_FRESH_START = [
    # EOD / audit trail
    "eod_stage_events",
    "eod_runs",
    # Period-close artefacts
    "financial_statement_snapshot_lines",
    "financial_statement_snapshots",
    # GL (lines first is unnecessary with CASCADE; listed for clarity)
    "journal_items",
    "journal_entries",
    "allocation_audit_log",
    # Accrual + engine snapshots (engine_resume JSON lives here)
    "loan_daily_state",
    # Repayments / allocation / unapplied
    "loan_repayment_allocation",
    "loan_repayments",
    "unapplied_funds_ledger",
    "unapplied_funds",
    "loan_recasts",
    "loan_modifications",
    # Debtor schedules
    "schedule_lines",
    "loan_schedules",
    "agent_commission_invoice_lines",
    "agent_commission_invoices",
    "agent_commission_accruals",
    "loan_approval_drafts",
    "loan_application_ref_sequences",
    "loan_applications",
    "loans",
    # Creditor mirror (migration 84 + 90 split); leaves creditor_loan_types seeded
    "creditor_schedule_lines",
    "creditor_loan_schedules",
    "creditor_loan_daily_state",
    "creditor_repayment_allocation",
    "creditor_repayments",
    "creditor_unapplied_funds",
    "creditor_drawdowns",
    "creditor_facilities",
    "creditor_loans",
    "creditor_counterparties",
    # Uploaded files linked to entities (customers/loans via entity_type + entity_id)
    "documents",
    # Draft queues (names vary by migration — absent tables are skipped)
    "customer_documents",
    "customer_agent_approval_drafts",
    "customer_approval_drafts",
    # Borrowers
    "corporate_shareholders",
    "corporate_directors",
    "corporate_contact_persons",
    "corporates",
    "individuals",
    "customers",
    # Closed-period flags (fresh GL — no legacy closed periods)
    "financial_periods",
]

OPTIONAL_AGENTS = [
    "agents",
]

OPTIONAL_SECURITY_AUDIT = [
    "security_audit_log",
]


def _existing_base_tables(cur, table_names: list[str]) -> list[str]:
    """Return names that exist as ordinary or partitioned tables in ``public``."""
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
        if row and row[0] in ("r", "p"):
            existing.append(tbl)
    return existing


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Truncate operational loan/customer/GL/EOD data for a clean slate.",
    )
    p.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Confirm destructive truncate (required unless --dry-run).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="List tables that would be truncated; no DB changes.",
    )
    p.add_argument(
        "--reset-business-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="After wipe, set system_business_config.current_system_date (if table exists).",
    )
    p.add_argument(
        "--include-agents",
        action="store_true",
        help="Also truncate ``agents`` (loan officer / broker directory).",
    )
    p.add_argument(
        "--clear-security-audit",
        action="store_true",
        help="Also truncate ``security_audit_log`` (login / auth audit).",
    )
    return p.parse_args()


def _validate_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid --reset-business-date '{value}'. Use YYYY-MM-DD.") from exc


def main() -> None:
    args = _parse_args()
    if not args.dry_run and not args.yes:
        print("Refusing to wipe without --yes (or use --dry-run).")
        raise SystemExit(2)

    tables = list(TABLES_FRESH_START)
    if args.include_agents:
        tables.extend(OPTIONAL_AGENTS)
    if args.clear_security_audit:
        tables.extend(OPTIONAL_SECURITY_AUDIT)

    target_date: dt.date | None = None
    if args.reset_business_date:
        target_date = _validate_date(args.reset_business_date)

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            existing = _existing_base_tables(cur, tables)
            missing = [t for t in tables if t not in existing]

            print("Tables to truncate (exist in DB):")
            for t in existing:
                print(f"  - {t}")
            if missing:
                print("Skipped (not present):")
                for t in missing:
                    print(f"  - {t}")

            if args.dry_run:
                print("\nDry run - no changes.")
                if target_date:
                    print(f"Would set system business date to: {target_date.isoformat()}")
                return

            if existing:
                qn = ", ".join(f'"{t}"' for t in existing)
                cur.execute(f"TRUNCATE TABLE {qn} RESTART IDENTITY CASCADE")
                print(f"\nTruncated {len(existing)} table(s) with RESTART IDENTITY CASCADE.")

            cur.execute("SELECT to_regclass('public.system_business_config')")
            if cur.fetchone()[0] is not None and target_date is not None:
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
                print(f"system_business_config.current_system_date set to {target_date.isoformat()}.")

        conn.commit()
        print("Done. Operational data cleared - masters (COA, products, users, config) kept.")
    except Exception as exc:
        conn.rollback()
        print(f"Wipe failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    finally:
        conn.close()


if __name__ == "__main__":
    main()
