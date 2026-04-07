"""
Truncate customer and loan transactional data for a clean slate.

Does not touch: products, chart of accounts, users, sectors, system config, etc.

Usage:
  python scripts/wipe_customers_and_loans.py          # prompts for YES
  python scripts/wipe_customers_and_loans.py --yes    # non-interactive
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url

# Order does not need to be FK-perfect when using CASCADE; list is explicit for clarity.
TABLES_TO_WIPE = [
    "eod_runs",
    "journal_entries",
    "allocation_audit_log",
    "statement_events",
    "loan_daily_state",
    "loan_repayment_allocation",
    "loan_repayments",
    "unapplied_funds_ledger",
    "unapplied_funds",
    "loan_recasts",
    "loan_modifications",
    "schedule_lines",
    "loan_schedules",
    "loan_approval_drafts",
    "loans",
    "customer_documents",
    "customer_approval_drafts",
    "customer_addresses",
    "corporate_shareholders",
    "corporate_directors",
    "corporate_contact_persons",
    "corporates",
    "individuals",
    "agents",
    "customers",
]


def main() -> None:
    auto = "--yes" in sys.argv or "-y" in sys.argv
    if not auto:
        confirm = input(
            "This will DELETE all customers, loans, repayments, unapplied funds, "
            "loan drafts, and related GL journal rows. Type YES to confirm: "
        )
        if confirm != "YES":
            print("Aborted.")
            return

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """
            )
            existing = {r[0] for r in cur.fetchall()}
            valid = [t for t in TABLES_TO_WIPE if t in existing]
            if not valid:
                print("No matching tables found to truncate.")
                return
            tables_sql = ", ".join(valid)
            print(f"Truncating: {tables_sql}")
            cur.execute(f"TRUNCATE TABLE {tables_sql} CASCADE")
        conn.commit()
        print("Done. Customers and loan data cleared.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
