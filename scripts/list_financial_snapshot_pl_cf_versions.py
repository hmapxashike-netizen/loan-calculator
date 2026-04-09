"""
Read-only: list PROFIT_AND_LOSS and CASH_FLOW snapshot headers by calculation_version.

- v2: economic P&L / CF P&L leg (excludes MONTH_END_PNL closing journals).
- v1 and older: captured before that semantics change; numbers may net to ~zero on nominals.

New period closes write v2 for those statement types (see AccountingService.save_period_close_snapshots).
Rebuilding historical snapshots is a data migration (supersede or delete old rows) — not done here.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import get_database_url


def main() -> None:
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT statement_type, period_type, period_end_date, calculation_version,
                       COUNT(*) AS cnt
                FROM financial_statement_snapshots
                WHERE statement_type IN ('PROFIT_AND_LOSS', 'CASH_FLOW')
                GROUP BY statement_type, period_type, period_end_date, calculation_version
                ORDER BY period_end_date DESC, statement_type, calculation_version
                LIMIT 500
                """
            )
            rows = cur.fetchall()
        if not rows:
            print("No PROFIT_AND_LOSS / CASH_FLOW snapshots found.")
            return
        for r in rows:
            print(
                f"{r[0]} {r[1]} end={r[2]} version={r[3]} rows={r[4]}"
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
