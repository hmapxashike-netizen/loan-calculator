#!/usr/bin/env python3
"""
Print loan-related tables to stdout for debugging / following a loan through the system.

Run from project root:
  python scripts/print_loan_follow.py
  python scripts/print_loan_follow.py --loan-id 6
  python scripts/print_loan_follow.py --loan-id 6 --days 90
  python scripts/print_loan_follow.py --sections loans daily_state --max-rows 20
  # EOD ran in the past (e.g. Jan 2025): use an explicit range — default is only LAST --days FROM TODAY
  python scripts/print_loan_follow.py --sections daily_state --start-date 2025-01-01 --end-date 2025-06-30

Uses the same DB connection as the app (config.get_database_url / env FARNDACRED_DATABASE_URL or LMS_DATABASE_URL).
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import psycopg2  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

from config import get_database_url  # noqa: E402


def _conn():
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


def _print_section(title: str, rows: list, max_rows: int) -> None:
    print()
    print("=" * 80)
    print(f" {title}")
    print("=" * 80)
    if not rows:
        print("(no rows)")
        return
    df = pd.DataFrame(rows)
    n = len(df)
    if n > max_rows:
        print(f"(showing first {max_rows} of {n} rows)\n")
        df = df.head(max_rows)
    else:
        print(f"({n} rows)\n")
    # Avoid over-wide terminal blow-up: limit columns width in string
    with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 32):
        print(df.to_string(index=False))


def _run(cur, sql: str, params: tuple = ()) -> list:
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def _parse_iso_date(s: str) -> date:
    return date.fromisoformat(s.strip())


def main() -> int:
    p = argparse.ArgumentParser(description="Print loan tables for follow-the-data debugging.")
    p.add_argument("--loan-id", type=int, default=None, help="Focus on one loan (recommended).")
    p.add_argument(
        "--start-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Inclusive start for dated sections (use with --end-date). Overrides --days.",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Inclusive end for dated sections (use with --start-date). Overrides --days.",
    )
    p.add_argument(
        "--days",
        type=int,
        default=90,
        help="Lookback window ending **today** for dated tables when --start-date/--end-date not set. "
        "Default 90 — historical EOD (e.g. Jan 2025) will show **no rows** unless you widen this or set explicit dates.",
    )
    p.add_argument("--max-rows", type=int, default=150, help="Max rows printed per section.")
    p.add_argument(
        "--sections",
        nargs="*",
        default=["loans", "daily_state", "schedules", "repayments", "allocation", "unapplied"],
        choices=["loans", "daily_state", "schedules", "repayments", "allocation", "unapplied", "all"],
        help="Which sections to print. Default: all main tables (use 'all' explicitly same as default list).",
    )
    args = p.parse_args()
    sections = set(args.sections)
    if "all" in sections:
        sections = {"loans", "daily_state", "schedules", "repayments", "allocation", "unapplied"}

    if (args.start_date is None) ^ (args.end_date is None):
        p.error("--start-date and --end-date must be used together (or omit both).")
    if args.start_date and args.end_date:
        start = _parse_iso_date(args.start_date)
        end = _parse_iso_date(args.end_date)
        if start > end:
            p.error("--start-date must be on or before --end-date.")
    else:
        end = date.today()
        start = end - timedelta(days=max(0, args.days))

    print(
        f"# Date filter for daily_state / repayments / allocation / unapplied: {start} .. {end} (inclusive)\n",
        file=sys.stderr,
    )

    with _conn() as conn:
        with conn.cursor() as cur:
            loan_filter = ""
            params_loan: tuple = ()
            if args.loan_id is not None:
                loan_filter = " AND l.id = %s"
                params_loan = (args.loan_id,)

            if "loans" in sections:
                rows = _run(
                    cur,
                    f"""
                    SELECT
                        l.id, l.customer_id, l.loan_type, l.status, l.product_code,
                        l.principal, l.disbursed_amount, l.term,
                        l.disbursement_date, l.end_date, l.installment,
                        COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name
                    FROM loans l
                    LEFT JOIN individuals i ON i.customer_id = l.customer_id
                    LEFT JOIN corporates c ON c.customer_id = l.customer_id
                    WHERE 1=1 {loan_filter}
                    ORDER BY l.id
                    """,
                    params_loan,
                )
                _print_section("loans", rows, args.max_rows)

            if "daily_state" in sections:
                loan_ds = ""
                prm: tuple = (start, end)
                if args.loan_id is not None:
                    loan_ds = " AND lds.loan_id = %s"
                    prm = (start, end, args.loan_id)
                rows = _run(
                    cur,
                    f"""
                    SELECT
                        lds.*,
                        COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name
                    FROM loan_daily_state lds
                    JOIN loans l ON l.id = lds.loan_id
                    LEFT JOIN individuals i ON i.customer_id = l.customer_id
                    LEFT JOIN corporates c ON c.customer_id = l.customer_id
                    WHERE lds.as_of_date BETWEEN %s AND %s {loan_ds}
                    ORDER BY lds.loan_id, lds.as_of_date
                    """,
                    prm,
                )
                _print_section("loan_daily_state", rows, args.max_rows)

            if "schedules" in sections:
                loan_sch = ""
                prm2: tuple = ()
                if args.loan_id is not None:
                    loan_sch = " WHERE ls.loan_id = %s"
                    prm2 = (args.loan_id,)
                rows = _run(
                    cur,
                    f"""
                    SELECT ls.loan_id, ls.version, ls.id AS loan_schedule_id
                    FROM loan_schedules ls
                    {loan_sch}
                    ORDER BY ls.loan_id, ls.version DESC
                    """,
                    prm2,
                )
                _print_section("loan_schedules (headers)", rows, args.max_rows)

                loan_sl = ""
                prm3: tuple = ()
                if args.loan_id is not None:
                    loan_sl = " AND ls.loan_id = %s"
                    prm3 = (args.loan_id,)
                rows = _run(
                    cur,
                    f"""
                    SELECT ls.loan_id, ls.version AS schedule_version,
                           sl."Period", sl."Date", sl.payment, sl.principal, sl.interest,
                           sl.principal_balance, sl.total_outstanding
                    FROM schedule_lines sl
                    JOIN loan_schedules ls ON ls.id = sl.loan_schedule_id
                    WHERE 1=1 {loan_sl}
                    ORDER BY ls.loan_id, ls.version, sl."Period"
                    """,
                    prm3,
                )
                _print_section("schedule_lines", rows, args.max_rows)

            if "repayments" in sections:
                loan_rp = ""
                prm4: tuple = (start, end)
                if args.loan_id is not None:
                    loan_rp = " AND lr.loan_id = %s"
                    prm4 = (start, end, args.loan_id)
                rows = _run(
                    cur,
                    f"""
                    SELECT
                        lr.id, lr.loan_id, lr.amount, lr.payment_date, lr.value_date,
                        lr.status, lr.reference,
                        COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name
                    FROM loan_repayments lr
                    JOIN loans l ON l.id = lr.loan_id
                    LEFT JOIN individuals i ON i.customer_id = l.customer_id
                    LEFT JOIN corporates c ON c.customer_id = l.customer_id
                    WHERE COALESCE(lr.value_date, lr.payment_date) BETWEEN %s AND %s {loan_rp}
                    ORDER BY COALESCE(lr.value_date, lr.payment_date) DESC, lr.id DESC
                    """,
                    prm4,
                )
                _print_section("loan_repayments", rows, args.max_rows)

            if "allocation" in sections:
                loan_al = ""
                prm5: tuple = (start, end)
                if args.loan_id is not None:
                    loan_al = " AND lr.loan_id = %s"
                    prm5 = (start, end, args.loan_id)
                rows = _run(
                    cur,
                    f"""
                    SELECT
                        lra.*,
                        lr.loan_id,
                        COALESCE(lr.value_date, lr.payment_date) AS value_date
                    FROM loan_repayment_allocation lra
                    JOIN loan_repayments lr ON lr.id = lra.repayment_id
                    WHERE COALESCE(lr.value_date, lr.payment_date) BETWEEN %s AND %s {loan_al}
                    ORDER BY value_date DESC, lra.repayment_id
                    """,
                    prm5,
                )
                _print_section("loan_repayment_allocation", rows, args.max_rows)

            if "unapplied" in sections:
                loan_uf = ""
                prm6: tuple = (start, end)
                if args.loan_id is not None:
                    loan_uf = " AND uf.loan_id = %s"
                    prm6 = (start, end, args.loan_id)
                rows = _run(
                    cur,
                    f"""
                    SELECT uf.*
                    FROM unapplied_funds uf
                    WHERE uf.value_date BETWEEN %s AND %s {loan_uf}
                    ORDER BY uf.loan_id, uf.value_date, uf.id
                    """,
                    prm6,
                )
                _print_section("unapplied_funds", rows, args.max_rows)

    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
