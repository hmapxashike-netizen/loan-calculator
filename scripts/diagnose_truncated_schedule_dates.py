"""Report schedule line dates that look like dd-Mon-yyy (3-digit year) and column width."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url

_TRUNC_RE = r"^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{3}$"


def main() -> None:
    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT character_maximum_length::int AS maxlen
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'schedule_lines'
                  AND column_name = 'Date'
                """
            )
            row = cur.fetchone()
            if row and row.get("maxlen") is not None:
                print(f"schedule_lines.\"Date\" column width: VARCHAR({row['maxlen']})")
                if row["maxlen"] < 11:
                    print(
                        "  WARNING: values shorter than 11 can truncate 4-digit years "
                        "(e.g. 01-Jan-2024 -> 01-Jan-202). Run: python scripts/run_migration_76.py"
                    )
            else:
                print("schedule_lines.\"Date\" column not found in information_schema.")

            cur.execute(
                f"""
                SELECT COUNT(*)::int AS n
                FROM schedule_lines
                WHERE "Date" IS NOT NULL AND TRIM("Date") <> ''
                  AND "Date" ~ '{_TRUNC_RE}'
                """
            )
            n_bad = int((cur.fetchone() or {}).get("n") or 0)
            print(f"Rows with 3-digit year pattern (likely truncated): {n_bad}")

            cur.execute(
                f"""
                SELECT COUNT(DISTINCT ls.loan_id)::int AS n_loans
                FROM schedule_lines sl
                JOIN loan_schedules ls ON ls.id = sl.loan_schedule_id
                WHERE sl."Date" IS NOT NULL AND TRIM(sl."Date") <> ''
                  AND sl."Date" ~ '{_TRUNC_RE}'
                """
            )
            n_loans = int((cur.fetchone() or {}).get("n_loans") or 0)
            print(f"Distinct loans affected (any schedule version): {n_loans}")

            cur.execute(
                f"""
                SELECT ls.loan_id, sl."Period", sl."Date"
                FROM schedule_lines sl
                JOIN loan_schedules ls ON ls.id = sl.loan_schedule_id
                WHERE sl."Date" IS NOT NULL
                  AND sl."Date" ~ '{_TRUNC_RE}'
                ORDER BY ls.loan_id, sl."Period"
                LIMIT 30
                """
            )
            sample = cur.fetchall()
            if sample:
                print("Sample (up to 30 lines):")
                for r in sample:
                    print(f"  loan_id={r['loan_id']} Period={r['Period']} Date={r['Date']!r}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
