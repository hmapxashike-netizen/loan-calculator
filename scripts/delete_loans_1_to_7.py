"""
Delete loans 1 through 7 and all related data (CASCADE).
Run from project root:  python scripts/delete_loans_1_to_7.py
Use --yes to skip the confirmation prompt.
"""

import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    try:
        from config import get_database_url
        import psycopg2
    except ImportError as e:
        print("Error: need config and psycopg2. Run from project root.", file=sys.stderr)
        raise SystemExit(1) from e

    if "--yes" not in sys.argv and "-y" not in sys.argv:
        reply = input("Delete loans 1–7 and all related data (repayments, schedules, daily state, etc.)? Type 'yes' to confirm: ")
        if reply.strip().lower() != "yes":
            print("Aborted.")
            return

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM loans WHERE id BETWEEN 1 AND 7")
            deleted = cur.rowcount
        conn.commit()
        print(f"Deleted {deleted} loan(s) (IDs 1–7). All related rows were removed by CASCADE.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1) from e
    finally:
        conn.close()


if __name__ == "__main__":
    main()
