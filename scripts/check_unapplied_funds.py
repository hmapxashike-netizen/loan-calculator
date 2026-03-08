"""
Show unapplied funds (suspense) in the DB.

Run from project root:
  python scripts/check_unapplied_funds.py
  python scripts/check_unapplied_funds.py --loan 9
  python scripts/check_unapplied_funds.py --status pending
  python scripts/check_unapplied_funds.py --from 2025-11-01 --to 2025-12-31
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _parse_date(s: str):
    from datetime import datetime
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid date: {s!r}. Use YYYY-MM-DD or DD.MM.YYYY.")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Show unapplied funds in DB.")
    parser.add_argument("--loan", "-l", type=int, help="Filter by loan_id")
    parser.add_argument("--status", "-s", default="pending", help="Filter by status (default: pending)")
    parser.add_argument("--from", dest="from_date", metavar="DATE", help="Value date >= DATE")
    parser.add_argument("--to", dest="to_date", metavar="DATE", help="Value date <= DATE")
    parser.add_argument("--all", action="store_true", help="Show all statuses (ignore --status)")
    args = parser.parse_args()

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    cur = conn.cursor()

    sql = """
        SELECT id, loan_id, repayment_id, amount, currency, value_date, status, created_at, applied_at, notes
        FROM unapplied_funds
        WHERE 1=1
    """
    params = []
    if args.loan is not None:
        sql += " AND loan_id = %s"
        params.append(args.loan)
    if not getattr(args, "all", False):
        sql += " AND status = %s"
        params.append(args.status or "pending")
    if getattr(args, "from_date", None):
        sql += " AND value_date >= %s"
        params.append(_parse_date(args.from_date))
    if getattr(args, "to_date", None):
        sql += " AND value_date <= %s"
        params.append(_parse_date(args.to_date))
    sql += " ORDER BY loan_id, value_date, id"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No unapplied funds rows found.")
        return

    print(f"Unapplied funds ({len(rows)} row(s))")
    print("-" * 80)
    for r in rows:
        print(f"  id={r['id']}  loan_id={r['loan_id']}  repayment_id={r['repayment_id']}  amount={r['amount']}  value_date={r['value_date']}  status={r['status']}")
    print("-" * 80)
    total = sum(float(r.get("amount") or 0) for r in rows)
    print(f"Total amount: {total:.2f}")


if __name__ == "__main__":
    main()
