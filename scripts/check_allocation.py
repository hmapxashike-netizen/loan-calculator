"""
Show allocation stored in the DB for receipt(s).

By repayment_id:
  python scripts/check_allocation.py 2

By loan_id (all receipts for that loan):
  python scripts/check_allocation.py --loan 9

By loan_id and date range:
  python scripts/check_allocation.py --loan 9 --from 2025-11-01 --to 2025-12-31
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
    parser = argparse.ArgumentParser(description="Show allocation in DB for receipt(s).")
    parser.add_argument("repayment_id", nargs="?", type=int, help="Repayment ID (e.g. 2)")
    parser.add_argument("--loan", "-l", type=int, help="Loan ID: show all allocations for this loan")
    parser.add_argument("--from", dest="from_date", metavar="DATE", help="Start date (with --loan)")
    parser.add_argument("--to", dest="to_date", metavar="DATE", help="End date (with --loan)")
    args = parser.parse_args()

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    cur = conn.cursor()

    if args.loan is not None:
        # By loan_id (optional date filter)
        sql = """
            SELECT lr.id AS repayment_id, lr.loan_id, lr.amount,
                   COALESCE(lr.value_date, lr.payment_date) AS value_date,
                   lr.status,
                   lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                   lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                   lra.alloc_default_interest, lra.alloc_penalty_interest, lra.alloc_fees_charges,
                   lra.alloc_principal_total, lra.alloc_interest_total, lra.alloc_fees_total
            FROM loan_repayments lr
            LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
            WHERE lr.loan_id = %s
        """
        params = [args.loan]
        if getattr(args, "from_date", None):
            sql += " AND COALESCE(lr.value_date, lr.payment_date) >= %s"
            params.append(_parse_date(args.from_date))
        if getattr(args, "to_date", None):
            sql += " AND COALESCE(lr.value_date, lr.payment_date) <= %s"
            params.append(_parse_date(args.to_date))
        sql += " ORDER BY COALESCE(lr.value_date, lr.payment_date), lr.id"
        cur.execute(sql, params)
    elif args.repayment_id is not None:
        cur.execute(
            """
            SELECT lr.id AS repayment_id, lr.loan_id, lr.amount,
                   COALESCE(lr.value_date, lr.payment_date) AS value_date,
                   lr.status,
                   lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                   lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                   lra.alloc_default_interest, lra.alloc_penalty_interest, lra.alloc_fees_charges,
                   lra.alloc_principal_total, lra.alloc_interest_total, lra.alloc_fees_total
            FROM loan_repayments lr
            LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
            WHERE lr.id = %s
            """,
            (args.repayment_id,),
        )
    else:
        print("Usage: python scripts/check_allocation.py <repayment_id>")
        print("   or: python scripts/check_allocation.py --loan <loan_id> [--from DATE] [--to DATE]")
        conn.close()
        sys.exit(1)

    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No receipt(s) found.")
        return

    # Columns to show
    bucket_cols = [
        "alloc_fees_charges", "alloc_penalty_interest", "alloc_default_interest",
        "alloc_interest_arrears", "alloc_interest_accrued",
        "alloc_principal_arrears", "alloc_principal_not_due",
    ]
    total_cols = ["alloc_principal_total", "alloc_interest_total", "alloc_fees_total"]

    from loan_management import get_loan_daily_state_balances
    for r in rows:
        rid = r.get("repayment_id")
        loan_id = r.get("loan_id")
        amount = float(r.get("amount") or 0)
        vd = r.get("value_date")
        status = r.get("status") or ""
        print(f"\n--- Repayment ID {rid} | loan_id={loan_id} | amount={amount} | value_date={vd} | status={status} ---")
        if r.get("alloc_principal_total") is None and r.get("alloc_interest_total") is None:
            print("  (No allocation row in DB.)")
            continue
        # Show state on value_date (after this receipt's allocation) for sanity check
        if loan_id and vd:
            try:
                vd_date = vd.date() if hasattr(vd, "date") else vd
                bal = get_loan_daily_state_balances(loan_id, vd_date)
                if bal:
                    print("  State on value_date (after allocation):")
                    print(f"    interest_arrears_balance={bal.get('interest_arrears_balance', 0):.2f}  principal_arrears={bal.get('principal_arrears', 0):.2f}")
            except Exception:
                pass
        print("  Buckets:")
        for c in bucket_cols:
            val = float(r.get(c) or 0)
            if val != 0:
                print(f"    {c}: {val}")
        print("  Totals:")
        for c in total_cols:
            print(f"    {c}: {float(r.get(c) or 0)}")
        alloc_sum = sum(float(r.get(c) or 0) for c in total_cols)
        print(f"  Sum(alloc) vs receipt amount: {alloc_sum:.2f} vs {amount:.2f}")

    print()


if __name__ == "__main__":
    main()
