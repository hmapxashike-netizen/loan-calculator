"""
Fix missing accruals on specific dates (e.g. days that had receipts and were overwritten with zero).

Re-runs EOD for each date to restore regular_interest_daily, default_interest_daily,
penalty_interest_daily (and period_to_date) in loan_daily_state, then reallocates
receipts for those dates so balances stay correct.

Run from project root:
  python scripts/fix_missing_accruals_for_dates.py
  python scripts/fix_missing_accruals_for_dates.py 2025-12-02 2025-12-03
  python scripts/fix_missing_accruals_for_dates.py --repayment-id 4   # force reallocate one receipt
"""
import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    from datetime import datetime, timedelta

    parser = argparse.ArgumentParser(description="Fix missing accruals and reallocate receipts for given dates.")
    parser.add_argument("start", nargs="?", default="2025-12-02", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end", nargs="?", default="2025-12-03", help="End date (YYYY-MM-DD)")
    parser.add_argument("--repayment-id", type=int, metavar="ID", help="Force reallocate this repayment only (EOD for its value_date still runs).")
    args = parser.parse_args()

    if args.repayment_id:
        from loan_management import reallocate_repayment, _connection
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(value_date, payment_date) FROM loan_repayments WHERE id = %s",
                    (args.repayment_id,),
                )
                row = cur.fetchone()
        if not row:
            print(f"Repayment {args.repayment_id} not found.")
            return
        eff_date = row[0]
        if hasattr(eff_date, "date"):
            eff_date = eff_date.date()
        from eod import run_eod_for_date
        print(f"Running EOD for {eff_date} (repayment {args.repayment_id})...")
        run_eod_for_date(eff_date)
        print(f"Reallocating repayment {args.repayment_id}...")
        reallocate_repayment(args.repayment_id)
        print("Done.")
        return

    start = datetime.strptime(args.start.strip(), "%Y-%m-%d").date()
    end = datetime.strptime(args.end.strip(), "%Y-%m-%d").date()
    if start > end:
        start, end = end, start

    from eod import run_eod_for_date
    from loan_management import get_repayment_ids_for_value_date, reallocate_repayment

    current = start
    while current <= end:
        print(f"Running EOD for {current}...")
        result = run_eod_for_date(current)
        print(f"  EOD OK (loans_processed={result.loans_processed})")
        rids = get_repayment_ids_for_value_date(current)
        print(f"  Receipts on {current}: {rids}")
        for rid in rids:
            try:
                reallocate_repayment(rid)
                print(f"  Reallocated receipt {rid}")
            except Exception as e:
                print(f"  Reallocate {rid} failed: {e}")
        current += timedelta(days=1)
    print("Done. Check loan_daily_state: interest_arrears_balance and accruals on those dates should now be correct.")


if __name__ == "__main__":
    main()
