"""
Diagnose why a loan's daily accrual wasn't updated after running EOD date range.

Run from project root:
  python scripts/diagnose_loan_accrual.py 9 2025-11-30

If loan 9 has no schedule, EOD skips it and the old row is never overwritten.
This script reports schedule status and, if schedule exists, recomputes and saves that day.
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/diagnose_loan_accrual.py <loan_id> <as_of_date>")
        print("  e.g. python scripts/diagnose_loan_accrual.py 9 2025-11-30")
        sys.exit(1)

    from datetime import datetime
    loan_id = int(sys.argv[1])
    as_of_date = datetime.strptime(sys.argv[2].strip(), "%Y-%m-%d").date()

    from loan_management import (
        get_schedule_lines,
        save_loan_daily_state,
        load_system_config_from_db,
    )
    from eod import _get_conn, _fetch_active_loans, _loan_config_from_row, _build_schedule_entries
    from loan_daily_engine import Loan
    from decimal import Decimal

    schedule_rows = get_schedule_lines(loan_id)
    if not schedule_rows:
        print(f"Loan {loan_id} has NO schedule lines in the DB. EOD skips loans without schedules,")
        print("so loan_daily_state is never updated. Add a schedule for this loan and re-run EOD.")
        sys.exit(1)

    print(f"Loan {loan_id} has {len(schedule_rows)} schedule line(s).")

    with _get_conn() as conn:
        loans = _fetch_active_loans(conn)
    loan_row = next((r for r in loans if int(r["id"]) == loan_id), None)
    if not loan_row:
        print(f"Loan {loan_id} not found or not active.")
        sys.exit(1)

    sys_cfg = load_system_config_from_db() or {}
    config = _loan_config_from_row(loan_row, sys_cfg)
    schedule_entries = _build_schedule_entries(loan_row, schedule_rows)

    disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
    if not hasattr(disb_date, "isoformat"):
        disb_date = as_of_date
    principal = Decimal(str(loan_row.get("principal") or loan_row.get("disbursed_amount") or 0))

    engine_loan = Loan(
        loan_id=str(loan_id),
        disbursement_date=disb_date,
        original_principal=principal,
        config=config,
        schedule=schedule_entries,
    )

    from datetime import timedelta
    current = disb_date
    while current <= as_of_date:
        engine_loan.process_day(current)
        current += timedelta(days=1)

    new_daily = float(engine_loan.last_regular_interest_daily)
    print(f"Computed regular_interest_daily for {as_of_date}: {new_daily}")

    # Show which schedule period covers as_of_date
    for e in schedule_entries:
        if e.period_start <= as_of_date < e.due_date:
            days = (e.due_date - e.period_start).days
            print(f"  Period: {e.period_start} to {e.due_date} ({days} days), interest={e.interest_component}, daily={float(e.interest_component)/days:.4f}")
            break
    else:
        print("  No schedule period covers this date (past last due or before first).")

    # Overwrite loan_daily_state for this loan/date so the table is corrected
    save_loan_daily_state(
        loan_id=loan_id,
        as_of_date=as_of_date,
        regular_interest_daily=new_daily,
        principal_not_due=float(engine_loan.principal_not_due),
        principal_arrears=float(engine_loan.principal_arrears),
        interest_accrued_balance=float(engine_loan.interest_accrued_balance),
        interest_arrears_balance=float(engine_loan.interest_arrears),
        default_interest_daily=float(engine_loan.last_default_interest_daily),
        default_interest_balance=float(engine_loan.default_interest_balance),
        penalty_interest_daily=float(engine_loan.last_penalty_interest_daily),
        penalty_interest_balance=float(engine_loan.penalty_interest_balance),
        fees_charges_balance=float(engine_loan.fees_charges_balance),
        days_overdue=engine_loan.days_overdue,
    )
    print(f"Saved to loan_daily_state: regular_interest_daily={new_daily}. Run check script again to confirm.")


if __name__ == "__main__":
    main()
