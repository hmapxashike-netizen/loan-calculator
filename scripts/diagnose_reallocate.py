"""
Print loan_daily_state for a repayment's loan/date before and after reallocating.
Use to verify that the $100 (or any) receipt is reflected in loan_daily_state.

Run from project root:
  python scripts/diagnose_reallocate.py REPAYMENT_ID
  python scripts/diagnose_reallocate.py 4
  python scripts/diagnose_reallocate.py 4 --verbose   # show waterfall config and allocation
"""
import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _get_repayment_row(repayment_id: int):
    from loan_management import _connection
    from psycopg2.extras import RealDictCursor
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, amount,
                       COALESCE(value_date, payment_date) AS eff_date
                FROM loan_repayments WHERE id = %s
                """,
                (repayment_id,),
            )
            return cur.fetchone()


def _get_daily_state(loan_id: int, as_of_date):
    from loan_management import _connection, get_loan_daily_state_balances
    d = get_loan_daily_state_balances(loan_id, as_of_date.date() if hasattr(as_of_date, "date") else as_of_date)
    return d


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("repayment_id", type=int, help="Repayment ID to reallocate")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show waterfall config and allocation")
    args = parser.parse_args()
    repayment_id = args.repayment_id
    verbose = args.verbose

    from loan_management import (
        load_system_config_from_db,
        _get_waterfall_config,
        compute_waterfall_allocation,
        reallocate_repayment,
        get_allocation_totals_for_loan_date,
    )
    from eod import get_engine_state_for_loan_date

    row = _get_repayment_row(repayment_id)
    if not row:
        print(f"Repayment {repayment_id} not found.")
        sys.exit(1)
    loan_id = int(row["loan_id"])
    eff_date = row["eff_date"]
    if hasattr(eff_date, "date"):
        eff_date = eff_date.date()
    amount = float(row["amount"] or 0)
    print(f"Repayment {repayment_id}: loan_id={loan_id}, eff_date={eff_date}, amount={amount}")

    if verbose:
        cfg = load_system_config_from_db() or {}
        try:
            profile_key, bucket_order = _get_waterfall_config(cfg)
            print(f"  Waterfall: profile_key={profile_key}, bucket_order={bucket_order}")
            engine = get_engine_state_for_loan_date(loan_id, eff_date)
            other = get_allocation_totals_for_loan_date(loan_id, eff_date, exclude_repayment_id=repayment_id)
            def _sub(a, b):
                return max(0.0, (a or 0) - (b or 0))
            bal = {
                "principal_not_due": _sub(engine.get("principal_not_due"), other.get("alloc_principal_not_due")),
                "principal_arrears": _sub(engine.get("principal_arrears"), other.get("alloc_principal_arrears")),
                "interest_accrued_balance": _sub(engine.get("interest_accrued_balance"), other.get("alloc_interest_accrued")),
                "interest_arrears_balance": _sub(engine.get("interest_arrears_balance"), other.get("alloc_interest_arrears")),
                "default_interest_balance": _sub(engine.get("default_interest_balance"), other.get("alloc_default_interest")),
                "penalty_interest_balance": _sub(engine.get("penalty_interest_balance"), other.get("alloc_penalty_interest")),
                "fees_charges_balance": _sub(engine.get("fees_charges_balance"), other.get("alloc_fees_charges")),
            }
            alloc, unapplied = compute_waterfall_allocation(amount, bal, bucket_order, profile_key)
            print(f"  Simulated alloc: alloc_interest_arrears={alloc.get('alloc_interest_arrears')}, unapplied={unapplied}")
        except Exception as e:
            print(f"  Waterfall config error: {e}")

    before = _get_daily_state(loan_id, eff_date)
    print(f"Before reallocate: loan_daily_state(loan_id={loan_id}, as_of_date={eff_date})")
    if before:
        print(f"  interest_arrears_balance={before.get('interest_arrears_balance')}")
        print(f"  principal_arrears={before.get('principal_arrears')}")
    else:
        print("  (no row)")

    print("Running reallocate_repayment(...)")
    reallocate_repayment(repayment_id)
    print("Done.")

    after = _get_daily_state(loan_id, eff_date)
    print(f"After reallocate: loan_daily_state(loan_id={loan_id}, as_of_date={eff_date})")
    if after:
        print(f"  interest_arrears_balance={after.get('interest_arrears_balance')}")
    else:
        print("  (no row)")

    if before and after:
        b_ia = before.get("interest_arrears_balance")
        a_ia = after.get("interest_arrears_balance")
        if b_ia == a_ia:
            print("WARNING: interest_arrears_balance did not change after reallocate.")
        else:
            print(f"OK: interest_arrears_balance changed {b_ia} -> {a_ia}")


if __name__ == "__main__":
    main()
