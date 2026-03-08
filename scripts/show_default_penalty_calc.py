"""
Show which rate and balance were used to compute default interest and penalty interest
on a given date. Uses engine state at start of day (so for 5 Aug, uses balances at end of 4 Aug)
and config rates. Run from project root:

  python scripts/show_default_penalty_calc.py 2025-08-05
  python scripts/show_default_penalty_calc.py   # defaults to 2025-08-05
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta

# Project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def _parse_date(s: str) -> date:
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {s!r}")


def main() -> None:
    as_of_s = "2025-08-05"
    if len(sys.argv) > 1:
        as_of_s = sys.argv[1]
    as_of_date = _parse_date(as_of_s)
    yesterday = as_of_date - timedelta(days=1)

    from loan_management import (
        load_system_config_from_db,
        get_loan,
        get_loan_daily_state_balances,
        get_loan_daily_state_range,
        _connection,
    )
    from eod import get_engine_state_for_loan_date, _loan_config_from_row, _effective_config_for_loan

    sys_cfg = load_system_config_from_db() or {}

    # Loan IDs that have daily state on the target date
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT loan_id FROM loan_daily_state WHERE as_of_date = %s ORDER BY loan_id",
                (as_of_date,),
            )
            loan_ids = [r[0] for r in cur.fetchall()]

    if not loan_ids:
        print(f"No loan_daily_state rows for {as_of_date}. Try another date or run EOD first.")
        return

    print(f"Default & penalty interest calculation for {as_of_date}")
    print("=" * 100)

    for loan_id in loan_ids:
        loan = get_loan(loan_id)
        if not loan:
            continue
        loan_type = loan.get("loan_type") or "term_loan"

        # Engine state at start of as_of_date (end of yesterday) = balances used for today's default/penalty
        engine_start = get_engine_state_for_loan_date(loan_id, yesterday)
        # Engine state at end of as_of_date = daily amounts computed for as_of_date
        engine_end = get_engine_state_for_loan_date(loan_id, as_of_date)

        if not engine_start or not engine_end:
            print(f"Loan {loan_id}: no engine state (no schedule or date before disbursement?)")
            continue

        effective_cfg = _effective_config_for_loan(loan, sys_cfg)
        config = _loan_config_from_row(loan, effective_cfg)
        default_rate_monthly = float(config.default_interest_absolute_rate_per_month)
        penalty_rate_monthly = float(config.penalty_interest_absolute_rate_per_month)
        default_rate_pct = default_rate_monthly * 100
        penalty_rate_pct = penalty_rate_monthly * 100

        # Rate source: loan capture (metadata) vs config
        md = loan.get("metadata") or {}
        if isinstance(md, str):
            try:
                md = json.loads(md)
            except Exception:
                md = {}
        from_capture = md.get("penalty_rate_pct") if isinstance(md, dict) else None
        rate_source = f"loan capture (metadata): {from_capture}%" if from_capture is not None else f"loan capture: 0% (metadata key missing, system uses 0)"

        # Default: on interest arrears. Correct formula: balance * (rate/100) / 30.
        int_arrears = engine_start["interest_arrears_balance"]
        default_daily_engine = engine_end["default_interest_daily"]
        default_daily_calc = int_arrears * default_rate_monthly / 30.0 if int_arrears else 0.0
        expected_at_2_pct = int_arrears * 0.02 / 30.0 if int_arrears else 0.0

        # Penalty: on principal arrears only or principal arrears + not due
        if config.penalty_on_principal_arrears_only:
            penalty_basis = engine_start["principal_arrears"]
            penalty_basis_label = "principal_arrears"
        else:
            penalty_basis = engine_start["principal_arrears"] + engine_start["principal_not_due"]
            penalty_basis_label = "principal_arrears + principal_not_due"
        penalty_daily_engine = engine_end["penalty_interest_daily"]
        penalty_daily_calc = penalty_basis * penalty_rate_monthly / 30.0 if penalty_basis else 0.0
        expected_penalty_at_2_pct = penalty_basis * 0.02 / 30.0 if penalty_basis else 0.0

        # What we saved in loan_daily_state for as_of_date (can differ due to grace / no_arrears rule)
        saved_balances = get_loan_daily_state_balances(loan_id, as_of_date) or {}
        range_rows = get_loan_daily_state_range(loan_id, as_of_date, as_of_date)
        saved_row = range_rows[0] if range_rows else {}
        saved = {**saved_balances, "default_interest_daily": saved_row.get("default_interest_daily"), "penalty_interest_daily": saved_row.get("penalty_interest_daily"), "days_overdue": saved_balances.get("days_overdue")}

        print(f"\nLoan ID: {loan_id}  (loan_type={loan_type})")
        print(f"  Rate source:       {rate_source}")
        print(f"  At 2% would be:    default_daily={expected_at_2_pct:.2f}, penalty_daily={expected_penalty_at_2_pct:.2f}")
        print("-" * 60)
        print("DEFAULT INTEREST (on interest arrears)")
        print(f"  Rate used:         {default_rate_pct:.2f}% per month")
        print(f"  Balance used:     interest_arrears = {int_arrears:,.2f}  (engine at start of day = end of {yesterday})")
        print(f"  Formula:           interest_arrears * (rate/100) / 30  =  {int_arrears:,.2f} * {default_rate_pct:.2f}% / 30")
        print(f"  Computed daily:    {default_daily_calc:.2f}")
        print(f"  Engine daily:     {default_daily_engine:.2f}")
        saved_default = saved.get("default_interest_daily")
        saved_penalty = saved.get("penalty_interest_daily")
        print(f"  Saved in DB:      {saved_default:.2f}" if isinstance(saved_default, (int, float)) else f"  Saved in DB:      {saved_default}")
        print("PENALTY INTEREST")
        print(f"  Rate used:         {penalty_rate_pct:.2f}% per month")
        print(f"  Basis:             {penalty_basis_label} = {penalty_basis:,.2f}")
        print(f"  Formula:           basis * (rate/100) / 30  =  {penalty_basis:,.2f} * {penalty_rate_pct:.2f}% / 30")
        print(f"  Computed daily:    {penalty_daily_calc:.2f}")
        print(f"  Engine daily:     {penalty_daily_engine:.2f}")
        print(f"  Saved in DB:      {saved_penalty:.2f}" if isinstance(saved_penalty, (int, float)) else f"  Saved in DB:      {saved_penalty}")
        print(f"  (penalty_balance_basis: {'Arrears only' if config.penalty_on_principal_arrears_only else 'Arrears + principal not due'})")
        print(f"  days_overdue (engine at end of {yesterday}): {engine_start.get('days_overdue', 'N/A')}  ->  saved for {as_of_date}: {saved.get('days_overdue', 'N/A')}")
        if from_capture is None:
            print("  NOTE: metadata.penalty_rate_pct missing; system uses 0. To set a rate, update loan metadata then re-run EOD.")

    print("\n" + "=" * 100)
    print("Done.")


if __name__ == "__main__":
    main()
