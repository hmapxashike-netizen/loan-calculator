"""
Per-day balance-identity diagnostic for loan 10.

For each date in loan_daily_state we compute:
  residual = (opening_total + daily_reg + daily_pen + daily_def - net_alloc_table) - closing_total

where:
  - opening_total  = previous day's total_exposure (or disbursement on day 1)
  - daily_reg/pen/def = loan_daily_state.regular_interest_daily / penalty_interest_daily / default_interest_daily
  - net_alloc_table = SUM of (alloc_principal_total + alloc_interest_total + alloc_fees_total) for that date (net, including negative rows)
  - closing_total  = loan_daily_state.total_exposure

A non-zero residual means the saved daily columns + allocation table do NOT account for the actual balance change on that date.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal

LOAN_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 10

def get_conn():
    return psycopg2.connect(config.get_database_url())

def main():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    # Pull every daily_state row for this loan
    cur.execute("""
        SELECT as_of_date,
               regular_interest_daily, penalty_interest_daily, default_interest_daily,
               total_exposure,
               principal_not_due, principal_arrears,
               interest_accrued_balance, interest_arrears_balance,
               default_interest_balance, penalty_interest_balance, fees_charges_balance
        FROM loan_daily_state
        WHERE loan_id = %s
        ORDER BY as_of_date
    """, (LOAN_ID,))
    rows = cur.fetchall()

    # Pull net allocation per date (positive = cleared, negative = unallocation rows)
    cur.execute("""
        SELECT lr.value_date AS alloc_date,
               SUM(lra.alloc_principal_total + lra.alloc_interest_total + lra.alloc_fees_total) AS net_alloc
        FROM loan_repayment_allocation lra
        JOIN loan_repayments lr ON lr.id = lra.repayment_id
        WHERE lr.loan_id = %s
        GROUP BY lr.value_date
        ORDER BY lr.value_date
    """, (LOAN_ID,))
    alloc_rows = cur.fetchall()
    alloc_by_date = {r["alloc_date"]: Decimal(str(r["net_alloc"] or 0)) for r in alloc_rows}

    # ----------------------------------------------------------------
    D0 = Decimal("0")
    prev_total = D0
    cum_residual = D0
    nonzero_days = []

    print(f"{'Date':<12} {'Open':>10} {'RegD':>8} {'PenD':>7} {'DefD':>7} {'NetAlloc':>10} "
          f"{'ExpClose':>10} {'ActClose':>10} {'Residual':>10}")
    print("-" * 100)

    for r in rows:
        d          = r["as_of_date"]
        reg_d      = Decimal(str(r["regular_interest_daily"]  or 0))
        pen_d      = Decimal(str(r["penalty_interest_daily"]  or 0))
        def_d      = Decimal(str(r["default_interest_daily"]  or 0))
        act_close  = Decimal(str(r["total_exposure"] or 0))
        net_alloc  = alloc_by_date.get(d, D0)

        # First day: opening = 0, first closing comes from disbursement + accruals
        # The disbursement is NOT in daily columns; we detect day-1 by prev_total == 0
        # We use the ACTUAL closing as the base and compare with formula
        exp_close  = prev_total + reg_d + pen_d + def_d - net_alloc

        # On disbursement day (total goes from 0 → ~10000), the disbursement itself
        # is the gap. We detect this as: first date where total_exposure >> 0 and prev==0
        disbursement_delta = act_close - exp_close  # includes disbursement on day 1
        if prev_total == D0 and act_close > 1000:
            # Day 1: opening = 0 after disbursement credit; just record
            residual = D0   # ignore day 1 (disbursement itself is not an accrual)
            prev_total = act_close
            print(f"{d!s:<12} {'(disbursement day - skipped)':>70}")
            continue

        residual = exp_close - act_close
        cum_residual += residual

        if abs(residual) > Decimal("0.001"):
            nonzero_days.append((d, residual))
            flag = " <<<< *** "
        else:
            flag = ""

        print(f"{d!s:<12} {float(prev_total):>10.2f} {float(reg_d):>8.4f} {float(pen_d):>7.4f} "
              f"{float(def_d):>7.4f} {float(net_alloc):>10.4f} {float(exp_close):>10.4f} "
              f"{float(act_close):>10.4f} {float(residual):>10.4f}{flag}")

        prev_total = act_close

    print("-" * 100)
    print(f"\nCumulative residual across all days: {cum_residual}")
    print(f"\nDates with non-zero residual (|residual| > 0.001):")
    for d, res in nonzero_days:
        print(f"  {d}  residual={res}")
    print(f"\nSum of non-zero residuals: {sum(r for _, r in nonzero_days)}")
    print("\nNote: positive residual = balance is LOWER than daily columns predict (4.21 target)")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
