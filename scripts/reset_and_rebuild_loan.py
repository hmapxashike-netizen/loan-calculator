"""
Delete loan 10, re-create it with identical parameters and schedule,
then replay all EOD runs and customer receipts through 2025-10-31.

Run from project root:
    python scripts/reset_and_rebuild_loan.py

After completion, runs the per-day identity diagnostic automatically.
"""
import os, sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import psycopg2
import config
from loan_management import (
    save_loan,
    record_repayment,
    reverse_repayment,
    reallocate_repayment,
    get_repayment_ids_for_value_date,
)
from eod import run_eod_for_date

# ─────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────
OLD_LOAN_ID   = 15
CUSTOMER_ID   = 6
AGENT_ID      = 2
RM_ID         = "4383506e-88dd-4973-9276-f2cf7ece2ad8"
START_DATE    = date(2025, 6, 30)
END_DATE      = date(2025, 10, 31)


# ─────────────────────────────────────────────────────────────────
# Schedule (13 rows: period 0 = disbursement, 1-12 = instalments)
# ─────────────────────────────────────────────────────────────────
SCHEDULE_ROWS = [
    {"Period": 0,  "Date": "30-Jun-2025", "Payment":    0.00, "Principal":    0.00, "Interest":    0.00, "Principal Balance": 10000.00, "Total Outstanding": 10000.00},
    {"Period": 1,  "Date": "31-Jul-2025", "Payment": 1481.22, "Principal":  447.88, "Interest": 1033.33, "Principal Balance":  9552.12, "Total Outstanding":  9552.12},
    {"Period": 2,  "Date": "31-Aug-2025", "Payment": 1481.22, "Principal":  494.17, "Interest":  987.05, "Principal Balance":  9057.95, "Total Outstanding":  9057.95},
    {"Period": 3,  "Date": "30-Sep-2025", "Payment": 1481.22, "Principal":  575.42, "Interest":  905.79, "Principal Balance":  8482.53, "Total Outstanding":  8482.53},
    {"Period": 4,  "Date": "31-Oct-2025", "Payment": 1481.22, "Principal":  604.69, "Interest":  876.53, "Principal Balance":  7877.83, "Total Outstanding":  7877.83},
    {"Period": 5,  "Date": "30-Nov-2025", "Payment": 1481.22, "Principal":  693.43, "Interest":  787.78, "Principal Balance":  7184.40, "Total Outstanding":  7184.40},
    {"Period": 6,  "Date": "31-Dec-2025", "Payment": 1481.22, "Principal":  738.83, "Interest":  742.39, "Principal Balance":  6445.57, "Total Outstanding":  6445.57},
    {"Period": 7,  "Date": "31-Jan-2026", "Payment": 1481.22, "Principal":  815.18, "Interest":  666.04, "Principal Balance":  5630.39, "Total Outstanding":  5630.39},
    {"Period": 8,  "Date": "28-Feb-2026", "Payment": 1481.22, "Principal":  955.71, "Interest":  525.50, "Principal Balance":  4674.68, "Total Outstanding":  4674.68},
    {"Period": 9,  "Date": "31-Mar-2026", "Payment": 1481.22, "Principal":  998.17, "Interest":  483.05, "Principal Balance":  3676.51, "Total Outstanding":  3676.51},
    {"Period": 10, "Date": "30-Apr-2026", "Payment": 1481.22, "Principal": 1113.57, "Interest":  367.65, "Principal Balance":  2562.94, "Total Outstanding":  2562.94},
    {"Period": 11, "Date": "31-May-2026", "Payment": 1481.22, "Principal": 1216.38, "Interest":  264.84, "Principal Balance":  1346.56, "Total Outstanding":  1346.56},
    {"Period": 12, "Date": "30-Jun-2026", "Payment": 1481.22, "Principal": 1346.56, "Interest":  134.66, "Principal Balance":     0.00, "Total Outstanding":     0.00},
]


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────
def log(msg: str):
    print(msg, flush=True)


def eod_only(d: date):
    result = run_eod_for_date(d, skip_reallocate_after_reversals=True)
    log(f"  EOD {d}  loans_processed={result.loans_processed}")


def eod_and_allocate(d: date):
    """Run EOD for d (skip auto-realloc), then manually reallocate every
    posted non-system receipt for that date in arrival order."""
    result = run_eod_for_date(d, skip_reallocate_after_reversals=True)
    log(f"  EOD {d}  loans_processed={result.loans_processed}")
    rids = get_repayment_ids_for_value_date(d)
    for rid in rids:
        reallocate_repayment(rid)
        log(f"    allocated receipt {rid}")


def capture(loan_id: int, amount: float, ref: str, value_date: date) -> int:
    rid = record_repayment(
        loan_id=loan_id,
        amount=amount,
        payment_date=value_date,
        value_date=value_date,
        reference=ref,
    )
    log(f"  captured receipt {rid}: {amount:.2f}  ref='{ref}'  date={value_date}")
    return rid


def reverse(rid: int) -> int:
    rev_id = reverse_repayment(rid)
    log(f"  reversed receipt {rid}  -> reversal row {rev_id}")
    return rev_id


# ─────────────────────────────────────────────────────────────────
# Step 1: Delete old loan
# ─────────────────────────────────────────────────────────────────
def delete_loan(loan_id: int):
    conn = psycopg2.connect(config.get_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM loans WHERE id = %s", (loan_id,))
            if cur.fetchone() is None:
                log(f"Loan {loan_id} not found – skipping delete.")
                return
            cur.execute("DELETE FROM loans WHERE id = %s", (loan_id,))
        conn.commit()
        log(f"Deleted loan {loan_id} and all related data.")
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────
# Step 2: Create new loan
# ─────────────────────────────────────────────────────────────────
def create_loan() -> int:
    schedule_df = pd.DataFrame(SCHEDULE_ROWS)
    details = {
        "principal":            10000.00,
        "disbursed_amount":     10000.00,
        "term":                 12,
        "annual_rate":          1.20,
        "drawdown_fee":         0.03,
        "arrangement_fee":      0.02,
        "admin_fee":            None,
        "disbursement_date":    START_DATE,
        "start_date":           START_DATE,
        "end_date":             date(2026, 6, 30),
        "first_repayment_date": date(2025, 7, 31),
        "installment":          1481.22,
        "grace_type":           "No grace period",
        "moratorium_months":    0,
        "payment_timing":       "anniversary",
        "status":               "active",
        "agent_id":             AGENT_ID,
        "relationship_manager_id": RM_ID,
        "penalty_rate_pct":     5.0,
        "penalty_quotation":    "Absolute Rate",
        "currency":             "USD",
    }
    loan_id = save_loan(
        customer_id=CUSTOMER_ID,
        loan_type="Term Loan",
        details=details,
        schedule_df=schedule_df,
        schedule_version=1,
        product_code="TERM-USD",
    )
    log(f"Created loan id={loan_id}")
    return loan_id


# ─────────────────────────────────────────────────────────────────
# Step 3: Run EOD range
# ─────────────────────────────────────────────────────────────────
def eod_range(start: date, end: date, label: str):
    log(f"\n-- {label} ({start} -> {end}) --")
    d = start
    while d <= end:
        eod_only(d)
        d += timedelta(days=1)


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    # -- 1. Delete --------------------------------------------------
    log("\n=== STEP 1: Delete loan 10 ===")
    delete_loan(OLD_LOAN_ID)

    # -- 2. Create --------------------------------------------------
    log("\n=== STEP 2: Create new loan ===")
    loan_id = create_loan()

    # -- 3. Jun 30 -> Jul 30: EOD only -----------------------------
    eod_range(date(2025, 6, 30), date(2025, 7, 30), "STEP 3  Jun 30 -> Jul 30 (EOD, no receipts)")

    # -- 4. Jul 31: installment 1 (1481.22) then EOD ---------------
    log("\n-- STEP 4  Jul 31: capture 1481.22 then EOD --")
    capture(loan_id, 1481.22, "Cabs account", date(2025, 7, 31))
    eod_and_allocate(date(2025, 7, 31))

    # -- 5. Aug 1 -> Sep 30: EOD only (2 missed instalments) -------
    eod_range(date(2025, 8, 1), date(2025, 9, 30), "STEP 5  Aug 1 -> Sep 30 (EOD, no receipts)")

    # -- 6. Oct 1 -> Oct 20: EOD only -------------------------------
    eod_range(date(2025, 10, 1), date(2025, 10, 20), "STEP 6  Oct 1 -> Oct 20 (EOD, no receipts)")

    # -- 7. Oct 21: two receipts then EOD ---------------------------
    log("\n-- STEP 7  Oct 21: capture 1000 + 1100 then EOD --")
    capture(loan_id, 1000.00, "rec 123",  date(2025, 10, 21))
    capture(loan_id, 1100.00, "rec452",   date(2025, 10, 21))
    eod_and_allocate(date(2025, 10, 21))

    # -- 8. Oct 22: capture 500, reverse, capture 600, then EOD ----
    log("\n-- STEP 8  Oct 22: 500 reversed, 600, EOD --")
    rid_500 = capture(loan_id, 500.00, "rec 123", date(2025, 10, 22))
    # Allocate before reversing (reverse_repayment requires an allocation)
    eod_only(date(2025, 10, 22))
    reallocate_repayment(rid_500)
    log(f"    allocated receipt {rid_500}")
    reverse(rid_500)
    capture(loan_id, 600.00, "rec 562", date(2025, 10, 22))
    eod_and_allocate(date(2025, 10, 22))

    # -- 9. Oct 23 -> Oct 27: EOD only ------------------------------
    eod_range(date(2025, 10, 23), date(2025, 10, 27), "STEP 9  Oct 23 -> Oct 27 (EOD, no receipts)")

    # -- 10. Oct 28: capture 10, reverse, capture 788.33, EOD -------
    log("\n-- STEP 10  Oct 28: 10 reversed, 788.33, EOD --")
    rid_10 = capture(loan_id, 10.00, "l", date(2025, 10, 28))
    eod_only(date(2025, 10, 28))
    reallocate_repayment(rid_10)
    log(f"    allocated receipt {rid_10}")
    reverse(rid_10)
    capture(loan_id, 788.33, "p", date(2025, 10, 28))
    eod_and_allocate(date(2025, 10, 28))

    # -- 11. Oct 29: EOD only ---------------------------------------
    eod_range(date(2025, 10, 29), date(2025, 10, 29), "STEP 11  Oct 29 (EOD only)")

    # -- 12. Oct 30: 1481.22 then EOD --------------------------------
    log("\n-- STEP 12  Oct 30: capture 1481.22 then EOD --")
    capture(loan_id, 1481.22, "A", date(2025, 10, 30))
    eod_and_allocate(date(2025, 10, 30))

    # -- 13. Oct 31: EOD (due date, engine auto-applies unapplied) -
    log("\n-- STEP 13  Oct 31: EOD (due date) --")
    eod_and_allocate(date(2025, 10, 31))

    # -- 14. Verify --------------------------------------------------
    log("\n=== STEP 14: Per-day identity verification ===")
    sys.argv = [sys.argv[0], str(loan_id)]   # pass loan_id as arg to diagnostic
    import runpy
    import pathlib
    runpy.run_path(
        str(pathlib.Path(__file__).parent / "diagnose_identity_perday.py"),
        run_name="__main__",
    )

    log("\n=== ALL DONE ===")
    log(f"New loan id = {loan_id}")


if __name__ == "__main__":
    main()
