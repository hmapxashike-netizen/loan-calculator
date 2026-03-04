from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from loan_engine import LoanConfig, ScheduleEntry, Loan, WaterfallType


def build_sample_loan() -> Loan:
    # Sample parameters
    principal = Decimal("10000")

    # 10% per month regular interest
    regular_rate_per_month = Decimal("0.10")

    # Absolute penalty/default rates (15% per month on both interest and principal)
    default_absolute_rate_per_month = Decimal("0.15")
    penalty_absolute_rate_per_month = Decimal("0.15")

    config = LoanConfig(
        regular_rate_per_month=regular_rate_per_month,
        default_interest_absolute_rate_per_month=default_absolute_rate_per_month,
        penalty_interest_absolute_rate_per_month=penalty_absolute_rate_per_month,
        grace_period_days=5,
        penalty_on_principal_arrears_only=True,
        waterfall_type=WaterfallType.STANDARD,
    )

    # Single instalment due one month after disbursement
    disbursement = date(2026, 1, 1)
    first_due = date(2026, 1, 31)

    # For demo purposes we assume a simple interest-only instalment
    # with scheduled principal and interest components.
    scheduled_interest = principal * Decimal("0.10")
    scheduled_principal = Decimal("1000")

    schedule = [
        ScheduleEntry(
            period_start=disbursement,
            due_date=first_due,
            principal_component=scheduled_principal,
            interest_component=scheduled_interest,
        )
    ]

    return Loan(
        loan_id="DEMO-1",
        disbursement_date=disbursement,
        original_principal=principal,
        config=config,
        schedule=schedule,
    )


def export_to_excel(
    daily_logs: List[Dict[str, Any]],
    output_path: Path,
) -> None:
    """
    Export the 45-day loan simulation into a single Excel sheet.
    """

    daily_logs_df = pd.DataFrame(daily_logs)

    # Add an Excel formula column for total exposure per day:
    # = principal_not_due + principal_arrears + all interest buckets + fees
    formulas = []
    for idx in range(len(daily_logs_df)):
        excel_row = idx + 2  # 1-based row index + header row
        formulas.append(
            f"=C{excel_row}+D{excel_row}+E{excel_row}+F{excel_row}+G{excel_row}+H{excel_row}+I{excel_row}"
        )
    daily_logs_df["Total Exposure (currency)"] = formulas

    # Overwrite safely if file already exists
    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
        daily_logs_df.to_excel(
            writer, sheet_name="Loan_45_Day_Simulation", index=False
        )


def run_45_day_simulation() -> None:
    loan = build_sample_loan()

    start_date = loan.disbursement_date
    # Simulate two calendar months from disbursement (Jan and Feb 2026)
    # 2026-01-01 through 2026-02-28 inclusive => 59 calendar days
    days_to_simulate = 59

    current = start_date

    # Storage for Excel export
    daily_logs: List[Dict[str, Any]] = []

    for day_offset in range(days_to_simulate):
        loan.process_day(current)

        # Daily log row – capture the state of all key buckets
        daily_logs.append(
            {
                "Date": current,
                "Day Number (1=disbursement)": day_offset + 1,
                "Regular Interest Daily (currency)": float(
                    loan.last_regular_interest_daily
                ),
                "Principal Not Due (currency)": float(loan.principal_not_due),
                "Principal Arrears (currency)": float(loan.principal_arrears),
                "Interest Accrued Balance (currency)": float(
                    loan.interest_accrued_balance
                ),
                "Interest Arrears Balance (currency)": float(loan.interest_arrears),
                "Default Interest Daily (currency)": float(
                    loan.last_default_interest_daily
                ),
                "Default Interest Balance (currency)": float(
                    loan.default_interest_balance
                ),
                "Penalty Interest Daily (currency)": float(
                    loan.last_penalty_interest_daily
                ),
                "Penalty Interest Balance (currency)": float(
                    loan.penalty_interest_balance
                ),
                "Fees & Charges (currency)": float(loan.fees_charges_balance),
                "Days Overdue (days)": loan.days_overdue,
            }
        )

        # Print a few key checkpoints
        if day_offset in (0, 29, 30, 44):
            print(f"Day {day_offset + 1} - {current.isoformat()}")
            print(f"  Principal Not Due   : {loan.principal_not_due}")
            print(f"  Principal Arrears   : {loan.principal_arrears}")
            print(f"  Interest Accrued    : {loan.interest_accrued_balance}")
            print(f"  Interest Arrears    : {loan.interest_arrears}")
            print(f"  Default Interest    : {loan.default_interest_balance}")
            print(f"  Penalty Interest    : {loan.penalty_interest_balance}")
            print(f"  Fees & Charges      : {loan.fees_charges_balance}")
            print(f"  Days Overdue        : {loan.days_overdue}")
            print("-" * 50)

        current += timedelta(days=1)

    output_path = Path("simulation_results.xlsx")
    export_to_excel(daily_logs, output_path)


if __name__ == "__main__":
    run_45_day_simulation()

