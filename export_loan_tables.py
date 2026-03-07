"""
Export loan tables to CSV in the LMS project folder.
Run from the project root:  python export_loan_tables.py

Date range: used for daily state, repayments. Change START_DATE and END_DATE below (YYYY-MM-DD).
Saves files to ./lms_exports/ (created if missing). If a file is open (e.g. in Excel), writes to *_new.csv instead.
"""

import csv
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
EXPORT_DIR = os.path.join(PROJECT_ROOT, "lms_exports")

START_DATE = "2025-10-08"
END_DATE = "2025-12-01"

QUERIES = [
    (
        "loans.csv",
        """
        SELECT
            l.id AS loan_id,
            l.customer_id,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            l.product_code,
            l.loan_type,
            l.status,
            l.principal,
            l.disbursed_amount,
            l.term,
            l.annual_rate,
            l.disbursement_date,
            l.start_date,
            l.end_date,
            l.first_repayment_date,
            l.installment,
            l.created_at AS loan_created_at
        FROM loans l
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        ORDER BY l.id
        """,
        (),
    ),
    (
        "loan_daily_state.csv",
        """
        SELECT
            lds.loan_id,
            l.customer_id,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            l.product_code,
            lds.as_of_date,
            lds.principal_not_due,
            lds.principal_arrears,
            lds.interest_accrued_balance,
            lds.interest_arrears_balance,
            lds.default_interest_balance,
            lds.penalty_interest_balance,
            lds.fees_charges_balance,
            lds.days_overdue,
            lds.total_exposure,
            lds.regular_interest_daily,
            lds.penalty_interest_daily
        FROM loan_daily_state lds
        JOIN loans l ON l.id = lds.loan_id
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        WHERE lds.as_of_date BETWEEN %s AND %s
        ORDER BY lds.loan_id, lds.as_of_date
        """,
        (START_DATE, END_DATE),
    ),
    (
        "loan_daily_state_range.csv",
        """
        SELECT
            lds.loan_id,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            lds.as_of_date,
            lds.principal_not_due,
            lds.principal_arrears,
            lds.interest_accrued_balance,
            lds.interest_arrears_balance,
            lds.total_exposure,
            lds.days_overdue
        FROM loan_daily_state lds
        JOIN loans l ON l.id = lds.loan_id
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        WHERE lds.as_of_date BETWEEN %s AND %s
        ORDER BY lds.loan_id, lds.as_of_date
        """,
        (START_DATE, END_DATE),
    ),
    (
        "loan_repayments.csv",
        """
        SELECT
            lr.id AS repayment_id,
            lr.loan_id,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            lr.amount,
            lr.payment_date,
            lr.value_date,
            lr.reference,
            lr.customer_reference,
            lr.status,
            lr.created_at
        FROM loan_repayments lr
        JOIN loans l ON l.id = lr.loan_id
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        WHERE lr.payment_date BETWEEN %s AND %s
        ORDER BY lr.payment_date DESC, lr.id DESC
        """,
        (START_DATE, END_DATE),
    ),
    (
        "loan_repayment_allocation.csv",
        """
        SELECT
            lr.id AS repayment_id,
            lr.loan_id,
            lr.amount AS repayment_amount,
            lr.payment_date,
            COALESCE(lra.alloc_principal_total, 0) AS alloc_prin_total,
            COALESCE(lra.alloc_interest_total, 0) AS alloc_int_total,
            COALESCE(lra.alloc_fees_total, 0) AS alloc_fees_total,
            COALESCE(lra.alloc_principal_not_due, 0) AS alloc_prin_not_due,
            COALESCE(lra.alloc_principal_arrears, 0) AS alloc_prin_arrears,
            COALESCE(lra.alloc_interest_accrued, 0) AS alloc_int_accrued,
            COALESCE(lra.alloc_interest_arrears, 0) AS alloc_int_arrears,
            COALESCE(lra.alloc_default_interest, 0) AS alloc_default_int,
            COALESCE(lra.alloc_penalty_interest, 0) AS alloc_penalty_int,
            COALESCE(lra.alloc_fees_charges, 0) AS alloc_fees_charges
        FROM loan_repayments lr
        LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
        WHERE lr.payment_date BETWEEN %s AND %s
        ORDER BY lr.payment_date DESC, lr.id DESC
        """,
        (START_DATE, END_DATE),
    ),
    (
        "loans_with_latest_state.csv",
        """
        SELECT
            l.id AS loan_id,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            l.product_code,
            l.loan_type,
            l.principal,
            l.disbursed_amount,
            l.status,
            lds.as_of_date,
            lds.principal_not_due,
            lds.principal_arrears,
            lds.interest_accrued_balance + lds.interest_arrears_balance AS interest_balance,
            lds.penalty_interest_balance,
            lds.total_exposure,
            lds.days_overdue
        FROM loans l
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        LEFT JOIN LATERAL (
            SELECT *
            FROM loan_daily_state
            WHERE loan_id = l.id
              AND as_of_date <= %s
            ORDER BY as_of_date DESC
            LIMIT 1
        ) lds ON true
        WHERE l.status = 'active'
        ORDER BY l.id
        """,
        (END_DATE,),
    ),
]


def main():
    try:
        from config import get_database_url
        import psycopg2
    except ImportError as e:
        print("Error: need config and psycopg2. Run from project root: python export_loan_tables.py", file=sys.stderr)
        raise SystemExit(1) from e

    os.makedirs(EXPORT_DIR, exist_ok=True)
    conn = psycopg2.connect(get_database_url())
    try:
        for filename, query, params in QUERIES:
            path = os.path.join(EXPORT_DIR, filename)
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                colnames = [d[0] for d in cur.description]
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(colnames)
                    w.writerows(rows)
                print(f"  {len(rows):5} rows -> {path}")
            except PermissionError:
                base, ext = os.path.splitext(filename)
                alt_path = os.path.join(EXPORT_DIR, f"{base}_new{ext}")
                with open(alt_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(colnames)
                    w.writerows(rows)
                print(f"  {len(rows):5} rows -> {alt_path} (original in use)")
    finally:
        conn.close()

    print(f"\nExports saved to: {os.path.abspath(EXPORT_DIR)}")
    print("Open the CSV files in Excel or any spreadsheet.")


if __name__ == "__main__":
    main()
