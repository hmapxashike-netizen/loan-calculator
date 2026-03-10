"""
Export loan tables to CSV in the LMS project folder.
Run from project root:  python scripts/export_loan_tables.py

Date range: change START_DATE and END_DATE below (YYYY-MM-DD).
Saves to ./lms_exports/ (at project root). If a file is open (e.g. Excel), writes to *_new.csv.

Repayments/allocation: filtered by value_date (or payment_date) so receipts on END_DATE are included.
Loan daily state: filtered by as_of_date; include END_DATE or later to see impact of receipts on state.

Loan tables covered (all):
  loans, loan_schedules, schedule_lines, loan_repayments, loan_repayment_allocation,
  loan_daily_state, unapplied_funds (ledger-style), allocation_audit_log, loan_modifications, loan_recasts, config.

  unapplied_funds_ledger.csv: signed unapplied ledger linked by repayment_id.
  - credit rows: +unapplied_delta for overpayments
  - liquidation rows: -unapplied_delta with bucket breakdown (principal/interest/penalty/default/fees arrears)

Rates per product: config table stores system_config and product_config:{code} as JSON.
  config.csv = raw config key/value/updated_at.
  config_rates_per_product.csv = flattened default_rates and penalty_rates per product/loan_type for verification.

Rates captured at loan (loan parameters): stored on the loans table.
  loans.csv = includes annual_rate, monthly_rate, loan_metadata (JSON).
  loans_capture_rates.csv = flattened loan_id, annual_rate, monthly_rate, penalty_rate_pct, penalty_quotation from metadata.
"""

import csv
import json
import os
import sys
from datetime import date

# Allow imports from project root when run as scripts/export_loan_tables.py
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

EXPORT_DIR = os.path.join(_PROJECT_ROOT, "lms_exports")

START_DATE = "2025-06-30"
# Include at least the latest receipt value_date so allocation and state impact appear in export
END_DATE = "2026-03-08"

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
            l.monthly_rate,
            l.metadata AS loan_metadata,
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
            lds.default_interest_daily,
            lds.penalty_interest_daily,
            lds.net_allocation AS "net allocation",
            lds.unallocated,
            (COALESCE(lds.net_allocation, 0) + COALESCE(lds.unallocated, 0)) AS credit
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
            lds.default_interest_daily,
            lds.default_interest_balance,
            lds.penalty_interest_daily,
            lds.penalty_interest_balance,
            lds.fees_charges_balance,
            lds.total_exposure,
            lds.days_overdue,
            lds.net_allocation AS "net allocation",
            lds.unallocated,
            (COALESCE(lds.net_allocation, 0) + COALESCE(lds.unallocated, 0)) AS credit
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
            CASE
                WHEN lr.status = 'reversed' AND lr.original_repayment_id IS NOT NULL
                    THEN 'REV-' || LPAD(lr.original_repayment_id::text, 2, '0')
                ELSE LPAD(lr.id::text, 2, '0')
            END AS repayment_code,
            lr.loan_id,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            lr.amount,
            lr.payment_date,
            lr.value_date,
            lr.reference,
            lr.customer_reference,
            lr.status,
            CASE
                WHEN lr.status = 'reversed' AND lr.original_repayment_id IS NOT NULL
                    THEN 'REV-' || LPAD(lr.original_repayment_id::text, 2, '0')
                ELSE LPAD(lr.id::text, 2, '0')
            END AS repayment_key,
            CASE
                WHEN lr.reference = 'Unapplied funds allocation' THEN 'unapplied_liquidation'
                WHEN lr.status = 'reversed' AND lr.amount < 0 THEN 'reversal'
                ELSE 'cash_receipt'
            END AS receipt_type,
            lr.original_repayment_id,
            lr.created_at
        FROM loan_repayments lr
        JOIN loans l ON l.id = lr.loan_id
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        WHERE COALESCE(lr.value_date, lr.payment_date) BETWEEN %s AND %s
          AND NOT (
            COALESCE(lr.reference, '') ILIKE 'Unapplied funds allocation%%'
            OR COALESCE(lr.customer_reference, '') ILIKE 'Unapplied funds allocation%%'
            OR COALESCE(lr.company_reference, '') ILIKE 'Unapplied funds allocation%%'
          )
        ORDER BY COALESCE(lr.value_date, lr.payment_date) DESC, lr.id DESC
        """,
        (START_DATE, END_DATE),
    ),
    (
        "loan_repayment_allocation.csv",
        """
        WITH alloc AS (
            SELECT
                lr.id AS repayment_id,
                lr.loan_id,
                lr.amount AS repayment_amount,
                lr.payment_date,
                lr.value_date,
                CASE
                    WHEN lr.status = 'reversed' AND lr.original_repayment_id IS NOT NULL
                        THEN 'REV-' || LPAD(lr.original_repayment_id::text, 2, '0')
                    ELSE LPAD(lr.id::text, 2, '0')
                END AS repayment_key,
                CASE
                    WHEN lr.reference = 'Unapplied funds allocation' THEN 'unapplied_liquidation'
                    WHEN lr.status = 'reversed' AND lr.amount < 0 THEN 'reversal'
                    ELSE 'cash_receipt'
                END AS receipt_type,
                COALESCE(SUM(lra.alloc_principal_total), 0)      AS alloc_prin_total,
                COALESCE(SUM(lra.alloc_interest_total), 0)       AS alloc_int_total,
                COALESCE(SUM(lra.alloc_fees_total), 0)           AS alloc_fees_total,
                COALESCE(SUM(lra.alloc_principal_not_due), 0)    AS alloc_prin_not_due,
                COALESCE(SUM(lra.alloc_principal_arrears), 0)    AS alloc_prin_arrears,
                COALESCE(SUM(lra.alloc_interest_accrued), 0)     AS alloc_int_accrued,
                COALESCE(SUM(lra.alloc_interest_arrears), 0)     AS alloc_int_arrears,
                COALESCE(SUM(lra.alloc_default_interest), 0)     AS alloc_default_int,
                COALESCE(SUM(lra.alloc_penalty_interest), 0)     AS alloc_penalty_int,
                COALESCE(SUM(lra.alloc_fees_charges), 0)         AS alloc_fees_charges
            FROM loan_repayments lr
            LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
            WHERE COALESCE(lr.value_date, lr.payment_date) BETWEEN %s AND %s
              AND NOT (
                COALESCE(lr.reference, '') ILIKE 'Unapplied funds allocation%%'
                OR COALESCE(lr.customer_reference, '') ILIKE 'Unapplied funds allocation%%'
                OR COALESCE(lr.company_reference, '') ILIKE 'Unapplied funds allocation%%'
              )
            GROUP BY lr.id, lr.loan_id, lr.amount, lr.payment_date, lr.value_date, lr.status, lr.original_repayment_id, lr.reference
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY COALESCE(value_date, payment_date) DESC, repayment_id) AS allocation_id,
            repayment_id,
            loan_id,
            repayment_amount,
            payment_date,
            value_date,
            repayment_key,
            'net_allocation' AS event_type,
            receipt_type,
            alloc_prin_total,
            alloc_int_total,
            alloc_fees_total,
            alloc_prin_not_due,
            alloc_prin_arrears,
            alloc_int_accrued,
            alloc_int_arrears,
            alloc_default_int,
            alloc_penalty_int,
            alloc_fees_charges,
            (repayment_amount - (alloc_prin_total + alloc_int_total + alloc_fees_total)) AS unapplied_amount,
            NULL::timestamptz AS allocation_created_at
        FROM alloc
        ORDER BY COALESCE(value_date, payment_date) DESC, repayment_id
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
    # --- All loan tables (schedules, unapplied, modifications, recasts) ---
    (
        "loan_schedules.csv",
        """
        SELECT ls.id AS schedule_id, ls.loan_id, ls.version, ls.created_at
        FROM loan_schedules ls
        ORDER BY ls.loan_id, ls.version
        """,
        (),
    ),
    (
        "schedule_lines.csv",
        """
        SELECT ls.loan_id, ls.version AS schedule_version, sl.id AS line_id,
               sl."Period", sl."Date", sl.payment, sl.principal, sl.interest,
               sl.principal_balance, sl.total_outstanding
        FROM schedule_lines sl
        JOIN loan_schedules ls ON ls.id = sl.loan_schedule_id
        ORDER BY ls.loan_id, ls.version, sl."Period"
        """,
        (),
    ),
    (
        "unapplied_funds.csv",
        """
        SELECT
            uf.id,
            uf.loan_id,
            uf.repayment_id,
            uf.amount,
            uf.currency,
            uf.value_date,
            uf.entry_type,
            uf.reference,
            uf.allocation_repayment_id,
            uf.source_repayment_id,
            uf.source_unapplied_id,
            CASE
                WHEN uf.entry_type = 'credit' AND uf.reference = 'Overpayment' THEN 'from_receipt'
                WHEN uf.entry_type = 'debit' AND uf.reference = 'Applied to arrears (EOD)' THEN 'to_loan_arrears_eod'
                WHEN uf.entry_type = 'debit' AND uf.reference = 'Applied via recast' THEN 'to_loan_recast'
                ELSE 'other'
            END AS movement_type,
            uf.created_at
        FROM unapplied_funds uf
        WHERE uf.value_date BETWEEN %s AND %s
        ORDER BY uf.loan_id, uf.value_date, uf.id
        """,
        (START_DATE, END_DATE),
    ),
    (
        "allocation_audit_log.csv",
        """
        SELECT aal.id, aal.created_at, aal.event_type, aal.loan_id, aal.as_of_date,
               aal.repayment_id, aal.original_repayment_id, aal.narration, aal.details
        FROM allocation_audit_log aal
        WHERE aal.as_of_date BETWEEN %s AND %s
        ORDER BY aal.created_at
        """,
        (START_DATE, END_DATE),
    ),
    (
        "loan_modifications.csv",
        """
        SELECT lm.id, lm.loan_id, lm.modification_date, lm.previous_schedule_version,
               lm.new_schedule_version, lm.outstanding_interest_treatment,
               lm.new_loan_type, lm.new_term, lm.new_annual_rate, lm.new_principal, lm.created_at, lm.notes
        FROM loan_modifications lm
        ORDER BY lm.loan_id, lm.modification_date
        """,
        (),
    ),
    (
        "loan_recasts.csv",
        """
        SELECT lr.id, lr.loan_id, lr.recast_date, lr.previous_schedule_version,
               lr.new_schedule_version, lr.new_installment, lr.trigger_repayment_id, lr.created_at, lr.notes
        FROM loan_recasts lr
        ORDER BY lr.loan_id, lr.recast_date
        """,
        (),
    ),
    (
        "config.csv",
        """
        SELECT key, value, updated_at
        FROM config
        ORDER BY key
        """,
        (),
    ),
]


def _export_config_rates(conn, export_dir: str) -> None:
    """Export flattened default_rates and penalty_rates per config key and loan_type for verification."""
    PRODUCT_PREFIX = "product_config:"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT key, value FROM config WHERE key = %s OR key LIKE %s ORDER BY key",
            ("system_config", PRODUCT_PREFIX + "%"),
        )
        rows = cur.fetchall()
    out_path = os.path.join(export_dir, "config_rates_per_product.csv")
    colnames = [
        "config_key",
        "product_code",
        "loan_type",
        "default_interest_pct",
        "penalty_pct",
        "penalty_balance_basis",
        "penalty_quotation",
    ]
    flat_rows = []
    for key, value in rows:
        if not value:
            continue
        try:
            cfg = json.loads(value) if isinstance(value, str) else value
        except Exception:
            continue
        product_code = "(system)" if key == "system_config" else key[len(PRODUCT_PREFIX):] if key.startswith(PRODUCT_PREFIX) else key
        default_rates = cfg.get("default_rates") or {}
        penalty_rates = cfg.get("penalty_rates") or {}
        penalty_balance_basis = cfg.get("penalty_balance_basis") or ""
        penalty_quotation = cfg.get("penalty_interest_quotation") or ""
        loan_types = set(default_rates) | set(penalty_rates)
        if not loan_types:
            flat_rows.append([key, product_code, "", "", "", penalty_balance_basis, penalty_quotation])
        for lt in sorted(loan_types):
            dr = default_rates.get(lt) or {}
            interest_pct = dr.get("interest_pct") if isinstance(dr, dict) else ""
            penalty_pct = penalty_rates.get(lt) if lt in penalty_rates else ""
            flat_rows.append([
                key,
                product_code,
                lt,
                interest_pct,
                penalty_pct,
                penalty_balance_basis,
                penalty_quotation,
            ])
    try:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(flat_rows)
        print(f"  {len(flat_rows):5} rows -> {out_path}")
    except PermissionError:
        alt_path = os.path.join(export_dir, "config_rates_per_product_new.csv")
        with open(alt_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(flat_rows)
        print(f"  {len(flat_rows):5} rows -> {alt_path} (original in use)")


def _export_repayment_application(conn, export_dir: str) -> None:
    """
    Export unapplied funds ledger view linked to allocations.

    - For credits into unapplied: one row per receipt showing +unapplied_delta and zero bucket columns.
    - For liquidations from unapplied (event_type='unapplied_funds_allocation'):
      one row per source receipt showing -unapplied_delta and bucket breakdown
      (principal_arrears, interest_arrears, penalty_interest, default_interest, fees).

    No raw receipt amounts are included here; use repayment_id to link back to
    loan_repayments.csv and loan_repayment_allocation.csv.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH alloc_receipts AS (
                SELECT
                    lr.id AS repayment_id,
                    lr.loan_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    COALESCE(SUM(lra.alloc_principal_total), 0) AS alloc_prin_total,
                    COALESCE(SUM(lra.alloc_interest_total), 0) AS alloc_int_total,
                    COALESCE(SUM(lra.alloc_fees_total), 0) AS alloc_fees_total
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE 'Unapplied funds allocation%%'
                  )
                GROUP BY lr.id, lr.loan_id, lr.value_date, lr.payment_date, lr.amount
            ),
            credits_and_reversals AS (
                -- Single source for credits/reversals: unapplied derived from receipt allocation view.
                SELECT
                    ar.repayment_id,
                    CASE
                        WHEN lr.status = 'reversed' AND lr.original_repayment_id IS NOT NULL
                            THEN 'REV-' || LPAD(lr.original_repayment_id::text, 2, '0')
                        ELSE LPAD(ar.repayment_id::text, 2, '0')
                    END AS repayment_key,
                    ar.loan_id,
                    ar.value_date,
                    CASE WHEN (lr.amount - (ar.alloc_prin_total + ar.alloc_int_total + ar.alloc_fees_total)) >= 0
                         THEN 'credit' ELSE 'reversal' END AS entry_kind,
                    NULL::integer AS liquidation_repayment_id,
                    (lr.amount - (ar.alloc_prin_total + ar.alloc_int_total + ar.alloc_fees_total)) AS unapplied_delta,
                    0::numeric AS alloc_prin_arrears,
                    0::numeric AS alloc_int_arrears,
                    0::numeric AS alloc_penalty_int,
                    0::numeric AS alloc_default_int,
                    0::numeric AS alloc_fees_charges
                FROM alloc_receipts ar
                JOIN loan_repayments lr ON lr.id = ar.repayment_id
                WHERE ABS(lr.amount - (ar.alloc_prin_total + ar.alloc_int_total + ar.alloc_fees_total)) > 1e-9
            ),
            liquidations AS (
                -- Liquidations come only from unapplied_funds_allocation (always negative unapplied delta).
                SELECT
                    lra.source_repayment_id AS repayment_id,
                    LPAD(lra.source_repayment_id::text, 2, '0') AS repayment_key,
                    lr.loan_id AS loan_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    'liquidation' AS entry_kind,
                    MIN(lra.repayment_id) AS liquidation_repayment_id,
                    -SUM(COALESCE(lra.alloc_principal_total,0)
                       + COALESCE(lra.alloc_interest_total,0)
                       + COALESCE(lra.alloc_fees_total,0)) AS unapplied_delta,
                    SUM(COALESCE(lra.alloc_principal_arrears,0)) AS alloc_prin_arrears,
                    SUM(COALESCE(lra.alloc_interest_arrears,0)) AS alloc_int_arrears,
                    SUM(COALESCE(lra.alloc_penalty_interest,0)) AS alloc_penalty_int,
                    SUM(COALESCE(lra.alloc_default_interest,0)) AS alloc_default_int,
                    SUM(COALESCE(lra.alloc_fees_charges,0)) AS alloc_fees_charges
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lra.event_type = 'unapplied_funds_allocation'
                  AND (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND lra.source_repayment_id IS NOT NULL
                GROUP BY lra.source_repayment_id, lr.loan_id, (COALESCE(lr.value_date, lr.payment_date))::date
            ),
            ledger AS (
                SELECT * FROM credits_and_reversals
                UNION ALL
                SELECT * FROM liquidations
            )
            SELECT
                l.repayment_id,
                l.repayment_key,
                l.loan_id,
                l.value_date,
                l.entry_kind,
                l.liquidation_repayment_id,
                l.unapplied_delta,
                l.alloc_prin_arrears,
                l.alloc_int_arrears,
                l.alloc_penalty_int,
                l.alloc_default_int,
                l.alloc_fees_charges,
                SUM(l.unapplied_delta) OVER (
                    PARTITION BY l.loan_id
                    ORDER BY l.value_date, l.repayment_id, l.entry_kind
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS unapplied_running_balance
            FROM ledger l
            ORDER BY l.value_date, l.repayment_id, l.entry_kind
            """,
            (START_DATE, END_DATE, START_DATE, END_DATE),
        )
        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]
    out_path = os.path.join(export_dir, "unapplied_funds_ledger.csv")
    try:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(rows)
        print(f"  {len(rows):5} rows -> {out_path}")
    except PermissionError:
        alt_path = os.path.join(export_dir, "unapplied_funds_ledger_new.csv")
        with open(alt_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(rows)
        print(f"  {len(rows):5} rows -> {alt_path} (original in use)")


def _export_loans_capture_rates(conn, export_dir: str) -> None:
    """Export rates captured at loan (loan parameters): annual_rate, monthly_rate, metadata.penalty_rate_pct."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, annual_rate, monthly_rate, metadata FROM loans ORDER BY id"
        )
        rows = cur.fetchall()
    out_path = os.path.join(export_dir, "loans_capture_rates.csv")
    colnames = ["loan_id", "annual_rate", "monthly_rate", "penalty_rate_pct", "penalty_quotation", "currency"]
    flat_rows = []
    for loan_id, annual_rate, monthly_rate, metadata in rows:
        penalty_rate_pct = ""
        penalty_quotation = ""
        currency = ""
        if metadata:
            try:
                md = json.loads(metadata) if isinstance(metadata, str) else metadata
                if isinstance(md, dict):
                    penalty_rate_pct = md.get("penalty_rate_pct", "")
                    penalty_quotation = md.get("penalty_quotation", "")
                    currency = md.get("currency", "")
            except Exception:
                pass
        flat_rows.append([loan_id, annual_rate or "", monthly_rate or "", penalty_rate_pct, penalty_quotation, currency])
    try:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(flat_rows)
        print(f"  {len(flat_rows):5} rows -> {out_path}")
    except PermissionError:
        alt_path = os.path.join(export_dir, "loans_capture_rates_new.csv")
        with open(alt_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(flat_rows)
        print(f"  {len(flat_rows):5} rows -> {alt_path} (original in use)")


def main():
    try:
        from config import get_database_url
        import psycopg2
    except ImportError as e:
        print("Error: need config and psycopg2. Run from project root: python scripts/export_loan_tables.py", file=sys.stderr)
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
        # Flatten config into rates-per-product for verification (default_rates, penalty_rates per loan_type)
        _export_config_rates(conn, EXPORT_DIR)
        # Flatten loan-level capture rates (annual_rate, monthly_rate, metadata.penalty_rate_pct)
        _export_loans_capture_rates(conn, EXPORT_DIR)
        # Unapplied funds ledger: +credits and -liquidations linked to repayment IDs
        _export_repayment_application(conn, EXPORT_DIR)

    finally:
        conn.close()

    print(f"\nExports saved to: {os.path.abspath(EXPORT_DIR)}")
    print("Open the CSV files in Excel or any spreadsheet.")
    print("Rates: see config.csv (raw) and config_rates_per_product.csv (flattened per product/loan_type).")


if __name__ == "__main__":
    main()
