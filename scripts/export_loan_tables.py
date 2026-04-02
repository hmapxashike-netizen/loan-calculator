"""
Export loan tables to CSV in the FarndaCred project folder.
Run from project root:
  python scripts/export_loan_tables.py
  python scripts/export_loan_tables.py --start-date 2025-01-01 --end-date 2025-06-30

Date range: pass --start-date and --end-date (YYYY-MM-DD), or edit DEFAULT_* below.
Saves to ./farndacred_exports/ (at project root). If a file is open (e.g. Excel), writes to *_new.csv.

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

import argparse
import csv
import json
import math
import os
import sys
from datetime import date
from decimal import Decimal, InvalidOperation

# Allow imports from project root when run as scripts/export_loan_tables.py
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

EXPORT_DIR = os.path.join(_PROJECT_ROOT, "farndacred_exports")


def _decimal_to_plain_string(d: Decimal) -> str:
    """
    Fixed-point decimal text: no scientific notation, no exponent.
    Trims trailing zeros after the decimal point.
    """
    if d.is_nan():
        return ""
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def _format_csv_value(v):
    """
    Format cell for CSV: numbers as plain decimal strings (no exponents, no leading ').
    Excel-friendly: avoids 1.23E+10 style and stray text markers on numeric strings.
    """
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(v).lower()
    # int (not bool)
    if isinstance(v, int):
        return str(v)

    if isinstance(v, str):
        s = v.strip()
        # Strip Excel-style leading apostrophe used to force text
        if s.startswith("'") and len(s) > 1:
            s = s[1:].strip()
        try:
            d = Decimal(s)
            return _decimal_to_plain_string(d)
        except InvalidOperation:
            return s

    if isinstance(v, Decimal):
        return _decimal_to_plain_string(v)

    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return ""
        try:
            return _decimal_to_plain_string(Decimal(str(v)))
        except InvalidOperation:
            return str(v)

    try:
        d = Decimal(str(v))
        return _decimal_to_plain_string(d)
    except (InvalidOperation, TypeError, ValueError):
        pass

    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _format_csv_row(row):
    """Format a row of values for CSV export."""
    return [_format_csv_value(cell) for cell in row]


# Defaults when --start-date / --end-date are omitted
DEFAULT_START_DATE = "2025-06-30"
# Include at least the latest receipt value_date so allocation and state impact appear in export
DEFAULT_END_DATE = "2026-03-08"


def build_export_queries(start_date: str, end_date: str) -> list:
    """Build (filename, sql, params) list for the given inclusive date range."""
    return [
    (
        "loans.csv",
        """
        SELECT
            l.*,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name
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
            lds.*,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            l.product_code,
            (COALESCE(lds.net_allocation, 0) + COALESCE(lds.unallocated, 0)) AS credit
        FROM loan_daily_state lds
        JOIN loans l ON l.id = lds.loan_id
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        WHERE lds.as_of_date BETWEEN %s AND %s
        ORDER BY lds.loan_id, lds.as_of_date
        """,
        (start_date, end_date),
    ),
    (
        "loan_daily_state_range.csv",
        """
        SELECT
            lds.*,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            (COALESCE(lds.net_allocation, 0) + COALESCE(lds.unallocated, 0)) AS credit
        FROM loan_daily_state lds
        JOIN loans l ON l.id = lds.loan_id
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        WHERE lds.as_of_date BETWEEN %s AND %s
        ORDER BY lds.loan_id, lds.as_of_date
        """,
        (start_date, end_date),
    ),
    (
        "loan_repayments.csv",
        """
        SELECT
            lr.*,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            CASE
                WHEN lr.status = 'reversed' AND lr.original_repayment_id IS NOT NULL
                    THEN 'REV-' || lr.original_repayment_id::text
                ELSE lr.id::text
            END AS repayment_key,
            CASE
                WHEN lr.status = 'reversed' AND lr.amount < 0 THEN 'reversal'
                ELSE 'cash_receipt'
            END AS receipt_type
        FROM loan_repayments lr
        JOIN loans l ON l.id = lr.loan_id
        LEFT JOIN individuals i ON i.customer_id = l.customer_id
        LEFT JOIN corporates c ON c.customer_id = l.customer_id
        WHERE COALESCE(lr.value_date, lr.payment_date) BETWEEN %s AND %s
          AND NOT (
            COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
            OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
            OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
          )
        ORDER BY COALESCE(lr.value_date, lr.payment_date) DESC, lr.id DESC
        """,
        (start_date, end_date),
    ),
    (
        "loan_repayment_allocation.csv",
        """
        SELECT lra.*, lr.loan_id, lr.amount AS repayment_amount,
               COALESCE(lr.value_date, lr.payment_date) AS value_date
        FROM loan_repayment_allocation lra
        JOIN loan_repayments lr ON lr.id = lra.repayment_id
        WHERE COALESCE(lr.value_date, lr.payment_date) BETWEEN %s AND %s
          AND NOT (
            COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
            OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
            OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
          )
        ORDER BY COALESCE(lr.value_date, lr.payment_date) DESC, lra.repayment_id, lra.id
        """,
        (start_date, end_date),
    ),
    (
        "loans_with_latest_state.csv",
        """
        SELECT
            l.*,
            COALESCE(i.name, c.trading_name, c.legal_name) AS customer_name,
            lds.as_of_date,
            lds.principal_not_due,
            lds.principal_arrears,
            lds.interest_accrued_balance,
            lds.interest_arrears_balance,
            lds.interest_accrued_balance + lds.interest_arrears_balance AS interest_balance,
            lds.default_interest_balance,
            lds.penalty_interest_balance,
            lds.fees_charges_balance,
            lds.total_exposure,
            lds.days_overdue,
            lds.regular_interest_daily,
            lds.default_interest_daily,
            lds.penalty_interest_daily,
            lds.regular_interest_in_suspense_balance,
            lds.penalty_interest_in_suspense_balance,
            lds.default_interest_in_suspense_balance,
            lds.total_interest_in_suspense_balance
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
        (end_date,),
    ),
    (
        "loan_schedules.csv",
        """
        SELECT ls.*
        FROM loan_schedules ls
        ORDER BY ls.loan_id, ls.version
        """,
        (),
    ),
    (
        "schedule_lines.csv",
        """
        SELECT ls.loan_id, ls.version AS schedule_version, sl.*
        FROM schedule_lines sl
        JOIN loan_schedules ls ON ls.id = sl.loan_schedule_id
        ORDER BY ls.loan_id, ls.version, sl."Period"
        """,
        (),
    ),
    (
        "unapplied_funds.csv",
        """
        SELECT uf.*,
            CASE
                WHEN uf.entry_type = 'credit' AND uf.reference = 'Overpayment' THEN 'from_receipt'
                WHEN uf.entry_type = 'debit' AND uf.reference = 'Applied to arrears (EOD)' THEN 'to_loan_arrears_eod'
                WHEN uf.entry_type = 'debit' AND uf.reference = 'Applied via recast' THEN 'to_loan_recast'
                ELSE 'other'
            END AS movement_type
        FROM unapplied_funds uf
        WHERE uf.value_date BETWEEN %s AND %s
        ORDER BY uf.loan_id, uf.value_date, uf.id
        """,
        (start_date, end_date),
    ),
    (
        "allocation_audit_log.csv",
        """
        SELECT aal.*
        FROM allocation_audit_log aal
        WHERE aal.as_of_date BETWEEN %s AND %s
        ORDER BY aal.created_at
        """,
        (start_date, end_date),
    ),
    (
        "loan_modifications.csv",
        """
        SELECT lm.*
        FROM loan_modifications lm
        ORDER BY lm.loan_id, lm.modification_date
        """,
        (),
    ),
    (
        "loan_recasts.csv",
        """
        SELECT lr.*
        FROM loan_recasts lr
        ORDER BY lr.loan_id, lr.recast_date
        """,
        (),
    ),
    (
        "config.csv",
        """
        SELECT *
        FROM config
        ORDER BY key
        """,
        (),
    ),
    ]


# Backwards-compatible name for any external import
QUERIES = build_export_queries(DEFAULT_START_DATE, DEFAULT_END_DATE)


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
            w.writerows(_format_csv_row(r) for r in flat_rows)
        print(f"  {len(flat_rows):5} rows -> {out_path}")
    except PermissionError:
        alt_path = os.path.join(export_dir, "config_rates_per_product_new.csv")
        with open(alt_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(_format_csv_row(r) for r in flat_rows)
        print(f"  {len(flat_rows):5} rows -> {alt_path} (original in use)")


def _export_repayment_application(conn, export_dir: str, start_date: str, end_date: str) -> None:
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
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                  )
                GROUP BY lr.id, lr.loan_id, lr.value_date, lr.payment_date, lr.amount
            ),
            credits_and_reversals AS (
                -- Single source for credits/reversals: unapplied derived from receipt allocation view.
                SELECT
                    ar.repayment_id,
                    CASE
                        WHEN lr.status = 'reversed' AND lr.original_repayment_id IS NOT NULL
                            THEN 'REV-' || lr.original_repayment_id::text
                        ELSE ar.repayment_id::text
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
                -- Liquidations come from unapplied_funds_allocation plus its reversal rows.
                SELECT
                    lr.id AS repayment_id,
                    CASE
                        WHEN lra.event_type = 'unapplied_funds_allocation' THEN lr.id::text
                        ELSE 'REV-' || lr.original_repayment_id::text
                    END AS repayment_key,
                    lr.loan_id AS loan_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    CASE
                        WHEN lra.event_type = 'unapplied_funds_allocation' THEN 'liquidation'
                        ELSE 'reversal'
                    END AS entry_kind,
                    NULL::integer AS liquidation_repayment_id,
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
                WHERE lra.event_type IN ('unapplied_funds_allocation', 'unallocation_parent_reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND lra.source_repayment_id IS NOT NULL
                GROUP BY
                    lr.id,
                    lr.original_repayment_id,
                    lr.loan_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date,
                    lra.event_type
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
            (start_date, end_date, start_date, end_date),
        )
        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]
    out_path = os.path.join(export_dir, "unapplied_funds_ledger.csv")
    try:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(_format_csv_row(r) for r in rows)
        print(f"  {len(rows):5} rows -> {out_path}")
    except PermissionError:
        alt_path = os.path.join(export_dir, "unapplied_funds_ledger_new.csv")
        with open(alt_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(_format_csv_row(r) for r in rows)
        print(f"  {len(rows):5} rows -> {alt_path} (original in use)")


def _export_statement_credits(conn, export_dir: str, start_date: str, end_date: str) -> None:
    """
    Statement-oriented credits view driven strictly by persisted tables:
    1) Credits from loan_repayment_allocation totals per receipt (non-system receipts).
    2) Credits from unapplied liquidations (event_type='unapplied_funds_allocation').
    3) One accrual summary line per scheduled due date (regular/default/penalty period sums).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH alloc_receipts AS (
                SELECT
                    lr.loan_id,
                    lr.id AS repayment_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    CASE
                        WHEN lr.status = 'reversed' AND lr.original_repayment_id IS NOT NULL
                            THEN 'REV-' || lr.original_repayment_id::text
                        ELSE lr.id::text
                    END AS repayment_key,
                    COALESCE(lr.customer_reference, '') AS customer_reference,
                    COALESCE(SUM(lra.alloc_principal_total), 0) AS alloc_prin_total,
                    COALESCE(SUM(lra.alloc_interest_total), 0) AS alloc_int_total,
                    COALESCE(SUM(lra.alloc_fees_total), 0) AS alloc_fees_total,
                    COALESCE(SUM(lra.alloc_principal_arrears), 0) AS alloc_prin_arrears,
                    COALESCE(SUM(lra.alloc_interest_arrears), 0) AS alloc_int_arrears,
                    COALESCE(SUM(lra.alloc_penalty_interest), 0) AS alloc_penalty_int,
                    COALESCE(SUM(lra.alloc_default_interest), 0) AS alloc_default_int,
                    COALESCE(SUM(lra.alloc_fees_charges), 0) AS alloc_fees_charges,
                    (lr.amount - COALESCE(SUM(lra.alloc_principal_total + lra.alloc_interest_total + lra.alloc_fees_total), 0)) AS unapplied_amount,
                    lr.amount AS receipt_amount
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                  )
                GROUP BY
                    lr.loan_id, lr.id, lr.value_date, lr.payment_date,
                    lr.status, lr.original_repayment_id, lr.customer_reference, lr.amount
            ),
            liquidation_credits AS (
                SELECT
                    lr.loan_id,
                    lr.id AS orig_liq_id,
                    lra.source_repayment_id AS repayment_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    CASE
                        WHEN lra.event_type = 'unapplied_funds_allocation' THEN lra.source_repayment_id::text
                        ELSE 'REV-' || lra.source_repayment_id::text
                    END AS repayment_key,
                    CASE
                        WHEN lra.event_type = 'unapplied_funds_allocation' THEN 'liquidation'
                        ELSE 'reversal'
                    END AS entry_kind,
                    ''::text AS customer_reference,
                    COALESCE(SUM(lra.alloc_principal_total), 0) AS alloc_prin_total,
                    COALESCE(SUM(lra.alloc_interest_total), 0) AS alloc_int_total,
                    COALESCE(SUM(lra.alloc_fees_total), 0) AS alloc_fees_total,
                    COALESCE(SUM(lra.alloc_principal_arrears), 0) AS alloc_prin_arrears,
                    COALESCE(SUM(lra.alloc_interest_arrears), 0) AS alloc_int_arrears,
                    COALESCE(SUM(lra.alloc_penalty_interest), 0) AS alloc_penalty_int,
                    COALESCE(SUM(lra.alloc_default_interest), 0) AS alloc_default_int,
                    COALESCE(SUM(lra.alloc_fees_charges), 0) AS alloc_fees_charges
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lra.event_type IN ('unapplied_funds_allocation', 'unallocation_parent_reversed')
                  AND lra.source_repayment_id IS NOT NULL
                  AND (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND NOT (
                      lra.event_type = 'unallocation_parent_reversed'
                      AND EXISTS (
                          SELECT 1 FROM loan_repayments lr_rev
                          WHERE lr_rev.original_repayment_id = lra.source_repayment_id
                      )
                  )
                GROUP BY
                    lr.loan_id,
                    lr.id,
                    lra.source_repayment_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date,
                    lra.event_type
            ),
            due_calendar AS (
                SELECT
                    ls.loan_id,
                    to_date(sl."Date", 'DD-Mon-YYYY')::date AS due_date,
                    LAG(to_date(sl."Date", 'DD-Mon-YYYY')::date) OVER (
                        PARTITION BY ls.loan_id ORDER BY sl."Period"
                    ) AS prev_due_date
                FROM schedule_lines sl
                JOIN loan_schedules ls ON ls.id = sl.loan_schedule_id
                WHERE to_date(sl."Date", 'DD-Mon-YYYY') BETWEEN %s::date AND %s::date
            ),
            due_accruals AS (
                SELECT
                    dc.loan_id,
                    dc.due_date AS value_date,
                    COALESCE(SUM(lds.regular_interest_daily), 0) AS regular_interest_period,
                    COALESCE(SUM(lds.penalty_interest_daily), 0) AS penalty_interest_period,
                    COALESCE(SUM(lds.default_interest_daily), 0) AS default_interest_period
                FROM due_calendar dc
                LEFT JOIN loan_daily_state lds
                    ON lds.loan_id = dc.loan_id
                   AND lds.as_of_date > COALESCE(dc.prev_due_date, (dc.due_date - INTERVAL '31 days')::date)
                   AND lds.as_of_date <= dc.due_date
                GROUP BY dc.loan_id, dc.due_date
            )
            SELECT
                'repayment_credit' AS line_type,
                CASE
                    WHEN a.unapplied_amount < 0 THEN 'reversal'
                    WHEN a.unapplied_amount > 0 THEN 'credit'
                    ELSE 'neutral'
                END AS entry_kind,
                a.loan_id,
                a.value_date,
                a.repayment_id,
                a.repayment_key,
                CASE
                    WHEN a.unapplied_amount < 0
                        THEN ('REV-RCPT-' || a.repayment_key || ' (Voiding OP-' || a.repayment_key || ')')
                    ELSE (
                        'OP-' || a.repayment_key || ' From Repayment id ' || a.repayment_key
                        || ' (Receipt ' || trim(to_char(a.receipt_amount, 'FM9999999999999990.99')) || ')'
                    )
                END AS narration,
                (a.alloc_prin_total + a.alloc_int_total + a.alloc_fees_total) AS credits,
                a.unapplied_amount AS unapplied_from_receipt,
                a.alloc_prin_arrears,
                a.alloc_int_arrears,
                a.alloc_penalty_int,
                a.alloc_default_int,
                a.alloc_fees_charges,
                NULL::numeric AS regular_interest_period,
                NULL::numeric AS penalty_interest_period,
                NULL::numeric AS default_interest_period
            FROM alloc_receipts a
            UNION ALL
            SELECT
                'liquidation_credit' AS line_type,
                l.entry_kind AS entry_kind,
                l.loan_id,
                l.value_date,
                l.repayment_id,
                l.repayment_key,
                CASE
                    WHEN l.entry_kind = 'reversal'
                        THEN 'REV-LIQ-' || l.orig_liq_id || COALESCE(' (Orig: OP-' || l.repayment_key || ')', '')
                    ELSE 'LIQ-' || l.orig_liq_id || COALESCE(' from OP-' || l.repayment_key, '')
                END AS narration,
                (l.alloc_prin_total + l.alloc_int_total + l.alloc_fees_total) AS credits,
                -(l.alloc_prin_total + l.alloc_int_total + l.alloc_fees_total) AS unapplied_from_receipt,
                l.alloc_prin_arrears,
                l.alloc_int_arrears,
                l.alloc_penalty_int,
                l.alloc_default_int,
                l.alloc_fees_charges,
                NULL::numeric AS regular_interest_period,
                NULL::numeric AS penalty_interest_period,
                NULL::numeric AS default_interest_period
            FROM liquidation_credits l
            UNION ALL
            SELECT
                'period_accrual' AS line_type,
                NULL::text AS entry_kind,
                d.loan_id,
                d.value_date,
                NULL::integer AS repayment_id,
                NULL::text AS repayment_key,
                ('Accruals for period ending ' || to_char(d.value_date, 'YYYY-MM-DD')) AS narration,
                NULL::numeric AS credits,
                NULL::numeric AS unapplied_from_receipt,
                NULL::numeric AS alloc_prin_arrears,
                NULL::numeric AS alloc_int_arrears,
                NULL::numeric AS alloc_penalty_int,
                NULL::numeric AS alloc_default_int,
                NULL::numeric AS alloc_fees_charges,
                d.regular_interest_period,
                d.penalty_interest_period,
                d.default_interest_period
            FROM due_accruals d
            ORDER BY loan_id, value_date, line_type, repayment_id
            """,
            (start_date, end_date, start_date, end_date, start_date, end_date),
        )
        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]

    out_path = os.path.join(export_dir, "statement_credits_view.csv")
    try:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(_format_csv_row(r) for r in rows)
        print(f"  {len(rows):5} rows -> {out_path}")
    except PermissionError:
        alt_path = os.path.join(export_dir, "statement_credits_view_new.csv")
        with open(alt_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(_format_csv_row(r) for r in rows)
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
            w.writerows(_format_csv_row(r) for r in flat_rows)
        print(f"  {len(flat_rows):5} rows -> {out_path}")
    except PermissionError:
        alt_path = os.path.join(export_dir, "loans_capture_rates_new.csv")
        with open(alt_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(colnames)
            w.writerows(_format_csv_row(r) for r in flat_rows)
        print(f"  {len(flat_rows):5} rows -> {alt_path} (original in use)")


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export loan-related tables to CSV under ./farndacred_exports/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n  %(prog)s --start-date 2025-01-01 --end-date 2025-06-30\n  %(prog)s   # uses DEFAULT_START_DATE / DEFAULT_END_DATE in script",
    )
    p.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        metavar="YYYY-MM-DD",
        help=f"Inclusive start for date-filtered exports (default: {DEFAULT_START_DATE})",
    )
    p.add_argument(
        "--end-date",
        default=DEFAULT_END_DATE,
        metavar="YYYY-MM-DD",
        help=f"Inclusive end for date-filtered exports (default: {DEFAULT_END_DATE})",
    )
    return p.parse_args(argv)


def main(argv=None) -> None:
    try:
        from config import get_database_url
        import psycopg2
    except ImportError as e:
        print("Error: need config and psycopg2. Run from project root: python scripts/export_loan_tables.py", file=sys.stderr)
        raise SystemExit(1) from e

    args = _parse_args(argv)
    start_date = args.start_date.strip()
    end_date = args.end_date.strip()
    try:
        d0 = date.fromisoformat(start_date)
        d1 = date.fromisoformat(end_date)
    except ValueError as e:
        print("Error: --start-date and --end-date must be YYYY-MM-DD", file=sys.stderr)
        raise SystemExit(2) from e
    if d0 > d1:
        print("Error: --start-date must be on or before --end-date", file=sys.stderr)
        raise SystemExit(2)

    # Hard cap: never export future schedule/statement rows beyond system business date.
    # (User may pass a future end date; we clamp it.)
    try:
        from eod.system_business_date import get_effective_date

        eff = get_effective_date()
    except Exception:
        eff = date.today()
    if d1 > eff:
        d1 = eff
        end_date = d1.isoformat()

    queries = build_export_queries(start_date, end_date)

    os.makedirs(EXPORT_DIR, exist_ok=True)
    conn = psycopg2.connect(get_database_url())
    try:
        print(f"Date range (inclusive): {start_date} .. {end_date}")
        for filename, query, params in queries:
            path = os.path.join(EXPORT_DIR, filename)
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                colnames = [d[0] for d in cur.description]
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(colnames)
                    w.writerows(_format_csv_row(r) for r in rows)
                print(f"  {len(rows):5} rows -> {path}")
            except PermissionError:
                base, ext = os.path.splitext(filename)
                alt_path = os.path.join(EXPORT_DIR, f"{base}_new{ext}")
                with open(alt_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(colnames)
                    w.writerows(_format_csv_row(r) for r in rows)
                print(f"  {len(rows):5} rows -> {alt_path} (original in use)")
        # Flatten config into rates-per-product for verification (default_rates, penalty_rates per loan_type)
        _export_config_rates(conn, EXPORT_DIR)
        # Flatten loan-level capture rates (annual_rate, monthly_rate, metadata.penalty_rate_pct)
        _export_loans_capture_rates(conn, EXPORT_DIR)
        # Unapplied funds ledger: +credits and -liquidations linked to repayment IDs
        _export_repayment_application(conn, EXPORT_DIR, start_date, end_date)
        # Statement-oriented lines for credits/unapplied/liquidation and period accrual summaries
        _export_statement_credits(conn, EXPORT_DIR, start_date, end_date)

    finally:
        conn.close()

    print(f"\nExports saved to: {os.path.abspath(EXPORT_DIR)}")
    print("Open the CSV files in Excel or any spreadsheet.")
    print("Rates: see config.csv (raw) and config_rates_per_product.csv (flattened per product/loan_type).")


if __name__ == "__main__":
    main()
