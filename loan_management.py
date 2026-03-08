"""
Loan management: persist loan details, schedules, and repayments to the database.
Uses loans.py for computation only; this module handles DB writes.
"""

from __future__ import annotations

import contextlib
import json
from datetime import date, datetime
from typing import Any

import pandas as pd

# Waterfall bucket name -> (alloc_* column name, loan_daily_state column name)
BUCKET_TO_ALLOC = {
    "fees_charges_balance": ("alloc_fees_charges", "fees_charges_balance"),
    "penalty_interest_balance": ("alloc_penalty_interest", "penalty_interest_balance"),
    "default_interest_balance": ("alloc_default_interest", "default_interest_balance"),
    "interest_arrears_balance": ("alloc_interest_arrears", "interest_arrears_balance"),
    "interest_accrued_balance": ("alloc_interest_accrued", "interest_accrued_balance"),
    "principal_arrears": ("alloc_principal_arrears", "principal_arrears"),
    "principal_not_due": ("alloc_principal_not_due", "principal_not_due"),
}

# Map config/display variants to internal bucket key so allocation works even if DB has different strings
_BUCKET_NAME_NORMALIZE: dict[str, str] = {}
for _key in BUCKET_TO_ALLOC:
    _norm = _key.lower().replace("_", " ")
    _BUCKET_NAME_NORMALIZE[_key] = _key
    _BUCKET_NAME_NORMALIZE[_key.lower()] = _key
    _BUCKET_NAME_NORMALIZE[_norm] = _key
    _BUCKET_NAME_NORMALIZE[_norm.replace(" ", "_")] = _key
# Common display names and UI/config variants (so waterfall_profiles can use friendly names)
_BUCKET_NAME_NORMALIZE["interest arrears"] = "interest_arrears_balance"
_BUCKET_NAME_NORMALIZE["interest_arrears"] = "interest_arrears_balance"
_BUCKET_NAME_NORMALIZE["principal arrears"] = "principal_arrears"
_BUCKET_NAME_NORMALIZE["principal_arrears"] = "principal_arrears"
_BUCKET_NAME_NORMALIZE["principal"] = "principal_arrears"  # ambiguous; map to principal_arrears
_BUCKET_NAME_NORMALIZE["fees"] = "fees_charges_balance"
_BUCKET_NAME_NORMALIZE["fees and charges"] = "fees_charges_balance"
_BUCKET_NAME_NORMALIZE["fees_charges"] = "fees_charges_balance"
_BUCKET_NAME_NORMALIZE["penalty"] = "penalty_interest_balance"
_BUCKET_NAME_NORMALIZE["penalty interest"] = "penalty_interest_balance"
_BUCKET_NAME_NORMALIZE["default interest"] = "default_interest_balance"
_BUCKET_NAME_NORMALIZE["default"] = "default_interest_balance"
_BUCKET_NAME_NORMALIZE["interest accrued"] = "interest_accrued_balance"
_BUCKET_NAME_NORMALIZE["principal not due"] = "principal_not_due"
_BUCKET_NAME_NORMALIZE["principal not due balance"] = "principal_not_due"
_BUCKET_NAME_NORMALIZE["interest"] = "interest_arrears_balance"  # ambiguous; map to arrears


def _normalize_bucket_order(raw_order: list) -> list[str]:
    """Map config bucket names to BUCKET_TO_ALLOC keys so allocation never skips due to name mismatch."""
    out: list[str] = []
    for name in raw_order or []:
        s = (name or "").strip()
        if not s:
            continue
        key = (
            _BUCKET_NAME_NORMALIZE.get(s)
            or _BUCKET_NAME_NORMALIZE.get(s.lower())
            or _BUCKET_NAME_NORMALIZE.get(s.lower().replace(" ", "_"))
        )
        if key and key in BUCKET_TO_ALLOC:
            out.append(key)
    return out


# Standard profile: do not allocate to these buckets (overpayment goes to unapplied).
# Use Borrower-friendly profile if you want payments to reduce principal_not_due / interest_accrued on up-to-date loans.
STANDARD_SKIP_BUCKETS = ("interest_accrued_balance", "principal_not_due")


def _get_waterfall_config(cfg: dict) -> tuple[str, list]:
    """Return (profile_key, bucket_order) from system config. Raises if not configured."""
    profile_name = (cfg.get("payment_waterfall") or "Standard").strip().lower()
    profile_key = "borrower_friendly" if profile_name.startswith("borrower") else "standard"
    profiles = cfg.get("waterfall_profiles") or {}
    raw_order = profiles.get(profile_key)
    if not raw_order:
        raise ValueError(
            "Waterfall profile is not configured. Please maintain System configuration: set "
            "waterfall_profiles with profile 'standard' and/or 'borrower_friendly', and "
            "payment_waterfall, then retry."
        )
    bucket_order = _normalize_bucket_order(raw_order)
    if not bucket_order:
        raise ValueError(
            "Waterfall profile has no valid bucket names. Use internal keys (e.g. interest_arrears_balance, "
            "principal_arrears) or ensure waterfall_profiles in config match expected names."
        )
    return profile_key, bucket_order


# Minimum remaining amount to treat as zero (avoids float noise; 1e-4 = 0.01 cent)
_WATERFALL_REMAINING_EPS = 1e-4


def compute_waterfall_allocation(
    amount: float,
    balances: dict[str, float],
    bucket_order: list,
    profile_key: str,
    *,
    state_as_of: date | None = None,
    repayment_id: int | None = None,
) -> tuple[dict[str, float], float]:
    """
    Compute allocation of a payment amount across buckets (waterfall). Pure logic, no DB.
    Returns (alloc_dict, unapplied_amount). alloc_dict uses alloc_* keys (e.g. alloc_principal_arrears).

    Standard profile skips interest_accrued_balance and principal_not_due (see STANDARD_SKIP_BUCKETS),
    so overpayments on up-to-date loans go to unapplied unless Borrower-friendly profile is used.
    """
    alloc: dict[str, float] = {alloc_key: 0.0 for _b, (alloc_key, _sk) in BUCKET_TO_ALLOC.items()}
    remaining = amount
    for bucket_name in bucket_order:
        if bucket_name not in BUCKET_TO_ALLOC:
            continue
        if profile_key == "standard" and bucket_name in STANDARD_SKIP_BUCKETS:
            continue
        alloc_key, state_key = BUCKET_TO_ALLOC[bucket_name]
        bucket_balance = max(0.0, balances.get(state_key, 0.0))
        to_alloc = min(remaining, bucket_balance)
        alloc[alloc_key] = to_alloc
        remaining -= to_alloc
        if remaining <= _WATERFALL_REMAINING_EPS:
            remaining = 0.0
            break
    for bucket_name in bucket_order:
        if bucket_name not in BUCKET_TO_ALLOC:
            continue
        alloc_key, state_key = BUCKET_TO_ALLOC[bucket_name]
        bal = balances.get(state_key, 0.0)
        a = alloc.get(alloc_key, 0.0)
        if a > bal + 0.01:
            raise ValueError(
                f"Allocation control: {alloc_key}={a:.2f} exceeds balance due {state_key}={bal:.2f} "
                f"(state as_of_date={state_as_of}) for repayment_id={repayment_id}. "
                "Cannot allocate more than balance due."
            )
    unapplied = round(remaining, 2) if remaining > _WATERFALL_REMAINING_EPS else 0.0
    return alloc, unapplied


def apply_allocations_for_loan_date(
    loan_id: int,
    as_of_date: date,
    balances: dict[str, float],
    days_overdue: int,
    sys_cfg: dict,
) -> dict[str, float]:
    """
    Apply all posted receipts with value_date = as_of_date for this loan, in order.
    Writes to loan_repayment_allocation and unapplied_funds; returns updated balances only.
    Caller must persist the returned state to loan_daily_state (e.g. via save_loan_daily_state).

    Note: The primary allocation path is allocate_repayment_waterfall at receipt save time,
    which persists allocation and daily state in one transaction. This function is for
    batch/EOD flows that apply multiple receipts and then persist once.
    """
    profile_key, bucket_order = _get_waterfall_config(sys_cfg)
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, amount FROM loan_repayments
                WHERE loan_id = %s AND status = 'posted'
                  AND COALESCE(value_date, payment_date) = %s
                ORDER BY id
                """,
                (loan_id, as_of_date),
            )
            receipts = cur.fetchall()
    state = dict(balances)
    for rec in receipts:
        repayment_id = int(rec["id"])
        amount = float(rec["amount"] or 0)
        if amount <= 0:
            continue
        alloc, unapplied = compute_waterfall_allocation(
            amount, state, bucket_order, profile_key,
            state_as_of=as_of_date, repayment_id=repayment_id,
        )
        alloc_principal_not_due = alloc.get("alloc_principal_not_due", 0.0)
        alloc_principal_arrears = alloc.get("alloc_principal_arrears", 0.0)
        alloc_interest_accrued = alloc.get("alloc_interest_accrued", 0.0)
        alloc_interest_arrears = alloc.get("alloc_interest_arrears", 0.0)
        alloc_default_interest = alloc.get("alloc_default_interest", 0.0)
        alloc_penalty_interest = alloc.get("alloc_penalty_interest", 0.0)
        alloc_fees_charges = alloc.get("alloc_fees_charges", 0.0)
        alloc_principal_total = alloc_principal_not_due + alloc_principal_arrears
        alloc_interest_total = (
            alloc_interest_accrued + alloc_interest_arrears
            + alloc_default_interest + alloc_penalty_interest
        )
        alloc_fees_total = alloc_fees_charges
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO loan_repayment_allocation (
                        repayment_id,
                        alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (repayment_id) DO UPDATE SET
                        alloc_principal_not_due = EXCLUDED.alloc_principal_not_due,
                        alloc_principal_arrears = EXCLUDED.alloc_principal_arrears,
                        alloc_interest_accrued = EXCLUDED.alloc_interest_accrued,
                        alloc_interest_arrears = EXCLUDED.alloc_interest_arrears,
                        alloc_default_interest = EXCLUDED.alloc_default_interest,
                        alloc_penalty_interest = EXCLUDED.alloc_penalty_interest,
                        alloc_fees_charges = EXCLUDED.alloc_fees_charges,
                        alloc_principal_total = EXCLUDED.alloc_principal_total,
                        alloc_interest_total = EXCLUDED.alloc_interest_total,
                        alloc_fees_total = EXCLUDED.alloc_fees_total
                    """,
                    (
                        repayment_id,
                        alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total,
                    ),
                )
                if unapplied > 1e-6:
                    _credit_unapplied_funds(conn, loan_id, repayment_id, unapplied, as_of_date)
        state["principal_not_due"] = max(0.0, state["principal_not_due"] - alloc_principal_not_due)
        state["principal_arrears"] = max(0.0, state["principal_arrears"] - alloc_principal_arrears)
        state["interest_accrued_balance"] = max(0.0, state["interest_accrued_balance"] - alloc_interest_accrued)
        state["interest_arrears_balance"] = max(0.0, state["interest_arrears_balance"] - alloc_interest_arrears)
        state["default_interest_balance"] = max(0.0, state["default_interest_balance"] - alloc_default_interest)
        state["penalty_interest_balance"] = max(0.0, state["penalty_interest_balance"] - alloc_penalty_interest)
        state["fees_charges_balance"] = max(0.0, state["fees_charges_balance"] - alloc_fees_charges)
    return state


class NeedOverpaymentDecision(Exception):
    """Reserved for future use. Standard waterfall now sends overpayment to unapplied by default (no raise)."""

    def __init__(self, repayment_id: int, loan_id: int, amount_remaining: float, effective_date: date):
        self.repayment_id = repayment_id
        self.loan_id = loan_id
        self.amount_remaining = amount_remaining
        self.effective_date = effective_date
        super().__init__(
            f"Overpayment at waterfall step 6: repayment_id={repayment_id} loan_id={loan_id} "
            f"amount_remaining={amount_remaining}"
        )

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor, Json
except ImportError:
    psycopg2 = None
    RealDictCursor = None
    Json = None

from config import get_database_url


def _get_conn():
    if psycopg2 is None:
        raise RuntimeError("Install psycopg2-binary to use loan_management.")
    return psycopg2.connect(get_database_url())


@contextlib.contextmanager
def _connection():
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _date_conv(v: Any) -> date | None:
    """Convert iso string or date to date for DB."""
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
    return None


def save_loan(
    customer_id: int,
    loan_type: str,
    details: dict[str, Any],
    schedule_df: pd.DataFrame,
    schedule_version: int = 1,
    product_code: str | None = None,
) -> int:
    """
    Persist loan details and schedule to DB.
    - Inserts one row into loans (loan details).
    - Inserts one row into loan_schedules (version).
    - Inserts one row per period into schedule_lines (instalments).

    details: principal (total loan amount), disbursed_amount (net proceeds), term,
             drawdown_fee, arrangement_fee, admin_fee (optional),
             disbursement_date, start_date, end_date, first_repayment_date (optional),
             end_date (optional), installment (optional), total_payment (optional),
             grace_type (optional), moratorium_months (optional), bullet_type (optional),
             scheme (optional), payment_timing (optional), metadata (optional).

    Returns loan_id.
    """
    loan_type_db = {
        "Consumer Loan": "consumer_loan",
        "Term Loan": "term_loan",
        "Bullet Loan": "bullet_loan",
        "Customised Repayments": "customised_repayments",
    }.get(loan_type, loan_type.replace(" ", "_").lower())

    metadata = details.get("metadata") or {}
    # Penalty rate % from loan capture only; null or missing → 0
    metadata["penalty_rate_pct"] = float(details.get("penalty_rate_pct") if details.get("penalty_rate_pct") is not None else 0)
    if details.get("penalty_quotation"):
        metadata["penalty_quotation"] = details["penalty_quotation"]
    if details.get("currency"):
        metadata["currency"] = details["currency"]

    # Single date from UI: disbursement date. start_date is always set equal (column kept for future use).
    disb_date = details.get("disbursement_date") or details.get("start_date")

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loans (
                    customer_id, loan_type, product_code, principal, disbursed_amount, term,
                    annual_rate, monthly_rate, drawdown_fee, arrangement_fee, admin_fee,
                    disbursement_date, start_date, end_date, first_repayment_date,
                    installment, total_payment, grace_type, moratorium_months, bullet_type, scheme,
                    payment_timing, metadata, status, agent_id, relationship_manager_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
                """,
                (
                    customer_id,
                    loan_type_db,
                    product_code,
                    float(details.get("principal", details.get("facility", 0))),
                    float(details.get("disbursed_amount", details.get("principal", 0))),
                    int(details.get("term", 0)),
                    details.get("annual_rate") if details.get("annual_rate") is not None else None,
                    details.get("monthly_rate") if details.get("monthly_rate") is not None else None,
                    details.get("drawdown_fee"),
                    details.get("arrangement_fee"),
                    details.get("admin_fee"),
                    _date_conv(disb_date),
                    _date_conv(disb_date),
                    _date_conv(details.get("end_date")),
                    _date_conv(details.get("first_repayment_date")),
                    float(details["installment"]) if details.get("installment") is not None else None,
                    float(details["total_payment"]) if details.get("total_payment") is not None else None,
                    details.get("grace_type"),
                    details.get("moratorium_months"),
                    details.get("bullet_type"),
                    details.get("scheme"),
                    details.get("payment_timing"),
                    Json(metadata) if metadata else None,
                    details.get("status", "active"),
                    details.get("agent_id"),
                    details.get("relationship_manager_id"),
                ),
            )
            loan_id = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO loan_schedules (loan_id, version) VALUES (%s, %s) RETURNING id",
                (loan_id, schedule_version),
            )
            schedule_id = cur.fetchone()[0]

        # Schedule lines: support both "Period"/"Date" and "Monthly Installment"/"Payment" column names
        with conn.cursor() as cur:
            for _, row in schedule_df.iterrows():
                period = int(row.get("Period", row.get("period", 0)))
                period_date = str(row.get("Date", row.get("Date", "")))[:32] if pd.notna(row.get("Date")) else None
                payment = float(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0)))) if pd.notna(row.get("Payment", row.get("Monthly Installment", 0))) else 0.0
                principal = float(row.get("Principal", row.get("principal", 0))) if pd.notna(row.get("Principal")) else 0.0
                interest = float(row.get("Interest", row.get("interest", 0))) if pd.notna(row.get("Interest")) else 0.0
                principal_balance = float(row.get("Principal Balance", row.get("principal_balance", 0))) if pd.notna(row.get("Principal Balance")) else 0.0
                total_outstanding = float(row.get("Total Outstanding", row.get("total_outstanding", 0))) if pd.notna(row.get("Total Outstanding")) else 0.0
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (schedule_id, period, period_date, payment, principal, interest, principal_balance, total_outstanding),
                )

    return loan_id


def record_repayment(
    loan_id: int,
    amount: float,
    payment_date: date | str,
    period_number: int | None = None,
    schedule_line_id: int | None = None,
    reference: str | None = None,
    customer_reference: str | None = None,
    company_reference: str | None = None,
    value_date: date | str | None = None,
    system_date: datetime | str | None = None,
    status: str = "posted",
) -> int:
    """
    Record an actual payment/receipt against a loan.
    customer_reference: appears on customer loan statement
    company_reference: appears in company general ledger
    value_date: effective date (default = payment_date)
    system_date: when captured (default = now)
    Returns repayment id.
    """
    pdate = _date_conv(payment_date) if payment_date else None
    if not pdate:
        raise ValueError("payment_date is required")
    vdate = _date_conv(value_date) if value_date else pdate
    sdate = system_date
    if sdate is None:
        sdate = datetime.now()
    elif isinstance(sdate, str):
        sdate = datetime.fromisoformat(sdate.replace("Z", "+00:00"))
    ref = customer_reference or reference
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO loan_repayments (
                    loan_id, schedule_line_id, period_number, amount, payment_date,
                    reference, customer_reference, company_reference, value_date, system_date, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (loan_id, schedule_line_id, period_number, amount, pdate, ref, customer_reference, company_reference, vdate, sdate, status),
            )
            return cur.fetchone()[0]


def reverse_repayment(
    original_repayment_id: int,
    *,
    system_date: datetime | str | None = None,
) -> int:
    """
    Insert a reversing repayment row, leave the original immutable, and undo its
    allocation so state is correct for any later receipts on the same loan/date.
    - Adds the original's allocation back to loan_daily_state for its value_date.
    - Removes pending unapplied_funds for this repayment.
    - Reversal row has negative amount and status 'reversed'; original is marked 'reversed'.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM loan_repayments WHERE id = %s",
                (original_repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Repayment {original_repayment_id} not found.")

            if row["status"] == "reversed":
                raise ValueError(f"Repayment {original_repayment_id} is already reversed.")

            loan_id = int(row["loan_id"])
            eff_date = row.get("value_date") or row["payment_date"]
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            # Undo allocation so state is correct for successive receipts: add back to daily state.
            cur.execute(
                """
                SELECT alloc_principal_not_due, alloc_principal_arrears,
                       alloc_interest_accrued, alloc_interest_arrears,
                       alloc_default_interest, alloc_penalty_interest, alloc_fees_charges
                FROM loan_repayment_allocation
                WHERE repayment_id = %s
                """,
                (original_repayment_id,),
            )
            alloc_row = cur.fetchone()
            if alloc_row:
                def _f(v):
                    return float(v or 0)
                cur.execute(
                    """
                    UPDATE loan_daily_state
                    SET
                        principal_not_due         = principal_not_due         + %s,
                        principal_arrears        = principal_arrears        + %s,
                        interest_accrued_balance = interest_accrued_balance + %s,
                        interest_arrears_balance = interest_arrears_balance + %s,
                        default_interest_balance = default_interest_balance + %s,
                        penalty_interest_balance = penalty_interest_balance + %s,
                        fees_charges_balance     = fees_charges_balance     + %s
                    WHERE loan_id = %s AND as_of_date = %s
                    """,
                    (
                        _f(alloc_row["alloc_principal_not_due"]),
                        _f(alloc_row["alloc_principal_arrears"]),
                        _f(alloc_row["alloc_interest_accrued"]),
                        _f(alloc_row["alloc_interest_arrears"]),
                        _f(alloc_row["alloc_default_interest"]),
                        _f(alloc_row["alloc_penalty_interest"]),
                        _f(alloc_row["alloc_fees_charges"]),
                        loan_id,
                        eff_date,
                    ),
                )
                cur.execute(
                    """
                    UPDATE loan_daily_state
                    SET total_exposure = principal_not_due + principal_arrears
                        + interest_accrued_balance + interest_arrears_balance
                        + default_interest_balance + penalty_interest_balance
                        + fees_charges_balance
                    WHERE loan_id = %s AND as_of_date = %s
                    """,
                    (loan_id, eff_date),
                )
            cur.execute(
                "DELETE FROM unapplied_funds WHERE repayment_id = %s AND status = 'pending'",
                (original_repayment_id,),
            )

            sdate = system_date
            if sdate is None:
                sdate = datetime.now()
            elif isinstance(sdate, str):
                sdate = datetime.fromisoformat(sdate.replace("Z", "+00:00"))

            cur.execute(
                """
                INSERT INTO loan_repayments (
                    loan_id, schedule_line_id, period_number, amount, payment_date,
                    reference, customer_reference, company_reference, value_date, system_date,
                    status, original_repayment_id
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    'reversed', %s
                )
                RETURNING id
                """,
                (
                    row["loan_id"],
                    row["schedule_line_id"],
                    row["period_number"],
                    -float(row["amount"]),
                    row["payment_date"],
                    row["reference"],
                    row["customer_reference"],
                    row["company_reference"],
                    row.get("value_date") or row["payment_date"],
                    sdate,
                    original_repayment_id,
                ),
            )
            new_id = cur.fetchone()[0]

            cur.execute(
                "UPDATE loan_repayments SET status = 'reversed' WHERE id = %s",
                (original_repayment_id,),
            )
            return new_id


def record_repayments_batch(rows: list[dict]) -> tuple[int, int, list[str]]:
    """
    Record multiple repayments. Each row: loan_id, amount, payment_date, customer_reference, company_reference, value_date (optional), system_date (optional).
    Returns (success_count, fail_count, list of error messages).
    """
    success = 0
    fail = 0
    errors: list[str] = []
    for i, row in enumerate(rows):
        try:
            repayment_id = record_repayment(
                loan_id=int(row["loan_id"]),
                amount=float(row["amount"]),
                payment_date=row["payment_date"],
                customer_reference=row.get("customer_reference"),
                company_reference=row.get("company_reference"),
                value_date=row.get("value_date"),
                system_date=row.get("system_date"),
            )
            allocate_repayment_waterfall(repayment_id)
            success += 1
        except Exception as e:
            fail += 1
            errors.append(f"Row {i + 1}: {e}")
    return success, fail, errors


def get_loan(loan_id: int) -> dict | None:
    """Fetch loan details by id."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM loans WHERE id = %s", (loan_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def get_loans_by_customer(customer_id: int) -> list[dict]:
    """Fetch all loans for a customer."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, loan_type, principal, disbursed_amount, term, status, created_at FROM loans WHERE customer_id = %s ORDER BY created_at DESC", (customer_id,))
            return [dict(r) for r in cur.fetchall()]


def get_latest_schedule_version(loan_id: int) -> int:
    """Return the latest schedule version number for a loan (1 = original)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(version), 1) FROM loan_schedules WHERE loan_id = %s",
                (loan_id,),
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] else 1


def get_schedule_lines(loan_id: int, schedule_version: int | None = None) -> list[dict]:
    """Fetch schedule lines for a loan. If schedule_version is None, use latest."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if schedule_version is None:
                cur.execute(
                    "SELECT id FROM loan_schedules WHERE loan_id = %s ORDER BY version DESC LIMIT 1",
                    (loan_id,),
                )
                row = cur.fetchone()
                if not row:
                    return []
                cur.execute(
                    'SELECT * FROM schedule_lines WHERE loan_schedule_id = %s ORDER BY "Period"',
                    (row["id"],),
                )
                return [dict(r) for r in cur.fetchall()]
            cur.execute(
                """
                SELECT sl.* FROM schedule_lines sl
                JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
                WHERE ls.loan_id = %s AND ls.version = %s
                ORDER BY sl."Period"
                """,
                (loan_id, schedule_version),
            )
            return [dict(r) for r in cur.fetchall()]


def get_loan_daily_state_balances(loan_id: int, as_of_date: date) -> dict[str, float] | None:
    """Get bucket balances for a loan as of a date (latest row on or before as_of_date)."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance,
                       penalty_interest_balance, fees_charges_balance, days_overdue,
                       COALESCE(regular_interest_period_to_date, 0) AS regular_interest_period_to_date,
                       COALESCE(penalty_interest_period_to_date, 0)  AS penalty_interest_period_to_date,
                       COALESCE(default_interest_period_to_date, 0)  AS default_interest_period_to_date
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date <= %s
                ORDER BY as_of_date DESC LIMIT 1
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "principal_not_due": float(row["principal_not_due"] or 0),
                "principal_arrears": float(row["principal_arrears"] or 0),
                "interest_accrued_balance": float(row["interest_accrued_balance"] or 0),
                "interest_arrears_balance": float(row["interest_arrears_balance"] or 0),
                "default_interest_balance": float(row["default_interest_balance"] or 0),
                "penalty_interest_balance": float(row["penalty_interest_balance"] or 0),
                "fees_charges_balance": float(row["fees_charges_balance"] or 0),
                "days_overdue": int(row["days_overdue"] or 0),
                "regular_interest_period_to_date": float(row["regular_interest_period_to_date"] or 0),
                "penalty_interest_period_to_date": float(row["penalty_interest_period_to_date"] or 0),
                "default_interest_period_to_date": float(row["default_interest_period_to_date"] or 0),
            }


def get_loan_daily_state_range(loan_id: int, start_date: date, end_date: date) -> list[dict]:
    """All loan_daily_state rows for a loan in [start_date, end_date] ordered by as_of_date."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT as_of_date, regular_interest_daily, principal_not_due, principal_arrears,
                       interest_accrued_balance, interest_arrears_balance,
                       default_interest_daily, default_interest_balance,
                       penalty_interest_daily, penalty_interest_balance,
                       fees_charges_balance, total_exposure,
                       COALESCE(regular_interest_period_to_date, 0) AS regular_interest_period_to_date,
                       COALESCE(penalty_interest_period_to_date, 0)  AS penalty_interest_period_to_date,
                       COALESCE(default_interest_period_to_date, 0)  AS default_interest_period_to_date
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date >= %s AND as_of_date <= %s
                ORDER BY as_of_date
                """,
                (loan_id, start_date, end_date),
            )
            return [dict(r) for r in cur.fetchall()]


def get_repayment_ids_for_value_date(value_date: date) -> list[int]:
    """Repayment IDs with value_date (or payment_date) on the given date, posted only. Order by id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayments
                WHERE status = 'posted'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY id
                """,
                (value_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def get_allocation_totals_for_loan_date(
    loan_id: int,
    value_date: date,
    *,
    exclude_repayment_id: int | None = None,
) -> dict[str, float]:
    """
    Sum of allocation amounts for this loan for all repayments with value_date (or payment_date) on the given date.
    Used by EOD so that when it overwrites loan_daily_state for that date, it subtracts allocations
    and keeps principal/interest arrears (and other buckets) reduced by receipts.
    If exclude_repayment_id is set, that repayment is excluded from the sum (for reallocate: state = engine - others).
    Returns dict with keys: alloc_principal_not_due, alloc_principal_arrears, alloc_interest_accrued,
    alloc_interest_arrears, alloc_default_interest, alloc_penalty_interest, alloc_fees_charges.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT
                    COALESCE(SUM(lra.alloc_principal_not_due), 0)   AS alloc_principal_not_due,
                    COALESCE(SUM(lra.alloc_principal_arrears), 0)   AS alloc_principal_arrears,
                    COALESCE(SUM(lra.alloc_interest_accrued), 0)   AS alloc_interest_accrued,
                    COALESCE(SUM(lra.alloc_interest_arrears), 0)   AS alloc_interest_arrears,
                    COALESCE(SUM(lra.alloc_default_interest), 0)   AS alloc_default_interest,
                    COALESCE(SUM(lra.alloc_penalty_interest), 0)   AS alloc_penalty_interest,
                    COALESCE(SUM(lra.alloc_fees_charges), 0)       AS alloc_fees_charges
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status = 'posted'
                  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
            """
            params: list = [loan_id, value_date]
            if exclude_repayment_id is not None:
                sql += " AND lr.id != %s"
                params.append(exclude_repayment_id)
            cur.execute(sql, params)
            row = cur.fetchone()
    if not row:
        return {
            "alloc_principal_not_due": 0.0,
            "alloc_principal_arrears": 0.0,
            "alloc_interest_accrued": 0.0,
            "alloc_interest_arrears": 0.0,
            "alloc_default_interest": 0.0,
            "alloc_penalty_interest": 0.0,
            "alloc_fees_charges": 0.0,
        }
    return {k: float(row.get(k, 0) or 0) for k in row}


def get_repayments_with_allocations(
    loan_id: int, start_date: date, end_date: date
) -> list[dict]:
    """Repayments with value_date in range (posted only) and their allocation breakdown (totals and per-bucket 1-5)."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT lr.id, lr.amount, lr.payment_date, lr.value_date, lr.customer_reference,
                       COALESCE(lra.alloc_interest_total, 0) AS alloc_interest_total,
                       COALESCE(lra.alloc_fees_total, 0) AS alloc_fees_total,
                       COALESCE(lra.alloc_principal_total, 0) AS alloc_principal_total,
                       COALESCE(lra.alloc_fees_charges, 0) AS alloc_fees_charges,
                       COALESCE(lra.alloc_penalty_interest, 0) AS alloc_penalty_interest,
                       COALESCE(lra.alloc_default_interest, 0) AS alloc_default_interest,
                       COALESCE(lra.alloc_interest_arrears, 0) AS alloc_interest_arrears,
                       COALESCE(lra.alloc_principal_arrears, 0) AS alloc_principal_arrears
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s AND lr.status = 'posted'
                  AND COALESCE(lr.value_date, lr.payment_date) >= %s
                  AND COALESCE(lr.value_date, lr.payment_date) <= %s
                ORDER BY COALESCE(lr.value_date, lr.payment_date), lr.id
                """,
                (loan_id, start_date, end_date),
            )
            return [dict(r) for r in cur.fetchall()]


def get_unapplied_balance(loan_id: int, as_of_date: date) -> float:
    """Sum of pending unapplied_funds for the loan with value_date <= as_of_date."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM unapplied_funds
                WHERE loan_id = %s AND status = 'pending' AND value_date <= %s
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)


def get_unapplied_entries(loan_id: int, through_date: date) -> list[tuple[date, float]]:
    """
    All pending unapplied_funds for the loan with value_date <= through_date.
    Returns list of (value_date, amount) for in-memory aggregation (e.g. unapplied at any date).
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value_date, amount
                FROM unapplied_funds
                WHERE loan_id = %s AND status = 'pending' AND value_date <= %s
                ORDER BY value_date
                """,
                (loan_id, through_date),
            )
            rows = cur.fetchall()
    out: list[tuple[date, float]] = []
    for r in rows:
        vd = r[0]
        if hasattr(vd, "date"):
            vd = vd.date() if callable(getattr(vd, "date")) else vd
        out.append((vd, float(r[1] or 0)))
    return out


def get_unapplied_repayment_ids(loan_id: int, as_of_date: date) -> set[int]:
    """Repayment IDs that have a pending unapplied_funds row for this loan with value_date <= as_of_date."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT repayment_id FROM unapplied_funds
                WHERE loan_id = %s AND status = 'pending' AND value_date <= %s
                """,
                (loan_id, as_of_date),
            )
            return {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}


def save_new_schedule_version(loan_id: int, schedule_df: pd.DataFrame, version: int) -> int:
    """Insert a new schedule version and its lines. Returns the new loan_schedules.id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO loan_schedules (loan_id, version) VALUES (%s, %s) RETURNING id",
                (loan_id, version),
            )
            schedule_id = cur.fetchone()[0]
        with conn.cursor() as cur:
            for _, row in schedule_df.iterrows():
                period = int(row.get("Period", row.get("period", 0)))
                period_date = str(row.get("Date", row.get("Date", "")))[:32] if pd.notna(row.get("Date")) else None
                payment = float(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0)))) if pd.notna(row.get("Payment", row.get("Monthly Installment", 0))) else 0.0
                principal = float(row.get("Principal", row.get("principal", 0))) if pd.notna(row.get("Principal")) else 0.0
                interest = float(row.get("Interest", row.get("interest", 0))) if pd.notna(row.get("Interest")) else 0.0
                principal_balance = float(row.get("Principal Balance", row.get("principal_balance", 0))) if pd.notna(row.get("Principal Balance")) else 0.0
                total_outstanding = float(row.get("Total Outstanding", row.get("total_outstanding", 0))) if pd.notna(row.get("Total Outstanding")) else 0.0
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (schedule_id, period, period_date, payment, principal, interest, principal_balance, total_outstanding),
                )
    return schedule_id


def update_loan_details(loan_id: int, **kwargs: Any) -> None:
    """Update selected columns on loans. Keys must be valid column names."""
    if not kwargs:
        return
    allowed = {"principal", "disbursed_amount", "term", "annual_rate", "monthly_rate", "installment", "total_payment",
               "end_date", "first_repayment_date", "loan_type"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    sets = ", ".join(f"{k} = %s" for k in updates) + ", updated_at = NOW()"
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE loans SET {sets} WHERE id = %s",
                (*updates.values(), loan_id),
            )


def get_amount_due_summary(loan_id: int, as_of: date | None = None) -> dict:
    """
    Compute a simple 'amount due today' view for a loan, based on:
      - total scheduled payments up to as_of (from schedule_lines)
      - total repayments up to as_of (from loan_repayments)
    Returns a dict with:
      {
        "amount_due": ...,
        "scheduled_total": ...,
        "repaid_total": ...,
      }
    """
    if as_of is None:
        as_of = date.today()

    with _connection() as conn:
        with conn.cursor() as cur:
            # Sum scheduled payments up to as_of
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(sl.payment), 0) AS scheduled_total
                FROM schedule_lines sl
                JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
                WHERE ls.loan_id = %s
                  AND (
                    sl."Date" IS NULL
                    OR TO_DATE(sl."Date", 'DD-Mon-YYYY') <= %s
                  )
                """,
                (loan_id, as_of),
            )
            row = cur.fetchone()
            scheduled_total = float(row[0]) if row and row[0] is not None else 0.0

            # Sum repayments up to as_of, using value_date when available
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM loan_repayments
                WHERE loan_id = %s
                  AND COALESCE(value_date, payment_date) <= %s
                """,
                (loan_id, as_of),
            )
            row = cur.fetchone()
            repaid_total = float(row[0]) if row and row[0] is not None else 0.0

    amount_due = max(scheduled_total - repaid_total, 0.0)
    return {
        "amount_due": amount_due,
        "scheduled_total": scheduled_total,
        "repaid_total": repaid_total,
    }


def save_loan_daily_state(
    loan_id: int,
    as_of_date: date,
    *,
    regular_interest_daily: float = 0.0,
    principal_not_due: float = 0.0,
    principal_arrears: float = 0.0,
    interest_accrued_balance: float = 0.0,
    interest_arrears_balance: float = 0.0,
    default_interest_daily: float = 0.0,
    default_interest_balance: float = 0.0,
    penalty_interest_daily: float = 0.0,
    penalty_interest_balance: float = 0.0,
    fees_charges_balance: float = 0.0,
    days_overdue: int = 0,
    regular_interest_period_to_date: float = 0.0,
    penalty_interest_period_to_date: float = 0.0,
    default_interest_period_to_date: float = 0.0,
    conn: Any = None,
) -> None:
    """
    Upsert daily loan bucket balances into loan_daily_state.
    total_exposure is computed as the sum of all principal + interest + fees buckets.
    Period-to-date columns are for efficient statement generation (no summing over days).
    If conn is provided, use it and do not commit (caller commits). Otherwise use a new connection and commit on exit.
    """
    as_of_date = _date_conv(as_of_date) or (as_of_date.date() if isinstance(as_of_date, datetime) else as_of_date)
    # Clamp balances to non-negative for storage
    principal_not_due = max(0.0, principal_not_due)
    principal_arrears = max(0.0, principal_arrears)
    interest_accrued_balance = max(0.0, interest_accrued_balance)
    interest_arrears_balance = max(0.0, interest_arrears_balance)
    default_interest_balance = max(0.0, default_interest_balance)
    penalty_interest_balance = max(0.0, penalty_interest_balance)
    fees_charges_balance = max(0.0, fees_charges_balance)
    # Control: if all arrears/penalty/default balances are zero, days_overdue must be 0
    arrears_total = principal_arrears + interest_arrears_balance + default_interest_balance + penalty_interest_balance
    if arrears_total <= 0:
        days_overdue = 0
    total_exposure = (
        principal_not_due
        + principal_arrears
        + interest_accrued_balance
        + interest_arrears_balance
        + default_interest_balance
        + penalty_interest_balance
        + fees_charges_balance
    )

    def _do_upsert(c: Any) -> None:
        with c.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_daily_state (
                    loan_id, as_of_date,
                    regular_interest_daily,
                    principal_not_due,
                    principal_arrears,
                    interest_accrued_balance,
                    interest_arrears_balance,
                    default_interest_daily,
                    default_interest_balance,
                    penalty_interest_daily,
                    penalty_interest_balance,
                    fees_charges_balance,
                    days_overdue,
                    total_exposure,
                    regular_interest_period_to_date,
                    penalty_interest_period_to_date,
                    default_interest_period_to_date
                )
                VALUES (
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (loan_id, as_of_date) DO UPDATE
                SET
                    regular_interest_daily   = EXCLUDED.regular_interest_daily,
                    principal_not_due        = EXCLUDED.principal_not_due,
                    principal_arrears        = EXCLUDED.principal_arrears,
                    interest_accrued_balance = EXCLUDED.interest_accrued_balance,
                    interest_arrears_balance = EXCLUDED.interest_arrears_balance,
                    default_interest_daily   = EXCLUDED.default_interest_daily,
                    default_interest_balance = EXCLUDED.default_interest_balance,
                    penalty_interest_daily   = EXCLUDED.penalty_interest_daily,
                    penalty_interest_balance = EXCLUDED.penalty_interest_balance,
                    fees_charges_balance     = EXCLUDED.fees_charges_balance,
                    days_overdue             = EXCLUDED.days_overdue,
                    total_exposure           = EXCLUDED.total_exposure,
                    regular_interest_period_to_date = EXCLUDED.regular_interest_period_to_date,
                    penalty_interest_period_to_date  = EXCLUDED.penalty_interest_period_to_date,
                    default_interest_period_to_date = EXCLUDED.default_interest_period_to_date
                """,
                (
                    loan_id,
                    as_of_date,
                    regular_interest_daily,
                    principal_not_due,
                    principal_arrears,
                    interest_accrued_balance,
                    interest_arrears_balance,
                    default_interest_daily,
                    default_interest_balance,
                    penalty_interest_daily,
                    penalty_interest_balance,
                    fees_charges_balance,
                    days_overdue,
                    total_exposure,
                    regular_interest_period_to_date,
                    penalty_interest_period_to_date,
                    default_interest_period_to_date,
                ),
            )

    if conn is not None:
        _do_upsert(conn)
    else:
        with _connection() as new_conn:
            _do_upsert(new_conn)


def apply_unapplied_funds_recast(
    unapplied_funds_id: int,
    *,
    as_of: date | None = None,
) -> None:
    """
    Apply pending unapplied funds to the loan via recast logic (separate from allocation).
    Reclassifies accrued interest → interest arrears and principal not due → principal arrears
    (up to the unapplied amount), then applies the payment to those buckets. Call only after
    funds have been credited to Unapplied (e.g. from Unapplied tab).
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, amount, value_date, status
                FROM unapplied_funds
                WHERE id = %s
                """,
                (unapplied_funds_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} not found.")
            if (row["status"] or "").lower() != "pending":
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} is not pending (status={row['status']}).")
            loan_id = int(row["loan_id"])
            amount = float(row["amount"])
            value_date = row["value_date"]
            if hasattr(value_date, "date"):
                value_date = value_date.date()
            eff_date = as_of or value_date

            cur.execute(
                """
                SELECT as_of_date, principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance,
                       penalty_interest_balance, fees_charges_balance, days_overdue,
                       regular_interest_daily, default_interest_daily, penalty_interest_daily,
                       regular_interest_period_to_date, penalty_interest_period_to_date, default_interest_period_to_date
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date <= %s
                ORDER BY as_of_date DESC LIMIT 1
                """,
                (loan_id, eff_date),
            )
            st_row = cur.fetchone()
            if not st_row:
                raise ValueError(f"No loan_daily_state for loan_id={loan_id} on or before {eff_date}.")
            balances = {k: float(st_row.get(k, 0) or 0) for k in (
                "principal_not_due", "principal_arrears", "interest_accrued_balance",
                "interest_arrears_balance", "default_interest_balance",
                "penalty_interest_balance", "fees_charges_balance", "days_overdue"
            )}
            # Preserve accruals for eff_date: use existing row if it's for eff_date, else engine state
            st_row_as_of = st_row.get("as_of_date")
            if hasattr(st_row_as_of, "date"):
                st_row_as_of = st_row_as_of.date() if callable(getattr(st_row_as_of, "date")) else st_row_as_of
            if st_row_as_of == eff_date:
                acc_daily = (float(st_row.get("regular_interest_daily") or 0), float(st_row.get("default_interest_daily") or 0), float(st_row.get("penalty_interest_daily") or 0))
                acc_period = (float(st_row.get("regular_interest_period_to_date") or 0), float(st_row.get("penalty_interest_period_to_date") or 0), float(st_row.get("default_interest_period_to_date") or 0))
            else:
                from eod import get_engine_state_for_loan_date
                _eng = get_engine_state_for_loan_date(loan_id, eff_date)
                if _eng:
                    acc_daily = (_eng.get("regular_interest_daily", 0) or 0, _eng.get("default_interest_daily", 0) or 0, _eng.get("penalty_interest_daily", 0) or 0)
                    acc_period = (_eng.get("regular_interest_period_to_date", 0) or 0, _eng.get("penalty_interest_period_to_date", 0) or 0, _eng.get("default_interest_period_to_date", 0) or 0)
                else:
                    acc_daily = (0.0, 0.0, 0.0)
                    acc_period = (0.0, 0.0, 0.0)

            remaining = amount
            move_accrued_to_arrears = min(balances["interest_accrued_balance"], remaining)
            move_accrued_to_arrears = round(move_accrued_to_arrears, 2)
            remaining -= move_accrued_to_arrears
            move_principal_not_due_to_arrears = 0.0
            if remaining > 1e-6:
                move_principal_not_due_to_arrears = min(balances["principal_not_due"], remaining)
                move_principal_not_due_to_arrears = round(move_principal_not_due_to_arrears, 2)

            new_interest_accrued = round(balances["interest_accrued_balance"] - move_accrued_to_arrears, 2)
            new_interest_arrears = round(balances["interest_arrears_balance"] - move_accrued_to_arrears, 2)
            new_principal_not_due = round(balances["principal_not_due"] - move_principal_not_due_to_arrears, 2)
            new_principal_arrears = round(balances["principal_arrears"] - move_principal_not_due_to_arrears, 2)

            save_loan_daily_state(
                loan_id=loan_id,
                as_of_date=eff_date,
                regular_interest_daily=acc_daily[0],
                principal_not_due=new_principal_not_due,
                principal_arrears=new_principal_arrears,
                interest_accrued_balance=new_interest_accrued,
                interest_arrears_balance=new_interest_arrears,
                default_interest_daily=acc_daily[1],
                default_interest_balance=balances["default_interest_balance"],
                penalty_interest_daily=acc_daily[2],
                penalty_interest_balance=balances["penalty_interest_balance"],
                fees_charges_balance=balances["fees_charges_balance"],
                days_overdue=int(balances["days_overdue"]),
                regular_interest_period_to_date=acc_period[0],
                penalty_interest_period_to_date=acc_period[1],
                default_interest_period_to_date=acc_period[2],
            )

            cur.execute(
                """
                UPDATE unapplied_funds
                SET status = 'applied', applied_at = NOW(), notes = COALESCE(notes, '') || ' Applied via recast.'
                WHERE id = %s
                """,
                (unapplied_funds_id,),
            )


def _credit_unapplied_funds(
    conn,
    loan_id: int,
    repayment_id: int,
    amount: float,
    value_date: date,
    currency: str = "USD",
) -> None:
    """Insert a row into unapplied_funds (suspense) for overpayment."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO unapplied_funds (loan_id, repayment_id, amount, currency, value_date, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            """,
            (loan_id, repayment_id, amount, currency, value_date),
        )


def reallocate_repayment(
    repayment_id: int,
    *,
    system_config: dict | None = None,
) -> None:
    """
    Correct an already-saved receipt: reverse its allocation and unapplied credits,
    then re-run allocation (e.g. after fixing waterfall config or logic).
    Allocation is done at save receipt, not at EOD.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT lr.id, lr.loan_id, lr.amount,
                       COALESCE(lr.value_date, lr.payment_date) AS eff_date
                FROM loan_repayments lr
                WHERE lr.id = %s
                """,
                (repayment_id,),
            )
            rrow = cur.fetchone()
            if not rrow:
                raise ValueError(f"Repayment {repayment_id} not found.")
            loan_id = int(rrow["loan_id"])
            eff_date = rrow["eff_date"]
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            cur.execute(
                """
                SELECT alloc_principal_not_due, alloc_principal_arrears,
                       alloc_interest_accrued, alloc_interest_arrears,
                       alloc_default_interest, alloc_penalty_interest, alloc_fees_charges
                FROM loan_repayment_allocation
                WHERE repayment_id = %s
                """,
                (repayment_id,),
            )
            alloc_row = cur.fetchone()
            if not alloc_row:
                # No prior allocation; ensure state exists then allocate
                conn.commit()
                from eod import run_eod_for_date
                run_eod_for_date(eff_date)
                allocate_repayment_waterfall(repayment_id, system_config=system_config)
                return

            cur.execute(
                "DELETE FROM unapplied_funds WHERE repayment_id = %s AND status = 'pending'",
                (repayment_id,),
            )

    # Restore state to engine minus *other* receipts' allocations (not add-back: add-back was wrong
    # when bucket had been capped to 0, giving 0+160=160 instead of engine.interest_arrears=101.75).
    from eod import get_engine_state_for_loan_date
    engine_state = get_engine_state_for_loan_date(loan_id, eff_date)
    if not engine_state:
        raise ValueError(
            f"Cannot get engine state for loan_id={loan_id} on {eff_date} (no schedule or loan not active). "
            "Run EOD up to that date first."
        )
    other_alloc = get_allocation_totals_for_loan_date(loan_id, eff_date, exclude_repayment_id=repayment_id)
    def _sub(a: float, b: float) -> float:
        return max(0.0, a - b)
    # Build restored balances and persist so EOD/queries see engine - others; then allocate using
    # the same in-memory state so we don't depend on a second connection seeing this row.
    restored = {
        "principal_not_due": _sub(engine_state["principal_not_due"], other_alloc.get("alloc_principal_not_due", 0.0)),
        "principal_arrears": _sub(engine_state["principal_arrears"], other_alloc.get("alloc_principal_arrears", 0.0)),
        "interest_accrued_balance": _sub(engine_state["interest_accrued_balance"], other_alloc.get("alloc_interest_accrued", 0.0)),
        "interest_arrears_balance": _sub(engine_state["interest_arrears_balance"], other_alloc.get("alloc_interest_arrears", 0.0)),
        "default_interest_balance": _sub(engine_state["default_interest_balance"], other_alloc.get("alloc_default_interest", 0.0)),
        "penalty_interest_balance": _sub(engine_state["penalty_interest_balance"], other_alloc.get("alloc_penalty_interest", 0.0)),
        "fees_charges_balance": _sub(engine_state["fees_charges_balance"], other_alloc.get("alloc_fees_charges", 0.0)),
        "days_overdue": engine_state["days_overdue"],
    }
    save_loan_daily_state(
        loan_id=loan_id,
        as_of_date=eff_date,
        regular_interest_daily=engine_state["regular_interest_daily"],
        principal_not_due=restored["principal_not_due"],
        principal_arrears=restored["principal_arrears"],
        interest_accrued_balance=restored["interest_accrued_balance"],
        interest_arrears_balance=restored["interest_arrears_balance"],
        default_interest_daily=engine_state["default_interest_daily"],
        default_interest_balance=restored["default_interest_balance"],
        penalty_interest_daily=engine_state["penalty_interest_daily"],
        penalty_interest_balance=restored["penalty_interest_balance"],
        fees_charges_balance=restored["fees_charges_balance"],
        days_overdue=restored["days_overdue"],
        regular_interest_period_to_date=engine_state.get("regular_interest_period_to_date", 0.0),
        penalty_interest_period_to_date=engine_state.get("penalty_interest_period_to_date", 0.0),
        default_interest_period_to_date=engine_state.get("default_interest_period_to_date", 0.0),
    )
    allocate_repayment_waterfall(
        repayment_id,
        system_config=system_config,
        preloaded_balances=restored,
        )


def allocate_repayment_waterfall(
    repayment_id: int,
    *,
    as_of: date | None = None,
    system_config: dict | None = None,
    preloaded_balances: dict | None = None,
) -> None:
    """
    Allocate a repayment across loan buckets using the configured waterfall
    (Standard or Borrower-friendly) and loan_daily_state. Persists allocation
    and updates daily state. Called at save receipt (real-time allocation).

    When reallocate_repayment calls this, it can pass preloaded_balances (engine - others)
    so we use that state instead of reading from DB, avoiding read-after-write visibility issues.

    Standard waterfall:
    - Allocate to buckets 1–5 (fees, penalty, default interest, interest arrears,
      principal arrears). Principal arrears are due and are paid.
    - Never allocate directly to interest accrued or principal not due.
    - Any remainder goes to Unapplied funds. Recast is a separate process
      (apply from Unapplied tab after funds are credited).
    """
    if as_of is None:
        as_of = date.today()

    cfg = system_config or load_system_config_from_db() or {}
    profile_key, bucket_order = _get_waterfall_config(cfg)

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT lr.id, lr.loan_id, lr.amount,
                       COALESCE(lr.value_date, lr.payment_date) AS eff_date
                FROM loan_repayments lr
                WHERE lr.id = %s
                """,
                (repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Repayment {repayment_id} not found.")

            amount = float(row["amount"])
            if amount <= 0:
                return

            loan_id = int(row["loan_id"])
            eff_date = row["eff_date"] or as_of
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            if preloaded_balances is not None:
                # Use state passed by reallocate_repayment so we don't depend on DB read seeing the restore.
                state_as_of = eff_date
                balances = {
                    "principal_not_due": float(preloaded_balances.get("principal_not_due", 0) or 0),
                    "principal_arrears": float(preloaded_balances.get("principal_arrears", 0) or 0),
                    "interest_accrued_balance": float(preloaded_balances.get("interest_accrued_balance", 0) or 0),
                    "interest_arrears_balance": float(preloaded_balances.get("interest_arrears_balance", 0) or 0),
                    "default_interest_balance": float(preloaded_balances.get("default_interest_balance", 0) or 0),
                    "penalty_interest_balance": float(preloaded_balances.get("penalty_interest_balance", 0) or 0),
                    "fees_charges_balance": float(preloaded_balances.get("fees_charges_balance", 0) or 0),
                }
                days_overdue = int(preloaded_balances.get("days_overdue", 0) or 0)
            else:
                cur.execute(
                    """
                    SELECT as_of_date,
                           principal_not_due, principal_arrears, interest_accrued_balance,
                           interest_arrears_balance, default_interest_balance,
                           penalty_interest_balance, fees_charges_balance, days_overdue
                    FROM loan_daily_state
                    WHERE loan_id = %s AND as_of_date <= %s
                    ORDER BY as_of_date DESC LIMIT 1
                    """,
                    (loan_id, eff_date),
                )
                st_row = cur.fetchone()
                if st_row is None:
                    from eod import run_eod_for_date
                    run_eod_for_date(eff_date)
                    cur.execute(
                        """
                        SELECT as_of_date,
                               principal_not_due, principal_arrears, interest_accrued_balance,
                               interest_arrears_balance, default_interest_balance,
                               penalty_interest_balance, fees_charges_balance, days_overdue
                        FROM loan_daily_state
                        WHERE loan_id = %s AND as_of_date <= %s
                        ORDER BY as_of_date DESC LIMIT 1
                        """,
                        (loan_id, eff_date),
                    )
                    st_row = cur.fetchone()
                state_as_of = st_row.get("as_of_date") if st_row else None
                balances = {
                    "principal_not_due": 0.0,
                    "principal_arrears": 0.0,
                    "interest_accrued_balance": 0.0,
                    "interest_arrears_balance": 0.0,
                    "default_interest_balance": 0.0,
                    "penalty_interest_balance": 0.0,
                    "fees_charges_balance": 0.0,
                }
                if st_row:
                    for k in balances:
                        balances[k] = float(st_row.get(k, 0) or 0)
                days_overdue = int(st_row["days_overdue"]) if st_row else 0

            alloc, unapplied = compute_waterfall_allocation(
                amount, balances, bucket_order, profile_key,
                state_as_of=state_as_of, repayment_id=repayment_id,
            )
            if unapplied > 1e-6:
                _credit_unapplied_funds(conn, loan_id, repayment_id, unapplied, eff_date)

            alloc_principal_not_due = alloc.get("alloc_principal_not_due", 0.0)
            alloc_principal_arrears = alloc.get("alloc_principal_arrears", 0.0)
            alloc_interest_accrued = alloc.get("alloc_interest_accrued", 0.0)
            alloc_interest_arrears = alloc.get("alloc_interest_arrears", 0.0)
            alloc_default_interest = alloc.get("alloc_default_interest", 0.0)
            alloc_penalty_interest = alloc.get("alloc_penalty_interest", 0.0)
            alloc_fees_charges = alloc.get("alloc_fees_charges", 0.0)

            alloc_principal_total = alloc_principal_not_due + alloc_principal_arrears
            alloc_interest_total = (
                alloc_interest_accrued
                + alloc_interest_arrears
                + alloc_default_interest
                + alloc_penalty_interest
            )
            alloc_fees_total = alloc_fees_charges
            total_alloc = alloc_principal_total + alloc_interest_total + alloc_fees_total
            if abs((total_alloc + unapplied) - amount) > 0.01:
                raise ValueError(
                    f"Allocation mismatch for repayment {repayment_id}: "
                    f"amount={amount}, allocated={total_alloc}, unapplied={unapplied}"
                )

            cur2 = conn.cursor()
            cur2.execute(
                """
                INSERT INTO loan_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (repayment_id) DO UPDATE SET
                    alloc_principal_not_due = EXCLUDED.alloc_principal_not_due,
                    alloc_principal_arrears = EXCLUDED.alloc_principal_arrears,
                    alloc_interest_accrued = EXCLUDED.alloc_interest_accrued,
                    alloc_interest_arrears = EXCLUDED.alloc_interest_arrears,
                    alloc_default_interest = EXCLUDED.alloc_default_interest,
                    alloc_penalty_interest = EXCLUDED.alloc_penalty_interest,
                    alloc_fees_charges = EXCLUDED.alloc_fees_charges,
                    alloc_principal_total = EXCLUDED.alloc_principal_total,
                    alloc_interest_total = EXCLUDED.alloc_interest_total,
                    alloc_fees_total = EXCLUDED.alloc_fees_total
                """,
                (
                    repayment_id,
                    alloc_principal_not_due,
                    alloc_principal_arrears,
                    alloc_interest_accrued,
                    alloc_interest_arrears,
                    alloc_default_interest,
                    alloc_penalty_interest,
                    alloc_fees_charges,
                    alloc_principal_total,
                    alloc_interest_total,
                    alloc_fees_total,
                ),
            )

            new_interest_accrued = max(0.0, balances["interest_accrued_balance"] - alloc_interest_accrued)
            new_interest_arrears = max(0.0, balances["interest_arrears_balance"] - alloc_interest_arrears)
            new_principal_not_due = max(0.0, balances["principal_not_due"] - alloc_principal_not_due)
            new_principal_arrears = max(0.0, balances["principal_arrears"] - alloc_principal_arrears)
            new_default_interest = max(0.0, balances["default_interest_balance"] - alloc_default_interest)
            new_penalty_interest = max(0.0, balances["penalty_interest_balance"] - alloc_penalty_interest)
            new_fees_charges = max(0.0, balances["fees_charges_balance"] - alloc_fees_charges)

            # Preserve accruals for eff_date: use engine state so we never overwrite to zero
            from eod import get_engine_state_for_loan_date
            engine_state = get_engine_state_for_loan_date(loan_id, eff_date)
            if engine_state:
                reg_daily = engine_state.get("regular_interest_daily", 0) or 0
                def_daily = engine_state.get("default_interest_daily", 0) or 0
                pen_daily = engine_state.get("penalty_interest_daily", 0) or 0
                reg_period = engine_state.get("regular_interest_period_to_date", 0) or 0
                pen_period = engine_state.get("penalty_interest_period_to_date", 0) or 0
                def_period = engine_state.get("default_interest_period_to_date", 0) or 0
            else:
                reg_daily = def_daily = pen_daily = 0.0
                reg_period = pen_period = def_period = 0.0

            total_exposure = (
                new_principal_not_due + new_principal_arrears + new_interest_accrued + new_interest_arrears
                + new_default_interest + new_penalty_interest + new_fees_charges
            )
            eff_date_val = _date_conv(eff_date) or (eff_date.date() if isinstance(eff_date, datetime) else eff_date)

            # Update loan_daily_state in the same connection/cursor so it commits with the allocation.
            # Explicit UPDATE so we don't rely on ON CONFLICT; row must exist (from restore or EOD).
            cur2.execute(
                """
                UPDATE loan_daily_state SET
                    regular_interest_daily = %s,
                    principal_not_due = %s,
                    principal_arrears = %s,
                    interest_accrued_balance = %s,
                    interest_arrears_balance = %s,
                    default_interest_daily = %s,
                    default_interest_balance = %s,
                    penalty_interest_daily = %s,
                    penalty_interest_balance = %s,
                    fees_charges_balance = %s,
                    days_overdue = %s,
                    total_exposure = %s,
                    regular_interest_period_to_date = %s,
                    penalty_interest_period_to_date = %s,
                    default_interest_period_to_date = %s
                WHERE loan_id = %s AND as_of_date = %s
                """,
                (
                    reg_daily,
                    new_principal_not_due,
                    new_principal_arrears,
                    new_interest_accrued,
                    new_interest_arrears,
                    def_daily,
                    new_default_interest,
                    pen_daily,
                    new_penalty_interest,
                    new_fees_charges,
                    days_overdue,
                    total_exposure,
                    reg_period,
                    pen_period,
                    def_period,
                    loan_id,
                    eff_date_val,
                ),
            )
            if cur2.rowcount == 0:
                # Row missing (e.g. first receipt of the day); upsert so we don't leave allocation without state
                save_loan_daily_state(
                    loan_id=loan_id,
                    as_of_date=eff_date_val,
                    regular_interest_daily=reg_daily,
                    principal_not_due=new_principal_not_due,
                    principal_arrears=new_principal_arrears,
                    interest_accrued_balance=new_interest_accrued,
                    interest_arrears_balance=new_interest_arrears,
                    default_interest_daily=def_daily,
                    default_interest_balance=new_default_interest,
                    penalty_interest_daily=pen_daily,
                    penalty_interest_balance=new_penalty_interest,
                    fees_charges_balance=new_fees_charges,
                    days_overdue=days_overdue,
                    regular_interest_period_to_date=reg_period,
                    penalty_interest_period_to_date=pen_period,
                    default_interest_period_to_date=def_period,
                    conn=conn,
                )
            cur2.close()


# -----------------------------------------------------------------------------
# Products
# -----------------------------------------------------------------------------

def list_products(active_only: bool = True) -> list[dict]:
    """List products. Each dict: id, code, name, loan_type, is_active, created_at, updated_at."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                where = " WHERE is_active = TRUE" if active_only else ""
                cur.execute(
                    f"SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products{where} ORDER BY code"
                )
                return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []


def get_product(product_id: int) -> dict | None:
    """Get product by id."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products WHERE id = %s",
                    (product_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
    except Exception:
        return None


def get_product_by_code(code: str) -> dict | None:
    """Get product by code."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products WHERE code = %s",
                    (code.strip(),),
                )
                row = cur.fetchone()
                return dict(row) if row else None
    except Exception:
        return None


def create_product(code: str, name: str, loan_type: str) -> int:
    """Create a product. Returns product id."""
    code = code.strip().upper()
    name = name.strip()
    lt = {"Consumer Loan": "consumer_loan", "Term Loan": "term_loan", "Bullet Loan": "bullet_loan", "Customised Repayments": "customised_repayments"}.get(
        loan_type, loan_type.replace(" ", "_").lower()
    )
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products (code, name, loan_type, is_active) VALUES (%s, %s, %s, TRUE) RETURNING id",
                (code, name, lt),
            )
            return cur.fetchone()[0]


def update_product(product_id: int, *, name: str | None = None, loan_type: str | None = None, is_active: bool | None = None) -> None:
    """Update product name, loan_type, and/or is_active."""
    updates = []
    args = []
    if name is not None:
        updates.append("name = %s")
        args.append(name.strip())
    if loan_type is not None:
        lt = {"Consumer Loan": "consumer_loan", "Term Loan": "term_loan", "Bullet Loan": "bullet_loan", "Customised Repayments": "customised_repayments"}.get(
            loan_type, loan_type.replace(" ", "_").lower()
        )
        updates.append("loan_type = %s")
        args.append(lt)
    if is_active is not None:
        updates.append("is_active = %s")
        args.append(is_active)
    if not updates:
        return
    args.append(product_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE products SET updated_at = NOW(), {', '.join(updates)} WHERE id = %s",
                args,
            )


def delete_product(product_id: int) -> None:
    """Delete a product and its config. Raises ValueError if any loans reference this product."""
    CONFIG_KEY_PRODUCT_PREFIX = "product_config:"
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, code FROM products WHERE id = %s", (product_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("Product not found.")
            code = row["code"]
            cur.execute("SELECT COUNT(*) AS n FROM loans WHERE product_code = %s", (code,))
            n = cur.fetchone()["n"] or 0
            if n > 0:
                raise ValueError(f"Cannot delete: {n} loan(s) use this product. Deactivate it instead.")
            cur.execute("DELETE FROM config WHERE key = %s", (CONFIG_KEY_PRODUCT_PREFIX + code,))
            cur.execute("DELETE FROM products WHERE id = %s", (product_id,))


CONFIG_KEY_PRODUCT_PREFIX = "product_config:"


def get_product_config_from_db(code: str) -> dict | None:
    """Load product config JSON from config table."""
    try:
        key = CONFIG_KEY_PRODUCT_PREFIX + code.strip()
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM config WHERE key = %s", (key,))
                row = cur.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
    except Exception:
        pass
    return None


def save_product_config_to_db(code: str, config: dict) -> bool:
    """Save product config JSON."""
    try:
        key = CONFIG_KEY_PRODUCT_PREFIX + code.strip()
        value_json = json.dumps(config)
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO config (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """,
                    (key, value_json),
                )
        return True
    except Exception:
        return False


# -----------------------------------------------------------------------------
# System configurations (stored in config table)
# -----------------------------------------------------------------------------

CONFIG_KEY_SYSTEM = "system_config"


def load_system_config_from_db() -> dict | None:
    """
    Load system configurations from the config table.
    Returns None if not found or on error.
    """
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM config WHERE key = %s", (CONFIG_KEY_SYSTEM,))
                row = cur.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
    except Exception:
        pass
    return None


def save_system_config_to_db(config: dict) -> bool:
    """
    Save system configurations to the config table.
    Uses INSERT ... ON CONFLICT (key) DO UPDATE for upsert.
    Returns True on success, False on error.
    """
    try:
        value_json = json.dumps(config)
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO config (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """,
                    (CONFIG_KEY_SYSTEM, value_json),
                )
        return True
    except Exception:
        return False
