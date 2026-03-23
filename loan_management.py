"""
Loan management: persist loan details, schedules, and repayments to the database.
Uses loans.py for computation only; this module handles DB writes.
"""

from __future__ import annotations

import contextlib
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd

from decimal_utils import as_10dp

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
        if name is None:
            continue
        s = str(name).strip()
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


def _repayment_journal_reference(loan_id: int, repayment_id: int) -> str:
    """Journal header reference for receipt allocation GL: always names loan and repayment."""
    return f"Loan {loan_id}, Repayment id {repayment_id}"


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
    unapplied = float(as_10dp(remaining)) if remaining > _WATERFALL_REMAINING_EPS else 0.0
    return {k: float(as_10dp(v)) for k, v in alloc.items()}, unapplied


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
                alloc_total = alloc_principal_total + alloc_interest_total + alloc_fees_total
                cur.execute(
                    """
                    INSERT INTO loan_repayment_allocation (
                        repayment_id,
                        alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total,
                        alloc_total, unallocated, event_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        repayment_id,
                        alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total,
                        float(as_10dp(alloc_total)), float(as_10dp(unapplied)), "new_allocation",
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


def _log_allocation_audit(
    event_type: str,
    loan_id: int,
    as_of_date: date,
    *,
    repayment_id: int | None = None,
    original_repayment_id: int | None = None,
    narration: str | None = None,
    details: dict | None = None,
    conn: Any = None,
) -> None:
    """Write to allocation_audit_log for reversal add-back and system reallocation. No-op if table missing."""
    try:
        def _do_insert(c: Any) -> None:
            with c.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO allocation_audit_log
                    (event_type, loan_id, as_of_date, repayment_id, original_repayment_id, narration, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event_type,
                        loan_id,
                        as_of_date,
                        repayment_id,
                        original_repayment_id,
                        narration,
                        Json(details) if details else None,
                    ),
                )
        if conn is not None:
            _do_insert(conn)
        else:
            with _connection() as c:
                _do_insert(c)
    except Exception:
        pass  # Table may not exist; do not fail the main operation


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


def build_loan_approval_journal_payload(details: dict[str, Any]) -> dict[str, Decimal]:
    """
    Amounts for LOAN_APPROVAL (Dr loan principal, Cr cash, Cr deferred fee liability).

    Total debits must equal total credits: gross loan asset at inception must equal
    cash disbursed plus deferred fees. Using only facility `principal` for the debit
    breaks double-entry when that field holds net disbursed while fees are non-zero.

    Schema intent (loan fee columns): disbursed_amount + fee amounts aligns with facility.
    Here the principal debit is disbursed + deferred fees so journals always balance.
    """
    prin_amt = Decimal(str(as_10dp(details.get("principal", details.get("facility", 0)))))
    disb_amt = Decimal(str(as_10dp(details.get("disbursed_amount", details.get("principal", 0)))))

    drawdown_fee = Decimal(
        str(
            as_10dp(
                details.get("drawdown_fee_amount")
                or (float(prin_amt) * float(details.get("drawdown_fee") or 0))
            )
        )
    )
    arrangement_fee = Decimal(
        str(
            as_10dp(
                details.get("arrangement_fee_amount")
                or (float(prin_amt) * float(details.get("arrangement_fee") or 0))
            )
        )
    )
    admin_fee = Decimal(
        str(
            as_10dp(
                details.get("admin_fee_amount")
                or (float(prin_amt) * float(details.get("admin_fee") or 0))
            )
        )
    )
    total_fees = as_10dp(drawdown_fee + arrangement_fee + admin_fee)
    disb_amt = as_10dp(disb_amt)
    gross_loan_principal = as_10dp(disb_amt + total_fees)

    return {
        "loan_principal": gross_loan_principal,
        "cash_operating": disb_amt,
        "deferred_fee_liability": total_fees,
    }


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
                    admin_fee_amount, drawdown_fee_amount, arrangement_fee_amount,
                    disbursement_date, start_date, end_date, first_repayment_date,
                    installment, total_payment, grace_type, moratorium_months, bullet_type, scheme,
                    payment_timing, metadata, status, agent_id, relationship_manager_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
                """,
                (
                    customer_id,
                    loan_type_db,
                    product_code,
                    float(as_10dp(details.get("principal", details.get("facility", 0)))),
                    float(as_10dp(details.get("disbursed_amount", details.get("principal", 0)))),
                    int(details.get("term", 0)),
                    float(as_10dp(details["annual_rate"])) if details.get("annual_rate") is not None else None,
                    float(as_10dp(details["monthly_rate"])) if details.get("monthly_rate") is not None else None,
                    float(as_10dp(details.get("drawdown_fee"))) if details.get("drawdown_fee") is not None else None,
                    float(as_10dp(details.get("arrangement_fee"))) if details.get("arrangement_fee") is not None else None,
                    float(as_10dp(details.get("admin_fee"))) if details.get("admin_fee") is not None else None,
                    # Absolute fee amounts: prefer explicitly passed value, else derive from rate * principal
                    float(as_10dp(details.get("admin_fee_amount") or (float(details.get("principal", details.get("facility", 0))) * float(details.get("admin_fee") or 0)))),
                    float(as_10dp(details.get("drawdown_fee_amount") or (float(details.get("principal", details.get("facility", 0))) * float(details.get("drawdown_fee") or 0)))),
                    float(as_10dp(details.get("arrangement_fee_amount") or (float(details.get("principal", details.get("facility", 0))) * float(details.get("arrangement_fee") or 0)))),
                    _date_conv(disb_date),
                    _date_conv(disb_date),
                    _date_conv(details.get("end_date")),
                    _date_conv(details.get("first_repayment_date")),
                    float(as_10dp(details["installment"])) if details.get("installment") is not None else None,
                    float(as_10dp(details["total_payment"])) if details.get("total_payment") is not None else None,
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
                payment = float(as_10dp(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0))))) if pd.notna(row.get("Payment", row.get("Monthly Installment", 0))) else 0.0
                principal = float(as_10dp(row.get("Principal", row.get("principal", 0)))) if pd.notna(row.get("Principal")) else 0.0
                interest = float(as_10dp(row.get("Interest", row.get("interest", 0)))) if pd.notna(row.get("Interest")) else 0.0
                principal_balance = float(as_10dp(row.get("Principal Balance", row.get("principal_balance", 0)))) if pd.notna(row.get("Principal Balance")) else 0.0
                total_outstanding = float(as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0)))) if pd.notna(row.get("Total Outstanding")) else 0.0
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (schedule_id, period, period_date, payment, principal, interest, principal_balance, total_outstanding),
                )

    try:
        from accounting_service import AccountingService

        svc = AccountingService()
        payload = build_loan_approval_journal_payload(details)

        disb_date_str = details.get("disbursement_date") or details.get("start_date")
        e_date = _date_conv(disb_date_str) if disb_date_str else None
        
        svc.post_event(
            event_type="LOAN_APPROVAL",
            reference=f"LOAN-{loan_id}",
            description=f"Loan Approval and Disbursement for {loan_id}",
            event_id=str(loan_id),
            created_by="system",
            entry_date=e_date,
            payload=payload
        )
    except Exception as e:
        print(f"Failed to post LOAN_APPROVAL journal for loan {loan_id}: {e}")

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
    Reversals must use reverse_repayment(); negative amounts are rejected.
    """
    if amount <= 0:
        raise ValueError(
            "Negative or zero amounts are not allowed. Use reverse_repayment() for reversals."
        )
    pdate = _date_conv(payment_date) if payment_date else None
    if not pdate:
        raise ValueError("payment_date is required")
    vdate = _date_conv(value_date) if value_date else pdate
    sdate = system_date
    if sdate is None:
        try:
            from system_business_date import get_effective_date
            sdate = datetime.combine(get_effective_date(), datetime.now().time())
        except ImportError:
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
                (loan_id, schedule_line_id, period_number, float(as_10dp(amount)), pdate, ref, customer_reference, company_reference, vdate, sdate, status),
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
    # Ensure original has allocation before we reverse (so reversal row gets unallocation_parent_reversed)
    alloc_row = _get_allocation_sum_for_repayment(original_repayment_id)
    if not alloc_row:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, amount FROM loan_repayments WHERE id = %s",
                    (original_repayment_id,),
                )
                r = cur.fetchone()
        if r and float(r.get("amount") or 0) > 0:
            allocate_repayment_waterfall(original_repayment_id)

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
            alloc_row = _get_allocation_sum_for_repayment(original_repayment_id, conn)
            if alloc_row:
                def _f(v):
                    return float(v or 0)
                _log_allocation_audit(
                    "reversal_add_back",
                    loan_id,
                    eff_date,
                    original_repayment_id=original_repayment_id,
                    narration="Reversal of receipt allocation",
                    details={
                        "alloc_principal_arrears": _f(alloc_row["alloc_principal_arrears"]),
                        "alloc_interest_arrears": _f(alloc_row["alloc_interest_arrears"]),
                        "alloc_penalty_interest": _f(alloc_row["alloc_penalty_interest"]),
                    },
                    conn=conn,
                )
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
            # Reverse unapplied credits: insert debit rows (ledger-style, no DELETE)
            cur.execute(
                """
                SELECT id, amount FROM unapplied_funds
                WHERE repayment_id = %s AND amount > 0
                """,
                (original_repayment_id,),
            )
            for uf_row in cur.fetchall():
                amt = float(as_10dp(-float(uf_row["amount"] or 0)))
                cur.execute(
                    """
                    INSERT INTO unapplied_funds (loan_id, amount, value_date, entry_type, reference, source_repayment_id, currency)
                    VALUES (%s, %s, %s, 'debit', 'Reversal of receipt', %s, 'USD')
                    """,
                    (loan_id, amt, eff_date, original_repayment_id),
                )

            # Reversal cascade: reverse any allocations that consumed this receipt's overpayment
            # (EOD apply-to-arrears with source_repayment_id = original)
            cur.execute(
                """
                SELECT lra.id, lra.repayment_id, lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                       lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                       lra.alloc_default_interest, lra.alloc_penalty_interest, lra.alloc_fees_charges,
                       lr.value_date AS alloc_value_date
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lra.source_repayment_id = %s AND lra.event_type = 'unapplied_funds_allocation'
                """,
                (original_repayment_id,),
            )
            for alloc in cur.fetchall():
                def _f(v):
                    return float(v or 0)
                apr = _f(alloc["alloc_principal_not_due"])
                apa = _f(alloc["alloc_principal_arrears"])
                aia = _f(alloc["alloc_interest_accrued"])
                aiar = _f(alloc["alloc_interest_arrears"])
                adi = _f(alloc["alloc_default_interest"])
                api = _f(alloc["alloc_penalty_interest"])
                afc = _f(alloc["alloc_fees_charges"])
                alloc_date = alloc["alloc_value_date"]
                if hasattr(alloc_date, "date"):
                    alloc_date = alloc_date.date() if callable(getattr(alloc_date, "date")) else alloc_date
                # Add back arrears to loan_daily_state
                cur.execute(
                    """
                    UPDATE loan_daily_state
                    SET principal_not_due = principal_not_due + %s, principal_arrears = principal_arrears + %s,
                        interest_accrued_balance = interest_accrued_balance + %s,
                        interest_arrears_balance = interest_arrears_balance + %s,
                        default_interest_balance = default_interest_balance + %s,
                        penalty_interest_balance = penalty_interest_balance + %s,
                        fees_charges_balance = fees_charges_balance + %s
                    WHERE loan_id = %s AND as_of_date = %s
                    """,
                    (apr, apa, aia, aiar, adi, api, afc, loan_id, alloc_date),
                )
                cur.execute(
                    """
                    UPDATE loan_daily_state SET total_exposure = principal_not_due + principal_arrears
                        + interest_accrued_balance + interest_arrears_balance
                        + default_interest_balance + penalty_interest_balance + fees_charges_balance
                    WHERE loan_id = %s AND as_of_date = %s
                    """,
                    (loan_id, alloc_date),
                )
                # Insert negative allocation row (unallocation_parent_reversed)
                rev_alloc_total = -(apr + apa + aia + aiar + adi + api + afc)
                cur.execute(
                    """
                    INSERT INTO loan_repayment_allocation (
                        repayment_id, alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total,
                        alloc_total, event_type, source_repayment_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'unallocation_parent_reversed', %s)
                    """,
                    (
                        alloc["repayment_id"],
                        float(as_10dp(-apr)), float(as_10dp(-apa)), float(as_10dp(-aia)), float(as_10dp(-aiar)),
                        float(as_10dp(-adi)), float(as_10dp(-api)), float(as_10dp(-afc)),
                        float(as_10dp(-(apr + apa))), float(as_10dp(-(aia + aiar + adi + api))), float(as_10dp(-afc)),
                        float(as_10dp(rev_alloc_total)), original_repayment_id,
                    ),
                )
                # Offset the unapplied debit we created when applying (insert credit to "unconsume")
                amount_applied = apr + apa + aia + aiar + adi + api + afc
                cur.execute(
                    """
                    INSERT INTO unapplied_funds (loan_id, amount, value_date, entry_type, reference, source_repayment_id, currency)
                    VALUES (%s, %s, %s, 'credit', 'Reversal of allocation (parent receipt reversed)', %s, 'USD')
                    """,
                    (loan_id, float(as_10dp(amount_applied)), alloc_date, original_repayment_id),
                )

            sdate = system_date
            if sdate is None:
                sdate = datetime.now()
            elif isinstance(sdate, str):
                sdate = datetime.fromisoformat(sdate.replace("Z", "+00:00"))

            # Prefix references with "r" so reversal is clearly linked to original (e.g. r16, rREC-001)
            orig_ref = row.get("reference") or str(original_repayment_id)
            orig_cust_ref = row.get("customer_reference") or str(original_repayment_id)
            orig_co_ref = row.get("company_reference") or str(original_repayment_id)
            rev_ref = ("r" + orig_ref) if orig_ref else None
            rev_cust_ref = ("r" + orig_cust_ref) if orig_cust_ref else None
            rev_co_ref = ("r" + orig_co_ref) if orig_co_ref else None

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
                    float(as_10dp(-float(row["amount"]))),
                    row["payment_date"],
                    rev_ref,
                    rev_cust_ref,
                    rev_co_ref,
                    row.get("value_date") or row["payment_date"],
                    sdate,
                    original_repayment_id,
                ),
            )
            # RealDictCursor returns a dict; fetch id by column name.
            row_new = cur.fetchone()
            new_id = int(row_new["id"]) if row_new and "id" in row_new else None

            # For reconciliation/GL, persist an explicit negative allocation row for the reversal,
            # mirroring the original allocation but with opposite sign.
            if alloc_row and new_id is not None:
                def _f(v):
                    return float(v or 0)

                rev_alloc_prin_not_due = -_f(alloc_row["alloc_principal_not_due"])
                rev_alloc_prin_arrears = -_f(alloc_row["alloc_principal_arrears"])
                rev_alloc_int_accrued = -_f(alloc_row["alloc_interest_accrued"])
                rev_alloc_int_arrears = -_f(alloc_row["alloc_interest_arrears"])
                rev_alloc_def = -_f(alloc_row["alloc_default_interest"])
                rev_alloc_pen = -_f(alloc_row["alloc_penalty_interest"])
                rev_alloc_fees = -_f(alloc_row["alloc_fees_charges"])
                # Mirror unallocated as negative so the reversal row reconciles symmetrically.
                rev_unallocated = -_f(alloc_row.get("unallocated", 0))

                rev_alloc_prin_total = rev_alloc_prin_not_due + rev_alloc_prin_arrears
                rev_alloc_int_total = (
                    rev_alloc_int_accrued
                    + rev_alloc_int_arrears
                    + rev_alloc_def
                    + rev_alloc_pen
                )
                rev_alloc_fees_total = rev_alloc_fees

                rev_alloc_total = rev_alloc_prin_total + rev_alloc_int_total + rev_alloc_fees_total
                cur.execute(
                    """
                    INSERT INTO loan_repayment_allocation (
                        repayment_id,
                        alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total,
                        alloc_total, unallocated, event_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        new_id,
                        float(as_10dp(rev_alloc_prin_not_due)),
                        float(as_10dp(rev_alloc_prin_arrears)),
                        float(as_10dp(rev_alloc_int_accrued)),
                        float(as_10dp(rev_alloc_int_arrears)),
                        float(as_10dp(rev_alloc_def)),
                        float(as_10dp(rev_alloc_pen)),
                        float(as_10dp(rev_alloc_fees)),
                        float(as_10dp(rev_alloc_prin_total)),
                        float(as_10dp(rev_alloc_int_total)),
                        float(as_10dp(rev_alloc_fees_total)),
                        float(as_10dp(rev_alloc_total)),
                        float(as_10dp(rev_unallocated)),
                        "unallocation_parent_reversed",
                    ),
                )

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
                       COALESCE(regular_interest_daily, 0)            AS regular_interest_daily,
                       COALESCE(penalty_interest_daily, 0)            AS penalty_interest_daily,
                       COALESCE(default_interest_daily, 0)            AS default_interest_daily,
                       COALESCE(regular_interest_period_to_date, 0)   AS regular_interest_period_to_date,
                       COALESCE(penalty_interest_period_to_date, 0)   AS penalty_interest_period_to_date,
                       COALESCE(default_interest_period_to_date, 0)   AS default_interest_period_to_date
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
                "regular_interest_daily": float(row["regular_interest_daily"] or 0),
                "penalty_interest_daily": float(row["penalty_interest_daily"] or 0),
                "default_interest_daily": float(row["default_interest_daily"] or 0),
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
                  AND COALESCE(reference, '') <> 'Unapplied funds allocation'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY id
                """,
                (value_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def get_loan_ids_with_reversed_receipts_on_date(value_date: date) -> list[int]:
    """
    Loan IDs that have at least one reversed receipt on the given value_date.
    Used by EOD to identify loans needing reallocation of other same-day receipts
    so the waterfall is preserved after reversals.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT loan_id FROM loan_repayments
                WHERE status = 'reversed'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY loan_id
                """,
                (value_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def get_repayment_ids_for_loan_and_date(loan_id: int, value_date: date) -> list[int]:
    """Posted repayment IDs for a specific loan on the given value_date. Order by id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayments
                WHERE loan_id = %s
                  AND status = 'posted'
                  AND COALESCE(reference, '') <> 'Unapplied funds allocation'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY id
                """,
                (loan_id, value_date),
            )
            return [int(r[0]) for r in cur.fetchall()]


def _get_allocation_sum_for_repayment(repayment_id: int, conn=None) -> dict | None:
    """
    Net allocation for a repayment (sum across all event rows – append-only model).
    Returns dict with alloc_* keys plus unallocated, or None if no rows.
    """
    def _run(c):
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS cnt,
                    COALESCE(SUM(alloc_principal_not_due), 0)   AS alloc_principal_not_due,
                    COALESCE(SUM(alloc_principal_arrears), 0)   AS alloc_principal_arrears,
                    COALESCE(SUM(alloc_interest_accrued), 0)    AS alloc_interest_accrued,
                    COALESCE(SUM(alloc_interest_arrears), 0)    AS alloc_interest_arrears,
                    COALESCE(SUM(alloc_default_interest), 0)    AS alloc_default_interest,
                    COALESCE(SUM(alloc_penalty_interest), 0)    AS alloc_penalty_interest,
                    COALESCE(SUM(alloc_fees_charges), 0)        AS alloc_fees_charges,
                    COALESCE(SUM(unallocated), 0)               AS unallocated
                FROM loan_repayment_allocation
                WHERE repayment_id = %s
                """,
                (repayment_id,),
            )
            return cur.fetchone()

    if conn is not None:
        row = _run(conn)
    else:
        with _connection() as c:
            row = _run(c)
    if not row or int(row.get("cnt", 0) or 0) == 0:
        return None
    out = dict(row)
    out.pop("cnt", None)
    return out


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
                  AND lr.status IN ('posted', 'reversed')
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


def _sum_net_allocations_earlier_same_day(
    cur,
    loan_id: int,
    eff_date: date,
    before_repayment_id: int,
) -> dict[str, float]:
    """
    Net allocation (sum of all allocation rows) for posted receipts on eff_date with id < before_repayment_id.
    Excludes reversed originals (status <> posted) and unapplied-liquidation receipts.
    """
    cur.execute(
        """
        SELECT
            COALESCE(SUM(lra.alloc_principal_not_due), 0)   AS alloc_principal_not_due,
            COALESCE(SUM(lra.alloc_principal_arrears), 0)   AS alloc_principal_arrears,
            COALESCE(SUM(lra.alloc_interest_accrued), 0)   AS alloc_interest_accrued,
            COALESCE(SUM(lra.alloc_interest_arrears), 0)   AS alloc_interest_arrears,
            COALESCE(SUM(lra.alloc_default_interest), 0)   AS alloc_default_interest,
            COALESCE(SUM(lra.alloc_penalty_interest), 0)   AS alloc_penalty_interest,
            COALESCE(SUM(lra.alloc_fees_charges), 0)       AS alloc_fees_charges
        FROM loan_repayments lr
        INNER JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
        WHERE lr.loan_id = %s
          AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
          AND lr.id < %s
          AND lr.amount > 0
          AND lr.status = 'posted'
          AND COALESCE(lr.reference, '') <> 'Unapplied funds allocation'
        """,
        (loan_id, eff_date, before_repayment_id),
    )
    row = cur.fetchone()
    keys = (
        "alloc_principal_not_due",
        "alloc_principal_arrears",
        "alloc_interest_accrued",
        "alloc_interest_arrears",
        "alloc_default_interest",
        "alloc_penalty_interest",
        "alloc_fees_charges",
    )
    if not row:
        return {k: 0.0 for k in keys}
    return {k: float(row.get(k, 0) or 0) for k in keys}


def _get_opening_balances_for_repayment(
    cur,
    loan_id: int,
    eff_date: date,
    repayment_id: int,
) -> tuple[dict[str, float], dict | None, int]:
    """
    Opening balances for the waterfall on value date eff_date:

      closing(loan_daily_state for eff_date - 1)
      minus net allocations from earlier *posted* receipts on the same eff_date (by repayment id).

    Reversed receipts are excluded (they are no longer posted). Reallocation updates allocation in place;
    earlier same-day receipts still net correctly from loan_repayment_allocation.

    Returns (balances dict keyed like loan_daily_state, st_prev row dict or None, days_overdue from opening).
    """
    prev_date = eff_date - timedelta(days=1)
    cur.execute(
        """
        SELECT as_of_date,
               principal_not_due, principal_arrears, interest_accrued_balance,
               interest_arrears_balance, default_interest_balance,
               penalty_interest_balance, fees_charges_balance, days_overdue,
               regular_interest_daily, penalty_interest_daily, default_interest_daily,
               regular_interest_period_to_date,
               penalty_interest_period_to_date,
               default_interest_period_to_date
        FROM loan_daily_state
        WHERE loan_id = %s AND as_of_date = %s
        FOR UPDATE
        """,
        (loan_id, prev_date),
    )
    st_prev = cur.fetchone()
    prior = _sum_net_allocations_earlier_same_day(cur, loan_id, eff_date, repayment_id)

    def _col_prev(key: str) -> float:
        if not st_prev:
            return 0.0
        return max(0.0, float(st_prev.get(key, 0) or 0))

    mapping = (
        ("principal_not_due", "alloc_principal_not_due"),
        ("principal_arrears", "alloc_principal_arrears"),
        ("interest_accrued_balance", "alloc_interest_accrued"),
        ("interest_arrears_balance", "alloc_interest_arrears"),
        ("default_interest_balance", "alloc_default_interest"),
        ("penalty_interest_balance", "alloc_penalty_interest"),
        ("fees_charges_balance", "alloc_fees_charges"),
    )
    balances: dict[str, float] = {}
    for state_key, alloc_key in mapping:
        balances[state_key] = max(
            0.0,
            _col_prev(state_key) - float(prior.get(alloc_key, 0.0) or 0.0),
        )
    days_od = int(st_prev.get("days_overdue", 0) or 0) if st_prev else 0
    st_prev_dict = dict(st_prev) if st_prev else None
    return balances, st_prev_dict, days_od


def get_repayment_opening_delinquency_total(repayment_id: int) -> float | None:
    """
    Sum of arrears-bucket balances at waterfall opening for this repayment (read-only snapshot).

    Same basis as allocate_repayment_waterfall: closing(loan_daily_state for value_date − 1)
    minus earlier same-day receipts' allocations. Used by customer statements so rows that
    appear before a receipt can show delinquency immediately before that receipt is applied.
    """
    from psycopg2.extras import RealDictCursor

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, COALESCE(value_date, payment_date)::date AS vd
                FROM loan_repayments
                WHERE id = %s AND status = 'posted'
                """,
                (repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            loan_id = int(row["loan_id"])
            vd = row["vd"]
            if hasattr(vd, "date"):
                vd = vd.date()
            if not isinstance(vd, date):
                return None
            balances, _, _ = _get_opening_balances_for_repayment(cur, loan_id, vd, repayment_id)
    return (
        float(balances.get("principal_arrears") or 0)
        + float(balances.get("interest_arrears_balance") or 0)
        + float(balances.get("default_interest_balance") or 0)
        + float(balances.get("penalty_interest_balance") or 0)
        + float(balances.get("fees_charges_balance") or 0)
    )


def get_credits_for_loan_date(loan_id: int, as_of_date: date) -> float:
    """
    Cumulative allocation affecting balances for this loan as of date.
    Credits = SUM(alloc_total) for all repayments with value_date <= as_of_date.
    Payment = + (reduces balance), Reversal = - (adds back). Excludes unapplied.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(lra.alloc_total), 0) AS credits
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status IN ('posted', 'reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date <= %s::date
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)


def get_net_allocation_for_loan_date(loan_id: int, as_of_date: date, conn: Any = None) -> float:
    """
    Per-day allocation that reduced balances for this loan on the given date.
    Net allocation = SUM(alloc_total) for repayments with value_date = as_of_date only. Payment = +, Reversal = -.
    """
    def _run(c: Any) -> float:
        with c.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(lra.alloc_total), 0) AS net_alloc
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status IN ('posted', 'reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE 'Unapplied funds allocation%%'
                  )
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)
    if conn is not None:
        return _run(conn)
    with _connection() as c:
        return _run(c)


def get_unallocated_for_loan_date(loan_id: int, as_of_date: date, conn: Any = None) -> float:
    """
    Per-day amount credited to unapplied (overpayment) for this loan on the given date.
    Sum of allocation.unallocated for receipts with value_date = as_of_date.
    """
    def _run(c: Any) -> float:
        with c.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(lra.unallocated), 0) AS unallocated
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status IN ('posted', 'reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE 'Unapplied funds allocation%%'
                  )
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)
    if conn is not None:
        return _run(conn)
    with _connection() as c:
        return _run(c)


def get_repayments_with_allocations(
    loan_id: int,
    start_date: date,
    end_date: date,
    *,
    include_reversed: bool = False,
) -> list[dict]:
    """Repayments with value_date in range and their allocation breakdown (totals and per-bucket 1-5).
    By default returns posted only. Set include_reversed=True to include reversed receipts (negative amount)."""
    status_filter = "lr.status IN ('posted', 'reversed')" if include_reversed else "lr.status = 'posted'"
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT lr.id, lr.amount, lr.payment_date, lr.value_date, lr.customer_reference,
                       COALESCE(SUM(lra.alloc_interest_total), 0) AS alloc_interest_total,
                       COALESCE(SUM(lra.alloc_fees_total), 0) AS alloc_fees_total,
                       COALESCE(SUM(lra.alloc_principal_total), 0) AS alloc_principal_total,
                       COALESCE(SUM(lra.alloc_total), 0) AS alloc_total,
                       COALESCE(SUM(lra.unallocated), 0) AS unallocated,
                       COALESCE(SUM(lra.alloc_fees_charges), 0) AS alloc_fees_charges,
                       COALESCE(SUM(lra.alloc_penalty_interest), 0) AS alloc_penalty_interest,
                       COALESCE(SUM(lra.alloc_default_interest), 0) AS alloc_default_interest,
                       COALESCE(SUM(lra.alloc_interest_arrears), 0) AS alloc_interest_arrears,
                       COALESCE(SUM(lra.alloc_principal_arrears), 0) AS alloc_principal_arrears
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s AND {status_filter}
                  AND COALESCE(lr.value_date, lr.payment_date) >= %s
                  AND COALESCE(lr.value_date, lr.payment_date) <= %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE 'Unapplied funds allocation%%'
                  )
                GROUP BY lr.id, lr.amount, lr.payment_date, lr.value_date, lr.customer_reference
                ORDER BY COALESCE(lr.value_date, lr.payment_date), lr.id
                """,
                (loan_id, start_date, end_date),
            )
            return [dict(r) for r in cur.fetchall()]


def get_unapplied_ledger_balance(loan_id: int, as_of_date: date) -> float:
    """Balance = SUM(amount) for loan_id, value_date <= as_of_date (ledger-style single table)."""
    return get_unapplied_balance(loan_id, as_of_date)


def get_unapplied_balance(loan_id: int, as_of_date: date) -> float:
    """Balance = SUM(amount) for the loan with value_date <= as_of_date (ledger-style)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM unapplied_funds
                WHERE loan_id = %s AND value_date <= %s
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)


def get_loans_with_unapplied_balance(as_of_date: date) -> list[int]:
    """Loan IDs with unapplied balance > 0 as of the given date (ledger-style)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT loan_id
                FROM unapplied_funds
                WHERE value_date <= %s
                GROUP BY loan_id
                HAVING COALESCE(SUM(amount), 0) > 0
                ORDER BY loan_id
                """,
                (as_of_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def apply_unapplied_funds_to_arrears_eod(
    loan_id: int,
    as_of_date: date,
    sys_cfg: dict,
) -> float:
    """
    Apply pending unapplied funds towards arrears (waterfall order) for a loan.
    Only runs if unapplied > 0 and any arrears > 0 (interest_arrears, penalty, default, principal_arrears).
    Constraints: allocate no more than unapplied balance, no more than each bucket balance.
    Creates a system repayment and allocation with event_type='unapplied_funds_allocation'.
    Returns amount applied (0 if none).
    """
    unapplied = get_unapplied_balance(loan_id, as_of_date)
    if unapplied <= 1e-6:
        return 0.0

    state = get_loan_daily_state_balances(loan_id, as_of_date)
    if not state:
        return 0.0

    # Arrears buckets only (waterfall order, excluding interest_accrued and principal_not_due for standard)
    interest_arrears = float(state.get("interest_arrears_balance") or 0)
    penalty_balance = float(state.get("penalty_interest_balance") or 0)
    default_balance = float(state.get("default_interest_balance") or 0)
    principal_arrears = float(state.get("principal_arrears") or 0)
    fees_balance = float(state.get("fees_charges_balance") or 0)

    arrears_total = interest_arrears + penalty_balance + default_balance + principal_arrears + fees_balance
    if arrears_total <= 1e-6:
        return 0.0

    profile_key, bucket_order = _get_waterfall_config(sys_cfg)
    # Standard profile skips interest_accrued_balance and principal_not_due
    skip = STANDARD_SKIP_BUCKETS if profile_key == "standard" else ()

    target_to_apply = round(min(unapplied, arrears_total), 2)
    if target_to_apply <= 1e-6:
        return 0.0

    # Consume FIFO by source; create one debit + allocation per source for lineage
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH source_balances AS (
                    SELECT
                        COALESCE(uf.repayment_id, uf.source_repayment_id) AS source_repayment_id,
                        MIN(uf.value_date) AS first_value_date,
                        COALESCE(SUM(uf.amount), 0) AS available_amount
                    FROM unapplied_funds uf
                    WHERE uf.loan_id = %s
                      AND uf.value_date <= %s
                      AND COALESCE(uf.repayment_id, uf.source_repayment_id) IS NOT NULL
                    GROUP BY COALESCE(uf.repayment_id, uf.source_repayment_id)
                )
                SELECT
                    sb.source_repayment_id AS repayment_id,
                    sb.available_amount AS amount,
                    sb.first_value_date
                FROM source_balances sb
                WHERE sb.available_amount > 0
                ORDER BY sb.first_value_date, sb.source_repayment_id
                """,
                (loan_id, as_of_date),
            )
            credit_rows = cur.fetchall()

        # Build consumption per source (FIFO)
        remaining_to_consume = target_to_apply
        consumptions: list[tuple[int | None, float]] = []  # (source_repayment_id, amount)
        for row in credit_rows:
            if remaining_to_consume <= 1e-6:
                break
            uf_amount = float(row["amount"] or 0)
            consume = min(uf_amount, remaining_to_consume)
            consume = float(as_10dp(consume))
            if consume <= 0:
                continue
            src_rep = row.get("repayment_id")
            consumptions.append((int(src_rep) if src_rep is not None else None, consume))
            remaining_to_consume -= consume
        amount_applied = float(as_10dp(sum(c for _s, c in consumptions)))
        if amount_applied <= 1e-6:
            # Nothing actually consumable from unapplied ledger -> no mutation.
            return 0.0

        # Per-source: debit unapplied_funds only.
        # Rule: loan_repayments holds ONLY teller receipts + reversals.
        #       EOD unapplied-to-arrears events are tracked in unapplied_funds (debits)
        #       and loan_daily_state only; no system repayment or allocation row is created.
        bucket_balances: dict[str, float] = {
            "principal_not_due": float(state.get("principal_not_due") or 0),
            "principal_arrears": principal_arrears,
            "interest_accrued_balance": float(state.get("interest_accrued_balance") or 0),
            "interest_arrears_balance": interest_arrears,
            "default_interest_balance": default_balance,
            "penalty_interest_balance": penalty_balance,
            "fees_charges_balance": fees_balance,
        }
        profile_key, bucket_order = _get_waterfall_config(sys_cfg)
        skip = STANDARD_SKIP_BUCKETS if profile_key == "standard" else ()
        alloc_principal_not_due = 0.0
        alloc_principal_arrears = 0.0
        alloc_interest_accrued = 0.0
        alloc_interest_arrears = 0.0
        alloc_default_interest = 0.0
        alloc_penalty_interest = 0.0
        alloc_fees_charges = 0.0

        for src_repayment_id, consumed in consumptions:
            remaining = consumed
            src_alloc: dict[str, float] = {k: 0.0 for k in BUCKET_TO_ALLOC}

            for bucket_name in bucket_order:
                if bucket_name not in BUCKET_TO_ALLOC or bucket_name in skip:
                    continue
                alloc_key, state_key = BUCKET_TO_ALLOC[bucket_name]
                bucket_balance = max(0.0, bucket_balances.get(state_key, 0))
                to_alloc = min(remaining, bucket_balance)
                src_alloc[alloc_key] = to_alloc
                remaining -= to_alloc
                bucket_balances[state_key] = bucket_balance - to_alloc
                if remaining <= 1e-6:
                    break

            apr = src_alloc.get("alloc_principal_not_due", 0.0)
            apa = src_alloc.get("alloc_principal_arrears", 0.0)
            aia = src_alloc.get("alloc_interest_accrued", 0.0)
            aiar = src_alloc.get("alloc_interest_arrears", 0.0)
            adi = src_alloc.get("alloc_default_interest", 0.0)
            api = src_alloc.get("alloc_penalty_interest", 0.0)
            afc = src_alloc.get("alloc_fees_charges", 0.0)
            alloc_principal_not_due += apr
            alloc_principal_arrears += apa
            alloc_interest_accrued += aia
            alloc_interest_arrears += aiar
            alloc_default_interest += adi
            alloc_penalty_interest += api
            alloc_fees_charges += afc

            with conn.cursor() as cur:
                # Debit unapplied_funds: link back to the source teller receipt.
                # allocation_repayment_id is NULL (no system repayment is created).
                cur.execute(
                    """
                    INSERT INTO unapplied_funds (loan_id, amount, value_date, entry_type, reference, allocation_repayment_id, source_repayment_id, currency)
                    VALUES (%s, %s, %s, 'debit', 'Applied to arrears (EOD)', NULL, %s, 'USD')
                    """,
                    (loan_id, float(as_10dp(-consumed)), as_of_date, src_repayment_id),
                )

        alloc_principal_total = alloc_principal_not_due + alloc_principal_arrears
        alloc_interest_total = (
            alloc_interest_accrued + alloc_interest_arrears
            + alloc_default_interest + alloc_penalty_interest
        )
        alloc_fees_total = alloc_fees_charges

        # Update loan_daily_state once (total reduction)
        new_principal_not_due = max(0.0, float(state.get("principal_not_due") or 0) - alloc_principal_not_due)
        new_principal_arrears = max(0.0, principal_arrears - alloc_principal_arrears)
        new_interest_accrued = max(0.0, float(state.get("interest_accrued_balance") or 0) - alloc_interest_accrued)
        new_interest_arrears = max(0.0, interest_arrears - alloc_interest_arrears)
        new_default_interest = max(0.0, default_balance - alloc_default_interest)
        new_penalty_interest = max(0.0, penalty_balance - alloc_penalty_interest)
        new_fees_charges = max(0.0, fees_balance - alloc_fees_charges)

        # Use the daily accrual values already saved by EOD for this date.
        # Never re-derive from the engine (which ignores grace periods and allocations).
        daily_state = get_loan_daily_state_balances(loan_id, as_of_date)
        if daily_state:
            reg_daily  = float(daily_state.get("regular_interest_daily", 0) or 0)
            def_daily  = float(daily_state.get("default_interest_daily", 0) or 0)
            pen_daily  = float(daily_state.get("penalty_interest_daily", 0) or 0)
            reg_period = float(daily_state.get("regular_interest_period_to_date", 0) or 0)
            def_period = float(daily_state.get("default_interest_period_to_date", 0) or 0)
            pen_period = float(daily_state.get("penalty_interest_period_to_date", 0) or 0)
        else:
            reg_daily = def_daily = pen_daily = 0.0
            reg_period = def_period = pen_period = 0.0

        arrears_after = new_interest_arrears + new_default_interest + new_penalty_interest + new_principal_arrears
        days_overdue = int(state.get("days_overdue") or 0)
        if arrears_after <= 1e-6:
            days_overdue = 0

        net_alloc = get_net_allocation_for_loan_date(loan_id, as_of_date, conn=conn)
        unalloc = get_unallocated_for_loan_date(loan_id, as_of_date, conn=conn)

        save_loan_daily_state(
            loan_id=loan_id,
            as_of_date=as_of_date,
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
            net_allocation=net_alloc,
            unallocated=unalloc,
            conn=conn,
        )

    return amount_applied


def get_unapplied_entries(loan_id: int, through_date: date) -> list[tuple[date, float]]:
    """
    Unapplied entries for statement from loan_repayment_allocation.unallocated only.
    Returns list of (value_date, amount) for in-memory aggregation.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    COALESCE(lra.unallocated, 0) AS amount
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND (COALESCE(lr.value_date, lr.payment_date))::date <= %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE 'Unapplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE 'Unapplied funds allocation%%'
                  )
                ORDER BY value_date, lr.id
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


def get_unapplied_ledger_entries_for_statement(
    loan_id: int,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """
    Statement-facing unapplied ledger lines from the unapplied_funds_ledger view.
    This view mirrors the export ledger logic:
      - Credits/reversals from receipt allocation (loan_repayment_allocation totals vs amount)
      - Liquidations from loan_repayment_allocation with event_type='unapplied_funds_allocation'
    Returns entry_kind in ('credit','reversal','liquidation') with unapplied_running_balance.
    Includes all entries with value_date <= end_date so running balance is correct.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    repayment_id,
                    repayment_key,
                    loan_id,
                    value_date,
                    entry_kind,
                    liquidation_repayment_id,
                    unapplied_delta,
                    alloc_prin_arrears,
                    alloc_int_arrears,
                    alloc_penalty_int,
                    alloc_default_int,
                    alloc_fees_charges,
                    unapplied_running_balance
                FROM unapplied_funds_ledger
                WHERE loan_id = %s
                  AND value_date <= %s
                ORDER BY value_date, repayment_id, entry_kind
                """,
                (loan_id, end_date),
            )
            return [dict(r) for r in cur.fetchall()]


def get_unapplied_repayment_ids(loan_id: int, as_of_date: date) -> set[int]:
    """Repayment IDs that created unapplied credits for this loan with value_date <= as_of_date."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT repayment_id FROM unapplied_funds
                WHERE loan_id = %s AND value_date <= %s AND amount > 0 AND repayment_id IS NOT NULL
                """,
                (loan_id, as_of_date),
            )
            return {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}


def replace_schedule_lines(loan_schedule_id: int, schedule_df: pd.DataFrame) -> None:
    """Replace all schedule_lines for a schedule with new values (e.g. after 10dp correction)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule_lines WHERE loan_schedule_id = %s", (loan_schedule_id,))
        with conn.cursor() as cur:
            for _, row in schedule_df.iterrows():
                period = int(row.get("Period", row.get("period", 0)))
                period_date = str(row.get("Date", row.get("Date", "")))[:32] if pd.notna(row.get("Date")) else None
                payment = float(as_10dp(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0))))) if pd.notna(row.get("Payment", row.get("Monthly Installment", 0))) else 0.0
                principal = float(as_10dp(row.get("Principal", row.get("principal", 0)))) if pd.notna(row.get("Principal")) else 0.0
                interest = float(as_10dp(row.get("Interest", row.get("interest", 0)))) if pd.notna(row.get("Interest")) else 0.0
                principal_balance = float(as_10dp(row.get("Principal Balance", row.get("principal_balance", 0)))) if pd.notna(row.get("Principal Balance")) else 0.0
                total_outstanding = float(as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0)))) if pd.notna(row.get("Total Outstanding")) else 0.0
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (loan_schedule_id, period, period_date, payment, principal, interest, principal_balance, total_outstanding),
                )


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
                payment = float(as_10dp(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0))))) if pd.notna(row.get("Payment", row.get("Monthly Installment", 0))) else 0.0
                principal = float(as_10dp(row.get("Principal", row.get("principal", 0)))) if pd.notna(row.get("Principal")) else 0.0
                interest = float(as_10dp(row.get("Interest", row.get("interest", 0)))) if pd.notna(row.get("Interest")) else 0.0
                principal_balance = float(as_10dp(row.get("Principal Balance", row.get("principal_balance", 0)))) if pd.notna(row.get("Principal Balance")) else 0.0
                total_outstanding = float(as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0)))) if pd.notna(row.get("Total Outstanding")) else 0.0
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
        from system_business_date import get_effective_date
        as_of = get_effective_date()

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
    regular_interest_daily: "Decimal | float" = 0.0,
    principal_not_due: float = 0.0,
    principal_arrears: float = 0.0,
    interest_accrued_balance: float = 0.0,
    interest_arrears_balance: float = 0.0,
    default_interest_daily: "Decimal | float" = 0.0,
    default_interest_balance: float = 0.0,
    penalty_interest_daily: "Decimal | float" = 0.0,
    penalty_interest_balance: float = 0.0,
    fees_charges_balance: float = 0.0,
    days_overdue: int = 0,
    regular_interest_period_to_date: "Decimal | float" = 0.0,
    penalty_interest_period_to_date: "Decimal | float" = 0.0,
    default_interest_period_to_date: "Decimal | float" = 0.0,
    credits: float | None = None,
    net_allocation: float | None = None,
    unallocated: float | None = None,
    conn: Any = None,
) -> None:
    """
    Upsert daily loan bucket balances into loan_daily_state.
    total_exposure is computed as the sum of all principal + interest + fees buckets.
    Period-to-date columns are for efficient statement generation (no summing over days).
    If conn is provided, use it and do not commit (caller commits). Otherwise use a new connection and commit on exit.
    """
    as_of_date = _date_conv(as_of_date) or (as_of_date.date() if isinstance(as_of_date, datetime) else as_of_date)
    # Quantize all numerics to 10dp for storage
    def _n(v): return float(as_10dp(v)) if v is not None else 0.0
    principal_not_due = max(0.0, _n(principal_not_due))
    principal_arrears = max(0.0, _n(principal_arrears))
    interest_accrued_balance = max(0.0, _n(interest_accrued_balance))
    interest_arrears_balance = max(0.0, _n(interest_arrears_balance))
    default_interest_balance = max(0.0, _n(default_interest_balance))
    penalty_interest_balance = max(0.0, _n(penalty_interest_balance))
    fees_charges_balance = max(0.0, _n(fees_charges_balance))
    regular_interest_daily = as_10dp(regular_interest_daily)
    default_interest_daily = as_10dp(default_interest_daily)
    penalty_interest_daily = as_10dp(penalty_interest_daily)
    regular_interest_period_to_date = as_10dp(regular_interest_period_to_date)
    penalty_interest_period_to_date = as_10dp(penalty_interest_period_to_date)
    default_interest_period_to_date = as_10dp(default_interest_period_to_date)
    net_allocation = as_10dp(net_allocation) if net_allocation is not None else None
    unallocated = as_10dp(unallocated) if unallocated is not None else None
    credits = as_10dp(credits) if credits is not None else None
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
            if net_allocation is not None and unallocated is not None:
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
                        default_interest_period_to_date,
                        net_allocation,
                        unallocated
                    )
                    VALUES (
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
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
                        default_interest_period_to_date = EXCLUDED.default_interest_period_to_date,
                        net_allocation           = EXCLUDED.net_allocation,
                        unallocated              = EXCLUDED.unallocated
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
                        net_allocation,
                        unallocated,
                    ),
                )
            elif credits is not None:
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
                        default_interest_period_to_date,
                        credits
                    )
                    VALUES (
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s
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
                        default_interest_period_to_date = EXCLUDED.default_interest_period_to_date,
                        credits                  = EXCLUDED.credits
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
                        credits,
                    ),
                )
            else:
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
    Apply unapplied funds to the loan via recast logic (separate from allocation).
    Reclassifies accrued interest → interest arrears and principal not due → principal arrears
    (up to the unapplied amount), then applies the payment to those buckets. Call only after
    funds have been credited to Unapplied (e.g. from Unapplied tab).
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, amount, value_date, repayment_id
                FROM unapplied_funds
                WHERE id = %s
                """,
                (unapplied_funds_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} not found.")
            if float(row.get("amount") or 0) <= 0:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} has no positive balance (already consumed or debit).")
            # Prevent double-apply: check if debit already exists for this credit
            cur.execute(
                "SELECT 1 FROM unapplied_funds WHERE source_unapplied_id = %s LIMIT 1",
                (unapplied_funds_id,),
            )
            if cur.fetchone():
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} was already applied via recast.")
            loan_id = int(row["loan_id"])
            amount = float(row["amount"])
            value_date = row["value_date"]
            if hasattr(value_date, "date"):
                value_date = value_date.date()
            eff_date = as_of or value_date
            source_repayment_id = int(row["repayment_id"]) if row.get("repayment_id") is not None else None

            # Ensure exact-date state exists before any mutation.
            from eod import run_single_loan_eod
            run_single_loan_eod(loan_id, eff_date)

            cur.execute(
                """
                SELECT as_of_date, principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance,
                       penalty_interest_balance, fees_charges_balance, days_overdue,
                       regular_interest_daily, default_interest_daily, penalty_interest_daily,
                       regular_interest_period_to_date, penalty_interest_period_to_date, default_interest_period_to_date
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date = %s
                FOR UPDATE
                """,
                (loan_id, eff_date),
            )
            st_row = cur.fetchone()
            if not st_row:
                raise ValueError(f"No exact loan_daily_state for loan_id={loan_id} on {eff_date}.")
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
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date, conn=conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date, conn=conn)

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
                net_allocation=net_alloc,
                unallocated=unalloc,
            )

            # Insert debit row (ledger-style, no UPDATE)
            cur.execute(
                """
                INSERT INTO unapplied_funds (loan_id, amount, value_date, entry_type, reference, source_repayment_id, source_unapplied_id, currency)
                VALUES (%s, %s, %s, 'debit', 'Applied via recast', %s, %s, 'USD')
                """,
                (loan_id, float(as_10dp(-amount)), eff_date, source_repayment_id, unapplied_funds_id),
            )


def _credit_unapplied_funds(
    conn,
    loan_id: int,
    repayment_id: int,
    amount: float,
    value_date: date,
    currency: str = "USD",
) -> None:
    """Insert a credit row into unapplied_funds (ledger-style, append-only)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO unapplied_funds (loan_id, repayment_id, amount, currency, value_date, entry_type, reference)
            VALUES (%s, %s, %s, %s, %s, 'credit', 'Overpayment')
            """,
            (loan_id, repayment_id, float(as_10dp(amount)), currency, value_date),
        )


def reallocate_repayment(
    repayment_id: int,
    *,
    system_config: dict | None = None,
    use_yesterday_state: bool = False,
) -> None:
    """
    Correct an already-saved receipt: reverse its allocation and unapplied credits,
    then re-run allocation (e.g. after fixing waterfall config or logic).
    Allocation is done at save receipt, not at EOD.

    When use_yesterday_state=True, uses loan_daily_state from the day before as
    state_before (avoids engine divergence when engine has wrong arrears).
    """
    opening_balances: dict[str, float] | None = None
    st_prev_realloc: dict | None = None
    opening_days_od: int = 0

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT lr.id, lr.loan_id, lr.amount, lr.reference,
                       COALESCE(lr.value_date, lr.payment_date) AS eff_date
                FROM loan_repayments lr
                WHERE lr.id = %s
                """,
                (repayment_id,),
            )
            rrow = cur.fetchone()
            if not rrow:
                raise ValueError(f"Repayment {repayment_id} not found.")
            amount = float(rrow["amount"] or 0)
            if amount <= 0:
                # Reversals (negative) are never reallocated; they get unallocation_parent_reversed
                # at creation (reverse_repayment) or via clear script's fix-reversed section.
                return
            if (rrow.get("reference") or "") == "Unapplied funds allocation":
                # System liquidation repayments must not be reallocated as cash receipts.
                return
            loan_id = int(rrow["loan_id"])
            eff_date = rrow["eff_date"]
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            alloc_row = _get_allocation_sum_for_repayment(repayment_id, conn)
            if not alloc_row:
                # No prior allocation; ensure state exists then allocate
                conn.commit()
                allocate_repayment_waterfall(repayment_id, system_config=system_config)
                return

            # Skip no-op: if allocation is already all zeros (all unapplied), reallocate would produce nothing
            def _fz(v):
                return abs(float(v or 0)) < 1e-6
            if all(
                _fz(alloc_row.get(k))
                for k in (
                    "alloc_principal_not_due", "alloc_principal_arrears",
                    "alloc_interest_accrued", "alloc_interest_arrears",
                    "alloc_default_interest", "alloc_penalty_interest", "alloc_fees_charges",
                )
            ):
                return

            # Idempotency: skip if allocation would be unchanged (prevents duplicates from double-calls).
            conn.commit()
            from eod import run_single_loan_eod
            prev_cal = eff_date - timedelta(days=1)
            run_single_loan_eod(loan_id, prev_cal, sys_cfg=system_config)
            with _connection() as _idem_conn:
                with _idem_conn.cursor(cursor_factory=RealDictCursor) as _idem_cur:
                    _idem_cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (4021001, loan_id))
                    opening_balances, st_prev_realloc, opening_days_od = _get_opening_balances_for_repayment(
                        _idem_cur, loan_id, eff_date, repayment_id
                    )
            cfg = system_config or load_system_config_from_db() or {}
            profile_key, bucket_order = _get_waterfall_config(cfg)
            desired_alloc, desired_unapplied = compute_waterfall_allocation(
                amount, opening_balances, bucket_order, profile_key,
                state_as_of=eff_date, repayment_id=repayment_id,
            )
            def _same_10dp(a, b):
                return as_10dp(a or 0) == as_10dp(b or 0)
            if all(
                _same_10dp(alloc_row.get(k), desired_alloc.get(k, 0))
                for k in (
                    "alloc_principal_not_due", "alloc_principal_arrears",
                    "alloc_interest_accrued", "alloc_interest_arrears",
                    "alloc_default_interest", "alloc_penalty_interest", "alloc_fees_charges",
                )
            ) and _same_10dp(alloc_row.get("unallocated", 0), desired_unapplied):
                return

            # Insert debit rows to offset prior unapplied credits (ledger-style)
            cur.execute(
                """
                SELECT id, amount FROM unapplied_funds
                WHERE repayment_id = %s AND amount > 0
                """,
                (repayment_id,),
            )
            for uf_row in cur.fetchall():
                amt = float(as_10dp(-float(uf_row["amount"] or 0)))
                cur.execute(
                    """
                    INSERT INTO unapplied_funds (loan_id, amount, value_date, entry_type, reference, source_repayment_id, currency)
                    VALUES (%s, %s, %s, 'debit', 'Reallocate (remove prior unapplied)', %s, 'USD')
                    """,
                    (loan_id, amt, eff_date, repayment_id),
                )

    # Override existing allocation row in place (policy: one row per repayment_id).
    # Same opening-basis policy as allocate_repayment_waterfall (closing eff_date-1 minus earlier receipts).
    _ = use_yesterday_state  # retained for backward compatibility; intentionally ignored.
    if opening_balances is None:
        raise RuntimeError("reallocate_repayment: opening balances missing after idempotency gate")

    state_before = opening_balances
    cfg = system_config or load_system_config_from_db() or {}
    profile_key, bucket_order = _get_waterfall_config(cfg)
    new_alloc, new_unapplied = compute_waterfall_allocation(
        amount, state_before, bucket_order, profile_key,
        state_as_of=eff_date, repayment_id=repayment_id,
    )

    new_apr = new_alloc.get("alloc_principal_not_due", 0.0)
    new_apa = new_alloc.get("alloc_principal_arrears", 0.0)
    new_aia = new_alloc.get("alloc_interest_accrued", 0.0)
    new_aiar = min(new_alloc.get("alloc_interest_arrears", 0.0), state_before.get("interest_arrears_balance", 0.0))
    new_adi = new_alloc.get("alloc_default_interest", 0.0)
    new_api = new_alloc.get("alloc_penalty_interest", 0.0)
    new_afc = new_alloc.get("alloc_fees_charges", 0.0)
    new_prin_total = new_apr + new_apa
    new_int_total = new_aia + new_aiar + new_adi + new_api
    new_fees_total = new_afc

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayment_allocation
                WHERE repayment_id = %s
                LIMIT 1
                """,
                (repayment_id,),
            )
            alloc_row_id = cur.fetchone()
            if not alloc_row_id:
                raise ValueError(f"No allocation row for repayment {repayment_id}; cannot reallocate.")
            alloc_row_id = int(alloc_row_id["id"])

            cur.execute(
                """
                UPDATE loan_repayment_allocation SET
                    alloc_principal_not_due = %s,
                    alloc_principal_arrears = %s,
                    alloc_interest_accrued = %s,
                    alloc_interest_arrears = %s,
                    alloc_default_interest = %s,
                    alloc_penalty_interest = %s,
                    alloc_fees_charges = %s,
                    alloc_principal_total = %s,
                    alloc_interest_total = %s,
                    alloc_fees_total = %s,
                    alloc_total = %s,
                    unallocated = %s
                WHERE id = %s
                """,
                (
                    float(as_10dp(new_apr)), float(as_10dp(new_apa)),
                    float(as_10dp(new_aia)), float(as_10dp(new_aiar)),
                    float(as_10dp(new_adi)), float(as_10dp(new_api)), float(as_10dp(new_afc)),
                    float(as_10dp(new_prin_total)), float(as_10dp(new_int_total)), float(as_10dp(new_fees_total)),
                    float(as_10dp(new_prin_total + new_int_total + new_fees_total)),
                    float(as_10dp(new_unapplied)),
                    alloc_row_id,
                ),
            )

            if new_unapplied > 1e-6:
                _credit_unapplied_funds(conn, loan_id, repayment_id, new_unapplied, eff_date)

            new_interest_accrued = max(0.0, state_before["interest_accrued_balance"] - new_aia)
            new_interest_arrears = max(0.0, state_before["interest_arrears_balance"] - new_aiar)
            new_principal_not_due = max(0.0, state_before["principal_not_due"] - new_apr)
            new_principal_arrears = max(0.0, state_before["principal_arrears"] - new_apa)
            new_default_interest = max(0.0, state_before["default_interest_balance"] - new_adi)
            new_penalty_interest = max(0.0, state_before["penalty_interest_balance"] - new_api)
            new_fees_charges = max(0.0, state_before["fees_charges_balance"] - new_afc)

            # Daily/period columns from closing(eff_date-1) (st_prev_realloc); aligned with allocate_repayment_waterfall.
            _sp = st_prev_realloc or {}
            reg_daily = float(_sp.get("regular_interest_daily", 0) or 0)
            def_daily = float(_sp.get("default_interest_daily", 0) or 0)
            pen_daily = float(_sp.get("penalty_interest_daily", 0) or 0)
            reg_period = float(_sp.get("regular_interest_period_to_date", 0) or 0)
            def_period = float(_sp.get("default_interest_period_to_date", 0) or 0)
            pen_period = float(_sp.get("penalty_interest_period_to_date", 0) or 0)

            total_exposure = (
                new_principal_not_due + new_principal_arrears + new_interest_accrued + new_interest_arrears
                + new_default_interest + new_penalty_interest + new_fees_charges
            )
            eff_date_val = _date_conv(eff_date) or (eff_date.date() if isinstance(eff_date, datetime) else eff_date)
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date_val, conn=conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date_val, conn=conn)
            days_overdue = int(opening_days_od)
            if new_interest_arrears + new_default_interest + new_penalty_interest + new_principal_arrears <= 1e-6:
                days_overdue = 0

            cur.execute(
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
                    default_interest_period_to_date = %s,
                    net_allocation = %s,
                    unallocated = %s
                WHERE loan_id = %s AND as_of_date = %s
                """,
                (
                    reg_daily, new_principal_not_due, new_principal_arrears,
                    new_interest_accrued, new_interest_arrears,
                    def_daily, new_default_interest,
                    pen_daily, new_penalty_interest,
                    new_fees_charges, days_overdue, total_exposure,
                    reg_period, pen_period, def_period,
                    net_alloc, unalloc,
                    loan_id, eff_date_val,
                ),
            )


def allocate_repayment_waterfall(
    repayment_id: int,
    *,
    as_of: date | None = None,
    system_config: dict | None = None,
    preloaded_balances: dict | None = None,
    event_type: str = "new_allocation",
) -> None:
    """
    Allocate a repayment across loan buckets using the configured waterfall
    (Standard or Borrower-friendly) and loan_daily_state. Persists allocation
    and updates daily state. Called at save receipt (real-time allocation).

    **Opening balance policy:** The waterfall uses **opening** balances for the
    receipt value date — i.e. **closing(eff_date − 1)** minus net allocations from
    **earlier posted receipts** on the same value date (by `repayment_id`). It does
    **not** use intraday EOD accrual for eff_date before allocating (so paying
    arrears does not pre-bill default/penalty for that day). Reversed receipts are
    excluded (no longer `posted`).

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
        from system_business_date import get_effective_date
        as_of = get_effective_date()

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
                # Negative repayments (reversals) must never get new_allocation.
                # They get unallocation_parent_reversed at creation (reverse_repayment)
                # or via clear script's fix-reversed section.
                return

            loan_id = int(row["loan_id"])
            eff_date = row["eff_date"] or as_of
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            # Always source balances from exact-date persisted state; ignore preloaded snapshots.
            _ = preloaded_balances  # backward compatibility only
            from eod import run_single_loan_eod

            # Ensure prior calendar day state exists (closing = opening for eff_date).
            prev_cal = eff_date - timedelta(days=1)
            run_single_loan_eod(loan_id, prev_cal, sys_cfg=system_config)

            # Serialize same-loan allocation (multiple receipts / concurrent saves).
            cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (4021001, loan_id))

            balances, st_prev, days_overdue = _get_opening_balances_for_repayment(
                cur, loan_id, eff_date, repayment_id,
            )
            state_as_of = eff_date

            alloc, unapplied = compute_waterfall_allocation(
                amount, balances, bucket_order, profile_key,
                state_as_of=state_as_of, repayment_id=repayment_id,
            )
            if unapplied > 1e-6:
                _credit_unapplied_funds(conn, loan_id, repayment_id, unapplied, eff_date)

            alloc_principal_not_due = alloc.get("alloc_principal_not_due", 0.0)
            alloc_principal_arrears = alloc.get("alloc_principal_arrears", 0.0)
            alloc_interest_accrued = alloc.get("alloc_interest_accrued", 0.0)
            alloc_interest_arrears = min(
                alloc.get("alloc_interest_arrears", 0.0),
                balances.get("interest_arrears_balance", 0.0),
            )
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
                    alloc_principal_total, alloc_interest_total, alloc_fees_total,
                    alloc_total, unallocated, event_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    float(as_10dp(total_alloc)),
                    float(as_10dp(unapplied)),
                    event_type,
                ),
            )

            try:
                from accounting_service import AccountingService
                from decimal import Decimal
                svc = AccountingService()

                # Each post_event uses transaction_templates for that event_type only.
                # Cash Dr must equal the sum of Cr lines for THAT journal — do not use full receipt
                # amount on PAYMENT_PRINCIPAL (only 2 lines: cash + principal_arrears); split not-yet-due
                # to PAYMENT_PRINCIPAL_NOT_YET_DUE, interest arrears vs accrued to separate events, etc.

                _rj = _repayment_journal_reference(loan_id, repayment_id)
                if alloc_principal_arrears > 0:
                    p = Decimal(str(alloc_principal_arrears))
                    svc.post_event(
                        event_type="PAYMENT_PRINCIPAL",
                        reference=_rj,
                        description=f"Principal (arrears) — {_rj}",
                        event_id=f"REPAY-{repayment_id}-PRIN-ARR",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "principal_arrears": p,
                        },
                    )

                if alloc_principal_not_due > 0:
                    p = Decimal(str(alloc_principal_not_due))
                    svc.post_event(
                        event_type="PAYMENT_PRINCIPAL_NOT_YET_DUE",
                        reference=_rj,
                        description=f"Principal (not yet due) — {_rj}",
                        event_id=f"REPAY-{repayment_id}-PRIN-NYD",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "loan_principal": p,
                        },
                    )

                if alloc_interest_arrears > 0:
                    p = Decimal(str(alloc_interest_arrears))
                    svc.post_event(
                        event_type="PAYMENT_REGULAR_INTEREST",
                        reference=_rj,
                        description=f"Interest (arrears) — {_rj}",
                        event_id=f"REPAY-{repayment_id}-INT-ARR",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "regular_interest_arrears": p,
                        },
                    )

                if alloc_interest_accrued > 0:
                    p = Decimal(str(alloc_interest_accrued))
                    svc.post_event(
                        event_type="PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
                        reference=_rj,
                        description=f"Interest (accrued / not billed) — {_rj}",
                        event_id=f"REPAY-{repayment_id}-INT-ACC",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "regular_interest_accrued": p,
                        },
                    )

                if alloc_penalty_interest > 0:
                    p = Decimal(str(alloc_penalty_interest))
                    svc.post_event(
                        event_type="PAYMENT_PENALTY_INTEREST",
                        reference=_rj,
                        description=f"Penalty interest — {_rj}",
                        event_id=f"REPAY-{repayment_id}-PEN",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "penalty_interest_asset": p,
                            "penalty_interest_suspense": p,
                            "penalty_interest_income": p,
                        },
                    )

                if alloc_default_interest > 0:
                    p = Decimal(str(alloc_default_interest))
                    svc.post_event(
                        event_type="PAYMENT_DEFAULT_INTEREST",
                        reference=_rj,
                        description=f"Default interest — {_rj}",
                        event_id=f"REPAY-{repayment_id}-DEF",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "default_interest_asset": p,
                            "default_interest_suspense": p,
                            "default_interest_income": p,
                        },
                    )

            except Exception as e:
                print(f"Failed to post repayment journals for {repayment_id}: {e}")

            new_interest_accrued = max(0.0, balances["interest_accrued_balance"] - alloc_interest_accrued)
            new_interest_arrears = max(0.0, balances["interest_arrears_balance"] - alloc_interest_arrears)
            new_principal_not_due = max(0.0, balances["principal_not_due"] - alloc_principal_not_due)
            new_principal_arrears = max(0.0, balances["principal_arrears"] - alloc_principal_arrears)
            new_default_interest = max(0.0, balances["default_interest_balance"] - alloc_default_interest)
            new_penalty_interest = max(0.0, balances["penalty_interest_balance"] - alloc_penalty_interest)
            new_fees_charges = max(0.0, balances["fees_charges_balance"] - alloc_fees_charges)

            # Use daily/period columns from **closing(eff_date-1)** (st_prev). Intraday EOD accrual for
            # eff_date is not applied before allocation; night EOD will refresh this row.
            # Never recalculate daily columns from post-allocation balances: doing so breaks the
            # bucket identity (opening + daily - alloc = closing) and inflates statement charges.
            _sp = st_prev or {}
            reg_daily   = float(_sp.get("regular_interest_daily", 0) or 0)
            pen_daily   = float(_sp.get("penalty_interest_daily", 0) or 0)
            def_daily   = float(_sp.get("default_interest_daily", 0) or 0)
            reg_period  = float(_sp.get("regular_interest_period_to_date", 0) or 0)
            pen_period  = float(_sp.get("penalty_interest_period_to_date", 0) or 0)
            def_period  = float(_sp.get("default_interest_period_to_date", 0) or 0)

            if (
                new_interest_arrears + new_default_interest + new_penalty_interest + new_principal_arrears
                <= 1e-6
            ):
                days_overdue = 0

            total_exposure = (
                new_principal_not_due + new_principal_arrears + new_interest_accrued + new_interest_arrears
                + new_default_interest + new_penalty_interest + new_fees_charges
            )
            eff_date_val = _date_conv(eff_date) or (eff_date.date() if isinstance(eff_date, datetime) else eff_date)
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date_val, conn=conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date_val, conn=conn)

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
                    default_interest_period_to_date = %s,
                    net_allocation = %s,
                    unallocated = %s
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
                    net_alloc,
                    unalloc,
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
                    net_allocation=net_alloc,
                    unallocated=unalloc,
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
