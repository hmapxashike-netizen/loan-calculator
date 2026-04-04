"""
Helpers for loan modification (approval workflow): balance snapshots and loan-type mapping.

Schedule generation stays in the UI layer with injected compute_* callables (same as Loan Capture).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from decimal_utils import as_10dp

LOAN_TYPE_DB_TO_DISPLAY: dict[str, str] = {
    "consumer_loan": "Consumer Loan",
    "term_loan": "Term Loan",
    "bullet_loan": "Bullet Loan",
    "customised_repayments": "Customised Repayments",
}

LOAN_TYPE_DISPLAY_TO_DB: dict[str, str] = {v: k for k, v in LOAN_TYPE_DB_TO_DISPLAY.items()}


def loan_type_display(db_loan_type: str) -> str:
    raw = (db_loan_type or "").strip()
    if raw in LOAN_TYPE_DB_TO_DISPLAY:
        return LOAN_TYPE_DB_TO_DISPLAY[raw]
    return raw.replace("_", " ").title()


def loan_type_db(display_or_db: str) -> str:
    s = (display_or_db or "").strip()
    if s in LOAN_TYPE_DISPLAY_TO_DB:
        return LOAN_TYPE_DISPLAY_TO_DB[s]
    if s in LOAN_TYPE_DB_TO_DISPLAY:
        return s
    return s.replace(" ", "_").lower()


# EOD summary table only (loan modification tab); keys from get_loan_daily_state_balances.
EOD_SUMMARY_BUCKET_ROWS: list[tuple[str, str]] = [
    ("Principal (not due)", "principal_not_due"),
    ("Principal (arrears)", "principal_arrears"),
    ("Interest accrued", "interest_accrued_balance"),
    ("Interest arrears", "interest_arrears_balance"),
    ("Default interest", "default_interest_balance"),
    ("Penalty interest", "penalty_interest_balance"),
    ("Fees & charges", "fees_charges_balance"),
]

# Extended breakdown (e.g. diagnostics); not shown on modification tab by default.
BALANCE_BREAKDOWN_ROWS: list[tuple[str, str]] = EOD_SUMMARY_BUCKET_ROWS + [
    ("Delinquency arrears", "total_delinquency_arrears"),
    ("Unallocated", "unallocated"),
    ("RI in suspense", "regular_interest_in_suspense_balance"),
    ("PI in suspense", "penalty_interest_in_suspense_balance"),
    ("DI in suspense", "default_interest_in_suspense_balance"),
    ("Total interest in suspense", "total_interest_in_suspense_balance"),
]


def bucket_snapshot_for_json(bal: dict[str, float] | None) -> dict[str, Any]:
    """Persist 10dp string amounts for approval payload (JSON-safe)."""
    if not bal:
        return {}
    out: dict[str, Any] = {}
    for _label, key in EOD_SUMMARY_BUCKET_ROWS:
        v = bal.get(key)
        if v is None:
            continue
        out[key] = str(as_10dp(v))
    te = bal.get("total_exposure")
    if te is not None:
        out["total_exposure"] = str(as_10dp(te))
    return out


def as_of_balance_date(restructure_date: date) -> date:
    return restructure_date - timedelta(days=1)
