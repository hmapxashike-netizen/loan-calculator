"""Persist new loan header, schedule version, and schedule lines; post LOAN_APPROVAL GL when possible."""

from __future__ import annotations

from typing import Any

import pandas as pd

from decimal_utils import as_10dp

from .approval_journal import build_loan_approval_journal_payload
from .cash_gl import (
    _parse_optional_uuid_str,
    _post_event_for_loan,
    get_cached_source_cash_account_entries,
    validate_source_cash_gl_account_id_for_new_posting,
)
from .db import Json, _connection
from .schema_ddl import _ensure_loans_schema_for_save_loan
from .serialization import _date_conv


def save_loan(
    customer_id: int,
    loan_type: str,
    details: dict[str, Any],
    schedule_df: pd.DataFrame,
    schedule_version: int = 1,
    product_code: str | None = None,
    *,
    originated_from_split: bool = False,
    modification_topup_applied: bool = False,
    remodified_in_place: bool = False,
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
    metadata["penalty_rate_pct"] = float(
        details.get("penalty_rate_pct") if details.get("penalty_rate_pct") is not None else 0
    )
    if details.get("penalty_quotation"):
        metadata["penalty_quotation"] = details["penalty_quotation"]
    if details.get("currency"):
        metadata["currency"] = details["currency"]

    # Single date from UI: disbursement date. start_date is always set equal (column kept for future use).
    disb_date = details.get("disbursement_date") or details.get("start_date")

    _dbo_raw = details.get("disbursement_bank_option_id")
    disbursement_bank_option_id = None
    if _dbo_raw is not None and str(_dbo_raw).strip() != "":
        try:
            disbursement_bank_option_id = int(_dbo_raw)
        except (TypeError, ValueError):
            disbursement_bank_option_id = None

    _coll_sub: int | None = None
    _raw_cs = details.get("collateral_security_subtype_id")
    if _raw_cs is not None and str(_raw_cs).strip() != "":
        try:
            _coll_sub = int(_raw_cs)
        except (TypeError, ValueError):
            _coll_sub = None
    _coll_chg_raw = details.get("collateral_charge_amount")
    _coll_val_raw = details.get("collateral_valuation_amount")
    _coll_chg = (
        float(as_10dp(_coll_chg_raw))
        if _coll_chg_raw is not None and str(_coll_chg_raw).strip() != ""
        else None
    )
    _coll_val = (
        float(as_10dp(_coll_val_raw))
        if _coll_val_raw is not None and str(_coll_val_raw).strip() != ""
        else None
    )

    cash_gl_account_id = _parse_optional_uuid_str(details.get("cash_gl_account_id"))
    if get_cached_source_cash_account_entries() and cash_gl_account_id is None:
        raise ValueError(
            "Operating cash / bank GL at loan capture is required when the source cash account list is configured. "
            "Select an account in loan capture step 1 (same list as Teller), or clear the cache only if migrating legacy data."
        )
    if cash_gl_account_id is not None:
        validate_source_cash_gl_account_id_for_new_posting(
            cash_gl_account_id,
            field_label="cash_gl_account_id",
        )

    _lp_raw = details.get("loan_purpose_id")
    loan_purpose_id: int | None = None
    if _lp_raw is not None and str(_lp_raw).strip() != "":
        try:
            loan_purpose_id = int(_lp_raw)
        except (TypeError, ValueError):
            loan_purpose_id = None

    with _connection() as conn:
        _ensure_loans_schema_for_save_loan(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loans (
                    customer_id, loan_type, product_code, principal, disbursed_amount, term,
                    annual_rate, monthly_rate, drawdown_fee, arrangement_fee, admin_fee,
                    admin_fee_amount, drawdown_fee_amount, arrangement_fee_amount,
                    disbursement_date, start_date, end_date, first_repayment_date,
                    installment, total_payment, grace_type, moratorium_months, bullet_type, scheme,
                    payment_timing, metadata, status, agent_id, relationship_manager_id,
                    disbursement_bank_option_id, cash_gl_account_id,
                    collateral_security_subtype_id, collateral_charge_amount, collateral_valuation_amount,
                    loan_purpose_id,
                    remodified_in_place, originated_from_split, modification_topup_applied
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
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
                    float(
                        as_10dp(
                            details.get("admin_fee_amount")
                            or (
                                float(details.get("principal", details.get("facility", 0)))
                                * float(details.get("admin_fee") or 0)
                            )
                        )
                    ),
                    float(
                        as_10dp(
                            details.get("drawdown_fee_amount")
                            or (
                                float(details.get("principal", details.get("facility", 0)))
                                * float(details.get("drawdown_fee") or 0)
                            )
                        )
                    ),
                    float(
                        as_10dp(
                            details.get("arrangement_fee_amount")
                            or (
                                float(details.get("principal", details.get("facility", 0)))
                                * float(details.get("arrangement_fee") or 0)
                            )
                        )
                    ),
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
                    disbursement_bank_option_id,
                    cash_gl_account_id,
                    _coll_sub,
                    _coll_chg,
                    _coll_val,
                    loan_purpose_id,
                    bool(remodified_in_place),
                    bool(originated_from_split),
                    bool(modification_topup_applied),
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
                period_date = (
                    str(row.get("Date", row.get("Date", "")))[:32] if pd.notna(row.get("Date")) else None
                )
                payment = (
                    float(as_10dp(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0)))))
                    if pd.notna(row.get("Payment", row.get("Monthly Installment", 0)))
                    else 0.0
                )
                principal = (
                    float(as_10dp(row.get("Principal", row.get("principal", 0))))
                    if pd.notna(row.get("Principal"))
                    else 0.0
                )
                interest = (
                    float(as_10dp(row.get("Interest", row.get("interest", 0))))
                    if pd.notna(row.get("Interest"))
                    else 0.0
                )
                principal_balance = (
                    float(as_10dp(row.get("Principal Balance", row.get("principal_balance", 0))))
                    if pd.notna(row.get("Principal Balance"))
                    else 0.0
                )
                total_outstanding = (
                    float(as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0))))
                    if pd.notna(row.get("Total Outstanding"))
                    else 0.0
                )
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        schedule_id,
                        period,
                        period_date,
                        payment,
                        principal,
                        interest,
                        principal_balance,
                        total_outstanding,
                    ),
                )

    try:
        from accounting.service import AccountingService

        svc = AccountingService()
        payload = build_loan_approval_journal_payload(details)

        disb_date_str = details.get("disbursement_date") or details.get("start_date")
        e_date = _date_conv(disb_date_str) if disb_date_str else None

        _post_event_for_loan(
            svc,
            loan_id,
            event_type="LOAN_APPROVAL",
            reference=f"LOAN-{loan_id}",
            description=f"Loan Approval and Disbursement for {loan_id}",
            event_id=str(loan_id),
            created_by="system",
            entry_date=e_date,
            payload=payload,
        )
    except Exception as e:
        print(f"Failed to post LOAN_APPROVAL journal for loan {loan_id}: {e}")

    return loan_id
