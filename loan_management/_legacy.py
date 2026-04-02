"""
Loan management: persist loan details, schedules, and repayments to the database.
Uses loans.py for computation only; this module handles DB writes.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import pandas as pd

from decimal_utils import as_10dp

from .cash_gl import (
    SOURCE_CASH_ACCOUNT_CACHE_KEY,
    SOURCE_CASH_TREE_ROOT_CODE,
    _merge_cash_gl_into_payload,
    _parse_optional_uuid_str,
    _post_event_for_loan,
    get_cached_source_cash_account_entries,
    validate_source_cash_gl_account_id_for_new_posting,
)
from .allocation_audit import _log_allocation_audit
from .allocation_queries import (
    _get_allocation_sum_for_repayment,
    _get_opening_balances_for_repayment,
    _sum_net_allocations_earlier_same_day,
    get_allocation_totals_for_loan_date,
    get_credits_for_loan_date,
    get_net_allocation_for_loan_date,
    get_repayment_opening_delinquency_total,
    get_repayments_with_allocations,
    get_unallocated_for_loan_date,
)
from .amount_due import get_amount_due_summary
from .apply_allocations_loan_date import apply_allocations_for_loan_date
from .approval_drafts import (
    approve_loan_approval_draft,
    dismiss_loan_approval_draft,
    get_loan_approval_draft,
    list_loan_approval_drafts,
    resubmit_loan_approval_draft,
    save_loan_approval_draft,
    send_back_loan_approval_draft,
    terminate_loan,
    update_loan_approval_draft_staged,
)
from .approval_journal import build_loan_approval_journal_payload
from .daily_state import (
    get_loan_daily_state_balances,
    get_loan_daily_state_range,
    save_loan_daily_state,
)
from .db import Json, RealDictCursor, _connection, _get_conn, psycopg2
from .delinquency_views import get_teller_amount_due_today, get_total_delinquency_arrears_summary
from .exceptions import NeedOverpaymentDecision
from .repayment_types import ReverseRepaymentResult, _reversal_posting_calendar_date
from .loan_purposes import (
    clear_all_loan_purposes,
    count_loan_purposes_rows,
    create_loan_purpose,
    ensure_loan_purpose_rows,
    get_loan_purpose_by_id,
    list_loan_purposes,
    set_loan_purpose_active,
    update_loan_purpose,
)
from .loan_records import get_loan, get_loans_by_customer, update_loan_details, update_loan_safe_details
from .product_catalog import (
    CONFIG_KEY_PRODUCT_PREFIX,
    CONFIG_KEY_SYSTEM,
    create_product,
    delete_product,
    get_product,
    get_product_by_code,
    get_product_config_from_db,
    list_products,
    load_system_config_from_db,
    save_product_config_to_db,
    save_system_config_to_db,
    update_product,
)
from .receipt_allocation_gl import post_receipt_allocation_gl_reversals
from .reallocation import reallocate_repayment
from .repayment_queries import (
    get_loan_ids_with_reversed_receipts_on_date,
    get_repayment_ids_for_loan_and_date,
    get_repayment_ids_for_value_date,
)
from .repayment_record import record_repayment, record_repayments_batch
from .repayment_waterfall import allocate_repayment_waterfall
from .repost_gl_range import repost_gl_for_loan_date_range
from .schema_ddl import (
    _ensure_loan_approval_drafts_table,
    _ensure_loan_purposes_schema,
    _ensure_loans_schema_for_save_loan,
)
from .schedules import (
    get_latest_schedule_version,
    get_schedule_lines,
    replace_schedule_lines,
    save_new_schedule_version,
)
from .serialization import _date_conv, _json_safe
from .unapplied_eod import apply_unapplied_funds_to_arrears_eod
from .unapplied_queries import (
    get_loans_with_unapplied_balance,
    get_unapplied_balance,
    get_unapplied_entries,
    get_unapplied_ledger_balance,
    get_unapplied_ledger_entries_for_statement,
    get_unapplied_repayment_ids,
)
from .unapplied_refs import (
    _repayment_journal_reference,
    _unapplied_original_reference,
    _unapplied_reversal_reference,
)
from .waterfall_core import (
    BUCKET_TO_ALLOC,
    STANDARD_SKIP_BUCKETS,
    _get_waterfall_config,
    compute_waterfall_allocation,
)


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
                    loan_purpose_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                    disbursement_bank_option_id,
                    cash_gl_account_id,
                    _coll_sub,
                    _coll_chg,
                    _coll_val,
                    loan_purpose_id,
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
        
        _post_event_for_loan(svc, loan_id,
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


def reverse_repayment(
    original_repayment_id: int,
    *,
    system_date: datetime | str | None = None,
) -> ReverseRepaymentResult:
    """
    Insert a reversing repayment row, leave the original immutable, and undo its
    allocation so state is correct for any later receipts on the same loan/date.
    - Adds the original's allocation back to loan_daily_state for its value_date.
    - Removes pending unapplied_funds for this repayment.
    - Reversal row has negative amount and status 'reversed'; original is marked 'reversed'.
    - After commit, replays single-loan EOD for each day from the receipt value date through
      max(system business date, reversal posting calendar date) so forward `loan_daily_state`
      rows match the reversed reality.

    Returns ReverseRepaymentResult; check eod_rerun_success / eod_rerun_error even when save succeeded.
    """
    saved_new_id: int | None = None
    saved_loan_id: int | None = None
    saved_value_date: date | None = None
    # Ensure original has allocation before we reverse (mirror row + liquidation unwind in DB).
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
                        fees_charges_balance     = fees_charges_balance     + %s,
                        regular_interest_in_suspense_balance =
                            regular_interest_in_suspense_balance + %s,
                        penalty_interest_in_suspense_balance =
                            penalty_interest_in_suspense_balance + %s,
                        default_interest_in_suspense_balance =
                            default_interest_in_suspense_balance + %s,
                        total_interest_in_suspense_balance =
                            total_interest_in_suspense_balance + %s + %s + %s
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
                        _f(alloc_row["alloc_interest_accrued"]),
                        _f(alloc_row["alloc_penalty_interest"]),
                        _f(alloc_row["alloc_default_interest"]),
                        _f(alloc_row["alloc_interest_accrued"]),
                        _f(alloc_row["alloc_penalty_interest"]),
                        _f(alloc_row["alloc_default_interest"]),
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
            try:
                from accounting_service import AccountingService
                svc_unapplied = AccountingService()
            except Exception:
                svc_unapplied = None
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

                # GL reversal for the unapplied overpayment credit.
                if svc_unapplied is not None and float(uf_row.get("amount") or 0) > 1e-6:
                    orig_ref = _unapplied_original_reference(
                        "credit",
                        loan_id=loan_id,
                        repayment_id=original_repayment_id,
                        value_date=eff_date,
                    )
                    rev_ref = _unapplied_reversal_reference(orig_ref)
                    _post_event_for_loan(
                        svc_unapplied,
                        loan_id,
                        repayment_id=original_repayment_id,
                        event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                        reference=rev_ref,
                        description=f"Reversal of unapplied overpayment: {orig_ref}",
                        event_id=rev_ref,
                        created_by="system",
                        entry_date=eff_date,
                        amount=Decimal(str(float(uf_row.get("amount") or 0))),
                        is_reversal=True,
                    )

            # Reversal cascade: reverse any allocations that consumed this receipt's overpayment
            # (EOD apply-to-arrears with source_repayment_id = original)
            cur.execute(
                """
                SELECT lra.id, lra.repayment_id, lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                       lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                       lra.alloc_default_interest, lra.alloc_penalty_interest, lra.alloc_fees_charges,
                       lra.unallocated,
                       lra.source_repayment_id,
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
                unallocated_orig = _f(alloc.get("unallocated"))
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
                        fees_charges_balance = fees_charges_balance + %s,
                        regular_interest_in_suspense_balance =
                            regular_interest_in_suspense_balance + %s,
                        penalty_interest_in_suspense_balance =
                            penalty_interest_in_suspense_balance + %s,
                        default_interest_in_suspense_balance =
                            default_interest_in_suspense_balance + %s,
                        total_interest_in_suspense_balance =
                            total_interest_in_suspense_balance + %s + %s + %s
                    WHERE loan_id = %s AND as_of_date = %s
                    """,
                    (
                        apr,
                        apa,
                        aia,
                        aiar,
                        adi,
                        api,
                        afc,
                        aia,
                        api,
                        adi,
                        aia,
                        api,
                        adi,
                        loan_id,
                        alloc_date,
                    ),
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
                cur.execute(
                    """
                    UPDATE loan_daily_state
                    SET total_delinquency_arrears =
                        COALESCE(principal_arrears, 0)
                      + COALESCE(interest_arrears_balance, 0)
                      + COALESCE(default_interest_balance, 0)
                      + COALESCE(penalty_interest_balance, 0)
                      + COALESCE(fees_charges_balance, 0)
                    WHERE loan_id = %s AND as_of_date = %s
                    """,
                    (loan_id, alloc_date),
                )
                # Persist the liquidation unwind on a NEW system repayment row so we don't overwrite the original.
                cur.execute(
                    """
                    INSERT INTO loan_repayments (
                        loan_id, amount, payment_date, reference, value_date, status, original_repayment_id
                    ) VALUES (%s, %s, %s, %s, %s, 'reversed', %s)
                    RETURNING id
                    """,
                    (
                        loan_id,
                        float(as_10dp(apr + apa + aia + aiar + adi + api + afc)),
                        alloc_date,
                        "Reversal of unapplied funds allocation",
                        alloc_date,
                        alloc["repayment_id"],
                    )
                )
                row_liq_rev = cur.fetchone()
                new_liq_rev_id = int(row_liq_rev["id"]) if row_liq_rev and "id" in row_liq_rev else None
                if new_liq_rev_id is None:
                    raise RuntimeError("Failed to insert reversal row for unapplied liquidation cascade.")

                # Link back to the original source_repayment_id so the reversal correctly appears
                # in the unapplied_funds_ledger to balance out the original liquidation.
                rev_alloc_total = -(apr + apa + aia + aiar + adi + api + afc)
                rev_unallocated = -unallocated_orig
                cur.execute(
                    """
                    INSERT INTO loan_repayment_allocation (
                        repayment_id, alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total,
                        alloc_total, unallocated, event_type, source_repayment_id
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        'unallocation_parent_reversed', %s
                    )
                    """,
                    (
                        new_liq_rev_id,
                        float(as_10dp(-apr)), float(as_10dp(-apa)), float(as_10dp(-aia)), float(as_10dp(-aiar)),
                        float(as_10dp(-adi)), float(as_10dp(-api)), float(as_10dp(-afc)),
                        float(as_10dp(-(apr + apa))), float(as_10dp(-(aia + aiar + adi + api))), float(as_10dp(-afc)),
                        float(as_10dp(rev_alloc_total)),
                        float(as_10dp(rev_unallocated)),
                        alloc.get("source_repayment_id") or original_repayment_id,
                    ),
                )
                cur.execute(
                    "UPDATE loan_repayments SET status = 'reversed' WHERE id = %s",
                    (alloc["repayment_id"],),
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

                # GL reversal for liquidation bucket journals (UNAPPLIED_LIQUIDATION_* templates).
                if svc_unapplied is not None and amount_applied > 1e-6:
                    liq_orig_ref = _unapplied_original_reference(
                        "liquidation",
                        loan_id=loan_id,
                        repayment_id=original_repayment_id,
                        value_date=alloc_date,
                    )
                    liq_rev_ref = _unapplied_reversal_reference(liq_orig_ref)

                    if apr > 1e-6:
                        _post_event_for_loan(
                            svc_unapplied,
                            loan_id,
                            repayment_id=original_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_NOT_YET_DUE",
                            reference=liq_rev_ref,
                            description=f"Reversal of unapplied liquidation: {liq_orig_ref}",
                            event_id=liq_rev_ref,
                            created_by="system",
                            entry_date=alloc_date,
                            amount=Decimal(str(apr)),
                            is_reversal=True,
                        )
                    if apa > 1e-6:
                        _post_event_for_loan(
                            svc_unapplied,
                            loan_id,
                            repayment_id=original_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS",
                            reference=liq_rev_ref,
                            description=f"Reversal of unapplied liquidation: {liq_orig_ref}",
                            event_id=liq_rev_ref,
                            created_by="system",
                            entry_date=alloc_date,
                            amount=Decimal(str(apa)),
                            is_reversal=True,
                        )
                    if aia > 1e-6:
                        _post_event_for_loan(
                            svc_unapplied,
                            loan_id,
                            repayment_id=original_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST_NOT_YET_DUE",
                            reference=liq_rev_ref,
                            description=f"Reversal of unapplied liquidation: {liq_orig_ref}",
                            event_id=liq_rev_ref,
                            created_by="system",
                            entry_date=alloc_date,
                            amount=Decimal(str(aia)),
                            is_reversal=True,
                        )
                    if aiar > 1e-6:
                        _post_event_for_loan(
                            svc_unapplied,
                            loan_id,
                            repayment_id=original_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST",
                            reference=liq_rev_ref,
                            description=f"Reversal of unapplied liquidation: {liq_orig_ref}",
                            event_id=liq_rev_ref,
                            created_by="system",
                            entry_date=alloc_date,
                            amount=Decimal(str(aiar)),
                            is_reversal=True,
                        )
                    if adi > 1e-6:
                        _post_event_for_loan(
                            svc_unapplied,
                            loan_id,
                            repayment_id=original_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST",
                            reference=liq_rev_ref,
                            description=f"Reversal of unapplied liquidation: {liq_orig_ref}",
                            event_id=liq_rev_ref,
                            created_by="system",
                            entry_date=alloc_date,
                            amount=Decimal(str(adi)),
                            is_reversal=True,
                        )
                    if api > 1e-6:
                        _post_event_for_loan(
                            svc_unapplied,
                            loan_id,
                            repayment_id=original_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_PENALTY_INTEREST",
                            reference=liq_rev_ref,
                            description=f"Reversal of unapplied liquidation: {liq_orig_ref}",
                            event_id=liq_rev_ref,
                            created_by="system",
                            entry_date=alloc_date,
                            amount=Decimal(str(api)),
                            is_reversal=True,
                        )
                    if afc > 1e-6:
                        _post_event_for_loan(
                            svc_unapplied,
                            loan_id,
                            repayment_id=original_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_PASS_THROUGH_COST_RECOVERY",
                            reference=liq_rev_ref,
                            description=f"Reversal of unapplied liquidation: {liq_orig_ref}",
                            event_id=liq_rev_ref,
                            created_by="system",
                            entry_date=alloc_date,
                            amount=Decimal(str(afc)),
                            is_reversal=True,
                        )

            # Always reverse the GL journals created for this receipt's own allocation buckets.
            # Without this, statements can show the reversal (via updated allocation rows),
            # but the GL (journal_entries) will not be netted for CUSTOMER-facing ledger totals.
            try:
                from accounting_service import AccountingService
                svc_alloc = AccountingService()
            except Exception:
                svc_alloc = None

            def _p(v):
                return float(v or 0)

            if svc_alloc is not None and alloc_row:
                _rj = _repayment_journal_reference(loan_id, original_repayment_id)
                # Principal
                prin_arr = _p(alloc_row.get("alloc_principal_arrears"))
                if prin_arr > 1e-6:
                    p = Decimal(str(prin_arr))
                    _post_event_for_loan(
                        svc_alloc,
                        loan_id,
                        repayment_id=original_repayment_id,
                        event_type="PAYMENT_PRINCIPAL",
                        reference=_rj,
                        description=f"Reversal of principal (arrears) — {_rj}",
                        event_id=f"REV-REPAY-{original_repayment_id}-PRIN-ARR",
                        created_by="system",
                        entry_date=eff_date,
                        payload={"cash_operating": p, "principal_arrears": p},
                        amount=p,
                        is_reversal=True,
                    )
                prin_nyd = _p(alloc_row.get("alloc_principal_not_due"))
                if prin_nyd > 1e-6:
                    p = Decimal(str(prin_nyd))
                    _post_event_for_loan(
                        svc_alloc,
                        loan_id,
                        repayment_id=original_repayment_id,
                        event_type="PAYMENT_PRINCIPAL_NOT_YET_DUE",
                        reference=_rj,
                        description=f"Reversal of principal (not yet due) — {_rj}",
                        event_id=f"REV-REPAY-{original_repayment_id}-PRIN-NYD",
                        created_by="system",
                        entry_date=eff_date,
                        payload={"cash_operating": p, "loan_principal": p},
                        amount=p,
                        is_reversal=True,
                    )
                # Interest
                int_arrears = _p(alloc_row.get("alloc_interest_arrears"))
                if int_arrears > 1e-6:
                    p = Decimal(str(int_arrears))
                    _post_event_for_loan(
                        svc_alloc,
                        loan_id,
                        repayment_id=original_repayment_id,
                        event_type="PAYMENT_REGULAR_INTEREST",
                        reference=_rj,
                        description=f"Reversal of interest (arrears) — {_rj}",
                        event_id=f"REV-REPAY-{original_repayment_id}-INT-ARR",
                        created_by="system",
                        entry_date=eff_date,
                        payload={"cash_operating": p, "regular_interest_arrears": p},
                        amount=p,
                        is_reversal=True,
                    )
                int_accrued = _p(alloc_row.get("alloc_interest_accrued"))
                if int_accrued > 1e-6:
                    p = Decimal(str(int_accrued))
                    _post_event_for_loan(
                        svc_alloc,
                        loan_id,
                        repayment_id=original_repayment_id,
                        event_type="PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
                        reference=_rj,
                        description=f"Reversal of interest (accrued / not billed) — {_rj}",
                        event_id=f"REV-REPAY-{original_repayment_id}-INT-ACC",
                        created_by="system",
                        entry_date=eff_date,
                        payload={"cash_operating": p, "regular_interest_accrued": p},
                        amount=p,
                        is_reversal=True,
                    )
                # Penalty & Default
                pen = _p(alloc_row.get("alloc_penalty_interest"))
                if pen > 1e-6:
                    p = Decimal(str(pen))
                    _post_event_for_loan(
                        svc_alloc,
                        loan_id,
                        repayment_id=original_repayment_id,
                        event_type="PAYMENT_PENALTY_INTEREST",
                        reference=_rj,
                        description=f"Reversal of penalty interest — {_rj}",
                        event_id=f"REV-REPAY-{original_repayment_id}-PEN",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "penalty_interest_asset": p,
                            "penalty_interest_suspense": p,
                            "penalty_interest_income": p,
                        },
                        amount=p,
                        is_reversal=True,
                    )
                default_i = _p(alloc_row.get("alloc_default_interest"))
                if default_i > 1e-6:
                    p = Decimal(str(default_i))
                    _post_event_for_loan(
                        svc_alloc,
                        loan_id,
                        repayment_id=original_repayment_id,
                        event_type="PAYMENT_DEFAULT_INTEREST",
                        reference=_rj,
                        description=f"Reversal of default interest — {_rj}",
                        event_id=f"REV-REPAY-{original_repayment_id}-DEF",
                        created_by="system",
                        entry_date=eff_date,
                        payload={
                            "cash_operating": p,
                            "default_interest_asset": p,
                            "default_interest_suspense": p,
                            "default_interest_income": p,
                        },
                        amount=p,
                        is_reversal=True,
                    )

            sdate = system_date
            if sdate is None:
                sdate = datetime.now()
            elif isinstance(sdate, str):
                sdate = datetime.fromisoformat(sdate.replace("Z", "+00:00"))

            # Reversal rows: reference fields show original receipt id (policy REV n), not the reversing row id.
            rev_label = f"REV {original_repayment_id}"
            rev_ref = rev_label
            rev_cust_ref = rev_label
            rev_co_ref = rev_label

            orig_src_cash = row.get("source_cash_gl_account_id")
            if orig_src_cash is not None:
                orig_src_cash = str(orig_src_cash).strip() or None

            cur.execute(
                """
                INSERT INTO loan_repayments (
                    loan_id, schedule_line_id, period_number, amount, payment_date,
                    reference, customer_reference, company_reference, value_date, system_date,
                    status, original_repayment_id, source_cash_gl_account_id
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    'reversed', %s, %s
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
                    orig_src_cash,
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
            saved_new_id = new_id
            saved_loan_id = loan_id
            saved_value_date = eff_date

    if saved_new_id is None or saved_loan_id is None or saved_value_date is None:
        raise RuntimeError(
            f"reverse_repayment: commit completed but capture failed for repayment {original_repayment_id}."
        )

    eod_from = saved_value_date
    posting_cal = _reversal_posting_calendar_date(system_date)
    try:
        from system_business_date import get_effective_date
        from eod import run_single_loan_eod_date_range

        # Replay daily state through the system date (accruals will be automatically
        # zeroed out by EOD for the system business date unless canonical EOD runs).
        sys_d = get_effective_date()
        desired_to = max(eod_from, posting_cal)
        eod_to = desired_to if desired_to <= sys_d else sys_d
        if eod_from > eod_to:
            return ReverseRepaymentResult(
                reversal_repayment_id=saved_new_id,
                loan_id=saved_loan_id,
                value_date=saved_value_date,
                eod_from_date=eod_from,
                eod_to_date=eod_to,
                eod_rerun_success=False,
                eod_rerun_error=(
                    "eod_replay: skipped (receipt value_date is after current system date)"
                ),
            )
        cfg = load_system_config_from_db() or {}
        eod_ok, eod_err = run_single_loan_eod_date_range(
            saved_loan_id, eod_from, eod_to, sys_cfg=cfg
        )
        if eod_ok:
            try:
                repost_gl_for_loan_date_range(saved_loan_id, eod_from, eod_to, created_by="system")
            except Exception as exc:
                eod_ok, eod_err = False, f"gl_repost: {exc}"
    except Exception as exc:
        eod_ok, eod_err = False, f"eod_replay: {exc}"

    return ReverseRepaymentResult(
        reversal_repayment_id=saved_new_id,
        loan_id=saved_loan_id,
        value_date=saved_value_date,
        eod_from_date=eod_from,
        eod_to_date=eod_to,
        eod_rerun_success=eod_ok,
        eod_rerun_error=None if eod_ok else eod_err,
    )


