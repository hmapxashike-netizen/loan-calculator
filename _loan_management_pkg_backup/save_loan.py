"""Persist new loan + schedule and post LOAN_APPROVAL journal."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pandas as pd

from decimal_utils import as_10dp

from .db import Json, _connection
from .loan_purposes import _ensure_loan_purposes_schema
from .serialization import _date_conv


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


def _ensure_loans_schema_for_save_loan(conn: Any) -> None:
    """
    Idempotent DDL so save_loan()'s INSERT matches the database when formal
    migrations have not been applied yet (same intent as schema 50, 51, and
    collateral columns from 53).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS disbursement_bank_options (
                id SERIAL PRIMARY KEY,
                label VARCHAR(255) NOT NULL,
                gl_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_disbursement_bank_options_active
            ON disbursement_bank_options (is_active, sort_order);
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS disbursement_bank_option_id INTEGER
            REFERENCES disbursement_bank_options(id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loans_disbursement_bank_option
            ON loans (disbursement_bank_option_id);
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS cash_gl_account_id UUID REFERENCES accounts(id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loans_cash_gl_account
            ON loans (cash_gl_account_id);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS provision_security_subtypes (
                id SERIAL PRIMARY KEY,
                security_type VARCHAR(128) NOT NULL,
                subtype_name VARCHAR(255) NOT NULL,
                typical_haircut_pct NUMERIC(22, 10) NOT NULL DEFAULT 0,
                system_notes TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (security_type, subtype_name)
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE loans
            ADD COLUMN IF NOT EXISTS collateral_security_subtype_id INTEGER
                REFERENCES provision_security_subtypes(id) ON DELETE SET NULL;
            """
        )
        cur.execute(
            "ALTER TABLE loans ADD COLUMN IF NOT EXISTS collateral_charge_amount NUMERIC(22, 10);"
        )
        cur.execute(
            "ALTER TABLE loans ADD COLUMN IF NOT EXISTS collateral_valuation_amount NUMERIC(22, 10);"
        )
    _ensure_loan_purposes_schema(conn)


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
    from . import _legacy as _lm

    loan_type_db = {
        "Consumer Loan": "consumer_loan",
        "Term Loan": "term_loan",
        "Bullet Loan": "bullet_loan",
        "Customised Repayments": "customised_repayments",
    }.get(loan_type, loan_type.replace(" ", "_").lower())

    metadata = details.get("metadata") or {}
    metadata["penalty_rate_pct"] = float(
        details.get("penalty_rate_pct") if details.get("penalty_rate_pct") is not None else 0
    )
    if details.get("penalty_quotation"):
        metadata["penalty_quotation"] = details["penalty_quotation"]
    if details.get("currency"):
        metadata["currency"] = details["currency"]

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

    cash_gl_account_id = _lm._parse_optional_uuid_str(details.get("cash_gl_account_id"))
    if _lm.get_cached_source_cash_account_entries() and cash_gl_account_id is None:
        raise ValueError(
            "Operating cash / bank GL at loan capture is required when the source cash account list is configured. "
            "Select an account in loan capture step 1 (same list as Teller), or clear the cache only if migrating legacy data."
        )
    if cash_gl_account_id is not None:
        _lm.validate_source_cash_gl_account_id_for_new_posting(
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
                ),
            )
            loan_id = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO loan_schedules (loan_id, version) VALUES (%s, %s) RETURNING id",
                (loan_id, schedule_version),
            )
            schedule_id = cur.fetchone()[0]

        with conn.cursor() as cur:
            for _, row in schedule_df.iterrows():
                period = int(row.get("Period", row.get("period", 0)))
                period_date = (
                    str(row.get("Date", row.get("Date", "")))[:32]
                    if pd.notna(row.get("Date"))
                    else None
                )
                payment = (
                    float(
                        as_10dp(
                            row.get("Payment", row.get("Monthly Installment", row.get("payment", 0)))
                        )
                    )
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
                    float(
                        as_10dp(row.get("Principal Balance", row.get("principal_balance", 0)))
                    )
                    if pd.notna(row.get("Principal Balance"))
                    else 0.0
                )
                total_outstanding = (
                    float(
                        as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0)))
                    )
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
        from accounting_service import AccountingService

        svc = AccountingService()
        payload = build_loan_approval_journal_payload(details)

        disb_date_str = details.get("disbursement_date") or details.get("start_date")
        e_date = _date_conv(disb_date_str) if disb_date_str else None

        _lm._post_event_for_loan(
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
