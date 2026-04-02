"""
Loan management: persist loan details, schedules, and repayments to the database.
Uses loans.py for computation only; this module handles DB writes.
"""

from __future__ import annotations

import contextlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import pandas as pd

from decimal_utils import as_10dp

from .db import Json, RealDictCursor, _connection, psycopg2
from .loan_purposes import (
    _ensure_loan_purposes_schema,
    clear_all_loan_purposes,
    count_loan_purposes_rows,
    create_loan_purpose,
    ensure_loan_purpose_rows,
    get_loan_purpose_by_id,
    list_loan_purposes,
    set_loan_purpose_active,
    update_loan_purpose,
)
from .system_config import load_system_config_from_db, save_system_config_to_db
from .serialization import _date_conv, _json_safe
from .products import (
    CONFIG_KEY_PRODUCT_PREFIX,
    create_product,
    delete_product,
    get_product,
    get_product_by_code,
    get_product_config_from_db,
    list_products,
    save_product_config_to_db,
    update_product,
)
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
from .save_loan import build_loan_approval_journal_payload, save_loan
from .loan_records import get_loan, get_loans_by_customer, update_loan_details
from .schedules import (
    get_latest_schedule_version,
    get_schedule_lines,
    replace_schedule_lines,
    save_new_schedule_version,
)
from .loan_daily_state import (
    get_loan_daily_state_balances,
    get_loan_daily_state_range,
    save_loan_daily_state,
)
from .unapplied import (
    _credit_unapplied_funds,
    _unapplied_original_reference,
    _unapplied_reversal_reference,
    apply_unapplied_funds_recast,
    apply_unapplied_funds_to_arrears_eod,
    get_loans_with_unapplied_balance,
    get_unapplied_balance,
    get_unapplied_entries,
    get_unapplied_ledger_balance,
    get_unapplied_ledger_entries_for_statement,
    get_unapplied_repayment_ids,
)
from .waterfall_min import (
    BUCKET_TO_ALLOC,
    STANDARD_SKIP_BUCKETS,
    _get_waterfall_config,
    compute_waterfall_allocation,
)


def _parse_optional_uuid_str(val: Any) -> str | None:
    """Return canonical UUID string or None; raises ValueError if non-empty but invalid."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return str(uuid.UUID(s))
    except ValueError as e:
        raise ValueError(f"Invalid UUID for GL account reference: {val!r}") from e


SOURCE_CASH_ACCOUNT_CACHE_KEY = "source_cash_account_cache"
SOURCE_CASH_TREE_ROOT_CODE = "A100000"


def get_cached_source_cash_account_entries() -> list[dict]:
    """Snapshot rows ``{id, code, name}`` from system config; empty if cache never built."""
    cfg = load_system_config_from_db() or {}
    block = cfg.get(SOURCE_CASH_ACCOUNT_CACHE_KEY) or {}
    entries = block.get("entries") or []
    if not isinstance(entries, list):
        return []
    return [dict(e) for e in entries]


def validate_source_cash_gl_account_id_for_new_posting(
    account_uuid: str | None,
    *,
    field_label: str = "Cash account",
) -> str:
    """
    Require a populated source-cash cache and an account id that appears in it.
    Returns canonical UUID string.
    """
    entries = get_cached_source_cash_account_entries()
    if not entries:
        raise ValueError(
            f"{field_label}: the source cash list has not been built. "
            "Go to **System configurations → Accounting configurations**, open "
            "**Maintenance — source cash account cache**, and rebuild it (administrators only)."
        )
    if account_uuid is None or str(account_uuid).strip() == "":
        raise ValueError(f"{field_label} is required.")
    canonical = _parse_optional_uuid_str(account_uuid)
    if canonical is None:
        raise ValueError(f"Invalid {field_label} UUID.")
    allowed = {str(e.get("id")) for e in entries if e.get("id")}
    if canonical not in allowed:
        raise ValueError(
            f"{field_label} is not in the allowed list (posting leaves under **{SOURCE_CASH_TREE_ROOT_CODE}** per branch). "
            "Pick an account from the dropdown or rebuild the cache after chart changes."
        )
    return canonical


def _merge_cash_gl_into_payload(
    loan_id: int | None,
    repayment_id: int | None,
    payload: dict | None,
) -> dict:
    """
    If payload does not already set account_overrides['cash_operating'], fill from:
    1) loan_repayments.source_cash_gl_account_id when repayment_id is set, else
    2) loans.cash_gl_account_id for the loan.
    Explicit account_overrides from the caller always win.
    """
    payload = dict(payload or {})
    ao = dict(payload.get("account_overrides") or {})
    if "cash_operating" in ao:
        return payload
    if loan_id is None:
        return payload
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor as _RDC

        from config import get_database_url as _gdb
    except ImportError:
        return payload
    src: str | None = None
    conn = psycopg2.connect(_gdb())
    try:
        with conn.cursor(cursor_factory=_RDC) as cur:
            if repayment_id is not None:
                cur.execute(
                    """
                    SELECT source_cash_gl_account_id FROM loan_repayments WHERE id = %s
                    """,
                    (int(repayment_id),),
                )
                r = cur.fetchone()
                if r and r.get("source_cash_gl_account_id"):
                    src = str(r["source_cash_gl_account_id"])
            if src is None:
                cur.execute(
                    "SELECT cash_gl_account_id FROM loans WHERE id = %s",
                    (int(loan_id),),
                )
                lr = cur.fetchone()
                if lr and lr.get("cash_gl_account_id"):
                    src = str(lr["cash_gl_account_id"])
    except Exception:
        src = None
    finally:
        conn.close()
    if src:
        ao["cash_operating"] = src
        payload["account_overrides"] = ao
    return payload


def _post_event_for_loan(
    svc,
    loan_id: int | None,
    *,
    repayment_id: int | None = None,
    **kwargs,
) -> None:
    """
    Post with loan_id / repayment_id so AccountingService.post_event applies the same
    cash GL merge as Teller (receipt) and loan capture (loan row).
    """
    kw = dict(kwargs)
    if loan_id is not None:
        kw["loan_id"] = int(loan_id)
    if repayment_id is not None:
        kw["repayment_id"] = int(repayment_id)
    svc.post_event(**kw)


def repost_gl_for_loan_date_range(
    loan_id: int,
    start_date: date,
    end_date: date,
    *,
    created_by: str = "system",
) -> None:
    """
    Re-post deterministic GL journals so GL matches the latest allocation state.

    Why needed:
    - EOD replay can overwrite allocations/daily_state for later receipts/liquidations.
    - Original GL journals posted at capture time remain stale unless we re-post.

    Safety:
    - AccountingRepository.save_journal_entry replaces journals by (event_id, event_tag),
      so re-posting is idempotent and converges GL to the latest computed values.
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    try:
        from accounting_service import AccountingService
    except Exception as exc:
        raise RuntimeError(f"gl_repost: AccountingService unavailable: {exc}") from exc

    svc = AccountingService()

    def _p(v: Any) -> float:
        return float(v or 0)

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Teller receipts (and their reversal rows), excluding internal system liquidations.
            cur.execute(
                """
                SELECT
                    lr.id,
                    lr.status,
                    lr.original_repayment_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS eff_date,
                    lra.alloc_principal_not_due,
                    lra.alloc_principal_arrears,
                    lra.alloc_interest_accrued,
                    lra.alloc_interest_arrears,
                    lra.alloc_default_interest,
                    lra.alloc_penalty_interest,
                    lra.alloc_fees_charges,
                    lra.unallocated
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.reference, '') ILIKE '%%Reversal of unapplied funds%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%Reversal of unapplied funds%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%Reversal of unapplied funds%%'
                  )
                ORDER BY (COALESCE(lr.value_date, lr.payment_date))::date, lr.id
                """,
                (loan_id, start_date, end_date),
            )
            receipt_rows = cur.fetchall()

            for r in receipt_rows:
                rid = int(r["id"])
                eff = r["eff_date"]
                status = (r.get("status") or "").strip().lower()
                orig_id = r.get("original_repayment_id")
                orig_id_int = int(orig_id) if orig_id is not None else None

                apr = _p(r.get("alloc_principal_arrears"))
                pnd = _p(r.get("alloc_principal_not_due"))
                aiar = _p(r.get("alloc_interest_arrears"))
                aia = _p(r.get("alloc_interest_accrued"))
                adi = _p(r.get("alloc_default_interest"))
                api = _p(r.get("alloc_penalty_interest"))
                afc = _p(r.get("alloc_fees_charges"))
                unalloc = _p(r.get("unallocated"))

                # Base reference for receipt journals.
                base_ref = _repayment_journal_reference(loan_id, rid)

                # Original receipt journals (only meaningful for posted receipts).
                if status == "posted":
                    if unalloc > 1e-6:
                        ref_u = _unapplied_original_reference(
                            "credit", loan_id=loan_id, repayment_id=rid, value_date=eff
                        )
                        _post_event_for_loan(svc, loan_id, repayment_id=rid,
                            event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                            reference=ref_u,
                            description="Unapplied funds on overpayment",
                            event_id=ref_u,
                            created_by=created_by,
                            entry_date=eff,
                            amount=Decimal(str(unalloc)),
                        )
                    if apr > 1e-6:
                        p = Decimal(str(apr))
                        _post_event_for_loan(svc, loan_id, repayment_id=rid,
                            event_type="PAYMENT_PRINCIPAL",
                            reference=base_ref,
                            description=f"Principal (arrears) — {base_ref}",
                            event_id=f"REPAY-{rid}-PRIN-ARR",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "principal_arrears": p},
                        )
                    if pnd > 1e-6:
                        p = Decimal(str(pnd))
                        _post_event_for_loan(svc, loan_id, repayment_id=rid,
                            event_type="PAYMENT_PRINCIPAL_NOT_YET_DUE",
                            reference=base_ref,
                            description=f"Principal (not yet due) — {base_ref}",
                            event_id=f"REPAY-{rid}-PRIN-NYD",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "loan_principal": p},
                        )
                    if aiar > 1e-6:
                        p = Decimal(str(aiar))
                        _post_event_for_loan(svc, loan_id, repayment_id=rid,
                            event_type="PAYMENT_REGULAR_INTEREST",
                            reference=base_ref,
                            description=f"Interest (arrears) — {base_ref}",
                            event_id=f"REPAY-{rid}-INT-ARR",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "regular_interest_arrears": p},
                        )
                    if aia > 1e-6:
                        p = Decimal(str(aia))
                        _post_event_for_loan(svc, loan_id, repayment_id=rid,
                            event_type="PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
                            reference=base_ref,
                            description=f"Interest (accrued / not billed) — {base_ref}",
                            event_id=f"REPAY-{rid}-INT-ACC",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "regular_interest_accrued": p},
                        )
                    if api > 1e-6:
                        p = Decimal(str(api))
                        _post_event_for_loan(svc, loan_id, repayment_id=rid,
                            event_type="PAYMENT_PENALTY_INTEREST",
                            reference=base_ref,
                            description=f"Penalty interest — {base_ref}",
                            event_id=f"REPAY-{rid}-PEN",
                            created_by=created_by,
                            entry_date=eff,
                            payload=None,
                        )
                    if adi > 1e-6:
                        p = Decimal(str(adi))
                        _post_event_for_loan(svc, loan_id, repayment_id=rid,
                            event_type="PAYMENT_DEFAULT_INTEREST",
                            reference=base_ref,
                            description=f"Default interest — {base_ref}",
                            event_id=f"REPAY-{rid}-DEF",
                            created_by=created_by,
                            entry_date=eff,
                            payload=None,
                        )
                    if afc > 1e-6:
                        p = Decimal(str(afc))
                        _post_event_for_loan(svc, loan_id, repayment_id=rid,
                            event_type="PASS_THROUGH_COST_RECOVERY",
                            reference=base_ref,
                            description=f"Fees/charges — {base_ref}",
                            event_id=f"REPAY-{rid}-FEES",
                            created_by=created_by,
                            entry_date=eff,
                            amount=p,
                            payload=None,
                        )

                # Reversal receipt journals (only meaningful when receipt is reversed).
                if status == "reversed" and orig_id_int is not None:
                    # Reverse the original receipt journals using deterministic REV ids.
                    if unalloc > 1e-6:
                        orig_ref_u = _unapplied_original_reference(
                            "credit", loan_id=loan_id, repayment_id=orig_id_int, value_date=eff
                        )
                        rev_ref_u = _unapplied_reversal_reference(orig_ref_u)
                        _post_event_for_loan(svc, loan_id, repayment_id=orig_id_int,
                            event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                            reference=rev_ref_u,
                            description=f"Reversal of unapplied overpayment: {orig_ref_u}",
                            event_id=rev_ref_u,
                            created_by=created_by,
                            entry_date=eff,
                            amount=Decimal(str(unalloc)),
                            is_reversal=True,
                        )
                    if apr > 1e-6:
                        p = Decimal(str(apr))
                        _post_event_for_loan(svc, loan_id, repayment_id=orig_id_int,
                            event_type="PAYMENT_PRINCIPAL",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of principal (arrears) — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-PRIN-ARR",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "principal_arrears": p},
                            amount=p,
                            is_reversal=True,
                        )
                    if pnd > 1e-6:
                        p = Decimal(str(pnd))
                        _post_event_for_loan(svc, loan_id, repayment_id=orig_id_int,
                            event_type="PAYMENT_PRINCIPAL_NOT_YET_DUE",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of principal (not yet due) — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-PRIN-NYD",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "loan_principal": p},
                            amount=p,
                            is_reversal=True,
                        )
                    if aiar > 1e-6:
                        p = Decimal(str(aiar))
                        _post_event_for_loan(svc, loan_id, repayment_id=orig_id_int,
                            event_type="PAYMENT_REGULAR_INTEREST",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of interest (arrears) — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-INT-ARR",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "regular_interest_arrears": p},
                            amount=p,
                            is_reversal=True,
                        )
                    if aia > 1e-6:
                        p = Decimal(str(aia))
                        _post_event_for_loan(svc, loan_id, repayment_id=orig_id_int,
                            event_type="PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of interest (accrued / not billed) — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-INT-ACC",
                            created_by=created_by,
                            entry_date=eff,
                            payload={"cash_operating": p, "regular_interest_accrued": p},
                            amount=p,
                            is_reversal=True,
                        )
                    if api > 1e-6:
                        p = Decimal(str(api))
                        _post_event_for_loan(svc, loan_id, repayment_id=orig_id_int,
                            event_type="PAYMENT_PENALTY_INTEREST",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of penalty interest — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-PEN",
                            created_by=created_by,
                            entry_date=eff,
                            amount=p,
                            is_reversal=True,
                        )
                    if adi > 1e-6:
                        p = Decimal(str(adi))
                        _post_event_for_loan(svc, loan_id, repayment_id=orig_id_int,
                            event_type="PAYMENT_DEFAULT_INTEREST",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of default interest — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-DEF",
                            created_by=created_by,
                            entry_date=eff,
                            amount=p,
                            is_reversal=True,
                        )
                    if afc > 1e-6:
                        p = Decimal(str(afc))
                        _post_event_for_loan(svc, loan_id, repayment_id=orig_id_int,
                            event_type="PASS_THROUGH_COST_RECOVERY",
                            reference=_repayment_journal_reference(loan_id, orig_id_int),
                            description=f"Reversal of fees/charges — Loan {loan_id}, Repayment id {orig_id_int}",
                            event_id=f"REV-REPAY-{orig_id_int}-FEES",
                            created_by=created_by,
                            entry_date=eff,
                            amount=p,
                            is_reversal=True,
                        )

            # Unapplied liquidations and liquidation reversals (system rows).
            cur.execute(
                """
                SELECT
                    (COALESCE(lr.value_date, lr.payment_date))::date AS eff_date,
                    lra.event_type,
                    lra.source_repayment_id,
                    lra.alloc_principal_not_due,
                    lra.alloc_principal_arrears,
                    lra.alloc_interest_accrued,
                    lra.alloc_interest_arrears,
                    lra.alloc_default_interest,
                    lra.alloc_penalty_interest,
                    lra.alloc_fees_charges
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE lr.loan_id = %s
                  AND (COALESCE(lr.value_date, lr.payment_date))::date BETWEEN %s AND %s
                  AND lra.event_type IN ('unapplied_funds_allocation', 'unallocation_parent_reversed')
                  AND lra.source_repayment_id IS NOT NULL
                ORDER BY (COALESCE(lr.value_date, lr.payment_date))::date, lr.id
                """,
                (loan_id, start_date, end_date),
            )
            liq_rows = cur.fetchall()
            for r in liq_rows:
                eff = r["eff_date"]
                src = int(r["source_repayment_id"])
                evt = str(r.get("event_type") or "")
                is_rev = evt == "unallocation_parent_reversed"

                liq_ref = _unapplied_original_reference(
                    "liquidation", loan_id=loan_id, repayment_id=src, value_date=eff
                )
                event_id = _unapplied_reversal_reference(liq_ref) if is_rev else liq_ref

                # unapplied_funds_allocation rows store positive bucket amounts.
                # unallocation_parent_reversed rows store the same magnitudes NEGATIVE; GL repost
                # needs positive amounts with is_reversal=True (matches reverse_repayment posting).
                def _liq_bucket(v: Any) -> float:
                    x = _p(v)
                    return abs(x) if is_rev else x

                apr = _liq_bucket(r.get("alloc_principal_not_due"))
                apa = _liq_bucket(r.get("alloc_principal_arrears"))
                aia = _liq_bucket(r.get("alloc_interest_accrued"))
                aiar = _liq_bucket(r.get("alloc_interest_arrears"))
                adi = _liq_bucket(r.get("alloc_default_interest"))
                api = _liq_bucket(r.get("alloc_penalty_interest"))
                afc = _liq_bucket(r.get("alloc_fees_charges"))

                if apr > 1e-6:
                    _post_event_for_loan(svc, loan_id, repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_NOT_YET_DUE",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: principal not yet due ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(apr)),
                        is_reversal=is_rev,
                    )
                if apa > 1e-6:
                    _post_event_for_loan(svc, loan_id, repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: principal arrears ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(apa)),
                        is_reversal=is_rev,
                    )
                if aia > 1e-6:
                    _post_event_for_loan(svc, loan_id, repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST_NOT_YET_DUE",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: interest accrued ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(aia)),
                        is_reversal=is_rev,
                    )
                if aiar > 1e-6:
                    _post_event_for_loan(svc, loan_id, repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: interest arrears ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(aiar)),
                        is_reversal=is_rev,
                    )
                if adi > 1e-6:
                    _post_event_for_loan(svc, loan_id, repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: default interest ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(adi)),
                        is_reversal=is_rev,
                    )
                if api > 1e-6:
                    _post_event_for_loan(svc, loan_id, repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_PENALTY_INTEREST",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: penalty interest ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(api)),
                        is_reversal=is_rev,
                    )
                if afc > 1e-6:
                    _post_event_for_loan(svc, loan_id, repayment_id=src,
                        event_type="UNAPPLIED_LIQUIDATION_PASS_THROUGH_COST_RECOVERY",
                        reference=event_id,
                        description=f"{'Reversal of ' if is_rev else ''}unapplied liquidation: fees/charges ({liq_ref})",
                        event_id=event_id,
                        created_by=created_by,
                        entry_date=eff,
                        amount=Decimal(str(afc)),
                        is_reversal=is_rev,
                    )


def update_loan_safe_details(
    loan_id: int,
    updates: dict[str, Any],
) -> None:
    """Update safe fields on an active loan without changing schedules or financials."""
    allowed_keys = {
        "collateral_security_subtype_id",
        "collateral_charge_amount",
        "collateral_valuation_amount",
        "metadata",
    }
    set_clauses = []
    params = []
    
    has_meta = False
    meta_val = None
    for k, v in updates.items():
        if k not in allowed_keys:
            continue
        if k == "metadata":
            has_meta = True
            meta_val = v
            continue
        set_clauses.append(f"{k} = %s")
        params.append(v)
        
    with _connection() as conn:
        with conn.cursor() as cur:
            if has_meta:
                cur.execute("SELECT metadata FROM loans WHERE id = %s", (loan_id,))
                row = cur.fetchone()
                existing_meta = row[0] if row and row[0] else {}
                if isinstance(existing_meta, str):
                    import json
                    try:
                        existing_meta = json.loads(existing_meta)
                    except Exception:
                        existing_meta = {}
                if isinstance(meta_val, dict):
                    existing_meta.update(meta_val)
                set_clauses.append("metadata = %s")
                params.append(Json(existing_meta))
                
            if set_clauses:
                set_clauses.append("updated_at = NOW()")
                query = f"UPDATE loans SET {', '.join(set_clauses)} WHERE id = %s"
                params.append(loan_id)
                cur.execute(query, tuple(params))
