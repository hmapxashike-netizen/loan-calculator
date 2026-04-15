"""Persist waterfall allocation for a receipt and refresh same-day ``loan_daily_state``."""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta
from decimal import Decimal

from decimal_utils import as_10dp

from .allocation_queries import (
    _get_opening_balances_for_repayment,
    get_net_allocation_for_loan_date,
    get_unallocated_for_loan_date,
)
from .cash_gl import _merge_cash_gl_into_payload
from .loan_approval_gl_guard import require_loan_approval_gl_before_repayment
from .daily_state import save_loan_daily_state
from .db import RealDictCursor, _connection
from .product_catalog import load_system_config_from_db
from .serialization import _date_conv
from .unapplied_recast import _credit_unapplied_funds
from .unapplied_refs import _repayment_journal_reference, _unapplied_original_reference
from .waterfall_core import _get_waterfall_config, compute_waterfall_allocation

_logger = logging.getLogger(__name__)


def _trace_enabled() -> bool:
    return os.environ.get("FARNDACRED_TRACE_TELLER", "").strip().lower() in ("1", "true", "yes", "on")


def allocate_repayment_waterfall(
    repayment_id: int,
    *,
    as_of: date | None = None,
    system_config: dict | None = None,
    preloaded_balances: dict | None = None,
    event_type: str = "new_allocation",
    conn=None,
    skip_loan_approval_guard: bool = False,
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
        from eod.system_business_date import get_effective_date

        as_of = get_effective_date()

    cfg = system_config or load_system_config_from_db() or {}
    profile_key, bucket_order = _get_waterfall_config(cfg)

    def _run(_conn) -> None:
        t_gl_s = 0.0
        with _conn.cursor(cursor_factory=RealDictCursor) as cur:
            t_wall0 = time.perf_counter()
            t_fetch0 = time.perf_counter()
            cur.execute(
                """
                SELECT lr.id, lr.loan_id, lr.amount, lr.source_cash_gl_account_id,
                       COALESCE(lr.value_date, lr.payment_date) AS eff_date
                FROM loan_repayments lr
                WHERE lr.id = %s
                """,
                (repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Repayment {repayment_id} not found.")
            t_fetch_s = time.perf_counter() - t_fetch0

            amount = float(row["amount"])
            if amount <= 0:
                # Negative repayments (reversals) must never get new_allocation.
                # They get unallocation_parent_reversed at creation (reverse_repayment)
                # or via clear script's fix-reversed section.
                return

            loan_id = int(row["loan_id"])
            if not skip_loan_approval_guard:
                require_loan_approval_gl_before_repayment(loan_id, conn=_conn)
            eff_date = row["eff_date"] or as_of
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            # Always source balances from exact-date persisted state; ignore preloaded snapshots.
            _ = preloaded_balances  # backward compatibility only
            from eod.core import run_single_loan_eod

            # Ensure prior calendar day state exists (closing = opening for eff_date).
            prev_cal = eff_date - timedelta(days=1)
            t_prev0 = time.perf_counter()
            cur.execute(
                "SELECT 1 FROM loan_daily_state WHERE loan_id = %s AND as_of_date = %s LIMIT 1",
                (loan_id, prev_cal),
            )
            if cur.fetchone() is None:
                run_single_loan_eod(loan_id, prev_cal, sys_cfg=system_config)
            t_prev_s = time.perf_counter() - t_prev0

            # Serialize same-loan allocation (multiple receipts / concurrent saves).
            t_lock0 = time.perf_counter()
            cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (4021001, loan_id))
            t_lock_s = time.perf_counter() - t_lock0

            t_open0 = time.perf_counter()
            balances, st_prev, days_overdue = _get_opening_balances_for_repayment(
                cur, loan_id, eff_date, repayment_id,
            )
            t_open_s = time.perf_counter() - t_open0
            state_as_of = eff_date

            t_alloc0 = time.perf_counter()
            alloc, unapplied = compute_waterfall_allocation(
                amount, balances, bucket_order, profile_key,
                state_as_of=state_as_of, repayment_id=repayment_id,
            )
            t_alloc_s = time.perf_counter() - t_alloc0

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
            remaining_arrears = float(
                as_10dp(
                    max(0.0, balances.get("interest_arrears_balance", 0.0) - alloc_interest_arrears)
                    + max(0.0, balances.get("default_interest_balance", 0.0) - alloc_default_interest)
                    + max(0.0, balances.get("penalty_interest_balance", 0.0) - alloc_penalty_interest)
                    + max(0.0, balances.get("principal_arrears", 0.0) - alloc_principal_arrears)
                    + max(0.0, balances.get("fees_charges_balance", 0.0) - alloc_fees_charges)
                )
            )
            if unapplied > 1e-6 and remaining_arrears > 1e-6:
                raise ValueError(
                    f"Policy violation for repayment {repayment_id}: unapplied={unapplied} while "
                    f"arrears still outstanding={remaining_arrears}."
                )
            total_alloc = alloc_principal_total + alloc_interest_total + alloc_fees_total
            if abs((total_alloc + unapplied) - amount) > 0.01:
                raise ValueError(
                    f"Allocation mismatch for repayment {repayment_id}: "
                    f"amount={amount}, allocated={total_alloc}, unapplied={unapplied}"
                )

            cur2 = _conn.cursor()
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
            if unapplied > 1e-6:
                _credit_unapplied_funds(_conn, loan_id, repayment_id, unapplied, eff_date)

            t_gl0 = time.perf_counter()
            try:
                from accounting.service import AccountingService

                svc = AccountingService()

                # Resolve cash GL once per receipt (avoids N extra DB connections from
                # AccountingService.post_event / _merge_cash_gl_into_payload per leg).
                cash_ao: dict[str, str] = {}
                sc = row.get("source_cash_gl_account_id")
                if sc:
                    cash_ao = {"cash_operating": str(sc).strip()}
                else:
                    cur.execute(
                        "SELECT cash_gl_account_id FROM loans WHERE id = %s",
                        (loan_id,),
                    )
                    rloan = cur.fetchone()
                    if rloan and rloan.get("cash_gl_account_id"):
                        cash_ao = {"cash_operating": str(rloan["cash_gl_account_id"]).strip()}
                    else:
                        merged = _merge_cash_gl_into_payload(loan_id, repayment_id, {})
                        mo = merged.get("account_overrides") or {}
                        co = mo.get("cash_operating")
                        if co:
                            cash_ao = {"cash_operating": str(co).strip()}

                def _gl_payload(tag_amounts: dict) -> dict:
                    p = dict(tag_amounts)
                    if cash_ao:
                        p["account_overrides"] = dict(cash_ao)
                    return p

                _rj = _repayment_journal_reference(loan_id, repayment_id)
                eff_date_gl = _date_conv(eff_date) or (
                    eff_date.date() if isinstance(eff_date, datetime) else eff_date
                )
                if hasattr(eff_date_gl, "date") and not isinstance(eff_date_gl, date):
                    eff_date_gl = eff_date_gl.date()

                gl_items: list[dict] = []

                if unapplied > 1e-6:
                    gl_items.append(
                        {
                            "event_type": "UNAPPLIED_FUNDS_OVERPAYMENT",
                            "reference": _unapplied_original_reference(
                                "credit",
                                loan_id=loan_id,
                                repayment_id=repayment_id,
                                value_date=eff_date,
                            ),
                            "description": "Unapplied funds on overpayment",
                            "event_id": _unapplied_original_reference(
                                "credit",
                                loan_id=loan_id,
                                repayment_id=repayment_id,
                                value_date=eff_date,
                            ),
                            "created_by": "system",
                            "entry_date": eff_date_gl,
                            "amount": Decimal(str(unapplied)),
                            "payload": _gl_payload({}),
                            "loan_id": loan_id,
                            "repayment_id": repayment_id,
                        }
                    )
                if alloc_principal_arrears > 0:
                    p = Decimal(str(alloc_principal_arrears))
                    gl_items.append(
                        {
                            "event_type": "PAYMENT_PRINCIPAL",
                            "reference": _rj,
                            "description": f"Principal (arrears) — {_rj}",
                            "event_id": f"REPAY-{repayment_id}-PRIN-ARR",
                            "created_by": "system",
                            "entry_date": eff_date_gl,
                            "payload": _gl_payload(
                                {"cash_operating": p, "principal_arrears": p}
                            ),
                            "loan_id": loan_id,
                            "repayment_id": repayment_id,
                        }
                    )
                if alloc_principal_not_due > 0:
                    p = Decimal(str(alloc_principal_not_due))
                    gl_items.append(
                        {
                            "event_type": "PAYMENT_PRINCIPAL_NOT_YET_DUE",
                            "reference": _rj,
                            "description": f"Principal (not yet due) — {_rj}",
                            "event_id": f"REPAY-{repayment_id}-PRIN-NYD",
                            "created_by": "system",
                            "entry_date": eff_date_gl,
                            "payload": _gl_payload({"cash_operating": p, "loan_principal": p}),
                            "loan_id": loan_id,
                            "repayment_id": repayment_id,
                        }
                    )
                if alloc_interest_arrears > 0:
                    p = Decimal(str(alloc_interest_arrears))
                    gl_items.append(
                        {
                            "event_type": "PAYMENT_REGULAR_INTEREST",
                            "reference": _rj,
                            "description": f"Interest (arrears) — {_rj}",
                            "event_id": f"REPAY-{repayment_id}-INT-ARR",
                            "created_by": "system",
                            "entry_date": eff_date_gl,
                            "payload": _gl_payload(
                                {"cash_operating": p, "regular_interest_arrears": p}
                            ),
                            "loan_id": loan_id,
                            "repayment_id": repayment_id,
                        }
                    )
                if alloc_interest_accrued > 0:
                    p = Decimal(str(alloc_interest_accrued))
                    gl_items.append(
                        {
                            "event_type": "PAYMENT_REGULAR_INTEREST_NOT_YET_DUE",
                            "reference": _rj,
                            "description": f"Interest (accrued / not billed) — {_rj}",
                            "event_id": f"REPAY-{repayment_id}-INT-ACC",
                            "created_by": "system",
                            "entry_date": eff_date_gl,
                            "payload": _gl_payload(
                                {"cash_operating": p, "regular_interest_accrued": p}
                            ),
                            "loan_id": loan_id,
                            "repayment_id": repayment_id,
                        }
                    )
                if alloc_penalty_interest > 0:
                    p = Decimal(str(alloc_penalty_interest))
                    gl_items.append(
                        {
                            "event_type": "PAYMENT_PENALTY_INTEREST",
                            "reference": _rj,
                            "description": f"Penalty interest — {_rj}",
                            "event_id": f"REPAY-{repayment_id}-PEN",
                            "created_by": "system",
                            "entry_date": eff_date_gl,
                            "payload": _gl_payload(
                                {
                                    "cash_operating": p,
                                    "penalty_interest_asset": p,
                                    "penalty_interest_suspense": p,
                                    "penalty_interest_income": p,
                                }
                            ),
                            "loan_id": loan_id,
                            "repayment_id": repayment_id,
                        }
                    )
                if alloc_default_interest > 0:
                    p = Decimal(str(alloc_default_interest))
                    gl_items.append(
                        {
                            "event_type": "PAYMENT_DEFAULT_INTEREST",
                            "reference": _rj,
                            "description": f"Default interest — {_rj}",
                            "event_id": f"REPAY-{repayment_id}-DEF",
                            "created_by": "system",
                            "entry_date": eff_date_gl,
                            "payload": _gl_payload(
                                {
                                    "cash_operating": p,
                                    "default_interest_asset": p,
                                    "default_interest_suspense": p,
                                    "default_interest_income": p,
                                }
                            ),
                            "loan_id": loan_id,
                            "repayment_id": repayment_id,
                        }
                    )

                if gl_items:
                    svc.bulk_post_events(gl_items)

            except Exception as e:
                _logger.warning(
                    "Failed to post repayment journals for repayment_id=%s: %s",
                    repayment_id,
                    e,
                )
            t_gl_s = time.perf_counter() - t_gl0

            new_interest_accrued = max(0.0, balances["interest_accrued_balance"] - alloc_interest_accrued)
            new_interest_arrears = max(0.0, balances["interest_arrears_balance"] - alloc_interest_arrears)
            new_principal_not_due = max(0.0, balances["principal_not_due"] - alloc_principal_not_due)
            new_principal_arrears = max(0.0, balances["principal_arrears"] - alloc_principal_arrears)
            new_default_interest = max(0.0, balances["default_interest_balance"] - alloc_default_interest)
            new_penalty_interest = max(0.0, balances["penalty_interest_balance"] - alloc_penalty_interest)
            new_fees_charges = max(0.0, balances["fees_charges_balance"] - alloc_fees_charges)
            new_reg_susp = max(
                0.0,
                float(
                    as_10dp(
                        float(balances.get("regular_interest_in_suspense_balance", 0) or 0)
                        - alloc_interest_accrued
                    )
                ),
            )
            new_pen_susp = max(0.0, float(as_10dp(new_penalty_interest)))
            new_def_susp = max(0.0, float(as_10dp(new_default_interest)))

            # Use daily/period columns from **closing(eff_date-1)** (st_prev). Intraday EOD accrual for
            # eff_date is not applied before allocation; night EOD will refresh this row.
            # Never recalculate daily columns from post-allocation balances: doing so breaks the
            # bucket identity (opening + daily - alloc = closing) and inflates statement charges.
            _sp = st_prev or {}
            reg_daily = float(_sp.get("regular_interest_daily", 0) or 0)
            pen_daily = float(_sp.get("penalty_interest_daily", 0) or 0)
            def_daily = float(_sp.get("default_interest_daily", 0) or 0)
            reg_period = float(_sp.get("regular_interest_period_to_date", 0) or 0)
            pen_period = float(_sp.get("penalty_interest_period_to_date", 0) or 0)
            def_period = float(_sp.get("default_interest_period_to_date", 0) or 0)

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
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date_val, conn=_conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date_val, conn=_conn)

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
                    total_delinquency_arrears = %s,
                    total_exposure = %s,
                    regular_interest_period_to_date = %s,
                    penalty_interest_period_to_date = %s,
                    default_interest_period_to_date = %s,
                    net_allocation = %s,
                    unallocated = %s,
                    regular_interest_in_suspense_balance = %s,
                    penalty_interest_in_suspense_balance = %s,
                    default_interest_in_suspense_balance = %s,
                    total_interest_in_suspense_balance = %s
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
                    float(as_10dp(new_principal_arrears + new_interest_arrears + new_default_interest + new_penalty_interest + new_fees_charges)),
                    total_exposure,
                    reg_period,
                    pen_period,
                    def_period,
                    net_alloc,
                    unalloc,
                    new_reg_susp,
                    new_pen_susp,
                    new_def_susp,
                    float(
                        as_10dp(
                            Decimal(str(new_reg_susp))
                            + Decimal(str(new_pen_susp))
                            + Decimal(str(new_def_susp))
                        )
                    ),
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
                    regular_interest_in_suspense_balance=new_reg_susp,
                    penalty_interest_in_suspense_balance=new_pen_susp,
                    default_interest_in_suspense_balance=new_def_susp,
                    conn=_conn,
                )
            cur2.close()

            if _trace_enabled():
                wall_s = time.perf_counter() - t_wall0
                _logger.info(
                    "TRACE allocate_repayment_waterfall repayment_id=%s loan_id=%s eff_date=%s amount=%.2f "
                    "fetch_s=%.3f prev_eod_s=%.3f lock_s=%.3f opening_s=%.3f alloc_compute_s=%.3f gl_bulk_s=%.3f "
                    "wall_s=%.3f",
                    repayment_id,
                    loan_id,
                    eff_date.isoformat() if hasattr(eff_date, "isoformat") else str(eff_date),
                    float(amount),
                    float(t_fetch_s),
                    float(t_prev_s),
                    float(t_lock_s),
                    float(t_open_s),
                    float(t_alloc_s),
                    float(t_gl_s),
                    float(wall_s),
                )

    if conn is not None:
        _run(conn)
        return
    with _connection() as _conn:
        _run(_conn)
