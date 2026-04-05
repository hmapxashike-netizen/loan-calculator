"""
Unapplied-funded loan recast: validations, liquidation allocation + GL, new schedule, audit row, EOD replay.

Uses UNAPPLIED_LIQUIDATION_* journals (suspense-funded), same subledger shapes as EOD unapplied apply.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

from decimal_utils import as_10dp

from .allocation_queries import (
    _get_opening_balances_for_repayment,
    get_net_allocation_for_loan_date,
    get_unallocated_for_loan_date,
)
from .cash_gl import _post_event_for_loan
from .db import RealDictCursor, _connection
from .loan_records import get_loan, update_loan_details
from .product_catalog import load_system_config_from_db
from .schedules import get_latest_schedule_version, save_new_schedule_version
from .serialization import _date_conv
from .unapplied_refs import _unapplied_original_reference
from .waterfall_core import BUCKET_TO_ALLOC

ARREARS_ZERO_TOLERANCE = 1e-6

# Phase 1: fees / penalty / default / interest arrears / accrued (unbilled)
_RECAST_PHASE1: tuple[str, ...] = (
    "fees_charges_balance",
    "penalty_interest_balance",
    "default_interest_balance",
    "interest_arrears_balance",
    "interest_accrued_balance",
)
_RECAST_PHASE2: tuple[str, ...] = ("principal_arrears", "principal_not_due")

RecastMode = Literal["maintain_term", "maintain_instalment"]


def _same_calendar_month(a: date, b: date) -> bool:
    return a.year == b.year and a.month == b.month


def validate_recast_effective_date(*, recast_effective_date: date, system_business_date: date) -> None:
    if not _same_calendar_month(recast_effective_date, system_business_date):
        raise ValueError(
            "Recast effective date must fall in the same calendar month as the system business date."
        )


def _delinquency_total(balances: dict[str, float]) -> float:
    return (
        float(balances.get("principal_arrears") or 0)
        + float(balances.get("interest_arrears_balance") or 0)
        + float(balances.get("default_interest_balance") or 0)
        + float(balances.get("penalty_interest_balance") or 0)
        + float(balances.get("fees_charges_balance") or 0)
    )


def compute_recast_unapplied_allocation(
    unapplied_amount: float,
    balances: dict[str, float],
) -> tuple[dict[str, float], float]:
    """
    Allocate unapplied across recast phases. Returns (alloc_* dict, unused_remainder).
    """
    work: dict[str, float] = {}
    for _bk, (_ak, sk) in BUCKET_TO_ALLOC.items():
        work[sk] = max(0.0, float(balances.get(sk, 0) or 0))

    alloc: dict[str, float] = {
        "alloc_principal_not_due": 0.0,
        "alloc_principal_arrears": 0.0,
        "alloc_interest_accrued": 0.0,
        "alloc_interest_arrears": 0.0,
        "alloc_default_interest": 0.0,
        "alloc_penalty_interest": 0.0,
        "alloc_fees_charges": 0.0,
    }
    remaining = float(unapplied_amount)
    for bucket in _RECAST_PHASE1 + _RECAST_PHASE2:
        _alloc_key, state_key = BUCKET_TO_ALLOC[bucket]
        take = min(remaining, work.get(state_key, 0.0))
        alloc[_alloc_key] = float(as_10dp(alloc.get(_alloc_key, 0.0) + take))
        remaining = float(as_10dp(remaining - take))
        work[state_key] = float(as_10dp(work.get(state_key, 0.0) - take))
        if remaining <= ARREARS_ZERO_TOLERANCE:
            remaining = 0.0
            break
    unused = float(as_10dp(remaining)) if remaining > ARREARS_ZERO_TOLERANCE else 0.0
    return alloc, unused


def _post_liquidation_gl(
    svc: Any,
    loan_id: int,
    src_repayment_id: int,
    eff_date: date,
    liq_ref: str,
    *,
    apr: float,
    apa: float,
    aia: float,
    aiar: float,
    adi: float,
    api: float,
    afc: float,
) -> None:
    if apr > 1e-6:
        _post_event_for_loan(
            svc,
            loan_id,
            repayment_id=src_repayment_id,
            event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_NOT_YET_DUE",
            reference=liq_ref,
            description=f"Recast unapplied liquidation: principal not yet due ({liq_ref})",
            event_id=f"{liq_ref}-pnd",
            created_by="system",
            entry_date=eff_date,
            amount=Decimal(str(apr)),
        )
    if apa > 1e-6:
        _post_event_for_loan(
            svc,
            loan_id,
            repayment_id=src_repayment_id,
            event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS",
            reference=liq_ref,
            description=f"Recast unapplied liquidation: principal arrears ({liq_ref})",
            event_id=f"{liq_ref}-pa",
            created_by="system",
            entry_date=eff_date,
            amount=Decimal(str(apa)),
        )
    if aia > 1e-6:
        _post_event_for_loan(
            svc,
            loan_id,
            repayment_id=src_repayment_id,
            event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST_NOT_YET_DUE",
            reference=liq_ref,
            description=f"Recast unapplied liquidation: interest accrued ({liq_ref})",
            event_id=f"{liq_ref}-ia",
            created_by="system",
            entry_date=eff_date,
            amount=Decimal(str(aia)),
        )
    if aiar > 1e-6:
        _post_event_for_loan(
            svc,
            loan_id,
            repayment_id=src_repayment_id,
            event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST",
            reference=liq_ref,
            description=f"Recast unapplied liquidation: interest arrears ({liq_ref})",
            event_id=f"{liq_ref}-iar",
            created_by="system",
            entry_date=eff_date,
            amount=Decimal(str(aiar)),
        )
    if adi > 1e-6:
        _post_event_for_loan(
            svc,
            loan_id,
            repayment_id=src_repayment_id,
            event_type="UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST",
            reference=liq_ref,
            description=f"Recast unapplied liquidation: default interest ({liq_ref})",
            event_id=f"{liq_ref}-def",
            created_by="system",
            entry_date=eff_date,
            amount=Decimal(str(adi)),
        )
    if api > 1e-6:
        _post_event_for_loan(
            svc,
            loan_id,
            repayment_id=src_repayment_id,
            event_type="UNAPPLIED_LIQUIDATION_PENALTY_INTEREST",
            reference=liq_ref,
            description=f"Recast unapplied liquidation: penalty interest ({liq_ref})",
            event_id=f"{liq_ref}-pen",
            created_by="system",
            entry_date=eff_date,
            amount=Decimal(str(api)),
        )
    if afc > 1e-6:
        _post_event_for_loan(
            svc,
            loan_id,
            repayment_id=src_repayment_id,
            event_type="UNAPPLIED_LIQUIDATION_PASS_THROUGH_COST_RECOVERY",
            reference=liq_ref,
            description=f"Recast unapplied liquidation: fees ({liq_ref})",
            event_id=f"{liq_ref}-fees",
            created_by="system",
            entry_date=eff_date,
            amount=Decimal(str(afc)),
        )


def _next_repayment_id_for_preview(cur) -> int:
    cur.execute("SELECT COALESCE(MAX(id), 0) + 1 AS n FROM loan_repayments")
    row = cur.fetchone()
    return int(row["n"] if isinstance(row, dict) else row[0])


def _ensure_loan_daily_state_through_recast_effective_date(
    loan_id: int,
    recast_effective_date: date,
    *,
    system_business_date: date,
    cfg: dict[str, Any],
) -> None:
    """
    Replay EOD for [recast_effective_date - 1 day, recast_effective_date] inclusive so
    loan_daily_state has a row for the recast effective date (execute UPDATE targets that row).
    """
    from eod.core import run_single_loan_eod_date_range

    prev_cal = recast_effective_date - timedelta(days=1)
    ok, err = run_single_loan_eod_date_range(
        loan_id,
        prev_cal,
        recast_effective_date,
        sys_cfg=cfg,
        allow_system_date_eod=(recast_effective_date >= system_business_date),
    )
    if not ok:
        raise ValueError(err or "Failed to refresh loan_daily_state through the recast effective date.")


def preview_recast_from_unapplied(
    loan_id: int,
    recast_effective_date: date,
    unapplied_funds_id: int,
    mode: RecastMode,
    *,
    system_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Read-only preview: validations, post-liquidation principal, schedule DataFrame, new instalment.
    """
    from eod.system_business_date import get_effective_date

    sys_d = get_effective_date()
    validate_recast_effective_date(recast_effective_date=recast_effective_date, system_business_date=sys_d)
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    lt = str(loan.get("loan_type") or "")
    if lt not in ("term_loan", "consumer_loan"):
        raise ValueError("Recast from unapplied is only supported for term_loan and consumer_loan.")

    cfg = system_config or load_system_config_from_db() or {}
    _ensure_loan_daily_state_through_recast_effective_date(
        loan_id,
        recast_effective_date,
        system_business_date=sys_d,
        cfg=cfg,
    )

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, amount, value_date, repayment_id, entry_type
                FROM unapplied_funds
                WHERE id = %s
                """,
                (unapplied_funds_id,),
            )
            uf = cur.fetchone()
            if not uf:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} not found.")
            if int(uf["loan_id"]) != int(loan_id):
                raise ValueError("Unapplied entry does not belong to this loan.")
            et = (uf.get("entry_type") or "credit").lower()
            if et != "credit" or float(uf.get("amount") or 0) <= 0:
                raise ValueError("Select a positive unapplied credit entry.")
            cur.execute(
                "SELECT 1 FROM unapplied_funds WHERE source_unapplied_id = %s LIMIT 1",
                (unapplied_funds_id,),
            )
            if cur.fetchone():
                raise ValueError("This unapplied entry was already consumed.")
            src_rid = uf.get("repayment_id")
            if src_rid is None:
                raise ValueError("Unapplied entry must have a source repayment_id.")
            src_rid = int(src_rid)
            vd = uf["value_date"]
            if hasattr(vd, "date"):
                vd = vd.date()
            if vd > recast_effective_date:
                raise ValueError("Unapplied value_date cannot be after the recast effective date.")

            fake_rid = _next_repayment_id_for_preview(cur)
            balances, _st_prev, _days = _get_opening_balances_for_repayment(
                cur, loan_id, recast_effective_date, fake_rid
            )

    if _delinquency_total(balances) > ARREARS_ZERO_TOLERANCE:
        raise ValueError(
            "Delinquency balances must be zero before recast (principal arrears, interest arrears, "
            "penalty, default interest, fees). Clear arrears first, then retry."
        )
    uf_amt = float(uf["amount"])
    accrued = float(balances.get("interest_accrued_balance") or 0)
    if uf_amt <= accrued + ARREARS_ZERO_TOLERANCE:
        raise ValueError(
            "Unapplied amount must exceed accrued unbilled interest; otherwise there is no principal "
            "reduction—leave funds in unapplied until the next due date."
        )

    alloc, unused = compute_recast_unapplied_allocation(uf_amt, balances)
    applied = float(as_10dp(uf_amt - unused))
    if applied <= ARREARS_ZERO_TOLERANCE:
        raise ValueError("Nothing to apply from unapplied after allocation.")

    new_principal = float(
        as_10dp(
            max(0.0, balances["principal_not_due"] - alloc["alloc_principal_not_due"])
            + max(0.0, balances["principal_arrears"] - alloc["alloc_principal_arrears"])
        )
    )
    if new_principal <= 0:
        raise ValueError("Recast would leave no positive principal; aborting.")

    from reamortisation import build_recast_schedule_for_mode

    df, new_inst = build_recast_schedule_for_mode(
        loan_id,
        recast_effective_date,
        new_principal,
        mode,
    )
    return {
        "schedule_df": df,
        "new_installment": new_inst,
        "new_principal_balance": new_principal,
        "unapplied_applied": applied,
        "unapplied_unused_remainder": unused,
        "allocation": alloc,
    }


def execute_recast_from_unapplied(
    loan_id: int,
    recast_effective_date: date,
    unapplied_funds_id: int,
    mode: RecastMode,
    *,
    system_config: dict[str, Any] | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    """
    Run full recast: liquidation + schedule + loan_recasts audit + EOD replay through system date.
    """
    from eod.core import run_single_loan_eod
    from eod.system_business_date import get_effective_date

    sys_d = get_effective_date()
    validate_recast_effective_date(recast_effective_date=recast_effective_date, system_business_date=sys_d)
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    lt = str(loan.get("loan_type") or "")
    if lt not in ("term_loan", "consumer_loan"):
        raise ValueError("Recast from unapplied is only supported for term_loan and consumer_loan.")

    cfg = system_config or load_system_config_from_db() or {}
    _ensure_loan_daily_state_through_recast_effective_date(
        loan_id,
        recast_effective_date,
        system_business_date=sys_d,
        cfg=cfg,
    )

    try:
        from accounting.service import AccountingService

        svc = AccountingService()
    except Exception:
        svc = None

    prev_principal = float(as_10dp(loan.get("principal") or 0))
    prev_installment = float(as_10dp(loan.get("installment") or 0))
    prev_end = loan.get("end_date")
    if hasattr(prev_end, "date"):
        prev_end = prev_end.date()
    prev_version = get_latest_schedule_version(loan_id)

    eff_date_val = _date_conv(recast_effective_date) or recast_effective_date

    tx_out: dict[str, Any] = {}

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (4021001, loan_id))

            cur.execute(
                """
                SELECT id, loan_id, amount, value_date, repayment_id, entry_type
                FROM unapplied_funds
                WHERE id = %s
                FOR UPDATE
                """,
                (unapplied_funds_id,),
            )
            uf = cur.fetchone()
            if not uf:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} not found.")
            if int(uf["loan_id"]) != int(loan_id):
                raise ValueError("Unapplied entry does not belong to this loan.")
            et = (uf.get("entry_type") or "credit").lower()
            if et != "credit" or float(uf.get("amount") or 0) <= 0:
                raise ValueError("Select a positive unapplied credit entry.")
            cur.execute(
                "SELECT 1 FROM unapplied_funds WHERE source_unapplied_id = %s LIMIT 1",
                (unapplied_funds_id,),
            )
            if cur.fetchone():
                raise ValueError("This unapplied entry was already consumed.")
            src_rid = int(uf["repayment_id"])
            vd = uf["value_date"]
            if hasattr(vd, "date"):
                vd = vd.date()
            if vd > recast_effective_date:
                raise ValueError("Unapplied value_date cannot be after the recast effective date.")

            cur.execute(
                """
                INSERT INTO loan_repayments (
                    loan_id, amount, payment_date, reference, value_date, status
                ) VALUES (%s, %s, %s, %s, %s, 'posted')
                RETURNING id
                """,
                (
                    loan_id,
                    -0.01,
                    recast_effective_date,
                    "Loan recast (unapplied liquidation)",
                    recast_effective_date,
                ),
            )
            liq_rid = int(cur.fetchone()["id"])

            balances, st_prev, days_overdue = _get_opening_balances_for_repayment(
                cur, loan_id, recast_effective_date, liq_rid
            )

            if _delinquency_total(balances) > ARREARS_ZERO_TOLERANCE:
                raise ValueError(
                    "Delinquency balances must be zero before recast. Clear arrears first, then retry."
                )

            uf_amt = float(uf["amount"])
            accrued = float(balances.get("interest_accrued_balance") or 0)
            if uf_amt <= accrued + ARREARS_ZERO_TOLERANCE:
                raise ValueError(
                    "Unapplied amount must exceed accrued unbilled interest; otherwise there is no "
                    "principal reduction."
                )

            alloc, unused = compute_recast_unapplied_allocation(uf_amt, balances)
            applied = float(as_10dp(uf_amt - unused))
            if applied <= ARREARS_ZERO_TOLERANCE:
                raise ValueError("Nothing to apply from unapplied after allocation.")

            cur.execute(
                "UPDATE loan_repayments SET amount = %s WHERE id = %s",
                (float(as_10dp(-applied)), liq_rid),
            )

            apr = alloc["alloc_principal_not_due"]
            apa = alloc["alloc_principal_arrears"]
            aia = alloc["alloc_interest_accrued"]
            aiar = alloc["alloc_interest_arrears"]
            adi = alloc["alloc_default_interest"]
            api = alloc["alloc_penalty_interest"]
            afc = alloc["alloc_fees_charges"]

            alloc_principal_total = apr + apa
            alloc_interest_total = aia + aiar + adi + api
            alloc_fees_total = afc
            alloc_total = alloc_principal_total + alloc_interest_total + alloc_fees_total
            if abs(float(as_10dp(alloc_total)) - float(as_10dp(applied))) > 0.02:
                raise ValueError("Internal error: liquidation total does not match applied unapplied amount.")

            cur.execute(
                """
                INSERT INTO loan_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total,
                    alloc_total, unallocated,
                    event_type,
                    source_repayment_id
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'unapplied_funds_allocation', %s
                )
                """,
                (
                    liq_rid,
                    float(as_10dp(apr)),
                    float(as_10dp(apa)),
                    float(as_10dp(aia)),
                    float(as_10dp(aiar)),
                    float(as_10dp(adi)),
                    float(as_10dp(api)),
                    float(as_10dp(afc)),
                    float(as_10dp(alloc_principal_total)),
                    float(as_10dp(alloc_interest_total)),
                    float(as_10dp(alloc_fees_total)),
                    float(as_10dp(alloc_total)),
                    0.0,
                    src_rid,
                ),
            )

            new_interest_accrued = max(0.0, balances["interest_accrued_balance"] - aia)
            new_interest_arrears = max(0.0, balances["interest_arrears_balance"] - aiar)
            new_principal_not_due = max(0.0, balances["principal_not_due"] - apr)
            new_principal_arrears = max(0.0, balances["principal_arrears"] - apa)
            new_default_interest = max(0.0, balances["default_interest_balance"] - adi)
            new_penalty_interest = max(0.0, balances["penalty_interest_balance"] - api)
            new_fees_charges = max(0.0, balances["fees_charges_balance"] - afc)
            open_reg_susp = float(balances.get("regular_interest_in_suspense_balance", 0) or 0)
            new_reg_susp = max(0.0, float(as_10dp(open_reg_susp - aia)))
            new_pen_susp = max(0.0, float(as_10dp(new_penalty_interest)))
            new_def_susp = max(0.0, float(as_10dp(new_default_interest)))

            _sp = st_prev or {}
            reg_daily = float(_sp.get("regular_interest_daily", 0) or 0)
            pen_daily = float(_sp.get("penalty_interest_daily", 0) or 0)
            def_daily = float(_sp.get("default_interest_daily", 0) or 0)
            reg_period = float(_sp.get("regular_interest_period_to_date", 0) or 0)
            pen_period = float(_sp.get("penalty_interest_period_to_date", 0) or 0)
            def_period = float(_sp.get("default_interest_period_to_date", 0) or 0)

            if (
                new_interest_arrears + new_default_interest + new_penalty_interest + new_principal_arrears
                <= ARREARS_ZERO_TOLERANCE
            ):
                days_overdue = 0

            total_exposure = (
                new_principal_not_due
                + new_principal_arrears
                + new_interest_accrued
                + new_interest_arrears
                + new_default_interest
                + new_penalty_interest
                + new_fees_charges
            )
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date_val, conn=conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date_val, conn=conn)

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
                    float(
                        as_10dp(
                            new_principal_arrears
                            + new_interest_arrears
                            + new_default_interest
                            + new_penalty_interest
                            + new_fees_charges
                        )
                    ),
                    float(as_10dp(total_exposure)),
                    reg_period,
                    pen_period,
                    def_period,
                    net_alloc,
                    unalloc,
                    new_reg_susp,
                    new_pen_susp,
                    new_def_susp,
                    float(as_10dp(new_reg_susp + new_pen_susp + new_def_susp)),
                    loan_id,
                    eff_date_val,
                ),
            )
            if cur.rowcount == 0:
                raise ValueError(
                    f"No loan_daily_state row for loan_id={loan_id} on {eff_date_val}. "
                    "Run EOD through the recast effective date first (opening replay may have failed or the loan has no schedule for that day)."
                )

            cur.execute(
                """
                INSERT INTO unapplied_funds (
                    loan_id, amount, value_date, entry_type, reference,
                    allocation_repayment_id, source_repayment_id, source_unapplied_id, currency
                )
                VALUES (%s, %s, %s, 'debit', 'Loan recast (unapplied)', %s, %s, %s, 'USD')
                """,
                (
                    loan_id,
                    float(as_10dp(-applied)),
                    eff_date_val,
                    liq_rid,
                    src_rid,
                    unapplied_funds_id,
                ),
            )

            if unused > ARREARS_ZERO_TOLERANCE:
                cur.execute(
                    """
                    INSERT INTO unapplied_funds (
                        loan_id, amount, value_date, entry_type, reference,
                        repayment_id, currency
                    )
                    VALUES (%s, %s, %s, 'credit', 'Recast remainder (excess over balances)', %s, 'USD')
                    """,
                    (
                        loan_id,
                        float(as_10dp(unused)),
                        eff_date_val,
                        src_rid,
                    ),
                )

            if svc is not None:
                liq_ref = _unapplied_original_reference(
                    "recast_liquidation",
                    loan_id=loan_id,
                    repayment_id=src_rid,
                    value_date=eff_date_val,
                )
                _post_liquidation_gl(
                    svc,
                    loan_id,
                    src_rid,
                    eff_date_val,
                    liq_ref,
                    apr=apr,
                    apa=apa,
                    aia=aia,
                    aiar=aiar,
                    adi=adi,
                    api=api,
                    afc=afc,
                )

            new_principal = float(as_10dp(new_principal_not_due + new_principal_arrears))
            if new_principal <= 0:
                raise ValueError("Recast would leave no positive principal; aborting.")

            tx_out["liq_rid"] = liq_rid
            tx_out["src_rid"] = src_rid
            tx_out["applied"] = applied
            tx_out["unused"] = unused
            tx_out["new_principal"] = new_principal

    liq_rid = int(tx_out["liq_rid"])
    src_rid = int(tx_out["src_rid"])
    applied = float(tx_out["applied"])
    unused = float(tx_out["unused"])
    new_principal = float(tx_out["new_principal"])

    from reamortisation import build_recast_schedule_for_mode

    schedule_df, new_installment = build_recast_schedule_for_mode(
        loan_id,
        recast_effective_date,
        new_principal,
        mode,
    )
    new_version = prev_version + 1
    save_new_schedule_version(loan_id, schedule_df, new_version)

    end_d = _last_schedule_due_date(schedule_df)
    upd: dict[str, Any] = {
        "principal": round(new_principal, 2),
        "installment": round(float(as_10dp(new_installment)), 2),
    }
    if end_d is not None:
        upd["end_date"] = end_d
    update_loan_details(loan_id, **upd)

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_recasts (
                    loan_id, recast_date, previous_schedule_version, new_schedule_version,
                    new_installment, trigger_repayment_id, notes,
                    recast_mode, previous_principal, previous_installment, previous_end_date,
                    unapplied_credit_id, liquidation_repayment_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    loan_id,
                    recast_effective_date,
                    prev_version,
                    new_version,
                    round(float(as_10dp(new_installment)), 2),
                    src_rid,
                    notes,
                    mode,
                    round(prev_principal, 2),
                    round(prev_installment, 2),
                    prev_end,
                    unapplied_funds_id,
                    liq_rid,
                ),
            )

    d = recast_effective_date
    while d <= sys_d:
        run_single_loan_eod(loan_id, d, sys_cfg=cfg)
        d += timedelta(days=1)

    return {
        "new_installment": float(as_10dp(new_installment)),
        "new_principal_balance": new_principal,
        "new_schedule_version": new_version,
        "liquidation_repayment_id": liq_rid,
        "unapplied_applied": applied,
        "unapplied_unused_remainder": unused,
        "schedule_df": schedule_df,
    }


def _last_schedule_due_date(schedule_df: Any) -> date | None:
    """Best-effort maturity from schedule DataFrame (last non-zero period line)."""
    import math

    import pandas as pd

    if schedule_df is None or len(schedule_df) < 2:
        return None
    last_s: date | None = None
    for _, row in schedule_df.iloc[1:].iterrows():
        raw = row.get("Date") or row.get("date")
        if raw is None:
            continue
        if isinstance(raw, float) and math.isnan(raw):
            continue
        try:
            if pd.isna(raw):
                continue
        except (TypeError, ValueError):
            pass
        try:
            p = datetime.strptime(str(raw).strip()[:32], "%d-%b-%Y").date()
            last_s = p
        except (ValueError, TypeError):
            continue
    return last_s


def try_undo_recast_after_parent_receipt_reversed(cur, loan_id: int, source_repayment_id: int) -> bool:
    """
    If the latest schedule version was created by a recast tied to this teller receipt, delete that
    schedule version and restore loan headers from loan_recasts. Call inside same transaction as reversal.
    """
    cur.execute(
        """
        SELECT lr.id, lr.new_schedule_version, lr.previous_principal, lr.previous_installment,
               lr.previous_end_date
        FROM loan_recasts lr
        WHERE lr.loan_id = %s AND lr.trigger_repayment_id = %s
        ORDER BY lr.id DESC
        LIMIT 1
        """,
        (loan_id, source_repayment_id),
    )
    row = cur.fetchone()
    if not row:
        return False
    cur.execute(
        "SELECT COALESCE(MAX(version), 1) FROM loan_schedules WHERE loan_id = %s",
        (loan_id,),
    )
    max_v = int(cur.fetchone()[0] or 1)
    if int(row["new_schedule_version"]) != max_v:
        return False
    cur.execute(
        "DELETE FROM loan_schedules WHERE loan_id = %s AND version = %s",
        (loan_id, int(row["new_schedule_version"])),
    )
    pp = row.get("previous_principal")
    pi = row.get("previous_installment")
    pe = row.get("previous_end_date")
    if hasattr(pe, "date"):
        pe = pe.date()
    cur.execute(
        """
        UPDATE loans SET
            principal = COALESCE(%s, principal),
            installment = COALESCE(%s, installment),
            end_date = COALESCE(%s, end_date),
            updated_at = NOW()
        WHERE id = %s
        """,
        (pp, pi, pe, loan_id),
    )
    cur.execute("DELETE FROM loan_recasts WHERE id = %s", (int(row["id"]),))
    return True


def list_unapplied_credit_rows_for_recast(loan_id: int) -> list[dict[str, Any]]:
    """Positive unapplied credits for a loan that are not yet consumed by recast/other debits."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.loan_id, u.amount, u.value_date, u.repayment_id, u.reference, u.created_at
                FROM unapplied_funds u
                WHERE u.loan_id = %s
                  AND u.amount > 0
                  AND COALESCE(NULLIF(TRIM(u.entry_type), ''), 'credit') = 'credit'
                  AND u.repayment_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM unapplied_funds d
                      WHERE d.source_unapplied_id = u.id
                  )
                ORDER BY u.value_date, u.id
                """,
                (loan_id,),
            )
            return [dict(r) for r in cur.fetchall()]
