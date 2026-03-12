from __future__ import annotations

"""
End-of-day (EOD) processing for the Loan Management System.

High-level design
-----------------
- EOD is the *only* place where daily loan bucket movements and interest
  accruals are computed and persisted into `loan_daily_state`.
- It is designed to be:
  - **Idempotent**: running EOD for the same date multiple times is safe.
  - **Deterministic**: results depend only on loan contracts, schedules,
    repayments and configuration, not on run order.
  - **Scheduler-friendly**: you can trigger it manually from the UI or
    via an external scheduler (cron, Windows Task Scheduler, Airflow, etc.).

Security and scalability notes
------------------------------
- EOD does not expose raw SQL or shell commands to end-users.
- Any database backup integration is optional and kept separate so that
  organisations can plug in their own hardened backup processes (e.g.
  managed Postgres backups, filesystem snapshots, or pg_dump via an ops
  user account).
"""

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url
from decimal_utils import as_10dp
from loan_daily_engine import LoanConfig, ScheduleEntry, Loan
from loan_management import (
    get_allocation_totals_for_loan_date,
    get_net_allocation_for_loan_date,
    get_unallocated_for_loan_date,
    get_loan_daily_state_balances,
    get_loan_ids_with_reversed_receipts_on_date,
    get_loans_with_unapplied_balance,
    get_repayment_ids_for_loan_and_date,
    get_schedule_lines,
    reallocate_repayment,
    save_loan_daily_state,
    apply_unapplied_funds_to_arrears_eod,
    load_system_config_from_db,
    get_product_config_from_db,
    _get_waterfall_config,
    _log_allocation_audit,
)

# Treat balances below this as zero for "no arrears" and default/penalty zeroing (avoids float drift).
ARREARS_ZERO_TOLERANCE = 1e-6


def _effective_config_for_loan(loan_row: Dict[str, Any], sys_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Merge product config over system config for this loan so balance/quotation/default penalty % come from product."""
    effective_cfg = dict(sys_cfg)
    product_code = loan_row.get("product_code")
    if product_code:
        p_cfg = get_product_config_from_db(product_code)
        if p_cfg:
            effective_cfg = {**sys_cfg, **p_cfg}
    return effective_cfg


def _get_conn():
    """
    Create a new psycopg2 connection for EOD work.

    We keep this local to avoid tight coupling to other modules' internals,
    but reuse the same database URL source.
    """
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


def _fetch_active_loans(
    conn,
    *,
    loan_ids_filter: List[int] | None = None,
) -> List[Dict[str, Any]]:
    """Load active loans from the database.

    When loan_ids_filter is provided, only those loan IDs are returned.
    Uses a server-side named cursor so the full result-set is streamed in
    chunks rather than materialised into client memory all at once.
    """
    with conn.cursor(name="fetch_active_loans", cursor_factory=RealDictCursor) as cur:
        if loan_ids_filter:
            cur.execute(
                "SELECT * FROM loans WHERE status = 'active' AND id = ANY(%s)",
                (loan_ids_filter,),
            )
        else:
            cur.execute("SELECT * FROM loans WHERE status = 'active'")
        result: List[Dict[str, Any]] = []
        while True:
            batch = cur.fetchmany(500)
            if not batch:
                break
            result.extend(dict(r) for r in batch)
    return result


def _batch_fetch_schedules(
    conn, loan_ids: List[int]
) -> Dict[int, List[Dict[str, Any]]]:
    """Fetch schedule lines for all given loans in two queries (latest version per loan)."""
    result: Dict[int, List[Dict[str, Any]]] = {lid: [] for lid in loan_ids}
    if not loan_ids:
        return result
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (loan_id) loan_id, id AS schedule_id
            FROM loan_schedules
            WHERE loan_id = ANY(%s)
            ORDER BY loan_id, version DESC
            """,
            (loan_ids,),
        )
        rows = cur.fetchall()
    if not rows:
        return result
    sched_to_loan = {int(r["schedule_id"]): int(r["loan_id"]) for r in rows}
    schedule_ids = list(sched_to_loan.keys())
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            'SELECT * FROM schedule_lines WHERE loan_schedule_id = ANY(%s) ORDER BY "Period"',
            (schedule_ids,),
        )
        for row in cur.fetchall():
            lid = sched_to_loan.get(int(row["loan_schedule_id"]))
            if lid is not None:
                result[lid].append(dict(row))
    return result


_EMPTY_ALLOC: Dict[str, float] = {
    "alloc_principal_not_due": 0.0,
    "alloc_principal_arrears": 0.0,
    "alloc_interest_accrued": 0.0,
    "alloc_interest_arrears": 0.0,
    "alloc_default_interest": 0.0,
    "alloc_penalty_interest": 0.0,
    "alloc_fees_charges": 0.0,
}


def _batch_fetch_allocation_totals(
    conn, loan_ids: List[int], as_of_date: date
) -> Dict[int, Dict[str, float]]:
    """Fetch allocation bucket sums for all given loans on as_of_date (one query)."""
    result: Dict[int, Dict[str, float]] = {lid: dict(_EMPTY_ALLOC) for lid in loan_ids}
    if not loan_ids:
        return result
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT lr.loan_id,
                COALESCE(SUM(lra.alloc_principal_not_due), 0) AS alloc_principal_not_due,
                COALESCE(SUM(lra.alloc_principal_arrears),  0) AS alloc_principal_arrears,
                COALESCE(SUM(lra.alloc_interest_accrued),   0) AS alloc_interest_accrued,
                COALESCE(SUM(lra.alloc_interest_arrears),   0) AS alloc_interest_arrears,
                COALESCE(SUM(lra.alloc_default_interest),   0) AS alloc_default_interest,
                COALESCE(SUM(lra.alloc_penalty_interest),   0) AS alloc_penalty_interest,
                COALESCE(SUM(lra.alloc_fees_charges),       0) AS alloc_fees_charges
            FROM loan_repayments lr
            JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
            WHERE lr.loan_id = ANY(%s)
              AND lr.status IN ('posted', 'reversed')
              AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
            GROUP BY lr.loan_id
            """,
            (loan_ids, as_of_date),
        )
        for row in cur.fetchall():
            result[int(row["loan_id"])] = {k: float(row[k] or 0) for k in _EMPTY_ALLOC}
    return result


def _batch_fetch_yesterday_states(
    conn, loan_ids: List[int], yesterday: date
) -> Dict[int, Dict[str, Any] | None]:
    """Fetch the most-recent daily-state row on or before yesterday for each loan."""
    result: Dict[int, Dict[str, Any] | None] = {lid: None for lid in loan_ids}
    if not loan_ids:
        return result
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (loan_id)
                loan_id,
                principal_not_due, principal_arrears,
                interest_accrued_balance, interest_arrears_balance,
                default_interest_balance, penalty_interest_balance,
                fees_charges_balance, days_overdue, total_exposure,
                COALESCE(regular_interest_daily, 0)           AS regular_interest_daily,
                COALESCE(penalty_interest_daily, 0)           AS penalty_interest_daily,
                COALESCE(default_interest_daily, 0)           AS default_interest_daily,
                COALESCE(regular_interest_period_to_date, 0)  AS regular_interest_period_to_date,
                COALESCE(penalty_interest_period_to_date, 0)  AS penalty_interest_period_to_date,
                COALESCE(default_interest_period_to_date, 0)  AS default_interest_period_to_date
            FROM loan_daily_state
            WHERE loan_id = ANY(%s) AND as_of_date <= %s
            ORDER BY loan_id, as_of_date DESC
            """,
            (loan_ids, yesterday),
        )
        for row in cur.fetchall():
            result[int(row["loan_id"])] = {
                "principal_not_due":          float(row["principal_not_due"] or 0),
                "principal_arrears":           float(row["principal_arrears"] or 0),
                "interest_accrued_balance":    float(row["interest_accrued_balance"] or 0),
                "interest_arrears_balance":    float(row["interest_arrears_balance"] or 0),
                "default_interest_balance":    float(row["default_interest_balance"] or 0),
                "penalty_interest_balance":    float(row["penalty_interest_balance"] or 0),
                "fees_charges_balance":        float(row["fees_charges_balance"] or 0),
                "days_overdue":                int(row["days_overdue"] or 0),
                "total_exposure":              float(row["total_exposure"] or 0),
                "regular_interest_daily":      float(row["regular_interest_daily"] or 0),
                "penalty_interest_daily":      float(row["penalty_interest_daily"] or 0),
                "default_interest_daily":      float(row["default_interest_daily"] or 0),
                "regular_interest_period_to_date":  float(row["regular_interest_period_to_date"] or 0),
                "penalty_interest_period_to_date":  float(row["penalty_interest_period_to_date"] or 0),
                "default_interest_period_to_date":  float(row["default_interest_period_to_date"] or 0),
            }
    return result


_UNAPPLIED_FILTER_SQL = """
    AND NOT (
        COALESCE(lr.reference, '')          ILIKE 'Unapplied funds allocation%%'
        OR COALESCE(lr.customer_reference, '') ILIKE 'Unapplied funds allocation%%'
        OR COALESCE(lr.company_reference, '')  ILIKE 'Unapplied funds allocation%%'
    )
"""


def _batch_fetch_net_alloc_and_unallocated(
    conn, loan_ids: List[int], as_of_date: date
) -> Tuple[Dict[int, float], Dict[int, float]]:
    """
    Batch-fetch net_allocation and unallocated amounts for all loans on as_of_date.
    Returns (net_alloc_by_loan, unallocated_by_loan).
    Replaces per-loan calls to get_net_allocation_for_loan_date /
    get_unallocated_for_loan_date with two portfolio-wide queries.
    """
    net_alloc:   Dict[int, float] = {lid: 0.0 for lid in loan_ids}
    unallocated: Dict[int, float] = {lid: 0.0 for lid in loan_ids}
    if not loan_ids:
        return net_alloc, unallocated
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT lr.loan_id,
                COALESCE(SUM(
                    lra.alloc_principal_total + lra.alloc_interest_total + lra.alloc_fees_total
                ), 0) AS net_alloc
            FROM loan_repayments lr
            JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
            WHERE lr.loan_id = ANY(%s)
              AND lr.status IN ('posted', 'reversed')
              AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
              {_UNAPPLIED_FILTER_SQL}
            GROUP BY lr.loan_id
            """,
            (loan_ids, as_of_date),
        )
        for row in cur.fetchall():
            lid = int(row["loan_id"])
            if lid in net_alloc:
                net_alloc[lid] = float(row["net_alloc"] or 0)

        cur.execute(
            f"""
            SELECT loan_id, COALESCE(SUM(amount - alloc_total), 0) AS unallocated
            FROM (
                SELECT lr.loan_id,
                    lr.amount,
                    COALESCE(SUM(
                        COALESCE(lra.alloc_principal_total, 0)
                        + COALESCE(lra.alloc_interest_total, 0)
                        + COALESCE(lra.alloc_fees_total, 0)
                    ), 0) AS alloc_total
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = ANY(%s)
                  AND lr.status IN ('posted', 'reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
                  {_UNAPPLIED_FILTER_SQL}
                GROUP BY lr.loan_id, lr.id, lr.amount
            ) sub
            GROUP BY loan_id
            """,
            (loan_ids, as_of_date),
        )
        for row in cur.fetchall():
            lid = int(row["loan_id"])
            if lid in unallocated:
                unallocated[lid] = float(row["unallocated"] or 0)
    return net_alloc, unallocated


def _get_loan_capture_rate_pct(loan_row: Dict[str, Any]) -> float:
    """
    Get penalty_rate_pct from loan metadata (loan capture). System relies purely on loan capture;
    if null or zero, returns 0.0. Handles metadata as dict (JSONB), str (JSON text), and key variants.
    """
    raw = loan_row.get("metadata") or loan_row.get("Metadata")
    if raw is None:
        return 0.0
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return 0.0
    if not isinstance(raw, dict):
        return 0.0
    pct = raw.get("penalty_rate_pct")
    if pct is None:
        pct = raw.get("Penalty_rate_pct")
    if pct is None:
        return 0.0
    try:
        return float(pct)
    except (TypeError, ValueError):
        return 0.0


def _loan_config_from_row(loan_row: Dict[str, Any], sys_cfg: Dict[str, Any]) -> LoanConfig:
    """
    Build a LoanConfig for the engine from a loan row and (merged) configuration.

    - regular_rate_per_month: from loans.annual_rate or monthly_rate.
    - Default Rate % = Penalty Rate % = loan capture only (metadata.penalty_rate_pct). Null or zero → 0.
    - At 5%: default_daily = 987.05*0.05/30 = 1.65, penalty_daily = 494.17*0.05/30 = 0.82.
    """
    loan_type = loan_row.get("loan_type") or "term_loan"
    default_rates = (sys_cfg.get("default_rates") or {}).get(loan_type, {}) or {}

    # Penalty/default rate from loan capture only; null or zero → 0 (no config fallback)
    loan_capture_pct = _get_loan_capture_rate_pct(loan_row)
    rate_pct = Decimal(str(loan_capture_pct)) / Decimal("100")
    default_abs_monthly = rate_pct
    penalty_pct = rate_pct

    # Regular rate per month
    monthly_rate = None
    if loan_row.get("monthly_rate") is not None:
        monthly_rate = Decimal(str(loan_row["monthly_rate"]))
    elif loan_row.get("annual_rate") is not None:
        monthly_rate = Decimal(str(loan_row["annual_rate"])) / Decimal("12")
    else:
        # Fallback to system default interest % per annum for this type, if present.
        dr_interest = Decimal(str(default_rates.get("interest_pct", 0))) / Decimal("100")
        monthly_rate = dr_interest / Decimal("12")

    # Grace period days and penalty basis (from config: product or system).
    grace_days = 5  # sensible default; can be made configurable later per product
    penalty_on_principal_arrears_only = (
        (sys_cfg.get("penalty_balance_basis") or "Arrears") == "Arrears"
    )

    # Use same waterfall config as loan_management (normalized bucket order) so engine and allocation stay in sync
    profile_key, waterfall_bucket_order = _get_waterfall_config(sys_cfg)

    flat_interest = (sys_cfg.get("interest_method") or "Reducing balance") == "Flat rate"

    return LoanConfig(
        regular_rate_per_month=monthly_rate,
        default_interest_absolute_rate_per_month=default_abs_monthly,
        penalty_interest_absolute_rate_per_month=penalty_pct,
        grace_period_days=grace_days,
        penalty_on_principal_arrears_only=penalty_on_principal_arrears_only,
        waterfall_bucket_order=waterfall_bucket_order,
        flat_interest=flat_interest,
    )


def _build_schedule_entries(
    loan_row: Dict[str, Any], schedule_rows: List[Dict[str, Any]]
) -> List[ScheduleEntry]:
    """
    Convert DB schedule_lines rows into engine ScheduleEntry objects.

    Period start is taken as:
      - disbursement_date for the first row;
      - previous row's due_date for subsequent rows.
    """
    entries: List[ScheduleEntry] = []
    prev_due: date | None = None

    disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
    if hasattr(disb_date, "isoformat"):
        period_start: date = disb_date
    else:
        from system_business_date import get_effective_date
        period_start = get_effective_date()

    for row in schedule_rows:
        raw_date = row.get("Date") or row.get("date")
        if not raw_date:
            continue
        # Stored as string like "31-Mar-2026"
        if isinstance(raw_date, str):
            due_date = datetime.strptime(raw_date[:32], "%d-%b-%Y").date()
        else:
            due_date = raw_date

        if prev_due is not None:
            period_start = prev_due

        principal_component = Decimal(str(row.get("principal") or row.get("Principal") or 0))
        interest_component = Decimal(str(row.get("interest") or row.get("Interest") or 0))

        entries.append(
            ScheduleEntry(
                period_start=period_start,
                due_date=due_date,
                principal_component=principal_component,
                interest_component=interest_component,
            )
        )
        prev_due = due_date

    return entries


@dataclass
class EODResult:
    as_of_date: date
    loans_processed: int
    started_at: datetime
    finished_at: datetime
    tasks_run: Tuple[str, ...] = ()


def get_engine_state_for_loan_date(loan_id: int, as_of_date: date) -> Dict[str, Any] | None:
    """
    Run the loan engine for one loan up to as_of_date and return the accrual-only state (no DB write).
    Used by reallocate_repayment to restore state = engine - other receipts' allocations.
    Returns dict with principal_not_due, principal_arrears, interest_accrued_balance, interest_arrears_balance,
    default_interest_balance, penalty_interest_balance, fees_charges_balance, days_overdue, and daily fields.
    """
    sys_cfg = load_system_config_from_db() or {}
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM loans WHERE id = %s AND status = 'active'", (loan_id,))
            row = cur.fetchone()
        if not row:
            return None
        loan_row = dict(row)
    schedule_rows = get_schedule_lines(loan_id)
    if not schedule_rows:
        return None
    effective_cfg = _effective_config_for_loan(loan_row, sys_cfg)
    config = _loan_config_from_row(loan_row, effective_cfg)
    schedule_entries = _build_schedule_entries(loan_row, schedule_rows)
    principal = Decimal(str(loan_row.get("principal") or loan_row.get("disbursed_amount") or 0))
    disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
    if not isinstance(disb_date, date):
        disb_date = as_of_date
    if disb_date > as_of_date:
        return None
    engine_loan = Loan(
        loan_id=str(loan_id),
        disbursement_date=disb_date,
        original_principal=principal,
        config=config,
        schedule=schedule_entries,
    )
    current = disb_date
    while current <= as_of_date:
        engine_loan.process_day(current)
        current += timedelta(days=1)
    return {
        "principal_not_due": float(engine_loan.principal_not_due),
        "principal_arrears": float(engine_loan.principal_arrears),
        "interest_accrued_balance": float(engine_loan.interest_accrued_balance),
        "interest_arrears_balance": float(engine_loan.interest_arrears),
        "default_interest_balance": float(engine_loan.default_interest_balance),
        "penalty_interest_balance": float(engine_loan.penalty_interest_balance),
        "fees_charges_balance": float(engine_loan.fees_charges_balance),
        "days_overdue": engine_loan.days_overdue,
        "regular_interest_daily": float(engine_loan.last_regular_interest_daily),
        "default_interest_daily": float(engine_loan.last_default_interest_daily),
        "penalty_interest_daily": float(engine_loan.last_penalty_interest_daily),
        "regular_interest_period_to_date": float(engine_loan.regular_interest_period_to_date),
        "penalty_interest_period_to_date": float(engine_loan.penalty_interest_period_to_date),
        "default_interest_period_to_date": float(engine_loan.default_interest_period_to_date),
    }


def _run_loan_engine_for_date(
    as_of_date: date,
    sys_cfg: Dict[str, Any],
    *,
    loan_ids_filter: List[int] | None = None,
) -> int:
    """
    Core loan engine step: recompute loan buckets and interest into loan_daily_state.

    When loan_ids_filter is provided, only those loans are processed.
    This is used by run_single_loan_eod to avoid the O(N) cost of reprocessing
    every active loan when only one receipt needs reallocation.

    Returns the number of loans that were actually processed (i.e. with schedules).
    """
    processed = 0
    yesterday = as_of_date - timedelta(days=1)

    with _get_conn() as conn:
        loans = _fetch_active_loans(conn, loan_ids_filter=loan_ids_filter)
        if not loans:
            return 0
        loan_ids = [int(r["id"]) for r in loans]

        # Batch-load all auxiliary data: O(1) queries regardless of portfolio size.
        schedules_map          = _batch_fetch_schedules(conn, loan_ids)
        alloc_map              = _batch_fetch_allocation_totals(conn, loan_ids, as_of_date)
        yesterday_map          = _batch_fetch_yesterday_states(conn, loan_ids, yesterday)
        net_alloc_map, unalloc_map = _batch_fetch_net_alloc_and_unallocated(conn, loan_ids, as_of_date)

    for loan_row in loans:
        loan_id_int = int(loan_row["id"])
        schedule_rows = schedules_map.get(loan_id_int, [])
        if not schedule_rows:
            # Skip loans without schedules; nothing to accrue yet.
            continue

        effective_cfg = _effective_config_for_loan(loan_row, sys_cfg)
        config = _loan_config_from_row(loan_row, effective_cfg)
        schedule_entries = _build_schedule_entries(loan_row, schedule_rows)

        # Opening principal for the engine is the total loan amount (principal column),
        # not the disbursed amount. This ensures interest is charged on the full debt.
        principal = Decimal(str(loan_row.get("principal") or loan_row.get("disbursed_amount") or 0))
        disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
        if not isinstance(disb_date, date):
            # Defensive fallback; real loans should always have a disbursement/start date.
            disb_date = as_of_date

        # Do not write daily state for dates before the loan existed.
        if disb_date > as_of_date:
            continue

        engine_loan = Loan(
            loan_id=str(loan_id_int),
            disbursement_date=disb_date,
            original_principal=principal,
            config=config,
            schedule=schedule_entries,
        )

        # Run engine to yesterday to get engine state at end of yesterday (for deltas)
        current = disb_date
        while current <= yesterday:
            engine_loan.process_day(current)
            current += timedelta(days=1)

        # Capture engine state at end of yesterday (accrual-only, no allocations)
        engine_yesterday = {
            "principal_not_due": float(engine_loan.principal_not_due),
            "principal_arrears": float(engine_loan.principal_arrears),
            "interest_accrued_balance": float(engine_loan.interest_accrued_balance),
            "interest_arrears": float(engine_loan.interest_arrears),
            "default_interest_balance": float(engine_loan.default_interest_balance),
            "penalty_interest_balance": float(engine_loan.penalty_interest_balance),
            "fees_charges_balance": float(engine_loan.fees_charges_balance),
        }

        # Run one more day to get engine state at end of today
        if as_of_date > yesterday:
            engine_loan.process_day(as_of_date)

        alloc = alloc_map.get(loan_id_int, dict(_EMPTY_ALLOC))
        yesterday_saved = yesterday_map.get(loan_id_int) if yesterday >= disb_date else None

        # Balance today = yesterday's balance + (engine today - engine yesterday) - allocations today.
        # So: interest_arrears_balance = yesterday_balance + new_arrears_today - receipts allocated to interest arrears.
        # Same for default interest, penalty interest, and all other buckets.
        def _today_balance(
            yesterday_key: str,
            engine_today_val: float,
            engine_yesterday_val: float,
            alloc_key: str,
        ) -> float:
            delta = engine_today_val - engine_yesterday_val
            if yesterday_saved is not None and yesterday_key in yesterday_saved:
                return max(0.0, yesterday_saved[yesterday_key] + delta - alloc.get(alloc_key, 0.0))
            return max(0.0, engine_today_val - alloc.get(alloc_key, 0.0))

        principal_not_due = _today_balance("principal_not_due", float(engine_loan.principal_not_due), engine_yesterday["principal_not_due"], "alloc_principal_not_due")
        principal_arrears = _today_balance("principal_arrears", float(engine_loan.principal_arrears), engine_yesterday["principal_arrears"], "alloc_principal_arrears")
        interest_accrued_balance = _today_balance("interest_accrued_balance", float(engine_loan.interest_accrued_balance), engine_yesterday["interest_accrued_balance"], "alloc_interest_accrued")
        interest_arrears_balance = _today_balance("interest_arrears_balance", float(engine_loan.interest_arrears), engine_yesterday["interest_arrears"], "alloc_interest_arrears")
        default_interest_balance = _today_balance("default_interest_balance", float(engine_loan.default_interest_balance), engine_yesterday["default_interest_balance"], "alloc_default_interest")
        penalty_interest_balance = _today_balance("penalty_interest_balance", float(engine_loan.penalty_interest_balance), engine_yesterday["penalty_interest_balance"], "alloc_penalty_interest")
        fees_charges_balance = _today_balance("fees_charges_balance", float(engine_loan.fees_charges_balance), engine_yesterday["fees_charges_balance"], "alloc_fees_charges")

        # Non-due-date guard: arrears principal/interest must only move by persisted allocations.
        # This prevents hidden drift from engine/session recomputation on dates without due transitions.
        due_today = any(e.due_date == as_of_date for e in schedule_entries)
        due_yesterday = any(e.due_date == yesterday for e in schedule_entries)
        if yesterday_saved is not None and not due_today:
            principal_not_due = max(
                0.0,
                float(yesterday_saved.get("principal_not_due", 0) or 0) - alloc.get("alloc_principal_not_due", 0.0),
            )
            principal_arrears = max(
                0.0,
                float(yesterday_saved.get("principal_arrears", 0) or 0) - alloc.get("alloc_principal_arrears", 0.0),
            )
            interest_arrears_balance = max(
                0.0,
                float(yesterday_saved.get("interest_arrears_balance", 0) or 0) - alloc.get("alloc_interest_arrears", 0.0),
            )

        # Post-allocation "no arrears": principal and interest arrears are zero (with tolerance).
        no_arrears = (
            principal_arrears <= ARREARS_ZERO_TOLERANCE
            and interest_arrears_balance <= ARREARS_ZERO_TOLERANCE
        )

        # Days overdue must be *consecutive* days in arrears from saved state, not the engine's
        # internal counter (which keeps counting from a past due date and would "pop" to 32 when
        # we stop forcing zero). So: when no arrears -> 0; when arrears -> yesterday_saved + 1 or 1.
        if no_arrears:
            days_overdue_save = 0
        else:
            if yesterday_saved is not None and "days_overdue" in yesterday_saved:
                days_overdue_save = yesterday_saved["days_overdue"] + 1
            else:
                days_overdue_save = 1

        # Grace period: only accrue default/penalty when *saved* days_overdue > grace_period_days.
        # When no arrears or within grace, persist 0 for default/penalty daily and balances.
        grace_days = config.grace_period_days
        within_grace_or_current = no_arrears or (days_overdue_save <= grace_days)

        if within_grace_or_current:
            default_interest_daily_save = 0.0
            penalty_interest_daily_save = 0.0
            default_interest_balance_save = 0.0
            penalty_interest_balance_save = 0.0
            # Period-to-date: always table(yesterday) + today's daily. Never use engine (it has no allocations).
            if due_yesterday:
                default_interest_period_to_date_save = default_interest_daily_save
                penalty_interest_period_to_date_save = penalty_interest_daily_save
            elif yesterday_saved is not None:
                default_interest_period_to_date_save = float(
                    yesterday_saved.get("default_interest_period_to_date", 0) or 0
                ) + default_interest_daily_save
                penalty_interest_period_to_date_save = float(
                    yesterday_saved.get("penalty_interest_period_to_date", 0) or 0
                ) + penalty_interest_daily_save
            else:
                default_interest_period_to_date_save = default_interest_daily_save
                penalty_interest_period_to_date_save = penalty_interest_daily_save
        else:
            # Daily accrual must be based on the OPENING balance at the start of the day
            # (before any of today's allocations).  Using the post-alloc balance (which
            # subtracts today's alloc from interest_arrears / principal_arrears) produces a
            # lower daily on re-runs and breaks the bucket identity:
            #   opening + daily - alloc = closing
            # Re-add today's alloc to recover the true pre-alloc opening balance.
            #
            # Use Decimal arithmetic throughout so the 10dp NUMERIC column receives the
            # full irrational fraction (e.g. 33.3333333333) rather than a float-rounded
            # 33.33, eliminating the per-period ±0.10 residual that comes from 2dp truncation.
            D = Decimal
            rate        = D(str(config.default_interest_absolute_rate_per_month))
            penalty_rate = D(str(config.penalty_interest_absolute_rate_per_month))
            _30 = D("30")
            int_arr_opening = max(
                D("0"),
                D(str(interest_arrears_balance)) + D(str(alloc.get("alloc_interest_arrears", 0.0))),
            )
            default_interest_daily_save = as_10dp(
                int_arr_opening * rate / _30
                if int_arr_opening > 0 and rate > 0
                else D("0")
            )
            prin_arr_opening = max(
                D("0"),
                D(str(principal_arrears)) + D(str(alloc.get("alloc_principal_arrears", 0.0))),
            )
            if config.penalty_on_principal_arrears_only:
                penalty_basis = prin_arr_opening
            else:
                penalty_basis = prin_arr_opening + (
                    D(str(principal_not_due)) + D(str(alloc.get("alloc_principal_not_due", 0.0)))
                )
            penalty_interest_daily_save = as_10dp(
                penalty_basis * penalty_rate / _30
                if penalty_basis > 0 and penalty_rate > 0
                else D("0")
            )
            # Reconcile default/penalty balances using our daily amounts (not engine's)
            if yesterday_saved is not None:
                default_interest_balance_save = max(
                    D("0"),
                    D(str(yesterday_saved.get("default_interest_balance", 0)))
                    + default_interest_daily_save
                    - D(str(alloc.get("alloc_default_interest", 0.0))),
                )
                penalty_interest_balance_save = max(
                    D("0"),
                    D(str(yesterday_saved.get("penalty_interest_balance", 0)))
                    + penalty_interest_daily_save
                    - D(str(alloc.get("alloc_penalty_interest", 0.0))),
                )
            else:
                default_interest_balance_save = max(
                    D("0"),
                    default_interest_daily_save - D(str(alloc.get("alloc_default_interest", 0.0))),
                )
                penalty_interest_balance_save = max(
                    D("0"),
                    penalty_interest_daily_save - D(str(alloc.get("alloc_penalty_interest", 0.0))),
                )
            # Period-to-date: accumulate daily amounts up to and including due date; reset day after due date
            if due_yesterday:
                default_interest_period_to_date_save = default_interest_daily_save
                penalty_interest_period_to_date_save = penalty_interest_daily_save
            elif yesterday_saved is not None:
                default_interest_period_to_date_save = (
                    D(str(yesterday_saved.get("default_interest_period_to_date", 0)))
                    + default_interest_daily_save
                )
                penalty_interest_period_to_date_save = (
                    D(str(yesterday_saved.get("penalty_interest_period_to_date", 0)))
                    + penalty_interest_daily_save
                )
            else:
                default_interest_period_to_date_save = default_interest_daily_save
                penalty_interest_period_to_date_save = penalty_interest_daily_save

        net_alloc = net_alloc_map.get(loan_id_int, 0.0)
        unalloc   = unalloc_map.get(loan_id_int, 0.0)
        # Balance columns are NUMERIC(22,10); quantize to 10dp.
        default_interest_balance_save = float(default_interest_balance_save)
        penalty_interest_balance_save = float(penalty_interest_balance_save)
        total_exposure_save = (
            principal_not_due
            + principal_arrears
            + interest_accrued_balance
            + interest_arrears_balance
            + default_interest_balance_save
            + penalty_interest_balance_save
            + fees_charges_balance
        )
        # Period-to-date: always table(yesterday) + today's daily. Never use engine (it has no allocations).
        # Use Decimal + as_10dp for 10dp precision (avoids float accumulation).
        regular_daily = engine_loan.last_regular_interest_daily
        if due_yesterday:
            regular_interest_period_to_date_save = as_10dp(regular_daily)
        elif yesterday_saved is not None:
            prev = Decimal(str(yesterday_saved.get("regular_interest_period_to_date", 0) or 0))
            regular_interest_period_to_date_save = as_10dp(prev + regular_daily)
        else:
            regular_interest_period_to_date_save = as_10dp(regular_daily)

        save_loan_daily_state(
            loan_id=loan_id_int,
            as_of_date=as_of_date,
            regular_interest_daily=engine_loan.last_regular_interest_daily,
            principal_not_due=principal_not_due,
            principal_arrears=principal_arrears,
            interest_accrued_balance=interest_accrued_balance,
            interest_arrears_balance=interest_arrears_balance,
            default_interest_daily=default_interest_daily_save,
            default_interest_balance=default_interest_balance_save,
            penalty_interest_daily=penalty_interest_daily_save,
            penalty_interest_balance=penalty_interest_balance_save,
            fees_charges_balance=fees_charges_balance,
            days_overdue=days_overdue_save,
            regular_interest_period_to_date=regular_interest_period_to_date_save,
            penalty_interest_period_to_date=penalty_interest_period_to_date_save,
            default_interest_period_to_date=default_interest_period_to_date_save,
            net_allocation=net_alloc,
            unallocated=unalloc,
        )
        processed += 1

    return processed


def _apply_unapplied_funds_to_arrears(as_of_date: date, sys_cfg: Dict[str, Any]) -> int:
    """
    For each loan with unapplied balance > 0 and arrears > 0, allocate unapplied
    towards arrears (waterfall order). Creates allocation with event_type='unapplied_funds_allocation'.
    Returns number of loans that had funds applied.
    """
    loan_ids = get_loans_with_unapplied_balance(as_of_date)
    applied_count = 0
    for loan_id in loan_ids:
        amount = apply_unapplied_funds_to_arrears_eod(loan_id, as_of_date, sys_cfg)
        if amount > 0:
            applied_count += 1
    return applied_count


def _reallocate_receipts_after_reversals(as_of_date: date, sys_cfg: Dict[str, Any]) -> int:
    """
    For loans that had receipts reversed on as_of_date, reallocate any remaining
    posted receipts on that loan/date so the waterfall is correct after reversals.
    Returns the number of receipts reallocated.
    """
    loan_ids = get_loan_ids_with_reversed_receipts_on_date(as_of_date)
    reallocated = 0
    for loan_id in loan_ids:
        posted_ids = get_repayment_ids_for_loan_and_date(loan_id, as_of_date)
        for rid in posted_ids:
            _log_allocation_audit(
                "reallocate_after_reversal",
                loan_id,
                as_of_date,
                repayment_id=rid,
                narration="system auto rev",
                details={"reason": "Receipt reallocated after reversal on same day"},
            )
            reallocate_repayment(rid, system_config=sys_cfg)
            reallocated += 1
    return reallocated


def _run_accounting_events(as_of_date: date, sys_cfg: Dict[str, Any]) -> None:
    """
    Placeholder for future accounting postings that depend on EOD.

    For now this is a no-op; organisations can plug in their own accounting
    integration here (e.g. posting journals to a GL system) while keeping
    the EOD orchestrator stable.
    """
    _ = (as_of_date, sys_cfg)  # unused for now


def _run_statement_batch(as_of_date: date, sys_cfg: Dict[str, Any]) -> None:
    """
    Placeholder for future statements batch generation.
    """
    _ = (as_of_date, sys_cfg)


def _run_notification_batch(as_of_date: date, sys_cfg: Dict[str, Any]) -> None:
    """
    Placeholder for future alerting/notification processing (e.g. SMS/email).
    """
    _ = (as_of_date, sys_cfg)


def run_eod_for_date(
    as_of_date: date,
    *,
    skip_reallocate_after_reversals: bool = False,
) -> EODResult:
    """
    Orchestrate EOD for a given calendar date.

    The exact *sequence* of steps is fixed in code for safety and auditability,
    but which high-level tasks are enabled is controlled by system configuration
    (system_config.eod_settings.tasks).

    When skip_reallocate_after_reversals=True (e.g. when called from reallocate_repayment),
    the reallocate step is skipped to avoid infinite recursion.
    """
    sys_cfg = load_system_config_from_db() or {}
    eod_settings = sys_cfg.get("eod_settings", {}) or {}
    tasks_cfg = (eod_settings.get("tasks") or {}) if isinstance(eod_settings, dict) else {}

    run_loan_engine = bool(tasks_cfg.get("run_loan_engine", True))
    reallocate_after_reversals = (
        bool(tasks_cfg.get("reallocate_after_reversals", True))
        and not skip_reallocate_after_reversals
    )
    post_accounting = bool(tasks_cfg.get("post_accounting_events", False))
    generate_statements = bool(tasks_cfg.get("generate_statements", False))
    send_notifications = bool(tasks_cfg.get("send_notifications", False))

    started = datetime.now(timezone.utc)
    loans_processed = 0
    tasks_run: list[str] = []

    # 1. Loan engine – always first to ensure loan_daily_state is up to date.
    if run_loan_engine:
        loans_processed = _run_loan_engine_for_date(as_of_date, sys_cfg)
        tasks_run.append("loan_engine")

    # 1b. Reallocate posted receipts on loans that had reversals this date (waterfall fix).
    if run_loan_engine and reallocate_after_reversals:
        _reallocate_receipts_after_reversals(as_of_date, sys_cfg)
        tasks_run.append("reallocate_after_reversals")

    # 1c. Apply unapplied funds towards arrears (waterfall order) for loans with arrears.
    apply_unapplied = bool(tasks_cfg.get("apply_unapplied_to_arrears", True))
    if run_loan_engine and apply_unapplied:
        _apply_unapplied_funds_to_arrears(as_of_date, sys_cfg)
        tasks_run.append("apply_unapplied_to_arrears")

    # 2. Accounting postings that depend on updated buckets.
    if post_accounting:
        _run_accounting_events(as_of_date, sys_cfg)
        tasks_run.append("accounting_events")

    # 3. Statements, then 4. Notifications.
    if generate_statements:
        _run_statement_batch(as_of_date, sys_cfg)
        tasks_run.append("statements")

    if send_notifications:
        _run_notification_batch(as_of_date, sys_cfg)
        tasks_run.append("notifications")

    finished = datetime.now(timezone.utc)
    return EODResult(
        as_of_date=as_of_date,
        loans_processed=loans_processed,
        started_at=started,
        finished_at=finished,
        tasks_run=tuple(tasks_run),
    )


def run_single_loan_eod(
    loan_id: int,
    as_of_date: date,
    sys_cfg: Dict[str, Any] | None = None,
) -> None:
    """
    Run the EOD engine computation for a single loan only.

    Much cheaper than run_eod_for_date when only one loan's daily state needs
    refreshing (e.g. after reallocate_repayment).  Bypasses the O(N) cost of
    fetching and reprocessing every active loan in the portfolio.

    sys_cfg is passed in when the caller already holds a loaded config so we
    avoid a redundant DB round-trip to load_system_config_from_db.
    """
    if sys_cfg is None:
        sys_cfg = load_system_config_from_db() or {}
    _run_loan_engine_for_date(as_of_date, sys_cfg, loan_ids_filter=[loan_id])


__all__ = ["run_eod_for_date", "run_single_loan_eod", "EODResult"]

