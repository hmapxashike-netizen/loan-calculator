from __future__ import annotations

"""
End-of-day (EOD) processing for FarndaCred.

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
import logging
import os
import time
import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url
from loan_management.loan_approval_gl_guard import post_deferred_loan_approval_journals_for_eod
from loan_management.product_catalog import batch_get_product_configs_from_db
from decimal_utils import as_10dp
from eod.engine_resume import (
    apply_engine_resume,
    engine_resume_is_valid_schema,
    parse_engine_resume_dict,
    product_code_matches_resume,
    serialize_engine_resume,
)
from eod.loan_daily_engine import LoanConfig, ScheduleEntry, Loan
from accounting.periods import (
    get_month_period_bounds,
    normalize_accounting_period_config,
    is_eom,
    is_eoy,
)
from accounting.posting_policy import use_gl_posting_policy
from eod.audit import (
    ConcurrentEODError,
    clear_stale_eod_audit_runs,
    eod_exclusive_session_lock,
    start_run as audit_start_run,
    finish_run as audit_finish_run,
    log_stage_event,
)
from loan_management.repayment_queries import get_batch_loan_ids_with_reversed_receipts_in_range
from loan_management.schedules import (
    apply_schedule_version_bumps,
    batch_list_schedule_bumping_events,
    list_schedule_bumping_events,
    parse_schedule_line_date,
)

from loan_management import (
    allocate_repayment_waterfall,
    get_allocation_totals_for_loan_date,
    get_liquidation_repayment_ids_for_value_date,
    get_net_allocation_for_loan_date,
    get_unallocated_for_loan_date,
    get_loan_daily_state_balances,
    get_loan_ids_with_reversed_receipts_on_date,
    get_loan_ids_with_unapplied_balance_and_arrears_for_eod,
    get_repayment_ids_for_loan_and_date,
    get_repayment_ids_for_value_date,
    reallocate_repayment,
    save_loan_daily_state,
    apply_unapplied_funds_to_arrears_eod,
    load_system_config_from_db,
    repost_gl_for_loan_date_range,
    get_product_config_from_db,
    _get_waterfall_config,
    _log_allocation_audit,
)

# Treat balances below this as zero for "no arrears" and default/penalty zeroing (avoids float drift).
ARREARS_ZERO_TOLERANCE = 1e-6

_logger = logging.getLogger(__name__)


def _persist_accrual_blocked_for_as_of(
    as_of_date: date,
    *,
    allow_system_date_eod: bool,
) -> bool:
    """
    True when loan_daily_state must not be written for as_of_date.

    Same rule as run_eod_for_date: replay/backfill must not persist accruals on the
    current system business date (or later). Only the canonical date-advancing EOD
    passes allow_system_date_eod=True.
    """
    if allow_system_date_eod:
        return False
    try:
        from eod.system_business_date import get_effective_date

        system_date = get_effective_date()
    except Exception:
        system_date = None
    return bool(system_date is not None and as_of_date >= system_date)


def _effective_config_for_loan(
    loan_row: Dict[str, Any],
    sys_cfg: Dict[str, Any],
    *,
    product_cfg_by_code: Dict[str, dict | None] | None = None,
) -> Dict[str, Any]:
    """Merge product config over system config for this loan so balance/quotation/default penalty % come from product."""
    effective_cfg = dict(sys_cfg)
    product_code = loan_row.get("product_code")
    if not product_code:
        return effective_cfg
    code = str(product_code).strip()
    if not code:
        return effective_cfg
    if product_cfg_by_code is not None:
        if code in product_cfg_by_code:
            p_cfg = product_cfg_by_code[code]
        else:
            p_cfg = get_product_config_from_db(code)
    else:
        p_cfg = get_product_config_from_db(code)
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


def _batch_fetch_schedule_versions_by_loan(
    conn, loan_ids: List[int]
) -> Dict[int, Dict[int, List[Dict[str, Any]]]]:
    """
    All saved schedule versions per loan: ``loan_id -> version -> [schedule_line rows]``.

    EOD replays accrual with the version **in force on each calendar day** (recast/modification
    bumps). Using only the latest version mis-states history (gaps on bump days, or a synthetic
    [disbursement → recast] period with Period 0's zero interest).
    """
    result: Dict[int, Dict[int, List[Dict[str, Any]]]] = {lid: {} for lid in loan_ids}
    if not loan_ids:
        return result
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ls.loan_id, ls.version,
                   sl.id AS line_id, sl.loan_schedule_id, sl."Period", sl."Date",
                   sl.payment, sl.principal, sl.interest, sl.principal_balance, sl.total_outstanding
            FROM schedule_lines sl
            INNER JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
            WHERE ls.loan_id = ANY(%s)
            ORDER BY ls.loan_id, ls.version, sl."Period"
            """,
            (loan_ids,),
        )
        for row in cur.fetchall():
            lid = int(row["loan_id"])
            ver = int(row["version"])
            d = {
                "id": row.get("line_id"),
                "loan_schedule_id": row.get("loan_schedule_id"),
                "Period": row.get("Period"),
                "Date": row.get("Date"),
                "payment": row.get("payment"),
                "principal": row.get("principal"),
                "interest": row.get("interest"),
                "principal_balance": row.get("principal_balance"),
                "total_outstanding": row.get("total_outstanding"),
            }
            result[lid].setdefault(ver, []).append(d)
    return result


def _schedule_rows_for_version(
    ver_rows: Dict[int, List[Dict[str, Any]]], version: int
) -> List[Dict[str, Any]]:
    """Lines for ``version``, or the nearest lower version that has rows."""
    if ver_rows.get(version):
        return ver_rows[version]
    for v in range(int(version), 0, -1):
        if ver_rows.get(v):
            return ver_rows[v]
    return []


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


def _batch_fetch_all_schedule_due_dates(conn, loan_ids: List[int]) -> Dict[int, frozenset[date]]:
    """
    Every due date appearing on any saved schedule version per loan.

    Used for ``*_interest_period_to_date`` resets: persisted period-to-date must restart
    after each contractual due.  After recast, the *latest* schedule often omits historical
    dues (e.g. 29 Mar); if we only checked that version, ``due_yesterday`` would never fire
    on the real boundary and ``regular_interest_period_to_date`` would drift from the sum
    of ``regular_interest_daily`` in the open period (and disagree with ``interest_accrued_balance`` evolution).
    """
    if not loan_ids:
        return {}
    buckets: Dict[int, set[date]] = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ls.loan_id AS loan_id, sl."Date" AS d
            FROM schedule_lines sl
            INNER JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
            WHERE ls.loan_id = ANY(%s)
            """,
            (loan_ids,),
        )
        for row in cur.fetchall():
            lid = int(row["loan_id"])
            raw = row.get("d")
            parsed = parse_schedule_line_date(raw)
            if parsed is None:
                continue
            buckets.setdefault(lid, set()).add(parsed)
    return {lid: frozenset(ds) for lid, ds in buckets.items()}


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
        # LATERAL + LIMIT 1 per loan: index-friendly on (loan_id, as_of_date DESC) vs scanning
        # all history rows before DISTINCT ON collapses.
        cur.execute(
            """
            SELECT
                x.loan_id AS loan_id,
                lds.principal_not_due,
                lds.principal_arrears,
                lds.interest_accrued_balance,
                lds.interest_arrears_balance,
                lds.default_interest_balance,
                lds.penalty_interest_balance,
                lds.fees_charges_balance,
                lds.days_overdue,
                lds.total_exposure,
                COALESCE(lds.regular_interest_daily, 0) AS regular_interest_daily,
                COALESCE(lds.penalty_interest_daily, 0) AS penalty_interest_daily,
                COALESCE(lds.default_interest_daily, 0) AS default_interest_daily,
                COALESCE(lds.regular_interest_period_to_date, 0) AS regular_interest_period_to_date,
                COALESCE(lds.penalty_interest_period_to_date, 0) AS penalty_interest_period_to_date,
                COALESCE(lds.default_interest_period_to_date, 0) AS default_interest_period_to_date,
                COALESCE(lds.regular_interest_in_suspense_balance, 0) AS regular_interest_in_suspense_balance,
                COALESCE(lds.penalty_interest_in_suspense_balance, 0) AS penalty_interest_in_suspense_balance,
                COALESCE(lds.default_interest_in_suspense_balance, 0) AS default_interest_in_suspense_balance,
                COALESCE(lds.total_interest_in_suspense_balance, 0) AS total_interest_in_suspense_balance
            FROM unnest(%s::int[]) AS x(loan_id)
            LEFT JOIN LATERAL (
                SELECT
                    principal_not_due, principal_arrears,
                    interest_accrued_balance, interest_arrears_balance,
                    default_interest_balance, penalty_interest_balance,
                    fees_charges_balance, days_overdue, total_exposure,
                    regular_interest_daily, penalty_interest_daily, default_interest_daily,
                    regular_interest_period_to_date, penalty_interest_period_to_date,
                    default_interest_period_to_date,
                    regular_interest_in_suspense_balance, penalty_interest_in_suspense_balance,
                    default_interest_in_suspense_balance, total_interest_in_suspense_balance
                FROM loan_daily_state
                WHERE loan_daily_state.loan_id = x.loan_id
                  AND loan_daily_state.as_of_date <= %s
                ORDER BY loan_daily_state.as_of_date DESC
                LIMIT 1
            ) lds ON TRUE
            """,
            (loan_ids, yesterday),
        )
        for row in cur.fetchall():
            lid = int(row["loan_id"])
            if row.get("principal_not_due") is None and row.get("principal_arrears") is None:
                result[lid] = None
                continue
            result[lid] = {
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
                "regular_interest_in_suspense_balance": float(
                    row["regular_interest_in_suspense_balance"] or 0
                ),
                "penalty_interest_in_suspense_balance": float(
                    row["penalty_interest_in_suspense_balance"] or 0
                ),
                "default_interest_in_suspense_balance": float(
                    row["default_interest_in_suspense_balance"] or 0
                ),
                "total_interest_in_suspense_balance": float(
                    row["total_interest_in_suspense_balance"] or 0
                ),
            }
    return result


def _batch_fetch_engine_resume_raw(
    conn, loan_ids: List[int], as_of_exact: date
) -> Dict[int, Any]:
    """``loan_id -> engine_resume`` JSON (or None) for the **exact** calendar ``as_of_exact`` row."""
    empty: Dict[int, Any] = {int(lid): None for lid in loan_ids}
    if not loan_ids:
        return empty
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT loan_id, engine_resume
                FROM loan_daily_state
                WHERE loan_id = ANY(%s) AND as_of_date = %s
                """,
                (loan_ids, as_of_exact),
            )
            for row in cur.fetchall():
                empty[int(row["loan_id"])] = row.get("engine_resume")
    except Exception as ex:
        _logger.debug("engine_resume column unavailable or fetch failed: %s", ex)
        return {int(lid): None for lid in loan_ids}
    return empty


def _bumps_invalidate_incremental_resume(
    bumps: List[tuple[date, int]], resume_anchor: date, as_of_date: date
) -> bool:
    """
    True when a recast/modification falls strictly between the resume day and ``as_of_date``.

    Bumps on ``as_of_date`` are applied via per-day schedule sync; bumps after ``as_of_date``
    do not affect this run.
    """
    for d, _ in bumps:
        if resume_anchor < d < as_of_date:
            return True
    return False


_UNAPPLIED_FILTER_SQL = """
    AND NOT (
        COALESCE(lr.reference, '')          ILIKE '%%napplied funds allocation%%'
        OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
        OR COALESCE(lr.company_reference, '')  ILIKE '%%napplied funds allocation%%'
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

    # Grace period days (calendar days after arrears before default/penalty accrue).
    # Prefer eod_settings.grace_period_days, then top-level grace_period_days, then default.
    _eod_cfg = sys_cfg.get("eod_settings") if isinstance(sys_cfg.get("eod_settings"), dict) else {}
    _grace_raw = _eod_cfg.get("grace_period_days", sys_cfg.get("grace_period_days"))
    if _grace_raw is None:
        grace_days = 5
    else:
        try:
            grace_days = max(0, int(_grace_raw))
        except (TypeError, ValueError):
            grace_days = 5
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


def _parse_schedule_row_due(row: Dict[str, Any]) -> date | None:
    raw_date = row.get("Date") or row.get("date")
    if raw_date is None or raw_date == "":
        return None
    return parse_schedule_line_date(raw_date)


def _validate_schedule_accrual_periods(
    entries: List[ScheduleEntry], disbursement_date: date
) -> None:
    """
    Enforce contiguous accrual periods: first period starts on disbursement; each instalment
    due date equals the next period's start (no gaps or overlaps before final maturity).
    """
    if not entries:
        return
    if entries[0].period_start < disbursement_date:
        raise ValueError(
            f"Schedule accrual: first period_start ({entries[0].period_start}) must be on or after "
            f"disbursement_date ({disbursement_date})."
        )
    for i in range(len(entries) - 1):
        cur, nxt = entries[i], entries[i + 1]
        if cur.due_date != nxt.period_start:
            raise ValueError(
                "Schedule accrual: periods must chain without gaps — instalment due "
                f"{cur.due_date} must equal next period_start ({nxt.period_start})."
            )


def _build_schedule_entries(
    loan_row: Dict[str, Any], schedule_rows: List[Dict[str, Any]]
) -> List[ScheduleEntry]:
    """
    Convert DB schedule_lines rows into engine ScheduleEntry objects.

    Period start is taken as:
      - disbursement_date for the first **instalment** row (after any Period 0 opening rows);
      - previous row's due_date for subsequent rows.

    **Period 0** rows (opening balance at booking or recast) do not create a ScheduleEntry: they
    only set where the next accrual period starts, so we do not fabricate a long
    [disbursement → recast] window with zero scheduled interest from the opening line.

    Rows are ordered by instalment due date before building so the period chain is chronological.
    """
    schedule_rows = sorted(
        schedule_rows,
        key=lambda r: (_parse_schedule_row_due(r) or date.max,),
    )
    entries: List[ScheduleEntry] = []
    prev_due: date | None = None

    disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
    if hasattr(disb_date, "isoformat"):
        period_start: date = disb_date
        disb_for_validate: date = disb_date
    else:
        from eod.system_business_date import get_effective_date

        period_start = get_effective_date()
        disb_for_validate = period_start

    for row in schedule_rows:
        due_date = _parse_schedule_row_due(row)
        if due_date is None:
            continue

        try:
            pnum = int(row.get("Period") or row.get("period") or 0)
        except (TypeError, ValueError):
            pnum = 0

        if pnum == 0:
            # Opening row: next contractual period accrues from this date (recast/modification
            # effective date), not from original disbursement.
            period_start = due_date
            continue

        if prev_due is not None:
            period_start = prev_due

        principal_component = Decimal(str(row.get("principal") or row.get("Principal") or 0))
        interest_component = Decimal(str(row.get("interest") or row.get("Interest") or 0))

        # Skip zero-length periods (e.g. stub line with due on period_start).
        if due_date <= period_start:
            continue

        entries.append(
            ScheduleEntry(
                period_start=period_start,
                due_date=due_date,
                principal_component=principal_component,
                interest_component=interest_component,
            )
        )
        prev_due = due_date

    _validate_schedule_accrual_periods(entries, disb_for_validate)
    return entries


def _diagnose_empty_schedule_entries(
    loan_row: Dict[str, Any],
    schedule_rows: List[Dict[str, Any]],
    version: int,
) -> str:
    """
    Explain why ``_build_schedule_entries`` returned no ``ScheduleEntry`` rows (same walk, no build).
    Typical data issues: truncated ``Date``, only Period 0 lines, or due <= period_start.
    """
    schedule_rows = sorted(
        schedule_rows,
        key=lambda r: (_parse_schedule_row_due(r) or date.max,),
    )
    unparsed_samples: list[str] = []
    period0_parsed = 0
    instalment_parsed = 0
    unparsed_rows = 0
    zero_length_instalments = 0
    built = 0

    disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
    if hasattr(disb_date, "isoformat"):
        period_start: date = disb_date
    else:
        from eod.system_business_date import get_effective_date

        period_start = get_effective_date()

    prev_due: date | None = None

    for row in schedule_rows:
        due_date = _parse_schedule_row_due(row)
        raw_d = row.get("Date") if row.get("Date") is not None else row.get("date")
        if due_date is None:
            unparsed_rows += 1
            if raw_d is not None and str(raw_d).strip():
                s = str(raw_d).strip()[:80]
                if s not in unparsed_samples and len(unparsed_samples) < 6:
                    unparsed_samples.append(s)
            continue
        try:
            pnum = int(row.get("Period") or row.get("period") or 0)
        except (TypeError, ValueError):
            pnum = 0
        if pnum == 0:
            period0_parsed += 1
            period_start = due_date
            continue
        instalment_parsed += 1
        if prev_due is not None:
            period_start = prev_due
        if due_date <= period_start:
            zero_length_instalments += 1
            continue
        built += 1
        prev_due = due_date

    hdr = (
        f"Version {version} at disbursement builds **no** accrual periods "
        f"({len(schedule_rows)} schedule line(s)). "
    )
    bits: list[str] = [hdr]
    if unparsed_rows:
        samp = ", ".join(repr(s) for s in unparsed_samples)
        extra = ""
        if unparsed_rows > len(unparsed_samples):
            extra = f" … ({unparsed_rows} row(s) with bad/missing Date in total). "
        bits.append(
            f"{unparsed_rows} row(s) have dates the engine cannot parse "
            f"(use **dd-Mon-yyyy** or **YYYY-MM-DD** with a **four-digit** year, e.g. 01-Jan-2024). "
            f"Samples: {samp}.{extra}"
        )
    if instalment_parsed == 0:
        if period0_parsed:
            bits.append(
                "Only **Period 0** rows have valid dates — add **Period >= 1** instalment lines "
                "with due dates after the opening/booking date. "
            )
        elif unparsed_rows == 0:
            bits.append("There are no instalment rows (**Period** >= 1) with valid dates. ")
    elif built == 0 and zero_length_instalments == instalment_parsed:
        bits.append(
            "Every instalment row is skipped as **zero-length** (due date is on or before the "
            "period start). Check Period 0 opening date vs first due, and disbursement date. "
        )
    elif built == 0:
        bits.append(
            f"{instalment_parsed} instalment row(s) parsed, but none form a positive-length "
            f"accrual window ({zero_length_instalments} zero-length). "
        )
    bits.append(
        "If values look like `01-Jan-202`, the year was truncated — run migration **76** and "
        "repair `schedule_lines`.\"Date\"."
    )
    return "".join(bits).strip()


def _eod_sync_engine_schedule_for_date(
    engine_loan: Loan,
    loan_row: Dict[str, Any],
    on_date: date,
    bumps: List[tuple[date, int]],
    ver_rows: Dict[int, List[Dict[str, Any]]],
) -> None:
    """Swap ``engine_loan.schedule`` when ``loan_schedules.version`` in force changes on ``on_date``."""
    v = apply_schedule_version_bumps(on_date, bumps)
    if v == getattr(engine_loan, "_eod_schedule_version", None):
        return
    rows = _schedule_rows_for_version(ver_rows, v)
    if not rows:
        _logger.warning(
            "EOD loan_id=%s: no schedule lines for version %s on %s; keeping prior schedule entries.",
            loan_row.get("id"),
            v,
            on_date.isoformat(),
        )
        return
    try:
        engine_loan.schedule = _build_schedule_entries(loan_row, rows)
        engine_loan._eod_schedule_version = v
    except ValueError as e:
        _logger.warning(
            "EOD loan_id=%s: version %s invalid on %s: %s",
            loan_row.get("id"),
            v,
            on_date.isoformat(),
            e,
        )


@dataclass
class EODResult:
    run_id: str
    as_of_date: date
    loans_processed: int
    started_at: datetime
    finished_at: datetime
    tasks_run: Tuple[str, ...] = ()
    run_status: str = "SUCCESS"
    failed_stage: str | None = None
    error_message: str | None = None
    should_advance_date: bool = True


class StageExecutionError(RuntimeError):
    def __init__(self, stage_name: str, message: str):
        super().__init__(message)
        self.stage_name = stage_name


def _format_stage_exception(e: BaseException) -> str:
    """Human-readable message; str(e) alone is often useless (e.g. KeyError(0) -> '0')."""
    return f"{type(e).__name__}: {e}"


def explain_single_loan_eod_skip_reason(loan_id: int, as_of_date: date) -> str | None:
    """
    If the loan EOD engine would skip ``loan_id`` on ``as_of_date`` (early exits only),
    return a short human-readable reason; otherwise ``None``.

    Does not run the full day engine; use when ``run_single_loan_eod`` processes 0 loans.
    """
    with _get_conn() as conn:
        loans = _fetch_active_loans(conn, loan_ids_filter=[loan_id])
        if not loans:
            return "Loan not found or not active."
        loan_row = dict(loans[0])
        loan_id_int = int(loan_row["id"])
        schedules_versions_map = _batch_fetch_schedule_versions_by_loan(conn, [loan_id_int])
        schedules_map = _batch_fetch_schedules(conn, [loan_id_int])
        ver_rows = schedules_versions_map.get(loan_id_int, {})
        if not ver_rows or not any(ver_rows.values()):
            if not schedules_map.get(loan_id_int):
                return "No schedule data linked to this loan in the EOD batch view."
            return (
                "Schedule version map is empty while a latest schedule header exists — "
                "check loan_schedules versions vs schedule_lines."
            )
        disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
        if not isinstance(disb_date, date):
            disb_date = as_of_date
        if disb_date > as_of_date:
            return (
                f"Disbursement/start ({disb_date}) is after as-of ({as_of_date}); "
                "engine does not write daily state before the loan exists."
            )
        bumps = list_schedule_bumping_events(loan_id_int)
        v0 = apply_schedule_version_bumps(disb_date, bumps)
        rows0 = _schedule_rows_for_version(ver_rows, v0)
        if not rows0:
            return (
                f"No schedule lines for version {v0} in force at disbursement ({disb_date}). "
                "Add lines for that version or fix recast/modification bump dates."
            )
        try:
            schedule_entries0 = _build_schedule_entries(loan_row, rows0)
        except ValueError as e:
            return f"Invalid schedule at disbursement (version {v0}): {e}"
        if not schedule_entries0:
            return _diagnose_empty_schedule_entries(loan_row, rows0, v0)
        return None


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
        ver_map = _batch_fetch_schedule_versions_by_loan(conn, [loan_id])
    ver_rows = ver_map.get(loan_id, {})
    if not ver_rows or not any(ver_rows.values()):
        return None
    effective_cfg = _effective_config_for_loan(loan_row, sys_cfg)
    config = _loan_config_from_row(loan_row, effective_cfg)
    principal = Decimal(str(loan_row.get("principal") or loan_row.get("disbursed_amount") or 0))
    disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
    if not isinstance(disb_date, date):
        disb_date = as_of_date
    if disb_date > as_of_date:
        return None
    bumps = list_schedule_bumping_events(loan_id)
    v0 = apply_schedule_version_bumps(disb_date, bumps)
    rows0 = _schedule_rows_for_version(ver_rows, v0)
    if not rows0:
        return None
    try:
        schedule_entries0 = _build_schedule_entries(loan_row, rows0)
    except ValueError:
        return None
    if not schedule_entries0:
        return None
    engine_loan = Loan(
        loan_id=str(loan_id),
        disbursement_date=disb_date,
        original_principal=principal,
        config=config,
        schedule=schedule_entries0,
    )
    setattr(engine_loan, "_eod_schedule_version", v0)
    current = disb_date
    while current <= as_of_date:
        _eod_sync_engine_schedule_for_date(engine_loan, loan_row, current, bumps, ver_rows)
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


def _trace_engine_timing_enabled(sys_cfg: Dict[str, Any] | None = None) -> bool:
    """
    Toggleable EOD timing trace.

    - Env: FARNDACRED_TRACE_EOD=1 (preferred for ad-hoc tracing)
    - Config: eod_settings.tasks.loan_engine_log_timing=true (existing)
    """
    if os.environ.get("FARNDACRED_TRACE_EOD", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    try:
        eod_st = (sys_cfg or {}).get("eod_settings") or {}
        tasks_cfg = (eod_st.get("tasks") if isinstance(eod_st, dict) else None) or {}
        return bool(tasks_cfg.get("loan_engine_log_timing", False))
    except Exception:
        return False


def _run_loan_engine_for_date(
    as_of_date: date,
    sys_cfg: Dict[str, Any],
    *,
    loan_ids_filter: List[int] | None = None,
    allow_system_date_eod: bool = False,
    replay_refresh_allocations: bool = False,
) -> int:
    """
    Core loan engine step: recompute loan buckets and interest into loan_daily_state.

    When loan_ids_filter is provided, only those loans are processed.
    This is used by run_single_loan_eod to avoid the O(N) cost of reprocessing
    every active loan when only one receipt needs reallocation.

    allow_system_date_eod must match run_eod_for_date: single-loan and replay paths
    default False so they cannot persist accruals on the system business date.

    replay_refresh_allocations forces a full accrual replay (no incremental engine resume).

    Returns the number of loans that were actually processed (i.e. with schedules).

    All ``save_loan_daily_state`` calls share one connection (no per-loan connect overhead).

    Commits are batched: ``eod_settings.tasks.loan_engine_commit_batch_size`` (default 250,
    clamped 1..10000). Set to 1 for legacy per-loan commits. Any uncommitted work is
    committed when the loop finishes.
    """
    block_accruals = _persist_accrual_blocked_for_as_of(
        as_of_date, allow_system_date_eod=allow_system_date_eod
    )
    processed = 0
    yesterday = as_of_date - timedelta(days=1)

    eod_st = sys_cfg.get("eod_settings") or {}
    tasks_cfg = (eod_st.get("tasks") if isinstance(eod_st, dict) else None) or {}
    # Default True when unset: same outputs as full replay when resume is valid; replay mode forces full replay.
    incremental_enabled = bool(tasks_cfg.get("incremental_loan_engine", True)) and (
        not replay_refresh_allocations
    )

    commit_batch_raw = tasks_cfg.get("loan_engine_commit_batch_size", 250)
    try:
        commit_batch_size = int(commit_batch_raw)
    except (TypeError, ValueError):
        commit_batch_size = 250
    commit_batch_size = max(1, min(10_000, commit_batch_size))

    log_engine_timing = _trace_engine_timing_enabled(sys_cfg) or (
        os.environ.get("FARNDACRED_EOD_LOG_ENGINE_TIMING", "").strip().lower() in ("1", "true", "yes", "on")
    )
    t_engine_wall0 = time.perf_counter()
    prefetch_s = 0.0
    compute_s = 0.0
    commit_s = 0.0
    n_incremental_path = 0
    n_full_replay_path = 0
    loans_since_commit = 0

    def _do_commit() -> None:
        nonlocal loans_since_commit, commit_s
        if loans_since_commit <= 0:
            return
        t_c0 = time.perf_counter()
        conn.commit()
        commit_s += time.perf_counter() - t_c0
        loans_since_commit = 0

    with _get_conn() as conn:
        t_pf0 = time.perf_counter()
        loans = _fetch_active_loans(conn, loan_ids_filter=loan_ids_filter)
        if not loans:
            return 0
        loan_ids = [int(r["id"]) for r in loans]

        product_codes_unique = {
            str(r.get("product_code") or "").strip()
            for r in loans
            if r.get("product_code") and str(r.get("product_code") or "").strip()
        }
        product_cfg_by_code = batch_get_product_configs_from_db(product_codes_unique)

        # Batch-load all auxiliary data: O(1) queries regardless of portfolio size.
        schedules_map          = _batch_fetch_schedules(conn, loan_ids)
        schedules_versions_map = _batch_fetch_schedule_versions_by_loan(conn, loan_ids)
        all_schedule_due_dates_map = _batch_fetch_all_schedule_due_dates(conn, loan_ids)
        alloc_map              = _batch_fetch_allocation_totals(conn, loan_ids, as_of_date)
        yesterday_map          = _batch_fetch_yesterday_states(conn, loan_ids, yesterday)
        net_alloc_map, unalloc_map = _batch_fetch_net_alloc_and_unallocated(conn, loan_ids, as_of_date)
        bumps_by_loan = batch_list_schedule_bumping_events(loan_ids)
        engine_resume_raw: Dict[int, Any] = {lid: None for lid in loan_ids}
        reversed_since_resume: set[int] = set()
        if incremental_enabled:
            engine_resume_raw = _batch_fetch_engine_resume_raw(conn, loan_ids, yesterday)
            reversed_since_resume = get_batch_loan_ids_with_reversed_receipts_in_range(
                loan_ids, yesterday, as_of_date
            )
        prefetch_s = time.perf_counter() - t_pf0

        for loan_row in loans:
            loan_id_int = int(loan_row["id"])
            ver_rows = schedules_versions_map.get(loan_id_int, {})
            if not ver_rows or not any(ver_rows.values()):
                if not schedules_map.get(loan_id_int):
                    continue
                _logger.warning(
                    "EOD loan_id=%s: latest schedule pointer exists but version map is empty; skipping.",
                    loan_id_int,
                )
                continue

            effective_cfg = _effective_config_for_loan(
                loan_row, sys_cfg, product_cfg_by_code=product_cfg_by_code
            )
            config = _loan_config_from_row(loan_row, effective_cfg)

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

            bumps = bumps_by_loan.get(loan_id_int) or []
            v0 = apply_schedule_version_bumps(disb_date, bumps)
            rows0 = _schedule_rows_for_version(ver_rows, v0)
            if not rows0:
                _logger.warning("EOD skipped loan_id=%s: no schedule lines for version %s at disbursement.", loan_id_int, v0)
                continue
            try:
                schedule_entries0 = _build_schedule_entries(loan_row, rows0)
            except ValueError as e:
                _logger.warning(
                    "EOD skipped loan_id=%s: invalid schedule v%s at disbursement (%s).",
                    loan_id_int,
                    v0,
                    e,
                )
                continue
            if not schedule_entries0:
                _logger.warning(
                    "EOD skipped loan_id=%s: %s",
                    loan_id_int,
                    _diagnose_empty_schedule_entries(dict(loan_row), rows0, v0),
                )
                continue

            t_loan0 = time.perf_counter()
            engine_loan = Loan(
                loan_id=str(loan_id_int),
                disbursement_date=disb_date,
                original_principal=principal,
                config=config,
                schedule=schedule_entries0,
            )
            setattr(engine_loan, "_eod_schedule_version", v0)

            resume_payload = parse_engine_resume_dict(engine_resume_raw.get(loan_id_int))
            _inc_checks = {
                "incremental_enabled": bool(incremental_enabled),
                "resume_present": resume_payload is not None,
                "resume_schema_ok": bool(resume_payload is not None and engine_resume_is_valid_schema(resume_payload)),
                "product_match": bool(resume_payload is not None and product_code_matches_resume(resume_payload, dict(loan_row))),
                "no_reversals_window": bool(loan_id_int not in reversed_since_resume),
                "no_bumps_invalidate": bool(not _bumps_invalidate_incremental_resume(bumps, yesterday, as_of_date)),
            }
            use_incremental = all(_inc_checks.values())
            if use_incremental:
                try:
                    apply_engine_resume(engine_loan, resume_payload)
                except Exception as ex:
                    _logger.debug(
                        "EOD loan_id=%s: incremental resume hydrate failed (%s); full replay.",
                        loan_id_int,
                        ex,
                    )
                    use_incremental = False

            incremental_path_for_loan = use_incremental

            # Run engine to yesterday: use schedule **version in force on each day** (recast/modification).
            if use_incremental:
                current = yesterday + timedelta(days=1)
            else:
                current = disb_date
            while current <= yesterday:
                _eod_sync_engine_schedule_for_date(engine_loan, loan_row, current, bumps, ver_rows)
                engine_loan.process_day(current)
                current += timedelta(days=1)
            if log_engine_timing:
                try:
                    if incremental_enabled:
                        if incremental_path_for_loan:
                            _logger.info(
                                "TRACE eod.engine_path loan_id=%s as_of=%s path=incremental",
                                loan_id_int,
                                as_of_date.isoformat(),
                            )
                        else:
                            failed = [k for k, ok in _inc_checks.items() if not ok]
                            _logger.info(
                                "TRACE eod.engine_path loan_id=%s as_of=%s path=full_replay failed_checks=%s",
                                loan_id_int,
                                as_of_date.isoformat(),
                                ",".join(failed) if failed else "",
                            )
                except Exception:
                    pass

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
            if as_of_date > yesterday and not block_accruals:
                _eod_sync_engine_schedule_for_date(engine_loan, loan_row, as_of_date, bumps, ver_rows)
                engine_loan.process_day(as_of_date)
            elif block_accruals:
                # Force daily accrual metrics to zero since we blocked processing for today
                engine_loan.last_regular_interest_daily = Decimal("0")
                engine_loan.last_default_interest_daily = Decimal("0")
                engine_loan.last_penalty_interest_daily = Decimal("0")

            alloc = alloc_map.get(loan_id_int, dict(_EMPTY_ALLOC))
            yesterday_saved = yesterday_map.get(loan_id_int) if yesterday >= disb_date else None

            # Contractual due on as_of_date (needed before interest_accrued persistence rule).
            v_asof = apply_schedule_version_bumps(as_of_date, bumps)
            rows_asof = _schedule_rows_for_version(ver_rows, v_asof)
            try:
                entries_asof = _build_schedule_entries(loan_row, rows_asof)
            except ValueError:
                entries_asof = list(engine_loan.schedule)
            due_today = any(e.due_date == as_of_date for e in entries_asof)

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
            # Interest accrued is not a waterfall receipt bucket: when nothing is allocated there,
            # closing must follow persisted opening + today's scheduled daily accrual (matches
            # ``regular_interest_daily`` / customer statement roll-ups). Engine delta can drift
            # from saved state after receipts on other buckets; do not zero out accrual growth.
            alloc_ia = float(alloc.get("alloc_interest_accrued", 0.0) or 0.0)
            rd_add = float(engine_loan.last_regular_interest_daily or 0)
            if (
                not block_accruals
                and yesterday_saved is not None
                and "interest_accrued_balance" in yesterday_saved
                and abs(alloc_ia) <= ARREARS_ZERO_TOLERANCE
                and not due_today
            ):
                interest_accrued_balance = max(
                    0.0,
                    float(yesterday_saved.get("interest_accrued_balance", 0) or 0) + rd_add,
                )
            else:
                interest_accrued_balance = _today_balance(
                    "interest_accrued_balance",
                    float(engine_loan.interest_accrued_balance),
                    engine_yesterday["interest_accrued_balance"],
                    "alloc_interest_accrued",
                )
            interest_arrears_balance = _today_balance("interest_arrears_balance", float(engine_loan.interest_arrears), engine_yesterday["interest_arrears"], "alloc_interest_arrears")
            default_interest_balance = _today_balance("default_interest_balance", float(engine_loan.default_interest_balance), engine_yesterday["default_interest_balance"], "alloc_default_interest")
            penalty_interest_balance = _today_balance("penalty_interest_balance", float(engine_loan.penalty_interest_balance), engine_yesterday["penalty_interest_balance"], "alloc_penalty_interest")
            fees_charges_balance = _today_balance("fees_charges_balance", float(engine_loan.fees_charges_balance), engine_yesterday["fees_charges_balance"], "alloc_fees_charges")

            # Non-due-date guard: arrears principal/interest must only move by persisted allocations.
            # This prevents hidden drift from engine/session recomputation on dates without due transitions.
            # Period-to-date resets: any contractual due on any saved version (not latest only).
            due_yesterday = yesterday in all_schedule_due_dates_map.get(loan_id_int, frozenset())
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

            # Auto-Suspense Logic
            suspense_logic = effective_cfg.get("suspension_logic", "Manual")
            suspense_days = int(effective_cfg.get("suspension_auto_days", 90))
            is_in_suspense = loan_row.get("interest_in_suspense", False)
        
            if suspense_logic == "Automatic" and not is_in_suspense and days_overdue_save >= suspense_days:
                # Same DB transaction as batch `save_loan_daily_state` (one commit when the engine step exits).
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE loans SET interest_in_suspense = TRUE WHERE id = %s",
                            (loan_id_int,),
                        )
                    is_in_suspense = True
                except Exception as e:
                    _logger.warning("Failed to auto-flag loan %s for suspense: %s", loan_id_int, e)

            # Grace period: only accrue default/penalty when *saved* days_overdue > grace_period_days.
            # When no arrears or within grace (or if accruals are blocked for the day), persist 0.
            grace_days = config.grace_period_days
            within_grace_or_current = no_arrears or (days_overdue_save <= grace_days)

            if within_grace_or_current or block_accruals:
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

            # Interest in suspense (provision reporting): regular rolls as prior + today's accrual
            # (only when loan is in suspense and accruals run) − allocations to accrued interest;
            # penalty/default suspense track the same closing balances as the economic buckets after EOD.
            D = Decimal
            y_reg_susp = (
                D(str(yesterday_saved.get("regular_interest_in_suspense_balance", 0) or 0))
                if yesterday_saved is not None
                else D("0")
            )
            alloc_ia = D(str(alloc.get("alloc_interest_accrued", 0.0)))
            rd_dec = regular_daily if isinstance(regular_daily, Decimal) else D(str(regular_daily))
            if block_accruals:
                acc_into_reg_susp = D("0")
            else:
                acc_into_reg_susp = rd_dec if is_in_suspense else D("0")
            regular_interest_in_suspense_save = float(
                as_10dp(max(D("0"), y_reg_susp + acc_into_reg_susp - alloc_ia))
            )
            penalty_interest_in_suspense_save = float(
                as_10dp(max(D("0"), D(str(penalty_interest_balance_save))))
            )
            default_interest_in_suspense_save = float(
                as_10dp(max(D("0"), D(str(default_interest_balance_save))))
            )

            pc_raw = loan_row.get("product_code")
            resume_to_save = serialize_engine_resume(
                engine_loan,
                product_code=str(pc_raw).strip() if pc_raw else None,
            )

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
                regular_interest_in_suspense_balance=regular_interest_in_suspense_save,
                penalty_interest_in_suspense_balance=penalty_interest_in_suspense_save,
                default_interest_in_suspense_balance=default_interest_in_suspense_save,
                engine_resume=resume_to_save,
                conn=conn,
            )
            processed += 1
            if incremental_path_for_loan:
                n_incremental_path += 1
            else:
                n_full_replay_path += 1
            compute_s += time.perf_counter() - t_loan0
            loans_since_commit += 1
            if loans_since_commit >= commit_batch_size:
                _do_commit()

        _do_commit()

    if processed > 0:
        inc_pct = 100.0 * n_incremental_path / processed
        _logger.info(
            "EOD loan_engine as_of=%s processed=%s incremental=%s full_replay=%s "
            "(incremental_pct=%.1f) commit_batch_size=%s",
            as_of_date.isoformat(),
            processed,
            n_incremental_path,
            n_full_replay_path,
            inc_pct,
            commit_batch_size,
        )
    if log_engine_timing:
        _logger.info(
            "EOD loan_engine timing detail as_of=%s processed=%s prefetch_s=%.3f compute_s=%.3f "
            "commit_s=%.3f wall_s=%.3f commit_batch_size=%s",
            as_of_date.isoformat(),
            processed,
            prefetch_s,
            compute_s,
            commit_s,
            time.perf_counter() - t_engine_wall0,
            commit_batch_size,
        )

    return processed


def _apply_unapplied_funds_to_arrears(as_of_date: date, sys_cfg: Dict[str, Any]) -> int:
    """
    For each loan with unapplied balance > 0 and arrears > 0, allocate unapplied
    towards arrears (waterfall order). Creates allocation with event_type='unapplied_funds_allocation'.
    Returns number of loans that had funds applied.
    """
    t0 = time.perf_counter()
    loan_ids = get_loan_ids_with_unapplied_balance_and_arrears_for_eod(as_of_date)
    fetch_s = time.perf_counter() - t0
    applied_count = 0
    t_loop0 = time.perf_counter()
    for loan_id in loan_ids:
        amount = apply_unapplied_funds_to_arrears_eod(loan_id, as_of_date, sys_cfg)
        if amount > 0:
            applied_count += 1
    loop_s = time.perf_counter() - t_loop0
    if _trace_engine_timing_enabled(sys_cfg):
        _logger.info(
            "EOD apply_unapplied_to_arrears detail as_of=%s candidate_loans=%s applied_loans=%s "
            "t_fetch_candidates_s=%.3f t_loop_s=%.3f",
            as_of_date,
            len(loan_ids),
            applied_count,
            fetch_s,
            loop_s,
        )
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


def _activate_scheduled_receipts_stage(
    as_of_date: date,
    sys_cfg: Dict[str, Any],
    *,
    allow_system_date_eod: bool,
) -> int:
    """Post loan_engine: flip scheduled → posted and allocate (GL on value date only)."""
    from loan_management.scheduled_receipts import activate_scheduled_receipts_for_eod_date

    n, errs = activate_scheduled_receipts_for_eod_date(
        as_of_date,
        sys_cfg,
        allow_system_date_eod=allow_system_date_eod,
    )
    if errs:
        raise RuntimeError("activate_scheduled_receipts failed: " + "; ".join(errs[:25]))
    if n:
        _logger.info(
            "EOD activate_scheduled_receipts as_of=%s activated=%s receipt(s).",
            as_of_date,
            n,
        )
    return n


def _replay_refresh_allocations_for_date(as_of_date: date, sys_cfg: Dict[str, Any]) -> int:
    """
    Replay/backfill only: refresh allocation rows and GL for every posted receipt on as_of_date.

    Normal receipts use :func:`reallocate_repayment` (engine-aware opening balances).
    System liquidations (reference ``Unapplied funds allocation``) use
    :func:`allocate_repayment_waterfall` only — they must not go through reallocate.

    GL idempotency relies on :meth:`accounting.dal.AccountingRepository.save_journal_entry`
    and stable ``(event_id, event_tag)`` keys from reallocation / waterfall paths.
    Per-receipt failures are logged and skipped so one bad ID does not abort the batch.
    """
    touched = 0
    cleared = _clear_unapplied_liquidations_for_date(as_of_date)
    if cleared:
        _logger.info(
            "replay_refresh_allocations: cleared %s prior unapplied liquidation repayment(s) on %s.",
            cleared,
            as_of_date,
        )
    for rid in get_repayment_ids_for_value_date(as_of_date):
        try:
            reallocate_repayment(rid, system_config=sys_cfg)
            touched += 1
        except Exception as ex:
            _logger.warning(
                "replay_refresh_allocations: reallocate_repayment failed for repayment_id=%s: %s",
                rid,
                ex,
            )
    return touched


def _clear_unapplied_liquidations_for_date(as_of_date: date) -> int:
    """
    Delete prior system liquidation artifacts for value_date before replay refresh.

    Why: replay previously called ``allocate_repayment_waterfall`` for liquidation
    repayment IDs, but that function returns early for negative repayments, leaving
    stale liquidation allocation rows in place. Those stale rows contaminate
    ``alloc_map`` in ``_run_loan_engine_for_date`` and can mask due-date interest
    arrears billing.

    During replay/backfill we rebuild liquidations in the later
    ``apply_unapplied_to_arrears`` stage, so clearing historical system legs for the
    date keeps the run deterministic and policy-aligned.
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM loan_repayments
                WHERE status = 'posted'
                  AND COALESCE(reference, '') = 'Unapplied funds allocation'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY id
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()
            ids: list[int] = []
            for r in rows:
                if isinstance(r, dict):
                    rid = r.get("id")
                else:
                    rid = r[0] if r else None
                if rid is None:
                    continue
                ids.append(int(rid))
            if not ids:
                return 0
            cur.execute(
                """
                DELETE FROM loan_repayment_allocation
                WHERE repayment_id = ANY(%s)
                """,
                (ids,),
            )
            cur.execute(
                """
                DELETE FROM unapplied_funds
                WHERE allocation_repayment_id = ANY(%s)
                  AND entry_type = 'debit'
                """,
                (ids,),
            )
            cur.execute(
                """
                DELETE FROM loan_repayments
                WHERE id = ANY(%s)
                """,
                (ids,),
            )
        conn.commit()
    return len(ids)


def _repost_gl_after_replay_for_date(as_of_date: date) -> int:
    """
    Replay/backfill only: re-post deterministic GL journals for affected loans on date.

    Replay can rebuild allocations/unapplied subledger rows while leaving previously posted
    journals that no longer match those rows. Re-posting converges GL to current subledger
    because journal writes are idempotent by deterministic ``(event_id, event_tag)`` keys.
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT loan_id
                FROM loan_repayments
                WHERE (COALESCE(value_date, payment_date))::date = %s::date
                  AND status IN ('posted', 'reversed')
                ORDER BY loan_id
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()
            loan_ids: list[int] = []
            for r in rows:
                if isinstance(r, dict):
                    lid = r.get("loan_id")
                else:
                    lid = r[0] if r else None
                if lid is None:
                    continue
                loan_ids.append(int(lid))
    if not loan_ids:
        return 0

    failures: list[tuple[int, str]] = []
    reposted = 0
    for loan_id in loan_ids:
        try:
            repost_gl_for_loan_date_range(loan_id, as_of_date, as_of_date, created_by="system")
            reposted += 1
        except Exception as ex:
            failures.append((loan_id, str(ex)))

    if failures:
        first = failures[0]
        raise RuntimeError(
            "Replay GL repost failed for "
            f"{len(failures)} loan(s) on {as_of_date}. "
            f"First failure loan_id={first[0]}: {first[1]}"
        )
    return reposted


def _run_eom_regular_interest_income_recognition(
    as_of_date: date,
    period_cfg: Any,
    events_to_run: set[str],
    svc: Any,
) -> int:
    """
    Accounting month-end: Dr regular_interest_income_holding / Cr regular_interest_income
    for SUM(regular_interest_daily) over the period (loans not in interest_in_suspense).

    Returns the number of journals bulk-posted (0 if skipped or nothing to post).
    """
    if "EOM_REGULAR_INTEREST_INCOME_RECOGNITION" not in events_to_run:
        return 0
    bounds = get_month_period_bounds(as_of_date, period_cfg)
    period_key = bounds.end_date.strftime("%Y-%m")
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT lds.loan_id,
                       SUM(COALESCE(lds.regular_interest_daily, 0)) AS reg_mtd
                FROM loan_daily_state lds
                INNER JOIN loans l ON l.id = lds.loan_id
                WHERE lds.as_of_date >= %s
                  AND lds.as_of_date <= %s
                  AND COALESCE(l.interest_in_suspense, FALSE) = FALSE
                GROUP BY lds.loan_id
                HAVING SUM(COALESCE(lds.regular_interest_daily, 0)) > 0
                """,
                (bounds.start_date, bounds.end_date),
            )
            m_rows = cur.fetchall()

    bulk_items: List[Dict[str, Any]] = []
    for r in m_rows:
        loan_id = r["loan_id"]
        amt = Decimal(str(r["reg_mtd"] or 0))
        if amt <= 0:
            continue
        event_id = f"EOM-REGINT-{period_key}-LOAN-{loan_id}"
        bulk_items.append(
            {
                "event_type": "EOM_REGULAR_INTEREST_INCOME_RECOGNITION",
                "reference": f"EOM-{as_of_date}-LOAN-{loan_id}-REGINT",
                "description": (
                    f"EOM regular interest income recognition for Loan {loan_id} ({period_key})"
                ),
                "event_id": event_id,
                "created_by": "system",
                "entry_date": as_of_date,
                "amount": amt,
                "loan_id": int(loan_id),
            }
        )
    if bulk_items:
        svc.bulk_post_events(bulk_items)
    return len(bulk_items)


def _run_eom_creditor_interest_expense_accrual(
    as_of_date: date,
    period_cfg: Any,
    events_to_run: set[str],
    svc: Any,
) -> int:
    """
    Month-end creditor interest expense:
    - ``daily_mirror``: SUM(regular_interest_daily) from ``creditor_loan_daily_state`` in the month.
    - ``periodic_schedule``: scheduled interest for installments dated in the month.
    """
    if "INTEREST_EXPENSE_ACCRUAL" not in events_to_run:
        return 0
    bounds = get_month_period_bounds(as_of_date, period_cfg)
    period_key = bounds.end_date.strftime("%Y-%m")
    from creditor_loans.creditor_eom_interest import periodic_scheduled_interest_in_calendar_month

    bulk_items: List[Dict[str, Any]] = []
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cds.creditor_drawdown_id,
                       cl.creditor_facility_id,
                       SUM(COALESCE(cds.regular_interest_daily, 0)) AS reg_mtd
                FROM creditor_loan_daily_state cds
                INNER JOIN creditor_drawdowns cl ON cl.id = cds.creditor_drawdown_id
                WHERE cds.as_of_date >= %s
                  AND cds.as_of_date <= %s
                  AND cl.status = 'active'
                  AND cl.accrual_mode = 'daily_mirror'
                GROUP BY cds.creditor_drawdown_id, cl.creditor_facility_id
                HAVING SUM(COALESCE(cds.regular_interest_daily, 0)) > 0
                """,
                (bounds.start_date, bounds.end_date),
            )
            m_rows = cur.fetchall()

            cur.execute(
                """
                SELECT id, creditor_facility_id
                FROM creditor_drawdowns
                WHERE status = 'active' AND accrual_mode = 'periodic_schedule'
                """
            )
            periodic_rows = cur.fetchall()

        with conn.cursor() as cur:
            for pr in periodic_rows:
                dd_id = int(pr["id"])
                cf_id = int(pr["creditor_facility_id"])
                amt = periodic_scheduled_interest_in_calendar_month(
                    cur, dd_id, bounds.start_date, bounds.end_date
                )
                if amt <= 0:
                    continue
                event_id = f"EOM-INTEXP-{period_key}-CL-{dd_id}-PER"
                bulk_items.append(
                    {
                        "event_type": "INTEREST_EXPENSE_ACCRUAL",
                        "reference": f"EOM-{as_of_date}-CL-{dd_id}-INTEXP",
                        "description": (
                            f"EOM interest expense (periodic schedule) drawdown {dd_id} ({period_key})"
                        ),
                        "event_id": event_id,
                        "created_by": "system",
                        "entry_date": as_of_date,
                        "amount": amt,
                        "creditor_drawdown_id": dd_id,
                        "creditor_facility_id": cf_id,
                    }
                )

    for r in m_rows:
        dd_id = int(r["creditor_drawdown_id"])
        cf_id = int(r["creditor_facility_id"])
        amt = Decimal(str(r["reg_mtd"] or 0))
        if amt <= 0:
            continue
        event_id = f"EOM-INTEXP-{period_key}-CL-{dd_id}-DM"
        bulk_items.append(
            {
                "event_type": "INTEREST_EXPENSE_ACCRUAL",
                "reference": f"EOM-{as_of_date}-CL-{dd_id}-INTEXP",
                "description": (
                    f"EOM interest expense accrual (daily mirror) drawdown {dd_id} ({period_key})"
                ),
                "event_id": event_id,
                "created_by": "system",
                "entry_date": as_of_date,
                "amount": amt,
                "creditor_drawdown_id": dd_id,
                "creditor_facility_id": cf_id,
            }
        )
    if bulk_items:
        svc.bulk_post_events(bulk_items)
    return len(bulk_items)


def _run_eom_creditor_borrowing_fees_amortisation(
    as_of_date: date,
    period_cfg: Any,
    events_to_run: set[str],
    svc: Any,
) -> int:
    """Straight-line monthly amortisation of drawdown fees (BORROWING_FEES_AMORTISATION)."""
    if "BORROWING_FEES_AMORTISATION" not in events_to_run:
        return 0
    bounds = get_month_period_bounds(as_of_date, period_cfg)
    period_key = bounds.end_date.strftime("%Y-%m")
    bulk_items: List[Dict[str, Any]] = []
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, creditor_facility_id, drawdown_fee_amount,
                       COALESCE(NULLIF(term, 0), 1) AS term_m
                FROM creditor_drawdowns
                WHERE status = 'active' AND drawdown_fee_amount > 0
                """
            )
            rows = cur.fetchall()
    for r in rows:
        dd_id = int(r["id"])
        cf_id = int(r["creditor_facility_id"])
        fee = Decimal(str(r["drawdown_fee_amount"] or 0))
        term_m = max(1, int(r["term_m"] or 1))
        monthly = (fee / Decimal(term_m)).quantize(Decimal("0.0000000001"))
        if monthly <= 0:
            continue
        event_id = f"EOM-BFA-{period_key}-CL-{dd_id}"
        bulk_items.append(
            {
                "event_type": "BORROWING_FEES_AMORTISATION",
                "reference": f"EOM-{as_of_date}-CL-{dd_id}-FEE",
                "description": f"Creditor drawdown fee amortisation CL-{dd_id} ({period_key})",
                "event_id": event_id,
                "created_by": "system",
                "entry_date": as_of_date,
                "amount": monthly,
                "creditor_drawdown_id": dd_id,
                "creditor_facility_id": cf_id,
            }
        )
    if bulk_items:
        svc.bulk_post_events(bulk_items)
    return len(bulk_items)


def _run_eom_creditor_facility_fee_amortisation(
    as_of_date: date,
    period_cfg: Any,
    events_to_run: set[str],
    svc: Any,
) -> int:
    """Amortise facility-level deferred fees (uses same templates as drawdown fee amort)."""
    if "BORROWING_FEES_AMORTISATION" not in events_to_run:
        return 0

    bounds = get_month_period_bounds(as_of_date, period_cfg)
    period_key = bounds.end_date.strftime("%Y-%m")
    bulk_items: List[Dict[str, Any]] = []
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, facility_fee_amount, facility_expiry_date, created_at::date AS started
                FROM creditor_facilities
                WHERE status = 'active' AND facility_fee_amount > 0
                """
            )
            rows = cur.fetchall()
    for r in rows:
        fid = int(r["id"])
        fee = Decimal(str(r["facility_fee_amount"] or 0))
        start_d = r["started"]
        exp = r["facility_expiry_date"]
        if not exp or not start_d:
            continue
        if isinstance(exp, str):
            from datetime import datetime as _dt

            exp = _dt.fromisoformat(str(exp)[:10]).date()
        if hasattr(start_d, "date"):
            start_d = start_d.date()
        months = max(
            1,
            (exp.year - start_d.year) * 12 + (exp.month - start_d.month) + 1,
        )
        monthly = (fee / Decimal(months)).quantize(Decimal("0.0000000001"))
        if monthly <= 0:
            continue
        event_id = f"EOM-BFF-{period_key}-CF-{fid}"
        bulk_items.append(
            {
                "event_type": "BORROWING_FEES_AMORTISATION",
                "reference": f"EOM-{as_of_date}-CF-{fid}-FEE",
                "description": f"Creditor facility fee amortisation CF-{fid} ({period_key})",
                "event_id": event_id,
                "created_by": "system",
                "entry_date": as_of_date,
                "amount": monthly,
                "creditor_facility_id": fid,
                "creditor_drawdown_id": None,
            }
        )
    if bulk_items:
        svc.bulk_post_events(bulk_items)
    return len(bulk_items)


def _run_accounting_events(as_of_date: date, sys_cfg: Dict[str, Any]) -> None:
    """
    Posts accounting journals for end of day activities based on transaction_templates.
    Dynamically executes templates marked with 'EOD', and 'EOM' if it is the last day of the
    accounting month.

    Regular interest: daily Dr/Cr accrued vs holding (ACCRUAL_REGULAR_INTEREST); on billing
    movement Dr arrears / Cr accrued (REGULAR_INTEREST_BILLING_RECEIVABLE); at month-end
    Dr holding / Cr income (EOM_REGULAR_INTEREST_INCOME_RECOGNITION) for MTD reg_daily sum.
    """
    is_month_end = False
    events_to_run: set[str] = set()
    svc = None
    try:
        from accounting.service import AccountingService
        from decimal import Decimal
        from datetime import timedelta
        
        svc = AccountingService()
        yesterday = as_of_date - timedelta(days=1)
        t0 = time.perf_counter()
        t_events = 0.0
        t_fetch = 0.0
        t_build = 0.0
        t_post = 0.0
        n_post_items = 0
        by_event_type: Dict[str, int] = {}
        
        period_cfg = normalize_accounting_period_config(sys_cfg)
        is_month_end = is_eom(as_of_date, period_cfg)
        
        with _get_conn() as conn:
            with conn.cursor() as cur:
                # 1. Get events to run today
                triggers = ['EOD']
                if is_month_end:
                    triggers.append('EOM')
                t_events0 = time.perf_counter()
                cur.execute("SELECT DISTINCT event_type FROM transaction_templates WHERE trigger_type = ANY(%s)", (triggers,))
                events = cur.fetchall()
                events_to_run = {row['event_type'] for row in events}
                t_events = time.perf_counter() - t_events0

                if not events_to_run:
                    return # Nothing to run
                
                # 2. Query data needed for amounts
                t_fetch0 = time.perf_counter()
                cur.execute("""
                    SELECT DISTINCT ON (t.loan_id)
                           t.loan_id, t.regular_interest_daily, t.penalty_interest_daily, t.default_interest_daily,
                           t.principal_arrears as t_prin_arr, y.principal_arrears as y_prin_arr,
                           t.interest_arrears_balance as t_int_arr, y.interest_arrears_balance as y_int_arr,
                           COALESCE(a.alloc_principal_arrears, 0) as alloc_prin_arr,
                           COALESCE(a.alloc_interest_arrears, 0) as alloc_int_arr,
                           COALESCE(a.alloc_interest_accrued, 0) as alloc_int_accrued,
                           l.interest_in_suspense
                    FROM loan_daily_state t
                    JOIN loans l ON t.loan_id = l.id
                    LEFT JOIN loan_daily_state y ON t.loan_id = y.loan_id AND y.as_of_date = %s
                    LEFT JOIN (
                        SELECT lr.loan_id, 
                               SUM(lra.alloc_principal_arrears) as alloc_principal_arrears,
                               SUM(lra.alloc_interest_arrears) as alloc_interest_arrears,
                               SUM(lra.alloc_interest_accrued) as alloc_interest_accrued
                        FROM loan_repayments lr
                        JOIN loan_repayment_allocation lra ON lr.id = lra.repayment_id
                        WHERE COALESCE(lr.value_date, lr.payment_date) = %s
                        GROUP BY lr.loan_id
                    ) a ON t.loan_id = a.loan_id
                    WHERE t.as_of_date = %s
                    ORDER BY t.loan_id, t.id DESC
                """, (yesterday, as_of_date, as_of_date))
                
                rows = cur.fetchall()
                t_fetch = time.perf_counter() - t_fetch0
        
        # Build items and bulk post in a single transaction (huge speedup vs per-loan post_event connections).
        bulk_items: List[Dict[str, Any]] = []
        t_build0 = time.perf_counter()
        
        for row in rows:
            loan_id = row["loan_id"]
            is_in_suspense = row.get("interest_in_suspense", False)
            
            # Calculate amounts
            reg_daily = Decimal(str(row["regular_interest_daily"] or 0))
            pen_daily = Decimal(str(row["penalty_interest_daily"] or 0))
            def_daily = Decimal(str(row["default_interest_daily"] or 0))
            
            y_prin_arr = Decimal(str(row["y_prin_arr"] or 0))
            t_prin_arr = Decimal(str(row["t_prin_arr"] or 0))
            alloc_prin_arr = Decimal(str(row["alloc_prin_arr"] or 0))
            billed_prin = t_prin_arr - y_prin_arr + alloc_prin_arr
            
            y_int_arr = Decimal(str(row["y_int_arr"] or 0))
            t_int_arr = Decimal(str(row["t_int_arr"] or 0))
            alloc_int_arr = Decimal(str(row["alloc_int_arr"] or 0))
            billed_int = t_int_arr - y_int_arr + alloc_int_arr

            # Map amounts to dynamic events
            amounts_map = {
                "ACCRUAL_PENALTY_INTEREST": pen_daily,
                "ACCRUAL_DEFAULT_INTEREST": def_daily,
                "BILLING_PRINCIPAL_ARREARS": billed_prin,
                "REGULAR_INTEREST_BILLING_RECEIVABLE": billed_int,
            }
            
            # Branch accrual logic based on suspense flag
            if is_in_suspense:
                amounts_map["ACCRUAL_REGULAR_INTEREST_SUSPENSE"] = reg_daily
            else:
                amounts_map["ACCRUAL_REGULAR_INTEREST"] = reg_daily
            for event_type in events_to_run:
                amt = amounts_map.get(event_type, Decimal('0'))
                if amt > 0:
                    desc_parts = event_type.replace('_', ' ').title()
                    bulk_items.append(
                        {
                            "event_type": event_type,
                            "reference": f"EOD-{as_of_date}-LOAN-{loan_id}",
                            "description": f"{desc_parts} for Loan {loan_id}",
                            "event_id": f"EOD-{as_of_date}-{loan_id}-{event_type}",
                            "created_by": "system",
                            "entry_date": as_of_date,
                            "amount": amt,
                            "loan_id": int(loan_id),
                        }
                    )
                    by_event_type[event_type] = by_event_type.get(event_type, 0) + 1

        t_build = time.perf_counter() - t_build0

        t_post0 = time.perf_counter()
        if bulk_items:
            svc.bulk_post_events(bulk_items)
            n_post_items = len(bulk_items)
        t_post = time.perf_counter() - t_post0

        n_eom_regint = 0
        t_eom_regint_s = 0.0
        if is_month_end and svc is not None and events_to_run:
            t_eom0 = time.perf_counter()
            n_eom_regint = _run_eom_regular_interest_income_recognition(
                as_of_date, period_cfg, events_to_run, svc
            )
            t_eom_regint_s = time.perf_counter() - t_eom0
            t_eom_cl0 = time.perf_counter()
            n_eom_creditor = _run_eom_creditor_interest_expense_accrual(
                as_of_date, period_cfg, events_to_run, svc
            )
            t_eom_creditor_s = time.perf_counter() - t_eom_cl0
            t_eom_cl_fee0 = time.perf_counter()
            n_eom_creditor_dd_fee = _run_eom_creditor_borrowing_fees_amortisation(
                as_of_date, period_cfg, events_to_run, svc
            )
            n_eom_creditor_cf_fee = _run_eom_creditor_facility_fee_amortisation(
                as_of_date, period_cfg, events_to_run, svc
            )
            t_eom_creditor_fee_s = time.perf_counter() - t_eom_cl_fee0
            _logger.info(
                "EOD accounting_events eom_creditor as_of=%s intexp_items=%s intexp_t_s=%.3f "
                "dd_fee_items=%s facility_fee_items=%s fee_t_s=%.3f",
                as_of_date.isoformat(),
                n_eom_creditor,
                t_eom_creditor_s,
                n_eom_creditor_dd_fee,
                n_eom_creditor_cf_fee,
                t_eom_creditor_fee_s,
            )
        _logger.info(
            "EOD accounting_events detail as_of=%s is_month_end=%s events_to_run=%s rows=%s posted_items=%s "
            "t_events_s=%.3f t_fetch_s=%.3f t_build_s=%.3f t_post_s=%.3f wall_s=%.3f",
            as_of_date.isoformat(),
            bool(is_month_end),
            len(events_to_run),
            len(rows) if 'rows' in locals() else 0,
            n_post_items,
            t_events,
            t_fetch,
            t_build,
            t_post,
            time.perf_counter() - t0,
        )
        if is_month_end:
            _logger.info(
                "EOD accounting_events eom_bulk as_of=%s eom_regint_items=%s t_eom_regint_s=%.3f",
                as_of_date.isoformat(),
                n_eom_regint,
                t_eom_regint_s,
            )
        if by_event_type:
            top = sorted(by_event_type.items(), key=lambda kv: kv[1], reverse=True)[:10]
            _logger.info(
                "EOD accounting_events top_event_counts as_of=%s %s",
                as_of_date.isoformat(),
                ", ".join(f"{k}={v}" for k, v in top),
            )

    except Exception:
        # Must propagate: otherwise hybrid/strict treat this stage as OK and the system
        # date can advance with incomplete GL (accounting_events is blocking by default).
        _logger.exception("Failed to post EOD accounting events for %s", as_of_date)
        raise

    # Month-end fee amortisation (loan origination fees).
    if is_month_end and svc is not None and events_to_run:
        t_fee0 = time.perf_counter()
        n_fee_amort = _run_fee_amortisation_month_end(as_of_date, events_to_run, svc)
        t_fee_amort_s = time.perf_counter() - t_fee0
        t_rest0 = time.perf_counter()
        n_restructure = _run_restructure_fee_amortisation_month_end(
            as_of_date, events_to_run, svc
        )
        t_restructure_s = time.perf_counter() - t_rest0
        t_comm0 = time.perf_counter()
        n_comm_amort = 0
        if "COMMISSION_AMORTISATION" in events_to_run:
            try:
                from loan_management.application_pipeline import recognise_agent_commission_eom

                n_comm_amort = recognise_agent_commission_eom(
                    as_of_date=as_of_date,
                    created_by="system",
                )
            except Exception:
                _logger.exception(
                    "EOM commission amortisation failed for %s",
                    as_of_date.isoformat(),
                )
                raise
        t_comm_amort_s = time.perf_counter() - t_comm0
        _logger.info(
            "EOD accounting_events eom_fee_restructure as_of=%s fee_amort_items=%s t_fee_amort_s=%.3f "
            "restructure_items=%s t_restructure_s=%.3f commission_items=%s t_commission_s=%.3f",
            as_of_date.isoformat(),
            n_fee_amort,
            t_fee_amort_s,
            n_restructure,
            t_restructure_s,
            n_comm_amort,
            t_comm_amort_s,
        )


def _run_fee_amortisation_month_end(
    as_of_date: date,
    events_to_run: set[str],
    svc,
) -> int:
    """
    Straight-line month-end amortisation of origination fees
    (drawdown, arrangement, admin) using loan-level fee columns.

    Uses templates:
    - FEE_AMORTISATION_DRAWDOWN (2)
    - FEE_AMORTISATION_ARRANGEMENT (2a)
    - FEE_AMORTISATION_ADMIN (2b)
    """
    # Only run if at least one of the templates is active.
    needed_events = {
        "FEE_AMORTISATION_DRAWDOWN",
        "FEE_AMORTISATION_ARRANGEMENT",
        "FEE_AMORTISATION_ADMIN",
    }
    active = needed_events.intersection(events_to_run)
    if not active:
        return 0

    with _get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # For each active loan, compute total origination fees.
            # Prefer explicit *_amount columns when present; fall back to
            # percentage * principal/facility.
            cur.execute(
                """
                SELECT
                    id,
                    term,
                    COALESCE(drawdown_fee_amount,
                             (principal * drawdown_fee)) AS drawdown_fee_amt,
                    COALESCE(arrangement_fee_amount,
                             (principal * arrangement_fee)) AS arrangement_fee_amt,
                    COALESCE(admin_fee_amount,
                             (principal * admin_fee)) AS admin_fee_amt
                FROM loans
                -- Only loans live on or before as_of (same idea as _run_loan_engine_for_date;
                -- missing dates follow engine fallback to as_of, so they stay eligible).
                WHERE status = 'active'
                  AND (
                      COALESCE(disbursement_date, start_date) IS NULL
                      OR COALESCE(disbursement_date, start_date)::date <= %s::date
                  )
                """,
                (as_of_date,),
            )
            loans = cur.fetchall()

    bulk_items: List[Dict[str, Any]] = []

    for row in loans:
        loan_id = int(row["id"])
        term = int(row.get("term") or 0)
        if term <= 0:
            continue

        draw_fee = float(as_10dp(row.get("drawdown_fee_amt") or 0))
        arr_fee = float(as_10dp(row.get("arrangement_fee_amt") or 0))
        adm_fee = float(as_10dp(row.get("admin_fee_amt") or 0))

        def _append_if_positive(event_type: str, component_fee: float, label: str) -> None:
            if event_type not in active:
                return
            if component_fee <= 0:
                return
            monthly_amt = float(as_10dp(component_fee / term))
            if monthly_amt <= 0:
                return
            bulk_items.append(
                {
                    "event_type": event_type,
                    "reference": f"LOAN-{loan_id}",
                    "description": f"Monthly {label} amortisation for Loan {loan_id}",
                    "event_id": f"EOM-{as_of_date}-LOAN-{loan_id}-{event_type}",
                    "created_by": "system",
                    "entry_date": as_of_date,
                    "amount": monthly_amt,
                    "loan_id": int(loan_id),
                }
            )

        _append_if_positive("FEE_AMORTISATION_DRAWDOWN", draw_fee, "drawdown fee")
        _append_if_positive("FEE_AMORTISATION_ARRANGEMENT", arr_fee, "arrangement fee")
        _append_if_positive("FEE_AMORTISATION_ADMIN", adm_fee, "administration fee")

    if bulk_items:
        svc.bulk_post_events(bulk_items)
    return len(bulk_items)


def _run_restructure_fee_amortisation_month_end(
    as_of_date: date,
    events_to_run: set[str],
    svc,
) -> int:
    """Straight-line month-end amortisation for restructure fee charges."""
    if "RESTRUCTURE_FEE_AMORTISATION" not in events_to_run:
        return 0
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (lm.loan_id)
                    lm.id,
                    lm.loan_id,
                    lm.modification_date,
                    COALESCE(lm.restructure_fee_amount, 0) AS restructure_fee_amount,
                    COALESCE(lm.new_term, 0) AS new_term
                FROM loan_modifications lm
                JOIN loans l ON l.id = lm.loan_id
                WHERE l.status = 'active'
                  AND COALESCE(lm.restructure_fee_amount, 0) > 0
                ORDER BY lm.loan_id, lm.modification_date DESC, lm.id DESC
                """
            )
            rows = cur.fetchall()

    bulk_items: List[Dict[str, Any]] = []
    for row in rows:
        loan_id = int(row["loan_id"])
        fee_amt = float(as_10dp(row.get("restructure_fee_amount") or 0))
        term = int(row.get("new_term") or 0)
        mod_date = row.get("modification_date")
        if hasattr(mod_date, "date"):
            mod_date = mod_date.date()
        if fee_amt <= 0 or term <= 0 or mod_date is None:
            continue
        if (as_of_date.year, as_of_date.month) < (mod_date.year, mod_date.month):
            continue
        months_elapsed = (as_of_date.year - mod_date.year) * 12 + (as_of_date.month - mod_date.month) + 1
        if months_elapsed < 1 or months_elapsed > term:
            continue
        monthly_amt = float(as_10dp(fee_amt / term))
        if monthly_amt <= 0:
            continue
        bulk_items.append(
            {
                "event_type": "RESTRUCTURE_FEE_AMORTISATION",
                "reference": f"LOAN-{loan_id}",
                "description": f"Monthly restructure fee amortisation for Loan {loan_id}",
                "event_id": f"EOM-{as_of_date}-LOAN-{loan_id}-RESTRUCTURE_FEE_AMORTISATION-{int(row['id'])}",
                "created_by": "system",
                "entry_date": as_of_date,
                "amount": monthly_amt,
                "loan_id": int(loan_id),
            }
        )
    if bulk_items:
        svc.bulk_post_events(bulk_items)
    return len(bulk_items)


def _run_equity_period_close(as_of_date: date, sys_cfg: Dict[str, Any]) -> None:
    """
    After operational EOM journals, close P&L into current year earnings (month-end)
    and sweep CYE to retained earnings (fiscal year-end). Runs **before** statement
    snapshots so TB/BS reflect closed nominal accounts where applicable.

    Order on a day that is both month-end and year-end: month P&L close first, then CYE→RE.
    """
    from accounting.service import AccountingService

    period_cfg = normalize_accounting_period_config(sys_cfg)
    if not (is_eom(as_of_date, period_cfg) or is_eoy(as_of_date, period_cfg)):
        return
    svc = AccountingService()
    if is_eom(as_of_date, period_cfg):
        svc.post_month_end_pnl_close_to_cye(
            as_of_date, created_by="system", system_config=sys_cfg
        )
    if is_eoy(as_of_date, period_cfg):
        svc.post_year_end_cye_to_re(
            as_of_date, created_by="system", system_config=sys_cfg
        )


def _run_statement_batch(as_of_date: date, sys_cfg: Dict[str, Any]) -> None:
    """
    Capture immutable statement snapshots on accounting month/year close.
    """
    period_cfg = normalize_accounting_period_config(sys_cfg)
    if not (is_eom(as_of_date, period_cfg) or is_eoy(as_of_date, period_cfg)):
        return
    from accounting.service import AccountingService

    svc = AccountingService()
    svc.save_period_close_snapshots(as_of_date=as_of_date, generated_by="system")


def _run_notification_batch(as_of_date: date, sys_cfg: Dict[str, Any]) -> None:
    """
    Placeholder for future alerting/notification processing (e.g. SMS/email).
    """
    _ = (as_of_date, sys_cfg)


def _run_creditor_loan_engine_stage(
    as_of_date: date,
    sys_cfg: Dict[str, Any],
    *,
    allow_system_date_eod: bool,
) -> int:
    """Persist creditor mirror daily state (separate connection; commits with caller policy)."""
    from creditor_loans.eod_engine import run_creditor_loans_engine_for_date

    with _get_conn() as conn:
        return run_creditor_loans_engine_for_date(
            conn, as_of_date, sys_cfg, allow_system_date_eod=allow_system_date_eod
        )


def run_eod_for_date(
    as_of_date: date,
    *,
    skip_reallocate_after_reversals: bool = False,
    allow_system_date_eod: bool = False,
    replay_refresh_allocations: bool = False,
) -> EODResult:
    """
    Orchestrate EOD for a given calendar date.

    The exact *sequence* of steps is fixed in code for safety and auditability,
    but which high-level tasks are enabled is controlled by system configuration
    (system_config.eod_settings.tasks).

    When skip_reallocate_after_reversals=True (e.g. when called from reallocate_repayment),
    the reallocate step is skipped to avoid infinite recursion.

    When replay_refresh_allocations=True (EOD backfill/replay only), before the loan engine
    runs, all posted receipts on ``as_of_date`` are re-run through reallocation or waterfall
    so allocation and GL stay consistent after data fixes. The later
    ``reallocate_after_reversals`` stage is skipped (redundant with the full-day refresh).

    Policy guard:
    - Replay/backfill must not accrue on system date (or future dates).
    - System-date accrual is only allowed in the canonical EOD flow that advances date.
    """
    try:
        from eod.system_business_date import get_effective_date
        system_date = get_effective_date()
    except Exception:
        system_date = None
    if (
        not allow_system_date_eod
        and system_date is not None
        and as_of_date >= system_date
    ):
        raise ValueError(
            f"EOD replay/backfill blocked for {as_of_date.isoformat()}: "
            f"system date is {system_date.isoformat()}. "
            "Accrual on system date is only allowed via date-advancing EOD."
        )

    with eod_exclusive_session_lock():
        try:
            clear_stale_eod_audit_runs()
        except Exception:
            pass
        sys_cfg = load_system_config_from_db() or {}
        eod_settings = sys_cfg.get("eod_settings", {}) or {}
        tasks_cfg = (eod_settings.get("tasks") or {}) if isinstance(eod_settings, dict) else {}
    
        run_loan_engine = bool(tasks_cfg.get("run_loan_engine", True))
        run_creditor_loan_engine = bool(tasks_cfg.get("run_creditor_loan_engine", True))
        reallocate_after_reversals = (
            bool(tasks_cfg.get("reallocate_after_reversals", True))
            and not skip_reallocate_after_reversals
            and not replay_refresh_allocations
        )
        post_accounting = bool(tasks_cfg.get("post_accounting_events", False))
        generate_statements = bool(tasks_cfg.get("generate_statements", False))
        snapshot_financial_statements = bool(tasks_cfg.get("snapshot_financial_statements", True))
        send_notifications = bool(tasks_cfg.get("send_notifications", False))
        apply_unapplied = bool(tasks_cfg.get("apply_unapplied_to_arrears", True))
        activate_scheduled_receipts = bool(tasks_cfg.get("activate_scheduled_receipts", True))
    
        # Stage policy (config-driven)
        policy_cfg = eod_settings.get("stage_policy", {}) if isinstance(eod_settings, dict) else {}
        policy_mode = str(policy_cfg.get("mode") or "hybrid").strip().lower()
        if policy_mode not in {"strict", "hybrid", "best_effort"}:
            policy_mode = "hybrid"
        blocking_default = [
            "replay_refresh_allocations",
            "loan_engine",
            "creditor_loan_engine",
            "activate_scheduled_receipts",
            "reallocate_after_reversals",
            "apply_unapplied_to_arrears",
            "accounting_events",
            "statements",
        ]
        configured_blocking = policy_cfg.get("blocking_stages")
        blocking_stages = set(configured_blocking) if isinstance(configured_blocking, list) else set(blocking_default)
        advance_on_degraded = bool(policy_cfg.get("advance_date_on_degraded", False))
    
        started = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        loans_processed = 0
        tasks_run: list[str] = []
        run_status = "SUCCESS"
        failed_stage: str | None = None
        error_message: str | None = None
    
        try:
            audit_start_run(
                run_id=run_id,
                as_of_date=as_of_date,
                tasks_cfg=tasks_cfg,
                policy_mode=policy_mode,
                advance_on_degraded=advance_on_degraded,
            )
        except Exception:
            # Audit must never block EOD; continue without DB audit row.
            pass
    
        def _stage(stage_name: str, enabled: bool, fn):
            nonlocal loans_processed, run_status, failed_stage, error_message
            is_blocking = stage_name in blocking_stages
            if not enabled:
                try:
                    log_stage_event(
                        run_id=run_id,
                        stage_name=stage_name,
                        is_blocking=is_blocking,
                        status="SKIPPED",
                    )
                except Exception:
                    pass
                return
    
            try:
                log_stage_event(
                    run_id=run_id,
                    stage_name=stage_name,
                    is_blocking=is_blocking,
                    status="STARTED",
                )
            except Exception:
                pass
    
            try:
                t_stage = time.perf_counter()
                stage_result = fn()
                _logger.info(
                    "EOD stage %r finished in %.2fs",
                    stage_name,
                    time.perf_counter() - t_stage,
                )
                if stage_name == "loan_engine" and isinstance(stage_result, int):
                    loans_processed = stage_result
                tasks_run.append(stage_name)
                try:
                    log_stage_event(
                        run_id=run_id,
                        stage_name=stage_name,
                        is_blocking=is_blocking,
                        status="OK",
                    )
                except Exception:
                    pass
            except Exception as e:
                failed_stage = stage_name
                error_message = _format_stage_exception(e)
                try:
                    log_stage_event(
                        run_id=run_id,
                        stage_name=stage_name,
                        is_blocking=is_blocking,
                        status="ERROR",
                        error_message=error_message[:2000],
                    )
                except Exception:
                    pass
    
                # strict: any failure fails run.
                # hybrid: blocking stage failure fails run; non-blocking degrades.
                # best_effort: continue as degraded.
                if policy_mode == "strict" or (policy_mode == "hybrid" and is_blocking):
                    run_status = "FAILED"
                    raise StageExecutionError(stage_name, error_message)
                run_status = "DEGRADED"
    
        try:
            n_deferred = post_deferred_loan_approval_journals_for_eod(as_of_date)
            if n_deferred:
                _logger.info(
                    "EOD %s: posted %s deferred LOAN_APPROVAL journal(s) for loans now effective.",
                    as_of_date,
                    n_deferred,
                )
        except Exception as ex:
            _logger.warning("EOD deferred LOAN_APPROVAL sweep failed (non-fatal): %s", ex)

        # Replay/backfill exemption: any GL posts triggered during this run use
        # eod_replay policy (calendar-month and closed-period restrictions bypassed).
        policy_scope = (
            use_gl_posting_policy("eod_replay")
            if replay_refresh_allocations
            else nullcontext()
        )
        try:
            with policy_scope:
                _stage(
                    "replay_refresh_allocations",
                    replay_refresh_allocations and run_loan_engine,
                    lambda: _replay_refresh_allocations_for_date(as_of_date, sys_cfg),
                )
                _stage(
                    "loan_engine",
                    run_loan_engine,
                    lambda: _run_loan_engine_for_date(
                        as_of_date,
                        sys_cfg,
                        allow_system_date_eod=allow_system_date_eod,
                        replay_refresh_allocations=replay_refresh_allocations,
                    ),
                )
                _stage(
                    "creditor_loan_engine",
                    run_creditor_loan_engine,
                    lambda: _run_creditor_loan_engine_stage(
                        as_of_date,
                        sys_cfg,
                        allow_system_date_eod=allow_system_date_eod,
                    ),
                )
                _stage(
                    "activate_scheduled_receipts",
                    run_loan_engine and activate_scheduled_receipts,
                    lambda: _activate_scheduled_receipts_stage(
                        as_of_date,
                        sys_cfg,
                        allow_system_date_eod=allow_system_date_eod,
                    ),
                )
                _stage(
                    "reallocate_after_reversals",
                    run_loan_engine and reallocate_after_reversals,
                    lambda: _reallocate_receipts_after_reversals(as_of_date, sys_cfg),
                )
                _stage(
                    "apply_unapplied_to_arrears",
                    run_loan_engine and apply_unapplied,
                    lambda: _apply_unapplied_funds_to_arrears(as_of_date, sys_cfg),
                )
                _stage(
                    "repost_gl_after_replay",
                    replay_refresh_allocations and run_loan_engine,
                    lambda: _repost_gl_after_replay_for_date(as_of_date),
                )
                _stage("accounting_events", post_accounting, lambda: _run_accounting_events(as_of_date, sys_cfg))
                _stage(
                    "equity_period_close",
                    post_accounting,
                    lambda: _run_equity_period_close(as_of_date, sys_cfg),
                )
                _stage(
                    "statements",
                    generate_statements or snapshot_financial_statements,
                    lambda: _run_statement_batch(as_of_date, sys_cfg),
                )
                _stage("notifications", send_notifications, lambda: _run_notification_batch(as_of_date, sys_cfg))
        except StageExecutionError:
            finished = datetime.now(timezone.utc)
            try:
                audit_finish_run(
                    run_id=run_id,
                    run_status="FAILED",
                    failed_stage=failed_stage,
                    error_message=(error_message or "")[:2000] or None,
                )
            except Exception:
                pass
            # Keep failure behavior for blocking stages so callers can stop date advance.
            raise
    
        finished = datetime.now(timezone.utc)
        final_status = run_status if run_status in {"DEGRADED", "FAILED"} else "SUCCESS"
        try:
            audit_finish_run(
                run_id=run_id,
                run_status=final_status,
                failed_stage=failed_stage,
                error_message=(error_message or "")[:2000] or None,
            )
        except Exception:
            pass
    
        should_advance_date = final_status == "SUCCESS" or (
            final_status == "DEGRADED" and advance_on_degraded
        )
    
        return EODResult(
            run_id=run_id,
            as_of_date=as_of_date,
            loans_processed=loans_processed,
            started_at=started,
            finished_at=finished,
            tasks_run=tuple(tasks_run),
            run_status=final_status,
            failed_stage=failed_stage,
            error_message=error_message,
            should_advance_date=should_advance_date,
        )


def run_single_loan_eod(
    loan_id: int,
    as_of_date: date,
    sys_cfg: Dict[str, Any] | None = None,
    *,
    allow_system_date_eod: bool = False,
) -> None:
    """
    Run the EOD engine computation for a single loan only.

    Much cheaper than run_eod_for_date when only one loan's daily state needs
    refreshing (e.g. after reallocate_repayment).  Bypasses the O(N) cost of
    fetching and reprocessing every active loan in the portfolio.

    sys_cfg is passed in when the caller already holds a loaded config so we
    avoid a redundant DB round-trip to load_system_config_from_db.

    By default does not persist for as_of_date on/after the system business date
    (same as run_eod_for_date replay guard). Pass allow_system_date_eod=True only
    for internal use aligned with canonical EOD.
    """
    if sys_cfg is None:
        sys_cfg = load_system_config_from_db() or {}
    processed = _run_loan_engine_for_date(
        as_of_date,
        sys_cfg,
        loan_ids_filter=[loan_id],
        allow_system_date_eod=allow_system_date_eod,
    )
    if processed == 0:
        hint = explain_single_loan_eod_skip_reason(loan_id, as_of_date)
        msg = (
            f"EOD engine wrote no loan_daily_state for loan_id={loan_id} "
            f"on {as_of_date.isoformat()}."
        )
        if hint:
            msg = f"{msg} {hint}"
        else:
            msg = (
                f"{msg} Check server logs for 'EOD skipped loan_id={loan_id}' "
                f"or 'EOD loan_id={loan_id}'."
            )
        raise RuntimeError(msg)

    # Guard: ensure we don't leave arrears unpaid if there are unapplied funds
    eod_settings = sys_cfg.get("eod_settings", {}) if isinstance(sys_cfg.get("eod_settings"), dict) else {}
    tasks_cfg = eod_settings.get("tasks", {}) if isinstance(eod_settings.get("tasks"), dict) else {}
    if tasks_cfg.get("apply_unapplied_to_arrears", True):
        apply_unapplied_funds_to_arrears_eod(loan_id, as_of_date, sys_cfg)


def run_single_loan_eod_date_range(
    loan_id: int,
    start_date: date,
    end_date: date,
    *,
    sys_cfg: Dict[str, Any] | None = None,
    allow_system_date_eod: bool = False,
) -> tuple[bool, str | None]:
    """
    Run the loan EOD engine for one loan for each calendar day in [start_date, end_date]
    (inclusive). Used after a receipt reversal so `loan_daily_state` is replayed from the
    receipt value date through the current business / posting horizon.

    Days on or after the system business date are skipped unless allow_system_date_eod=True
    (canonical full EOD only), so replay scripts cannot persist intraday/system-date accruals.

    Returns (success, error_message). On first failure, stops and returns False with detail.
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    if sys_cfg is None:
        sys_cfg = load_system_config_from_db() or {}
    current = start_date
    while current <= end_date:
        try:
            processed = _run_loan_engine_for_date(
                current,
                sys_cfg,
                loan_ids_filter=[loan_id],
                allow_system_date_eod=allow_system_date_eod,
            )
            if processed == 0:
                hint = explain_single_loan_eod_skip_reason(loan_id, current)
                msg = (
                    f"EOD engine wrote no loan_daily_state for loan_id={loan_id} "
                    f"on {current.isoformat()}."
                )
                if hint:
                    msg = f"{msg} {hint}"
                return False, msg

            # Guard: ensure we don't leave arrears unpaid if there are unapplied funds
            # (especially after a backdated reversal brings arrears back)
            eod_settings = sys_cfg.get("eod_settings", {}) if isinstance(sys_cfg.get("eod_settings"), dict) else {}
            tasks_cfg = eod_settings.get("tasks", {}) if isinstance(eod_settings.get("tasks"), dict) else {}
            if tasks_cfg.get("apply_unapplied_to_arrears", True):
                apply_unapplied_funds_to_arrears_eod(loan_id, current, sys_cfg)

        except Exception as e:
            return False, f"EOD failed for loan_id={loan_id} on {current.isoformat()}: {e}"
        current += timedelta(days=1)
    return True, None


__all__ = [
    "run_eod_for_date",
    "run_single_loan_eod",
    "run_single_loan_eod_date_range",
    "explain_single_loan_eod_skip_reason",
    "EODResult",
    "ConcurrentEODError",
]

