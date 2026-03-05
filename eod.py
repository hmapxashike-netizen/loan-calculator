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

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url
from loan_engine import LoanConfig, ScheduleEntry, Loan, WaterfallType
from loan_management import (
    get_schedule_lines,
    save_loan_daily_state,
    load_system_config_from_db,
)


def _get_conn():
    """
    Create a new psycopg2 connection for EOD work.

    We keep this local to avoid tight coupling to other modules' internals,
    but reuse the same database URL source.
    """
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


def _fetch_active_loans(conn) -> List[Dict[str, Any]]:
    """Load all active loans from the database."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM loans
            WHERE status = 'active'
            """
        )
        return [dict(r) for r in cur.fetchall()]


def _loan_config_from_row(loan_row: Dict[str, Any], sys_cfg: Dict[str, Any]) -> LoanConfig:
    """
    Build a LoanConfig for the engine from a loan row and system configuration.

    This is a pragmatic mapping:
    - regular_rate_per_month: from loans.annual_rate (per annum) or monthly_rate.
    - default/penalty absolute rates: from system-wide defaults per loan_type.
    """
    loan_type = loan_row.get("loan_type") or "term_loan"
    default_rates = (sys_cfg.get("default_rates") or {}).get(loan_type, {}) or {}
    penalty_rates = sys_cfg.get("penalty_rates") or {}

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

    # Default & penalty absolute monthly rates (simple mapping of %/month).
    default_abs_monthly = Decimal(str(default_rates.get("interest_pct", 0))) / Decimal(
        "100"
    )
    penalty_pct = Decimal(str(penalty_rates.get(loan_type, 0))) / Decimal("100")

    # Grace period days and penalty basis
    grace_days = 5  # sensible default; can be made configurable later per product
    penalty_on_principal_arrears_only = (
        (sys_cfg.get("penalty_balance_basis") or "Arrears") == "Arrears"
    )

    waterfall_name = (sys_cfg.get("payment_waterfall") or "Standard").strip().lower()
    if waterfall_name.startswith("borrower"):
        waterfall_type = WaterfallType.BORROWER_FRIENDLY
    else:
        waterfall_type = WaterfallType.STANDARD

    flat_interest = (sys_cfg.get("interest_method") or "Reducing balance") == "Flat rate"

    return LoanConfig(
        regular_rate_per_month=monthly_rate,
        default_interest_absolute_rate_per_month=default_abs_monthly,
        penalty_interest_absolute_rate_per_month=penalty_pct,
        grace_period_days=grace_days,
        penalty_on_principal_arrears_only=penalty_on_principal_arrears_only,
        waterfall_type=waterfall_type,
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
        # Fallback: today; in practice disbursement/start_date should always be set.
        period_start = date.today()

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


def _run_loan_engine_for_date(as_of_date: date, sys_cfg: Dict[str, Any]) -> int:
    """
    Core loan engine step: recompute loan buckets and interest into loan_daily_state.

    Returns the number of loans that were actually processed (i.e. with schedules).
    """
    processed = 0
    with _get_conn() as conn:
        loans = _fetch_active_loans(conn)

    for loan_row in loans:
        loan_id_int = int(loan_row["id"])
        schedule_rows = get_schedule_lines(loan_id_int)
        if not schedule_rows:
            # Skip loans without schedules; nothing to accrue yet.
            continue

        config = _loan_config_from_row(loan_row, sys_cfg)
        schedule_entries = _build_schedule_entries(loan_row, schedule_rows)

        # Opening principal for the engine should be the total facility amount,
        # not the net amount required. This ensures interest is charged on the
        # full debt, including the portion that effectively covers fees.
        principal = Decimal(str(loan_row.get("facility") or loan_row.get("principal") or 0))
        disb_date = loan_row.get("disbursement_date") or loan_row.get("start_date")
        if not isinstance(disb_date, date):
            # Defensive fallback; real loans should always have a disbursement/start date.
            disb_date = as_of_date

        engine_loan = Loan(
            loan_id=str(loan_id_int),
            disbursement_date=disb_date,
            original_principal=principal,
            config=config,
            schedule=schedule_entries,
        )

        current = disb_date
        while current <= as_of_date:
            engine_loan.process_day(current)
            current += timedelta(days=1)

        # Persist daily state snapshot for as_of_date.
        save_loan_daily_state(
            loan_id=loan_id_int,
            as_of_date=as_of_date,
            regular_interest_daily=float(engine_loan.last_regular_interest_daily),
            principal_not_due=float(engine_loan.principal_not_due),
            principal_arrears=float(engine_loan.principal_arrears),
            interest_accrued_balance=float(engine_loan.interest_accrued_balance),
            interest_arrears_balance=float(engine_loan.interest_arrears),
            default_interest_daily=float(engine_loan.last_default_interest_daily),
            default_interest_balance=float(engine_loan.default_interest_balance),
            penalty_interest_daily=float(engine_loan.last_penalty_interest_daily),
            penalty_interest_balance=float(engine_loan.penalty_interest_balance),
            fees_charges_balance=float(engine_loan.fees_charges_balance),
            days_overdue=engine_loan.days_overdue,
        )
        processed += 1

    return processed


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


def run_eod_for_date(as_of_date: date) -> EODResult:
    """
    Orchestrate EOD for a given calendar date.

    The exact *sequence* of steps is fixed in code for safety and auditability,
    but which high-level tasks are enabled is controlled by system configuration
    (system_config.eod_settings.tasks).
    """
    sys_cfg = load_system_config_from_db() or {}
    eod_settings = sys_cfg.get("eod_settings", {}) or {}
    tasks_cfg = (eod_settings.get("tasks") or {}) if isinstance(eod_settings, dict) else {}

    run_loan_engine = bool(tasks_cfg.get("run_loan_engine", True))
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


__all__ = ["run_eod_for_date", "EODResult"]

