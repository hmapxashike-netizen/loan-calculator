"""EOD mirror engine for creditor facilities (separate from debtor ``loan_daily_state``)."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor

from decimal_utils import as_10dp

from eod.core import (
    ARREARS_ZERO_TOLERANCE,
    _build_schedule_entries,
    _persist_accrual_blocked_for_as_of,
)
from eod.loan_daily_engine import Loan

from .daily_state import save_creditor_loan_daily_state
from .loan_config import loan_config_from_behavior

_logger = logging.getLogger(__name__)

_EMPTY_ALLOC: Dict[str, float] = {
    "alloc_principal_not_due": 0.0,
    "alloc_principal_arrears": 0.0,
    "alloc_interest_accrued": 0.0,
    "alloc_interest_arrears": 0.0,
    "alloc_default_interest": 0.0,
    "alloc_penalty_interest": 0.0,
    "alloc_fees_charges": 0.0,
}


def _batch_creditor_alloc_totals(conn, creditor_loan_ids: List[int], as_of_date: date) -> Dict[int, Dict[str, float]]:
    out: Dict[int, Dict[str, float]] = {int(x): dict(_EMPTY_ALLOC) for x in creditor_loan_ids}
    if not creditor_loan_ids:
        return out
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT cr.creditor_drawdown_id,
                COALESCE(SUM(cra.alloc_principal_not_due), 0) AS alloc_principal_not_due,
                COALESCE(SUM(cra.alloc_principal_arrears), 0) AS alloc_principal_arrears,
                COALESCE(SUM(cra.alloc_interest_accrued), 0) AS alloc_interest_accrued,
                COALESCE(SUM(cra.alloc_interest_arrears), 0) AS alloc_interest_arrears,
                COALESCE(SUM(cra.alloc_default_interest), 0) AS alloc_default_interest,
                COALESCE(SUM(cra.alloc_penalty_interest), 0) AS alloc_penalty_interest,
                COALESCE(SUM(cra.alloc_fees_charges), 0) AS alloc_fees_charges
            FROM creditor_repayments cr
            JOIN creditor_repayment_allocation cra ON cra.repayment_id = cr.id
            WHERE cr.creditor_drawdown_id = ANY(%s)
              AND cr.status = 'posted'
              AND (COALESCE(cr.value_date, cr.payment_date))::date = %s::date
            GROUP BY cr.creditor_drawdown_id
            """,
            (creditor_loan_ids, as_of_date),
        )
        for row in cur.fetchall():
            cid = int(row["creditor_drawdown_id"])
            out[cid] = {k: float(row[k] or 0) for k in _EMPTY_ALLOC}
    return out


def _batch_yesterday_creditor_states(
    conn, creditor_loan_ids: List[int], yesterday: date
) -> Dict[int, Dict[str, Any] | None]:
    res: Dict[int, Dict[str, Any] | None] = {int(x): None for x in creditor_loan_ids}
    if not creditor_loan_ids:
        return res
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (creditor_drawdown_id)
                creditor_drawdown_id,
                principal_not_due, principal_arrears, interest_accrued_balance, interest_arrears_balance,
                default_interest_balance, penalty_interest_balance, fees_charges_balance, days_overdue,
                regular_interest_period_to_date, penalty_interest_period_to_date, default_interest_period_to_date
            FROM creditor_loan_daily_state
            WHERE creditor_drawdown_id = ANY(%s) AND as_of_date <= %s
            ORDER BY creditor_drawdown_id, as_of_date DESC
            """,
            (creditor_loan_ids, yesterday),
        )
        for row in cur.fetchall():
            res[int(row["creditor_drawdown_id"])] = dict(row)
    return res


def _all_due_dates_from_entries(entries) -> frozenset[date]:
    return frozenset(e.due_date for e in entries)


def run_creditor_loans_engine_for_date(
    conn,
    as_of_date: date,
    sys_cfg: Dict[str, Any],
    *,
    allow_system_date_eod: bool = False,
) -> int:
    """
    Recompute ``creditor_loan_daily_state`` for all active creditor facilities for ``as_of_date``.

    Returns number of facilities processed.
    """
    eod_st = sys_cfg.get("eod_settings") or {}
    tasks_cfg = (eod_st.get("tasks") if isinstance(eod_st, dict) else None) or {}
    if not bool(tasks_cfg.get("run_creditor_loan_engine", True)):
        return 0

    block_accruals = _persist_accrual_blocked_for_as_of(
        as_of_date, allow_system_date_eod=allow_system_date_eod
    )
    yesterday = as_of_date - timedelta(days=1)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT cd.*, lt.behavior_json AS type_behavior_json
            FROM creditor_drawdowns cd
            JOIN creditor_loan_types lt ON lt.code = cd.creditor_loan_type_code
            WHERE cd.status = 'active'
            """
        )
        loans = cur.fetchall()

    if not loans:
        return 0

    ids = [int(r["id"]) for r in loans]
    alloc_map = _batch_creditor_alloc_totals(conn, ids, as_of_date)
    yesterday_map = _batch_yesterday_creditor_states(conn, ids, yesterday)

    processed = 0
    for loan_row in loans:
        cid = int(loan_row["id"])
        disb = loan_row.get("disbursement_date") or loan_row.get("start_date")
        if not isinstance(disb, date):
            continue
        if disb > as_of_date:
            continue

        behavior = loan_row.get("type_behavior_json") or {}
        if hasattr(behavior, "copy"):
            behavior = dict(behavior)
        config = loan_config_from_behavior(behavior)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT csl."Period", csl."Date", csl.payment, csl.principal, csl.interest,
                       csl.principal_balance, csl.total_outstanding
                FROM creditor_schedule_lines csl
                JOIN creditor_loan_schedules s ON s.id = csl.creditor_loan_schedule_id
                WHERE s.creditor_drawdown_id = %s AND s.version = 1
                ORDER BY csl."Period"
                """,
                (cid,),
            )
            rows = cur.fetchall()
        if not rows:
            _logger.warning("Creditor EOD skipped cl_id=%s: no schedule lines.", cid)
            continue

        schedule_rows = [dict(r) for r in rows]
        lr = dict(loan_row)
        lr["disbursement_date"] = disb
        lr["start_date"] = disb
        try:
            schedule_entries = _build_schedule_entries(lr, schedule_rows)
        except ValueError as e:
            _logger.warning("Creditor EOD skipped cl_id=%s: bad schedule (%s).", cid, e)
            continue
        if not schedule_entries:
            continue

        alloc = alloc_map.get(cid, dict(_EMPTY_ALLOC))
        yesterday_saved = yesterday_map.get(cid) if yesterday >= disb else None

        if str(loan_row.get("accrual_mode") or "daily_mirror").strip() == "periodic_schedule":
            from .periodic_engine import run_periodic_creditor_drawdown_for_date

            run_periodic_creditor_drawdown_for_date(
                conn,
                dict(loan_row),
                schedule_rows,
                as_of_date,
                yesterday=yesterday,
                alloc=alloc,
                yesterday_saved=yesterday_saved,
                block_accruals=block_accruals,
            )
            processed += 1
            continue

        principal = Decimal(str(loan_row.get("principal") or 0))
        engine_loan = Loan(
            loan_id=str(cid),
            disbursement_date=disb,
            original_principal=principal,
            config=config,
            schedule=list(schedule_entries),
        )

        current = disb
        while current <= yesterday:
            engine_loan.process_day(current)
            current += timedelta(days=1)

        engine_yesterday = {
            "principal_not_due": float(engine_loan.principal_not_due),
            "principal_arrears": float(engine_loan.principal_arrears),
            "interest_accrued_balance": float(engine_loan.interest_accrued_balance),
            "interest_arrears_balance": float(engine_loan.interest_arrears),
            "default_interest_balance": float(engine_loan.default_interest_balance),
            "penalty_interest_balance": float(engine_loan.penalty_interest_balance),
            "fees_charges_balance": float(engine_loan.fees_charges_balance),
        }

        if as_of_date > yesterday and not block_accruals:
            engine_loan.process_day(as_of_date)
        elif block_accruals:
            engine_loan.last_regular_interest_daily = Decimal("0")
            engine_loan.last_default_interest_daily = Decimal("0")
            engine_loan.last_penalty_interest_daily = Decimal("0")

        def _tb(yk: str, et_val: float, ey_val: float, ak: str) -> float:
            delta = et_val - ey_val
            if yesterday_saved is not None and yk in yesterday_saved:
                return max(0.0, float(yesterday_saved[yk] or 0) + delta - float(alloc.get(ak, 0.0) or 0))
            return max(0.0, et_val - float(alloc.get(ak, 0.0) or 0))

        due_today = any(e.due_date == as_of_date for e in schedule_entries)
        all_due = _all_due_dates_from_entries(schedule_entries)
        due_yesterday = yesterday in all_due

        principal_not_due = _tb("principal_not_due", float(engine_loan.principal_not_due), engine_yesterday["principal_not_due"], "alloc_principal_not_due")
        principal_arrears = _tb("principal_arrears", float(engine_loan.principal_arrears), engine_yesterday["principal_arrears"], "alloc_principal_arrears")

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
            interest_accrued_balance = _tb(
                "interest_accrued_balance",
                float(engine_loan.interest_accrued_balance),
                engine_yesterday["interest_accrued_balance"],
                "alloc_interest_accrued",
            )
        interest_arrears_balance = _tb(
            "interest_arrears_balance",
            float(engine_loan.interest_arrears),
            engine_yesterday["interest_arrears_balance"],
            "alloc_interest_arrears",
        )
        default_interest_balance = _tb(
            "default_interest_balance",
            float(engine_loan.default_interest_balance),
            engine_yesterday["default_interest_balance"],
            "alloc_default_interest",
        )
        penalty_interest_balance = _tb(
            "penalty_interest_balance",
            float(engine_loan.penalty_interest_balance),
            engine_yesterday["penalty_interest_balance"],
            "alloc_penalty_interest",
        )
        fees_charges_balance = _tb(
            "fees_charges_balance",
            float(engine_loan.fees_charges_balance),
            engine_yesterday["fees_charges_balance"],
            "alloc_fees_charges",
        )

        if yesterday_saved is not None and not due_today:
            principal_not_due = max(
                0.0,
                float(yesterday_saved.get("principal_not_due", 0) or 0) - float(alloc.get("alloc_principal_not_due", 0.0)),
            )
            principal_arrears = max(
                0.0,
                float(yesterday_saved.get("principal_arrears", 0) or 0) - float(alloc.get("alloc_principal_arrears", 0.0)),
            )
            interest_arrears_balance = max(
                0.0,
                float(yesterday_saved.get("interest_arrears_balance", 0) or 0) - float(alloc.get("alloc_interest_arrears", 0.0)),
            )

        no_arrears = principal_arrears <= ARREARS_ZERO_TOLERANCE and interest_arrears_balance <= ARREARS_ZERO_TOLERANCE
        if no_arrears:
            days_overdue_save = 0
        else:
            if yesterday_saved is not None and "days_overdue" in yesterday_saved:
                days_overdue_save = int(yesterday_saved["days_overdue"] or 0) + 1
            else:
                days_overdue_save = 1

        grace_days = int(config.grace_period_days)
        within_grace = no_arrears or (days_overdue_save <= grace_days)

        if within_grace or block_accruals:
            default_interest_daily_save = 0.0
            penalty_interest_daily_save = 0.0
            default_interest_balance_save = float(default_interest_balance)
            penalty_interest_balance_save = float(penalty_interest_balance)
        else:
            default_interest_daily_save = float(engine_loan.last_default_interest_daily or 0)
            penalty_interest_daily_save = float(engine_loan.last_penalty_interest_daily or 0)
            default_interest_balance_save = float(default_interest_balance)
            penalty_interest_balance_save = float(penalty_interest_balance)

        regular_daily = engine_loan.last_regular_interest_daily
        if due_yesterday:
            regular_interest_period_to_date_save = as_10dp(regular_daily)
        elif yesterday_saved is not None:
            prev = Decimal(str(yesterday_saved.get("regular_interest_period_to_date", 0) or 0))
            regular_interest_period_to_date_save = as_10dp(prev + (regular_daily if isinstance(regular_daily, Decimal) else Decimal(str(regular_daily))))
        else:
            regular_interest_period_to_date_save = as_10dp(regular_daily)

        save_creditor_loan_daily_state(
            cid,
            as_of_date,
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
            penalty_interest_period_to_date=float(penalty_interest_daily_save),
            default_interest_period_to_date=float(default_interest_daily_save),
            net_allocation=0.0,
            unallocated=0.0,
            conn=conn,
        )
        processed += 1

    if processed:
        _logger.info("EOD creditor_loan_engine as_of=%s processed=%s", as_of_date.isoformat(), processed)
    return processed
