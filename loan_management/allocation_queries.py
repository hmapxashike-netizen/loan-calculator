"""Read queries for ``loan_repayment_allocation`` and repayment windows."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from .db import RealDictCursor, _connection


def _get_allocation_sum_for_repayment(repayment_id: int, conn=None) -> dict | None:
    """
    Net allocation for a repayment (sum across all event rows – append-only model).
    Returns dict with alloc_* keys plus unallocated, or None if no rows.
    """

    def _run(c):
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS cnt,
                    COALESCE(SUM(alloc_principal_not_due), 0)   AS alloc_principal_not_due,
                    COALESCE(SUM(alloc_principal_arrears), 0)   AS alloc_principal_arrears,
                    COALESCE(SUM(alloc_interest_accrued), 0)    AS alloc_interest_accrued,
                    COALESCE(SUM(alloc_interest_arrears), 0)    AS alloc_interest_arrears,
                    COALESCE(SUM(alloc_default_interest), 0)    AS alloc_default_interest,
                    COALESCE(SUM(alloc_penalty_interest), 0)    AS alloc_penalty_interest,
                    COALESCE(SUM(alloc_fees_charges), 0)        AS alloc_fees_charges,
                    COALESCE(SUM(unallocated), 0)               AS unallocated
                FROM loan_repayment_allocation
                WHERE repayment_id = %s
                """,
                (repayment_id,),
            )
            return cur.fetchone()

    if conn is not None:
        row = _run(conn)
    else:
        with _connection() as c:
            row = _run(c)
    if not row or int(row.get("cnt", 0) or 0) == 0:
        return None
    out = dict(row)
    out.pop("cnt", None)
    return out


def get_allocation_totals_for_loan_date(
    loan_id: int,
    value_date: date,
    *,
    exclude_repayment_id: int | None = None,
) -> dict[str, float]:
    """
    Sum of allocation amounts for this loan for all repayments with value_date (or payment_date) on the given date.
    Used by EOD so that when it overwrites loan_daily_state for that date, it subtracts allocations
    and keeps principal/interest arrears (and other buckets) reduced by receipts.
    If exclude_repayment_id is set, that repayment is excluded from the sum (for reallocate: state = engine - others).
    Returns dict with keys: alloc_principal_not_due, alloc_principal_arrears, alloc_interest_accrued,
    alloc_interest_arrears, alloc_default_interest, alloc_penalty_interest, alloc_fees_charges.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT
                    COALESCE(SUM(lra.alloc_principal_not_due), 0)   AS alloc_principal_not_due,
                    COALESCE(SUM(lra.alloc_principal_arrears), 0)   AS alloc_principal_arrears,
                    COALESCE(SUM(lra.alloc_interest_accrued), 0)   AS alloc_interest_accrued,
                    COALESCE(SUM(lra.alloc_interest_arrears), 0)   AS alloc_interest_arrears,
                    COALESCE(SUM(lra.alloc_default_interest), 0)   AS alloc_default_interest,
                    COALESCE(SUM(lra.alloc_penalty_interest), 0)   AS alloc_penalty_interest,
                    COALESCE(SUM(lra.alloc_fees_charges), 0)       AS alloc_fees_charges
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status IN ('posted', 'reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
            """
            params: list = [loan_id, value_date]
            if exclude_repayment_id is not None:
                sql += " AND lr.id != %s"
                params.append(exclude_repayment_id)
            cur.execute(sql, params)
            row = cur.fetchone()
    if not row:
        return {
            "alloc_principal_not_due": 0.0,
            "alloc_principal_arrears": 0.0,
            "alloc_interest_accrued": 0.0,
            "alloc_interest_arrears": 0.0,
            "alloc_default_interest": 0.0,
            "alloc_penalty_interest": 0.0,
            "alloc_fees_charges": 0.0,
        }
    return {k: float(row.get(k, 0) or 0) for k in row}


def _sum_net_allocations_earlier_same_day(
    cur,
    loan_id: int,
    eff_date: date,
    before_repayment_id: int,
) -> dict[str, float]:
    """
    Net allocation (sum of all allocation rows) for posted receipts on eff_date with id < before_repayment_id.
    Excludes reversed originals (status <> posted) and unapplied-liquidation receipts.
    """
    cur.execute(
        """
        SELECT
            COALESCE(SUM(lra.alloc_principal_not_due), 0)   AS alloc_principal_not_due,
            COALESCE(SUM(lra.alloc_principal_arrears), 0)   AS alloc_principal_arrears,
            COALESCE(SUM(lra.alloc_interest_accrued), 0)   AS alloc_interest_accrued,
            COALESCE(SUM(lra.alloc_interest_arrears), 0)   AS alloc_interest_arrears,
            COALESCE(SUM(lra.alloc_default_interest), 0)   AS alloc_default_interest,
            COALESCE(SUM(lra.alloc_penalty_interest), 0)   AS alloc_penalty_interest,
            COALESCE(SUM(lra.alloc_fees_charges), 0)       AS alloc_fees_charges
        FROM loan_repayments lr
        INNER JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
        WHERE lr.loan_id = %s
          AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
          AND lr.id < %s
          AND lr.amount > 0
          AND lr.status = 'posted'
          AND COALESCE(lr.reference, '') <> 'Unapplied funds allocation'
        """,
        (loan_id, eff_date, before_repayment_id),
    )
    row = cur.fetchone()
    keys = (
        "alloc_principal_not_due",
        "alloc_principal_arrears",
        "alloc_interest_accrued",
        "alloc_interest_arrears",
        "alloc_default_interest",
        "alloc_penalty_interest",
        "alloc_fees_charges",
    )
    if not row:
        return {k: 0.0 for k in keys}
    return {k: float(row.get(k, 0) or 0) for k in keys}


def _get_opening_balances_for_repayment(
    cur,
    loan_id: int,
    eff_date: date,
    repayment_id: int,
) -> tuple[dict[str, float], dict | None, int]:
    """
    Opening balances for the waterfall on value date eff_date:

      closing(loan_daily_state for eff_date - 1)
      minus net allocations from earlier *posted* receipts on the same eff_date (by repayment id).

    Reversed receipts are excluded (they are no longer posted). Reallocation updates allocation in place;
    earlier same-day receipts still net correctly from loan_repayment_allocation.

    Returns (balances dict keyed like loan_daily_state, st_prev row dict or None, days_overdue from opening).
    """
    prev_date = eff_date - timedelta(days=1)
    cur.execute(
        """
        SELECT as_of_date,
               principal_not_due, principal_arrears, interest_accrued_balance,
               interest_arrears_balance, default_interest_balance,
               penalty_interest_balance, fees_charges_balance, days_overdue,
               regular_interest_daily, penalty_interest_daily, default_interest_daily,
               regular_interest_period_to_date,
               penalty_interest_period_to_date,
               default_interest_period_to_date,
               COALESCE(regular_interest_in_suspense_balance, 0) AS regular_interest_in_suspense_balance,
               COALESCE(penalty_interest_in_suspense_balance, 0) AS penalty_interest_in_suspense_balance,
               COALESCE(default_interest_in_suspense_balance, 0) AS default_interest_in_suspense_balance
        FROM loan_daily_state
        WHERE loan_id = %s AND as_of_date = %s
        FOR UPDATE
        """,
        (loan_id, prev_date),
    )
    st_prev = cur.fetchone()
    prior = _sum_net_allocations_earlier_same_day(cur, loan_id, eff_date, repayment_id)

    def _col_prev(key: str) -> float:
        if not st_prev:
            return 0.0
        return max(0.0, float(st_prev.get(key, 0) or 0))

    mapping = (
        ("principal_not_due", "alloc_principal_not_due"),
        ("principal_arrears", "alloc_principal_arrears"),
        ("interest_accrued_balance", "alloc_interest_accrued"),
        ("interest_arrears_balance", "alloc_interest_arrears"),
        ("default_interest_balance", "alloc_default_interest"),
        ("penalty_interest_balance", "alloc_penalty_interest"),
        ("fees_charges_balance", "alloc_fees_charges"),
        ("regular_interest_in_suspense_balance", "alloc_interest_accrued"),
        ("penalty_interest_in_suspense_balance", "alloc_penalty_interest"),
        ("default_interest_in_suspense_balance", "alloc_default_interest"),
    )
    balances: dict[str, float] = {}
    for state_key, alloc_key in mapping:
        balances[state_key] = max(
            0.0,
            _col_prev(state_key) - float(prior.get(alloc_key, 0.0) or 0.0),
        )
    days_od = int(st_prev.get("days_overdue", 0) or 0) if st_prev else 0
    st_prev_dict = dict(st_prev) if st_prev else None
    return balances, st_prev_dict, days_od


def get_repayment_opening_delinquency_total(repayment_id: int) -> float | None:
    """
    Sum of arrears-bucket balances at waterfall opening for this repayment (read-only snapshot).

    Same basis as allocate_repayment_waterfall: closing(loan_daily_state for value_date − 1)
    minus earlier same-day receipts' allocations. Used by customer statements so rows that
    appear before a receipt can show delinquency immediately before that receipt is applied.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, COALESCE(value_date, payment_date)::date AS vd
                FROM loan_repayments
                WHERE id = %s AND status = 'posted'
                """,
                (repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            loan_id = int(row["loan_id"])
            vd = row["vd"]
            if hasattr(vd, "date"):
                vd = vd.date()
            if not isinstance(vd, date):
                return None
            balances, _, _ = _get_opening_balances_for_repayment(cur, loan_id, vd, repayment_id)
    return (
        float(balances.get("principal_arrears") or 0)
        + float(balances.get("interest_arrears_balance") or 0)
        + float(balances.get("default_interest_balance") or 0)
        + float(balances.get("penalty_interest_balance") or 0)
        + float(balances.get("fees_charges_balance") or 0)
    )


def get_credits_for_loan_date(loan_id: int, as_of_date: date) -> float:
    """
    Cumulative allocation affecting balances for this loan as of date.
    Credits = SUM(alloc_total) for all repayments with value_date <= as_of_date.
    Payment = + (reduces balance), Reversal = - (adds back). Excludes unapplied.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(lra.alloc_total), 0) AS credits
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status IN ('posted', 'reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date <= %s::date
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)


def get_net_allocation_for_loan_date(loan_id: int, as_of_date: date, conn: Any = None) -> float:
    """
    Per-day allocation that reduced balances for this loan on the given date.
    Net allocation = SUM(alloc_total) for repayments with value_date = as_of_date only. Payment = +, Reversal = -.
    """

    def _run(c: Any) -> float:
        with c.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(lra.alloc_total), 0) AS net_alloc
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status IN ('posted', 'reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                  )
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)

    if conn is not None:
        return _run(conn)
    with _connection() as c:
        return _run(c)


def get_unallocated_for_loan_date(loan_id: int, as_of_date: date, conn: Any = None) -> float:
    """
    Per-day amount credited to unapplied (overpayment) for this loan on the given date.
    Sum of allocation.unallocated for receipts with value_date = as_of_date.
    """

    def _run(c: Any) -> float:
        with c.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(lra.unallocated), 0) AS unallocated
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status IN ('posted', 'reversed')
                  AND (COALESCE(lr.value_date, lr.payment_date))::date = %s::date
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                  )
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)

    if conn is not None:
        return _run(conn)
    with _connection() as c:
        return _run(c)


def get_repayments_with_allocations(
    loan_id: int,
    start_date: date,
    end_date: date,
    *,
    include_reversed: bool = False,
) -> list[dict]:
    """Repayments with value_date in range and their allocation breakdown (totals and per-bucket 1-5).
    By default returns posted only. Set include_reversed=True to include reversed receipts (negative amount).
    Excludes synthetic unapplied liquidations (allocation event_type ``unapplied_funds_allocation``): those
    are represented on the unapplied ledger / flow as the liquidation line, not as PAYMENT bucket splits."""
    status_filter = "lr.status IN ('posted', 'reversed')" if include_reversed else "lr.status = 'posted'"
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT lr.id, lr.amount, lr.payment_date, lr.value_date, lr.customer_reference,
                       lr.reference,
                       lr.original_repayment_id,
                       COALESCE(SUM(lra.alloc_interest_total), 0) AS alloc_interest_total,
                       COALESCE(SUM(lra.alloc_fees_total), 0) AS alloc_fees_total,
                       COALESCE(SUM(lra.alloc_principal_total), 0) AS alloc_principal_total,
                       COALESCE(SUM(lra.alloc_total), 0) AS alloc_total,
                       COALESCE(SUM(lra.unallocated), 0) AS unallocated,
                       COALESCE(SUM(lra.alloc_fees_charges), 0) AS alloc_fees_charges,
                       COALESCE(SUM(lra.alloc_penalty_interest), 0) AS alloc_penalty_interest,
                       COALESCE(SUM(lra.alloc_default_interest), 0) AS alloc_default_interest,
                       COALESCE(SUM(lra.alloc_interest_arrears), 0) AS alloc_interest_arrears,
                       COALESCE(SUM(lra.alloc_interest_accrued), 0) AS alloc_interest_accrued,
                       COALESCE(SUM(lra.alloc_principal_not_due), 0) AS alloc_principal_not_due,
                       COALESCE(SUM(lra.alloc_principal_arrears), 0) AS alloc_principal_arrears
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s AND {status_filter}
                  AND COALESCE(lr.value_date, lr.payment_date) >= %s
                  AND COALESCE(lr.value_date, lr.payment_date) <= %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.reference, '') ILIKE '%%Reversal of unapplied funds%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%Reversal of unapplied funds%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%Reversal of unapplied funds%%'
                    OR EXISTS (
                        SELECT 1
                        FROM loan_repayment_allocation lra_sys
                        WHERE lra_sys.repayment_id = lr.id
                          AND lra_sys.event_type = 'unapplied_funds_allocation'
                    )
                  )
                GROUP BY lr.id, lr.amount, lr.payment_date, lr.value_date, lr.customer_reference,
                         lr.reference, lr.original_repayment_id
                ORDER BY COALESCE(lr.value_date, lr.payment_date), lr.id
                """,
                (loan_id, start_date, end_date),
            )
            return [dict(r) for r in cur.fetchall()]
