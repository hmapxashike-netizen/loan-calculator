"""Future value-dated (scheduled) receipts: EOD activation and cancel-before-value-date."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from decimal_utils import as_10dp

from .allocation_audit import log_allocation_audit_event
from .db import RealDictCursor, _connection
from .repayment_waterfall import allocate_repayment_waterfall
from .serialization import _date_conv

_logger = logging.getLogger(__name__)


def get_scheduled_repayment_ids_due_on(eff_date: date) -> list[int]:
    """Repayment IDs to activate on ``eff_date`` (status scheduled, effective date = eff_date)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayments
                WHERE status = 'scheduled'
                  AND (COALESCE(value_date, payment_date))::date = %s::date
                ORDER BY id
                """,
                (eff_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def activate_scheduled_receipts_for_eod_date(
    as_of_date: date,
    sys_cfg: dict[str, Any],
    *,
    allow_system_date_eod: bool = False,
) -> tuple[int, list[str]]:
    """
    For each scheduled receipt with effective date ``as_of_date``, set status posted and run
    ``allocate_repayment_waterfall`` in its own committed transaction.

    Returns (activated_count, error_messages).
    """
    ids = get_scheduled_repayment_ids_due_on(as_of_date)
    if not ids:
        return 0, []
    errors: list[str] = []
    activated = 0
    for rid in ids:
        try:
            with _connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        UPDATE loan_repayments
                        SET status = 'posted', updated_at = NOW()
                        WHERE id = %s AND status = 'scheduled'
                        RETURNING loan_id
                        """,
                        (rid,),
                    )
                    row = cur.fetchone()
                    if not row:
                        continue
                    loan_id = int(row["loan_id"])
                allocate_repayment_waterfall(
                    rid,
                    system_config=sys_cfg,
                    conn=conn,
                    skip_loan_approval_guard=True,
                    eod_allow_system_date=allow_system_date_eod,
                )
                activated += 1
                log_allocation_audit_event(
                    "scheduled_activated",
                    loan_id,
                    as_of_date,
                    repayment_id=rid,
                    narration="EOD activated scheduled receipt",
                    details={"repayment_id": rid},
                )
        except Exception as e:
            errors.append(f"repayment_id={rid}: {type(e).__name__}: {e}")
            _logger.warning("activate_scheduled_receipts: failed repayment_id=%s: %s", rid, e)
    return activated, errors


def cancel_scheduled_repayment(
    repayment_id: int,
    *,
    reason: str,
    cancelled_by: str,
) -> None:
    """Void a scheduled receipt before its value date. No allocation or GL."""
    from eod.system_business_date import get_effective_date

    reason_clean = (reason or "").strip()
    if not reason_clean:
        raise ValueError("Cancellation reason is required.")
    by_clean = (cancelled_by or "").strip() or "unknown"
    biz = get_effective_date()
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT loan_id, status, value_date, payment_date
                FROM loan_repayments WHERE id = %s
                """,
                (repayment_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Repayment {repayment_id} not found.")
            if str(row.get("status") or "").lower() != "scheduled":
                raise ValueError(
                    f"Repayment {repayment_id} is not scheduled (status={row.get('status')!r}); "
                    "only scheduled receipts can be cancelled this way."
                )
            eff = _date_conv(row.get("value_date")) or _date_conv(row.get("payment_date"))
            if not eff:
                raise ValueError("Repayment has no effective date.")
            if eff <= biz:
                raise ValueError(
                    "Cannot cancel: effective date is not after the system business date "
                    f"(effective={eff.isoformat()}, business_date={biz.isoformat()})."
                )
            cur.execute(
                """
                UPDATE loan_repayments
                SET status = 'cancelled', updated_at = NOW()
                WHERE id = %s AND status = 'scheduled'
                """,
                (repayment_id,),
            )
            if cur.rowcount != 1:
                raise ValueError(f"Repayment {repayment_id} could not be cancelled (concurrent update?).")
            loan_id = int(row["loan_id"])
    log_allocation_audit_event(
        "scheduled_cancelled",
        loan_id,
        eff,
        repayment_id=repayment_id,
        narration=reason_clean[:500],
        details={"cancelled_by": by_clean, "reason": reason_clean[:2000]},
    )


def list_scheduled_receipts_for_loan(loan_id: int, *, limit: int = 200) -> list[dict[str, Any]]:
    """Posted support: recent scheduled rows for a loan (newest first by value_date, id)."""
    lim = max(1, min(2000, int(limit)))
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, amount, payment_date, value_date, system_date,
                       customer_reference, company_reference, status
                FROM loan_repayments
                WHERE loan_id = %s AND status = 'scheduled'
                ORDER BY COALESCE(value_date, payment_date) DESC, id DESC
                LIMIT %s
                """,
                (loan_id, lim),
            )
            return [dict(r) for r in cur.fetchall()]
