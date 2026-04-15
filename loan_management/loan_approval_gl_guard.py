"""
Defer LOAN_APPROVAL GL for future-dated disbursement; block receipts until journal exists.

- At capture, if disbursement/start is strictly after the system business date, do not post
  LOAN_APPROVAL. EOD posts it on the first run where as_of_date >= disbursement_date.
- Repayments require an active LOAN_APPROVAL journal so cash allocation cannot precede inception GL.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from .serialization import _date_conv

_LOGGER = logging.getLogger(__name__)

LOAN_APPROVAL_EVENT_TAG = "LOAN_APPROVAL"


def system_business_date_for_guard() -> date:
    try:
        from eod.system_business_date import get_effective_date

        return get_effective_date()
    except Exception:
        return date.today()


def effective_disbursement_date_from_details(details: dict[str, Any]) -> date | None:
    raw = details.get("disbursement_date") or details.get("start_date")
    if raw is None:
        return None
    return _date_conv(raw)


def should_defer_loan_approval_gl_at_capture(details: dict[str, Any]) -> bool:
    """True when contractual disbursement/start is strictly after the system business date."""
    ed = effective_disbursement_date_from_details(details)
    if ed is None:
        return False
    return ed > system_business_date_for_guard()


def loan_has_active_loan_approval_journal(loan_id: int, *, conn=None) -> bool:
    from .db import _connection

    def _run(c) -> bool:
        with c.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM journal_entries
                WHERE event_id = %s AND event_tag = %s AND is_active = TRUE
                LIMIT 1
                """,
                (str(int(loan_id)), LOAN_APPROVAL_EVENT_TAG),
            )
            return cur.fetchone() is not None

    if conn is not None:
        return _run(conn)
    with _connection() as c:
        return _run(c)


def require_loan_approval_gl_before_repayment(loan_id: int, *, conn=None) -> None:
    """Raise if the loan has no active LOAN_APPROVAL journal (e.g. future disbursement not yet reached)."""
    if loan_has_active_loan_approval_journal(int(loan_id), conn=conn):
        return
    raise ValueError(
        f"Loan {int(loan_id)} has no active LOAN_APPROVAL journal yet. "
        "Receipts cannot be recorded until disbursement is effective: either capture with "
        "disbursement on or before the system business date, or run EOD for a date on or after "
        "the disbursement date so LOAN_APPROVAL posts automatically."
    )


def post_deferred_loan_approval_journals_for_eod(as_of_date: date) -> int:
    """
    For each active loan with disbursement_date <= as_of_date and no active LOAN_APPROVAL,
    post LOAN_APPROVAL using current loan row (same as repair/repost path).

    Returns the number of journals successfully posted.
    """
    from accounting.service import AccountingService

    from .db import _connection

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT l.id FROM loans l
                WHERE l.status = 'active'
                  AND l.disbursement_date IS NOT NULL
                  AND l.disbursement_date <= %s
                  AND NOT EXISTS (
                    SELECT 1 FROM journal_entries je
                    WHERE je.event_id = CAST(l.id AS TEXT)
                      AND je.event_tag = %s
                      AND je.is_active = TRUE
                  )
                """,
                (as_of_date, LOAN_APPROVAL_EVENT_TAG),
            )
            loan_ids = [int(r[0]) for r in cur.fetchall()]

    if not loan_ids:
        return 0

    svc = AccountingService()
    posted = 0
    for lid in loan_ids:
        try:
            svc.repost_loan_approval_journal(lid, created_by="system")
            posted += 1
        except Exception as ex:
            _LOGGER.warning(
                "Deferred LOAN_APPROVAL post failed for loan_id=%s on EOD %s: %s",
                lid,
                as_of_date,
                ex,
            )
    return posted
