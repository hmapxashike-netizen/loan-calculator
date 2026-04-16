"""Month-end interest expense for periodic creditor drawdowns (scheduled interest in calendar month)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from psycopg2.extras import RealDictCursor

from decimal_utils import as_10dp

from .serialization import _date_conv


def periodic_scheduled_interest_in_calendar_month(
    cur,
    creditor_drawdown_id: int,
    month_start: date,
    month_end: date,
) -> Decimal:
    """
    Sum scheduled **interest** for installments whose **Date** falls in
    ``[month_start, month_end]`` (inclusive). Proration across partial months can be refined later.
    """
    cur.execute(
        """
        SELECT csl."Date", csl.interest
        FROM creditor_schedule_lines csl
        JOIN creditor_loan_schedules s ON s.id = csl.creditor_loan_schedule_id
        WHERE s.creditor_drawdown_id = %s AND s.version = 1
        ORDER BY csl."Period"
        """,
        (int(creditor_drawdown_id),),
    )
    rows = cur.fetchall()
    total = Decimal("0")
    for r in rows:
        raw = r.get("Date")
        if raw is None:
            continue
        d = _date_conv(str(raw).strip()) if not hasattr(raw, "isoformat") else raw
        if hasattr(d, "date"):
            d = d.date()
        if not isinstance(d, date):
            continue
        if month_start <= d <= month_end:
            total += Decimal(str(as_10dp(r.get("interest") or 0)))
    return as_10dp(total)
