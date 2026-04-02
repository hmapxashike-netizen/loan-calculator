"""Types and small date helpers for repayment reversal flows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class ReverseRepaymentResult:
    """Outcome of `reverse_repayment` after DB commit and optional EOD replay."""

    reversal_repayment_id: int
    loan_id: int
    value_date: date
    eod_from_date: date
    eod_to_date: date
    eod_rerun_success: bool
    eod_rerun_error: str | None = None


def _reversal_posting_calendar_date(system_date: datetime | str | date | None) -> date:
    """Calendar date used as the upper horizon when replaying EOD after a reversal."""
    sdate = system_date
    if sdate is None:
        sdate = datetime.now()
    elif isinstance(sdate, str):
        sdate = datetime.fromisoformat(sdate.replace("Z", "+00:00"))
    if isinstance(sdate, datetime):
        return sdate.date()
    if isinstance(sdate, date):
        return sdate
    return date.today()
