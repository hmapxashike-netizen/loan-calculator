"""Calendar-accurate subscription period end dates and delinquency bands."""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from enum import Enum
from typing import Literal

from dateutil.relativedelta import relativedelta

BillingCycle = Literal["Monthly", "Quarterly"]


class SubscriptionBand(str, Enum):
    """Access band from calendar-days past subscription period end (due date)."""

    CURRENT = "current"
    WARNING = "warning"  # 1–7 days overdue: banner, full access
    VIEW_ONLY_FROZEN = "view_only_frozen"  # 8–30: view-only, effective date capped
    RESTRICTED_NAV = "restricted_nav"  # 31–90: Portfolio reports + Subscription only
    TERMINATED = "terminated"  # 90+ or explicit termination timestamp


def get_billing_end_date(start_date: date, cycle: BillingCycle = "Monthly") -> date:
    """
    Last calendar day of the billing window anchored at ``start_date``.

    Monthly: last day of the month containing ``start_date`` after advancing by (1 - 1) months
    (i.e. same month as start). Quarterly: last day of the month that is two months after the
    month of ``start_date`` (three-month window ending that month).

    Examples
    --------
    >>> # Monthly from 3 Apr → 30 Apr
    >>> get_billing_end_date(date(2026, 4, 3), "Monthly")
    datetime.date(2026, 4, 30)
    >>> # Quarterly from 15 Jan → 31 Mar
    >>> get_billing_end_date(date(2026, 1, 15), "Quarterly")
    datetime.date(2026, 3, 31)
    """
    months_to_add = 1 if cycle == "Monthly" else 3
    target_date = start_date + relativedelta(months=months_to_add - 1)
    last_day = calendar.monthrange(target_date.year, target_date.month)[1]
    return date(target_date.year, target_date.month, last_day)


def next_period_start(after_end: date) -> date:
    """First day after a closed billing period (explicit, predictable)."""
    return after_end + timedelta(days=1)


def extend_period(current_period_end: date, cycle: BillingCycle) -> date:
    """Next period end after ``current_period_end`` (starts the day after it)."""
    return get_billing_end_date(next_period_start(current_period_end), cycle)


def days_overdue(*, today: date, due_date: date | None) -> int:
    """Whole days past ``due_date`` when ``today`` is strictly after due; else 0."""
    if due_date is None:
        return 0
    if today <= due_date:
        return 0
    return (today - due_date).days


def subscription_band(
    *,
    today: date,
    due_date: date | None,
    access_terminated_at=None,
) -> tuple[SubscriptionBand, int, date | None]:
    """
    Map delinquency to band and optional frozen effective date (due + 7 days).

    Returns
    -------
    band, days_overdue, frozen_effective_date (only for VIEW_ONLY_FROZEN, else None)
    """
    if access_terminated_at is not None:
        return SubscriptionBand.TERMINATED, days_overdue(today=today, due_date=due_date), None

    d = days_overdue(today=today, due_date=due_date)
    if d > 90:
        return SubscriptionBand.TERMINATED, d, None
    if d >= 31:
        return SubscriptionBand.RESTRICTED_NAV, d, None
    if d >= 8:
        frozen = (due_date + timedelta(days=7)) if due_date is not None else None
        return SubscriptionBand.VIEW_ONLY_FROZEN, d, frozen
    if d >= 1:
        return SubscriptionBand.WARNING, d, None
    return SubscriptionBand.CURRENT, d, None
