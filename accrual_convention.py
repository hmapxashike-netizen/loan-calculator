"""
Canonical scheduled regular-interest accrual (system / product config key only; behaviour is fixed).

**Rule:** For every instalment period before the final billing, each calendar day in that period
accrues a positive slice of the period's scheduled interest (when ``interest_component > 0``).

- **Period start** = loan disbursement date for the first period, then **each instalment due date**
  (the due date is the first accrual day of the *next* period).
- **Last accrual day** of a period = the calendar day **before** that period's instalment due date
  (the due date itself accrues in the following period, which starts on that date).

So the accrual window for an entry is ``period_start <= calendar_day < due_date`` (same count of
days as ``(due_date - period_start).days``).

The legacy ``NEXT_DAY`` value is still accepted in stored JSON for backwards compatibility but
**normalises to the same behaviour** as ``EFFECTIVE_DAY`` (canonical alias below).

Stored in system ``config`` JSON as top-level ``accrual_start_convention``; product config may
override by merging over system config in EOD.
"""
from __future__ import annotations

from typing import Any, Mapping

# Canonical stored value
ACCRUAL_START_EFFECTIVE_DAY = "EFFECTIVE_DAY"
# Deprecated: kept for imports and old JSON; normalised to EFFECTIVE_DAY everywhere.
ACCRUAL_START_NEXT_DAY = "NEXT_DAY"

ALLOWED_ACCRUAL_START_CONVENTIONS = frozenset(
    {ACCRUAL_START_NEXT_DAY, ACCRUAL_START_EFFECTIVE_DAY}
)


def normalize_accrual_start_convention(raw: Any) -> str:
    """
    Return ``ACCRUAL_START_EFFECTIVE_DAY`` always — single product accrual rule (period-first,
    accrue through the day before the instalment due date).
    """
    return ACCRUAL_START_EFFECTIVE_DAY


def accrual_start_convention_from_config(cfg: Mapping[str, Any] | None) -> str:
    """Read config and return the canonical accrual convention (always EFFECTIVE_DAY)."""
    return normalize_accrual_start_convention(
        cfg.get("accrual_start_convention") if cfg else None
    )


__all__ = [
    "ACCRUAL_START_NEXT_DAY",
    "ACCRUAL_START_EFFECTIVE_DAY",
    "ALLOWED_ACCRUAL_START_CONVENTIONS",
    "normalize_accrual_start_convention",
    "accrual_start_convention_from_config",
]
