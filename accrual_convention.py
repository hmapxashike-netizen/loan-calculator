"""
Interest accrual start convention (system / product config).

NEXT_DAY (legacy): first calendar day that accrues regular scheduled interest is the day
after period_start (disbursement or previous due date). Last accrual day of the period is due_date.

EFFECTIVE_DAY: accrual includes period_start; the due_date is a billing/settlement day only, not
an additional accrual day — same count of accrual days as NEXT_DAY for a given period, avoiding
an extra day at the end when the first day is included.

Stored in system `config` JSON (key `system_config`) as top-level `accrual_start_convention`.
Product config may override by merging over system config in EOD.
"""
from __future__ import annotations

from typing import Any, Mapping

# Canonical stored values (uppercase, underscore)
ACCRUAL_START_NEXT_DAY = "NEXT_DAY"
ACCRUAL_START_EFFECTIVE_DAY = "EFFECTIVE_DAY"

ALLOWED_ACCRUAL_START_CONVENTIONS = frozenset(
    {ACCRUAL_START_NEXT_DAY, ACCRUAL_START_EFFECTIVE_DAY}
)


def normalize_accrual_start_convention(raw: Any) -> str:
    """
    Return ACCRUAL_START_NEXT_DAY or ACCRUAL_START_EFFECTIVE_DAY.
    Unknown or empty values default to NEXT_DAY (preserve legacy behaviour).
    """
    if raw is None:
        return ACCRUAL_START_NEXT_DAY
    s = str(raw).strip().upper().replace("-", "_").replace(" ", "_")
    if s in ("EFFECTIVE_DAY", "OPTION_1", "EFFECTIVE"):
        return ACCRUAL_START_EFFECTIVE_DAY
    if s in ("NEXT_DAY", "OPTION_2", "NEXT"):
        return ACCRUAL_START_NEXT_DAY
    return ACCRUAL_START_NEXT_DAY


def accrual_start_convention_from_config(cfg: Mapping[str, Any] | None) -> str:
    """Read and normalize `accrual_start_convention` from merged system/product config."""
    if not cfg:
        return ACCRUAL_START_NEXT_DAY
    return normalize_accrual_start_convention(cfg.get("accrual_start_convention"))


__all__ = [
    "ACCRUAL_START_NEXT_DAY",
    "ACCRUAL_START_EFFECTIVE_DAY",
    "ALLOWED_ACCRUAL_START_CONVENTIONS",
    "normalize_accrual_start_convention",
    "accrual_start_convention_from_config",
]
