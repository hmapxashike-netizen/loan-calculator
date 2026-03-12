"""
Shared decimal precision utilities for LMS.
All numeric columns (except dates) store and compute at 10 decimal places.
"""
from decimal import Decimal, ROUND_HALF_UP

# 10dp: matches NUMERIC(22,10) for all monetary and rate columns
QUANT_10DP = Decimal("0.0000000001")


def as_10dp(value: Decimal | float | int | str) -> Decimal:
    """Quantize to 10 decimal places for storage and computation."""
    if value is None:
        return Decimal("0")
    d = Decimal(str(value))
    return d.quantize(QUANT_10DP, rounding=ROUND_HALF_UP)
