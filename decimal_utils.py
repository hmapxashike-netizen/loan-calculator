"""
Shared decimal precision utilities for FarndaCred.
All numeric columns (except dates) store and compute at 10 decimal places.
"""
from decimal import Decimal, ROUND_HALF_UP

# 10dp: matches NUMERIC(22,10) for all monetary and rate columns
QUANT_10DP = Decimal("0.0000000001")
# 2dp: display / material tolerance for some GL integrity checks
QUANT_2DP = Decimal("0.01")


def as_10dp(value: Decimal | float | int | str) -> Decimal:
    """Quantize to 10 decimal places for storage and computation."""
    if value is None:
        return Decimal("0")
    d = Decimal(str(value))
    return d.quantize(QUANT_10DP, rounding=ROUND_HALF_UP)


def as_2dp(value: Decimal | float | int | str) -> Decimal:
    """Quantize to 2 decimal places (e.g. material money display)."""
    if value is None:
        return Decimal("0")
    d = Decimal(str(value))
    return d.quantize(QUANT_2DP, rounding=ROUND_HALF_UP)


def amounts_equal_at_2dp(a: Decimal, b: Decimal) -> bool:
    """True when a and b match after half-up rounding to 2dp."""
    return as_2dp(a) == as_2dp(b)
