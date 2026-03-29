"""
IFRS-style security-based loan provision (DPD → PD%, collateral haircut on min(charge, valuation)).
RBZ / other regimes can be added separately (no security in formula).
"""
from __future__ import annotations

from decimal import Decimal

from decimal_utils import as_10dp


def resolve_pd_band(dpd: int, bands: list[dict]) -> dict | None:
    """
    Pick the active band where dpd_min <= dpd and (dpd_max is None or dpd <= dpd_max).
    bands: rows with dpd_min, dpd_max (optional), pd_rate_pct, status_label, is_active.
    """
    d = int(dpd)
    active = [b for b in bands if b.get("is_active") is not False]
    active.sort(key=lambda x: (int(x.get("dpd_min") or 0), int(x.get("sort_order") or 0)))
    for b in active:
        lo = int(b.get("dpd_min") or 0)
        hi = b.get("dpd_max")
        if d < lo:
            continue
        if hi is not None and d > int(hi):
            continue
        return b
    return None


def collateral_value_after_haircut(
    charge: Decimal | float | str | None,
    valuation: Decimal | float | str | None,
    haircut_pct: Decimal | float | str | None,
) -> Decimal:
    """
    Risk-adjusted collateral: min(charge, valuation) × (1 − haircut%).
    """
    c = as_10dp(charge or 0)
    v = as_10dp(valuation or 0)
    base = c if c <= v else v
    h = as_10dp(haircut_pct or 0)
    factor = as_10dp(Decimal(1) - h / Decimal(100))
    if factor < 0:
        factor = Decimal(0)
    return as_10dp(base * factor)


def unsecured_exposure(
    total_balance: Decimal | float | str | None,
    interest_in_suspense: Decimal | float | str | None,
    collateral_value_risk_adjusted: Decimal | float | str | None,
) -> Decimal:
    """max(0, total_balance − interest_in_suspense − collateral_value)."""
    t = as_10dp(total_balance or 0)
    iis = as_10dp(interest_in_suspense or 0)
    col = as_10dp(collateral_value_risk_adjusted or 0)
    raw = as_10dp(t - iis - col)
    return raw if raw > 0 else Decimal("0")


def provision_amount(
    unsecured: Decimal | float | str | None,
    pd_rate_pct: Decimal | float | str | None,
) -> Decimal:
    return as_10dp(as_10dp(unsecured or 0) * as_10dp(pd_rate_pct or 0) / Decimal(100))


def compute_security_provision_breakdown(
    *,
    dpd: int,
    total_balance: Decimal,
    interest_in_suspense: Decimal,
    charge: Decimal,
    valuation: Decimal,
    haircut_pct: Decimal,
    pd_bands: list,
) -> dict:
    band = resolve_pd_band(dpd, pd_bands)
    pd_rate = Decimal(str(band["pd_rate_pct"])) if band else Decimal(0)
    status = str(band["status_label"]) if band else "—"
    col_val = collateral_value_after_haircut(charge, valuation, haircut_pct)
    unsec = unsecured_exposure(total_balance, interest_in_suspense, col_val)
    prov = provision_amount(unsec, pd_rate)
    return {
        "status_label": status,
        "pd_rate_pct": pd_rate,
        "collateral_value": col_val,
        "unsecured_exposure": unsec,
        "provision": prov,
    }
