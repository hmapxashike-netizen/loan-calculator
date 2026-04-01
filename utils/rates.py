def pct_to_monthly(pct: float | int | None, rate_basis: str) -> float | None:
    if pct is None:
        return None
    pct_f = float(pct)
    return (pct_f / 12.0) if rate_basis == "Per annum" else pct_f
