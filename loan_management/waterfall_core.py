"""Waterfall profile config and pure allocation math (no database)."""

from __future__ import annotations

from datetime import date

from decimal_utils import as_10dp

# Waterfall bucket name -> (alloc_* column name, loan_daily_state column name)
BUCKET_TO_ALLOC = {
    "fees_charges_balance": ("alloc_fees_charges", "fees_charges_balance"),
    "penalty_interest_balance": ("alloc_penalty_interest", "penalty_interest_balance"),
    "default_interest_balance": ("alloc_default_interest", "default_interest_balance"),
    "interest_arrears_balance": ("alloc_interest_arrears", "interest_arrears_balance"),
    "interest_accrued_balance": ("alloc_interest_accrued", "interest_accrued_balance"),
    "principal_arrears": ("alloc_principal_arrears", "principal_arrears"),
    "principal_not_due": ("alloc_principal_not_due", "principal_not_due"),
}

# Map config/display variants to internal bucket key so allocation works even if DB has different strings
_BUCKET_NAME_NORMALIZE: dict[str, str] = {}
for _key in BUCKET_TO_ALLOC:
    _norm = _key.lower().replace("_", " ")
    _BUCKET_NAME_NORMALIZE[_key] = _key
    _BUCKET_NAME_NORMALIZE[_key.lower()] = _key
    _BUCKET_NAME_NORMALIZE[_norm] = _key
    _BUCKET_NAME_NORMALIZE[_norm.replace(" ", "_")] = _key
# Common display names and UI/config variants (so waterfall_profiles can use friendly names)
_BUCKET_NAME_NORMALIZE["interest arrears"] = "interest_arrears_balance"
_BUCKET_NAME_NORMALIZE["interest_arrears"] = "interest_arrears_balance"
_BUCKET_NAME_NORMALIZE["principal arrears"] = "principal_arrears"
_BUCKET_NAME_NORMALIZE["principal_arrears"] = "principal_arrears"
_BUCKET_NAME_NORMALIZE["principal"] = "principal_arrears"  # ambiguous; map to principal_arrears
_BUCKET_NAME_NORMALIZE["fees"] = "fees_charges_balance"
_BUCKET_NAME_NORMALIZE["fees and charges"] = "fees_charges_balance"
_BUCKET_NAME_NORMALIZE["fees_charges"] = "fees_charges_balance"
_BUCKET_NAME_NORMALIZE["penalty"] = "penalty_interest_balance"
_BUCKET_NAME_NORMALIZE["penalty interest"] = "penalty_interest_balance"
_BUCKET_NAME_NORMALIZE["default interest"] = "default_interest_balance"
_BUCKET_NAME_NORMALIZE["default"] = "default_interest_balance"
_BUCKET_NAME_NORMALIZE["interest accrued"] = "interest_accrued_balance"
_BUCKET_NAME_NORMALIZE["principal not due"] = "principal_not_due"
_BUCKET_NAME_NORMALIZE["principal not due balance"] = "principal_not_due"
_BUCKET_NAME_NORMALIZE["interest"] = "interest_arrears_balance"  # ambiguous; map to arrears


def _normalize_bucket_order(raw_order: list) -> list[str]:
    """Map config bucket names to BUCKET_TO_ALLOC keys so allocation never skips due to name mismatch."""
    out: list[str] = []
    for name in raw_order or []:
        if name is None:
            continue
        s = str(name).strip()
        if not s:
            continue
        key = (
            _BUCKET_NAME_NORMALIZE.get(s)
            or _BUCKET_NAME_NORMALIZE.get(s.lower())
            or _BUCKET_NAME_NORMALIZE.get(s.lower().replace(" ", "_"))
        )
        if key and key in BUCKET_TO_ALLOC:
            out.append(key)
    return out


# Standard profile: do not allocate to these buckets (overpayment goes to unapplied).
# Use Borrower-friendly profile if you want payments to reduce principal_not_due / interest_accrued on up-to-date loans.
STANDARD_SKIP_BUCKETS = ("interest_accrued_balance", "principal_not_due")


def _get_waterfall_config(cfg: dict) -> tuple[str, list]:
    """Return (profile_key, bucket_order) from system config. Raises if not configured."""
    profile_name = (cfg.get("payment_waterfall") or "Standard").strip().lower()
    profile_key = "borrower_friendly" if profile_name.startswith("borrower") else "standard"
    profiles = cfg.get("waterfall_profiles") or {}
    raw_order = profiles.get(profile_key)
    if not raw_order:
        raise ValueError(
            "Waterfall profile is not configured. Please maintain System configuration: set "
            "waterfall_profiles with profile 'standard' and/or 'borrower_friendly', and "
            "payment_waterfall, then retry."
        )
    bucket_order = _normalize_bucket_order(raw_order)
    if not bucket_order:
        raise ValueError(
            "Waterfall profile has no valid bucket names. Use internal keys (e.g. interest_arrears_balance, "
            "principal_arrears) or ensure waterfall_profiles in config match expected names."
        )
    return profile_key, bucket_order


# Minimum remaining amount to treat as zero (avoids float noise; 1e-4 = 0.01 cent)
_WATERFALL_REMAINING_EPS = 1e-4


def compute_waterfall_allocation(
    amount: float,
    balances: dict[str, float],
    bucket_order: list,
    profile_key: str,
    *,
    state_as_of: date | None = None,
    repayment_id: int | None = None,
) -> tuple[dict[str, float], float]:
    """
    Compute allocation of a payment amount across buckets (waterfall). Pure logic, no DB.
    Returns (alloc_dict, unapplied_amount). alloc_dict uses alloc_* keys (e.g. alloc_principal_arrears).

    Standard profile skips interest_accrued_balance and principal_not_due (see STANDARD_SKIP_BUCKETS),
    so overpayments on up-to-date loans go to unapplied unless Borrower-friendly profile is used.
    """
    alloc: dict[str, float] = {alloc_key: 0.0 for _b, (alloc_key, _sk) in BUCKET_TO_ALLOC.items()}
    remaining = amount
    for bucket_name in bucket_order:
        if bucket_name not in BUCKET_TO_ALLOC:
            continue
        if profile_key == "standard" and bucket_name in STANDARD_SKIP_BUCKETS:
            continue
        alloc_key, state_key = BUCKET_TO_ALLOC[bucket_name]
        bucket_balance = max(0.0, balances.get(state_key, 0.0))
        to_alloc = min(remaining, bucket_balance)
        alloc[alloc_key] = to_alloc
        remaining -= to_alloc
        if remaining <= _WATERFALL_REMAINING_EPS:
            remaining = 0.0
            break
    for bucket_name in bucket_order:
        if bucket_name not in BUCKET_TO_ALLOC:
            continue
        alloc_key, state_key = BUCKET_TO_ALLOC[bucket_name]
        bal = balances.get(state_key, 0.0)
        a = alloc.get(alloc_key, 0.0)
        if a > bal + 0.01:
            raise ValueError(
                f"Allocation control: {alloc_key}={a:.2f} exceeds balance due {state_key}={bal:.2f} "
                f"(state as_of_date={state_as_of}) for repayment_id={repayment_id}. "
                "Cannot allocate more than balance due."
            )
    unapplied = float(as_10dp(remaining)) if remaining > _WATERFALL_REMAINING_EPS else 0.0
    return {k: float(as_10dp(v)) for k, v in alloc.items()}, unapplied
