"""
Provision / regulatory reporting configuration (system_config-backed).

Internal collateral-based provisions can read haircut + PD tables from DB when added.
RBZ-style runs can ignore security and use overrides from ``provision_rbz_config``.
"""
from __future__ import annotations

from typing import Any

from loan_management import load_system_config_from_db

PROVISION_RBZ_CONFIG_KEY = "provision_rbz_config"


def default_provision_rbz_config() -> dict[str, Any]:
    return {
        "version": 1,
        "description": "RBZ / regulatory provision overrides (typically no security haircut).",
        "use_security_haircuts": False,
        "use_collateral_in_exposure": False,
        "pd_percent_by_status": {
            "Standard": 1.0,
            "Watchlist": 10.0,
            "Substandard": 25.0,
            "Doubtful": 50.0,
            "Loss": 100.0,
        },
        "haircut_percent_by_security_subtype": {},
    }


def get_provision_rbz_config() -> dict[str, Any]:
    cfg = load_system_config_from_db() or {}
    block = cfg.get(PROVISION_RBZ_CONFIG_KEY)
    if not isinstance(block, dict):
        return default_provision_rbz_config()
    out = default_provision_rbz_config()
    out.update(block)
    return out


def merge_provision_rbz_config(updates: dict[str, Any]) -> dict[str, Any]:
    """Merge updates into stored config; caller persists via save_system_config_to_db."""
    base = default_provision_rbz_config()
    current = get_provision_rbz_config()
    base.update(current)
    base.update(updates)
    return base
