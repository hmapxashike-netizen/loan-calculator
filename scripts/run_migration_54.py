"""
Migration 54: ensure system_config JSON includes accrual_start_convention.

There is no separate system_configurations table; settings live in the `config` table
as JSON under key `system_config` (see schema/14_config_product_capacity.sql).

- Adds accrual_start_convention = EFFECTIVE_DAY when missing (canonical period-first accrual).
- Normalizes existing values to EFFECTIVE_DAY (canonical).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accrual_convention import (  # noqa: E402
    ACCRUAL_START_EFFECTIVE_DAY,
    normalize_accrual_start_convention,
)
from loan_management import load_system_config_from_db, save_system_config_to_db  # noqa: E402


def main() -> None:
    cfg = load_system_config_from_db()
    if not cfg:
        print(
            "Migration 54: no system_config row in DB yet — nothing to merge. "
            "Defaults apply until System configurations is saved from the app."
        )
        return
    raw = cfg.get("accrual_start_convention")
    if raw is None:
        cfg["accrual_start_convention"] = ACCRUAL_START_EFFECTIVE_DAY
        print("Migration 54: set accrual_start_convention = EFFECTIVE_DAY (default).")
    else:
        norm = normalize_accrual_start_convention(raw)
        if norm != raw:
            print(f"Migration 54: normalized accrual_start_convention {raw!r} -> {norm!r}.")
        cfg["accrual_start_convention"] = norm
    if save_system_config_to_db(cfg):
        print("Migration 54 complete: system_config.accrual_start_convention ensured.")
    else:
        raise SystemExit("Migration 54 failed: could not save system_config.")


if __name__ == "__main__":
    main()
