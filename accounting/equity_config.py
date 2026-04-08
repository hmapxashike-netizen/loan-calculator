"""
Configurable equity account codes for P&L close (current year earnings) and retained earnings.

Values are read from ``system_config["accounting_equity"]`` with documented defaults.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

DEFAULT_RETAINED_EARNINGS_CODE = "C300003"
DEFAULT_CURRENT_YEAR_EARNINGS_CODE = "C300005"


@dataclass(frozen=True)
class AccountingEquityConfig:
    retained_earnings_account_code: str
    current_year_earnings_account_code: str


def merge_default_accounting_equity(system_config: dict[str, Any] | None) -> dict[str, Any]:
    """Return a copy of system_config with ``accounting_equity`` keys defaulted when missing."""
    cfg = dict(system_config or {})
    ae = dict(cfg.get("accounting_equity") or {})
    if not str(ae.get("retained_earnings_account_code") or "").strip():
        ae["retained_earnings_account_code"] = DEFAULT_RETAINED_EARNINGS_CODE
    if not str(ae.get("current_year_earnings_account_code") or "").strip():
        ae["current_year_earnings_account_code"] = DEFAULT_CURRENT_YEAR_EARNINGS_CODE
    cfg["accounting_equity"] = ae
    return cfg


def resolve_accounting_equity_config(system_config: dict[str, Any] | None) -> AccountingEquityConfig:
    """
    Resolve RE and CYE account codes from config.

    Raises:
        ValueError: if codes are missing or blank after defaults merge.
    """
    cfg = merge_default_accounting_equity(system_config)
    ae = cfg.get("accounting_equity") or {}
    re_code = str(ae.get("retained_earnings_account_code") or "").strip().upper()
    cye_code = str(ae.get("current_year_earnings_account_code") or "").strip().upper()
    if not re_code or not cye_code:
        raise ValueError(
            "system_config.accounting_equity must set retained_earnings_account_code and "
            "current_year_earnings_account_code (non-empty)."
        )
    return AccountingEquityConfig(
        retained_earnings_account_code=re_code,
        current_year_earnings_account_code=cye_code,
    )


def net_profit_loss_from_balance_rows(rows: list[dict[str, Any]]) -> Decimal:
    """
    Same sign convention as P&L UI: income (credit - debit) minus expense (debit - credit).

    Each row must have ``category`` in (INCOME, EXPENSE) and numeric ``debit`` / ``credit``.
    """
    total = Decimal("0")
    for row in rows or []:
        cat = (row.get("category") or "").upper()
        d = as_10dp(Decimal(str(row.get("debit") or 0)))
        c = as_10dp(Decimal(str(row.get("credit") or 0)))
        if cat == "INCOME":
            total += c - d
        elif cat == "EXPENSE":
            total -= d - c
    return as_10dp(total)
