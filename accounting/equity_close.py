"""
Month-end P&L close to current year earnings and year-end CYE sweep to retained earnings.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp


def build_month_end_pnl_close_lines(
    *,
    ie_balances: list[dict[str, Any]],
    cye_account_id: str,
) -> list[dict[str, Any]]:
    """
    Build balanced journal lines that zero cumulative INCOME/EXPENSE balances through the
    close date and offset to current year earnings.

    ``ie_balances`` rows: account_id, category, debit, credit (from get_balances_by_category).
    """
    lines: list[dict[str, Any]] = []
    total_debit = Decimal("0")
    total_credit = Decimal("0")

    for row in ie_balances or []:
        cat = (row.get("category") or "").upper()
        aid = row.get("account_id")
        if aid is None:
            continue
        d = as_10dp(Decimal(str(row.get("debit") or 0)))
        c = as_10dp(Decimal(str(row.get("credit") or 0)))
        if cat == "INCOME":
            bal = c - d
            if bal == 0:
                continue
            lines.append(
                {
                    "account_id": aid,
                    "debit": bal,
                    "credit": Decimal("0"),
                    "memo": "Month-end close — income",
                }
            )
            total_debit += bal
        elif cat == "EXPENSE":
            bal = d - c
            if bal == 0:
                continue
            lines.append(
                {
                    "account_id": aid,
                    "debit": Decimal("0"),
                    "credit": bal,
                    "memo": "Month-end close — expense",
                }
            )
            total_credit += bal

    diff = as_10dp(total_debit - total_credit)
    if diff > 0:
        lines.append(
            {
                "account_id": cye_account_id,
                "debit": Decimal("0"),
                "credit": diff,
                "memo": "Month-end close — net profit to current year earnings",
            }
        )
        total_credit += diff
    elif diff < 0:
        amt = as_10dp(-diff)
        lines.append(
            {
                "account_id": cye_account_id,
                "debit": amt,
                "credit": Decimal("0"),
                "memo": "Month-end close — net loss from current year earnings",
            }
        )
        total_debit += amt

    return lines
