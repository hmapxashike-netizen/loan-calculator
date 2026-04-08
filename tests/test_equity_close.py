"""Unit tests for P&L close helpers, net P&L aggregation, and equity config defaults."""

from __future__ import annotations

from decimal import Decimal

from accounting.equity_close import build_month_end_pnl_close_lines
from accounting.equity_config import (
    merge_default_accounting_equity,
    net_profit_loss_from_balance_rows,
    resolve_accounting_equity_config,
)


def test_net_profit_loss_from_balance_rows_income_minus_expense():
    rows = [
        {"category": "INCOME", "debit": Decimal("0"), "credit": Decimal("100")},
        {"category": "EXPENSE", "debit": Decimal("30"), "credit": Decimal("0")},
    ]
    assert net_profit_loss_from_balance_rows(rows) == Decimal("100") - Decimal("30")


def test_net_profit_loss_from_balance_rows_empty():
    assert net_profit_loss_from_balance_rows([]) == Decimal("0")


def test_merge_default_accounting_equity_inserts_keys():
    cfg = merge_default_accounting_equity({})
    ae = cfg["accounting_equity"]
    assert ae["retained_earnings_account_code"] == "C300003"
    assert ae["current_year_earnings_account_code"] == "C300005"


def test_resolve_accounting_equity_config():
    eq = resolve_accounting_equity_config({})
    assert eq.retained_earnings_account_code == "C300003"
    assert eq.current_year_earnings_account_code == "C300005"


def test_resolve_accounting_equity_config_custom_codes():
    eq = resolve_accounting_equity_config(
        {
            "accounting_equity": {
                "retained_earnings_account_code": "C300003",
                "current_year_earnings_account_code": "X9",
            }
        }
    )
    assert eq.current_year_earnings_account_code == "X9"


def test_build_month_end_pnl_close_lines_balanced_profit():
    lines = build_month_end_pnl_close_lines(
        ie_balances=[
            {"account_id": "i1", "category": "INCOME", "debit": Decimal("0"), "credit": Decimal("50")},
            {"account_id": "e1", "category": "EXPENSE", "debit": Decimal("20"), "credit": Decimal("0")},
        ],
        cye_account_id="cye",
    )
    td = sum((ln["debit"] for ln in lines), Decimal("0"))
    tc = sum((ln["credit"] for ln in lines), Decimal("0"))
    assert td == tc == Decimal("50")
    cye_line = [ln for ln in lines if ln["account_id"] == "cye"]
    assert len(cye_line) == 1
    assert cye_line[0]["credit"] == Decimal("30")


def test_build_month_end_pnl_close_lines_loss_to_cye_debit():
    lines = build_month_end_pnl_close_lines(
        ie_balances=[
            {"account_id": "i1", "category": "INCOME", "debit": Decimal("0"), "credit": Decimal("10")},
            {"account_id": "e1", "category": "EXPENSE", "debit": Decimal("40"), "credit": Decimal("0")},
        ],
        cye_account_id="cye",
    )
    td = sum((ln["debit"] for ln in lines), Decimal("0"))
    tc = sum((ln["credit"] for ln in lines), Decimal("0"))
    assert td == tc == Decimal("40")
    cye_line = [ln for ln in lines if ln["account_id"] == "cye"]
    assert len(cye_line) == 1
    assert cye_line[0]["debit"] == Decimal("30")


def test_build_month_end_pnl_close_lines_empty():
    assert build_month_end_pnl_close_lines(ie_balances=[], cye_account_id="cye") == []
