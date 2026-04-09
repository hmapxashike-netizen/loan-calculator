"""P&L reporting excludes month-end close journals; GL close still uses full balances."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from accounting.service import AccountingService, PNL_REPORT_EXCLUDED_EVENT_TAGS


@pytest.fixture
def svc() -> AccountingService:
    return AccountingService()


def test_get_profit_and_loss_passes_exclude_month_end_pnl(svc: AccountingService) -> None:
    mock_repo = MagicMock()
    mock_repo.get_balances_by_category.return_value = []
    mock_conn = MagicMock()
    with (
        patch("accounting.service.get_conn", return_value=mock_conn),
        patch("accounting.service.AccountingRepository", return_value=mock_repo),
    ):
        svc.get_profit_and_loss(date(2026, 1, 1), date(2026, 1, 31))
    mock_repo.get_balances_by_category.assert_called_once_with(
        ["INCOME", "EXPENSE"],
        date(2026, 1, 1),
        date(2026, 1, 31),
        exclude_event_tags=PNL_REPORT_EXCLUDED_EVENT_TAGS,
    )


def test_get_net_profit_loss_passes_exclude_month_end_pnl(svc: AccountingService) -> None:
    mock_repo = MagicMock()
    mock_repo.get_balances_by_category.return_value = []
    mock_conn = MagicMock()
    with (
        patch("accounting.service.get_conn", return_value=mock_conn),
        patch("accounting.service.AccountingRepository", return_value=mock_repo),
    ):
        svc.get_net_profit_loss(date(2026, 1, 1), date(2026, 1, 31))
    mock_repo.get_balances_by_category.assert_called_once_with(
        ["INCOME", "EXPENSE"],
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        exclude_event_tags=PNL_REPORT_EXCLUDED_EVENT_TAGS,
    )


def test_post_month_end_pnl_close_uses_unfiltered_balances(svc: AccountingService) -> None:
    """Close posting must use cumulative I&E without excluding MONTH_END_PNL."""
    mock_repo = MagicMock()
    mock_repo.get_active_journal_header.return_value = None
    mock_repo.get_account_id_by_code.return_value = {"id": "cye-uuid"}
    mock_repo.get_balances_by_category.return_value = [
        {
            "account_id": "inc1",
            "code": "4000",
            "name": "Revenue",
            "category": "INCOME",
            "debit": Decimal("0"),
            "credit": Decimal("100.0000000000"),
        }
    ]
    mock_conn = MagicMock()
    period_end = date(2026, 1, 31)

    def fake_build(*, ie_balances, cye_account_id):
        return [
            {
                "account_id": "inc1",
                "debit": Decimal("100"),
                "credit": Decimal("0"),
                "memo": "close",
            },
            {
                "account_id": cye_account_id,
                "debit": Decimal("0"),
                "credit": Decimal("100"),
                "memo": "cye",
            },
        ]

    with (
        patch("accounting.service.get_conn", return_value=mock_conn),
        patch("accounting.service.AccountingRepository", return_value=mock_repo),
        patch("accounting.service.load_system_config_from_db", return_value={}),
        patch("accounting.service.normalize_accounting_period_config") as npc,
        patch("accounting.service.get_month_period_bounds") as gmb,
        patch("accounting.service.resolve_accounting_equity_config") as rqe,
        patch("accounting.service.build_month_end_pnl_close_lines", side_effect=fake_build),
        patch("accounting.service.assert_journal_lines_balanced"),
        patch.object(svc, "_validate_not_posting_to_parent_after_transition"),
    ):
        from types import SimpleNamespace

        npc.return_value = SimpleNamespace()
        gmb.return_value = SimpleNamespace(start_date=date(2026, 1, 1), end_date=period_end)
        rqe.return_value = SimpleNamespace(current_year_earnings_account_code="CYE")

        out = svc.post_month_end_pnl_close_to_cye(
            period_end, created_by="test", system_config={}, force=True
        )

    assert out.get("status") == "posted"
    mock_repo.get_balances_by_category.assert_called_once_with(
        ["INCOME", "EXPENSE"], end_date=period_end
    )
    assert mock_repo.get_balances_by_category.call_args.kwargs.get("exclude_event_tags") is None
