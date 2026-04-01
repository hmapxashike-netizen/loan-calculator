"""Accounting UI facades: ensure each tab-scoped object delegates to one AccountingService."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from services.accounting_ui import AccountingUiBundle, build_accounting_ui_bundle


@pytest.fixture
def mock_svc() -> MagicMock:
    return MagicMock()


def test_build_bundle_exposes_same_service_on_all_facades(mock_svc: MagicMock) -> None:
    b = build_accounting_ui_bundle(svc=mock_svc)
    assert isinstance(b, AccountingUiBundle)
    assert b.svc is mock_svc
    # All facades should forward to the same underlying service object.
    assert b.coa._svc is mock_svc  # noqa: SLF001 — contract of thin wrapper
    assert b.templates._svc is mock_svc  # noqa: SLF001
    assert b.receipt_gl._svc is mock_svc  # noqa: SLF001
    assert b.reports._svc is mock_svc  # noqa: SLF001


def test_coa_ui_delegates(mock_svc: MagicMock) -> None:
    mock_svc.is_coa_initialized.return_value = True
    b = build_accounting_ui_bundle(svc=mock_svc)
    assert b.coa.is_coa_initialized() is True
    mock_svc.is_coa_initialized.assert_called_once_with()

    b.coa.list_accounts()
    mock_svc.list_accounts.assert_called_once_with()


def test_templates_ui_delegates(mock_svc: MagicMock) -> None:
    mock_svc.list_all_transaction_templates.return_value = []
    b = build_accounting_ui_bundle(svc=mock_svc)
    assert b.templates.list_all_transaction_templates() == []
    mock_svc.list_all_transaction_templates.assert_called_once_with()

    b.templates.link_journal("X", "tag_a", "DEBIT", "d", "EVENT")
    mock_svc.link_journal.assert_called_once_with("X", "tag_a", "DEBIT", "d", "EVENT")


def test_receipt_gl_ui_delegates(mock_svc: MagicMock) -> None:
    mock_svc.list_receipt_gl_mappings.return_value = [{"id": 1}]
    b = build_accounting_ui_bundle(svc=mock_svc)
    assert b.receipt_gl.list_receipt_gl_mappings() == [{"id": 1}]
    mock_svc.list_receipt_gl_mappings.assert_called_once_with()

    b.receipt_gl.delete_receipt_gl_mapping(99)
    mock_svc.delete_receipt_gl_mapping.assert_called_once_with(99)


def test_reports_ui_delegates(mock_svc: MagicMock) -> None:
    from datetime import date

    d = date(2026, 1, 15)
    mock_svc.get_trial_balance.return_value = []
    b = build_accounting_ui_bundle(svc=mock_svc)
    assert b.reports.get_trial_balance(d) == []
    mock_svc.get_trial_balance.assert_called_once_with(d)

    b.reports.list_statement_snapshots(statement_type="BALANCE_SHEET", limit=5)
    mock_svc.list_statement_snapshots.assert_called_once_with(
        statement_type="BALANCE_SHEET", limit=5
    )
