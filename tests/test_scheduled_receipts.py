"""Scheduled / future-dated receipts: EOD stage and service wiring."""

from __future__ import annotations

from datetime import date

import pytest


def test_activate_scheduled_stage_raises_when_activation_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_activate(*_a, **_k):
        return (0, ["repayment_id=99: ValueError: test"])

    monkeypatch.setattr(
        "loan_management.scheduled_receipts.activate_scheduled_receipts_for_eod_date",
        fake_activate,
    )
    from eod.core import _activate_scheduled_receipts_stage

    with pytest.raises(RuntimeError, match="activate_scheduled_receipts failed"):
        _activate_scheduled_receipts_stage(date(2026, 4, 1), {}, allow_system_date_eod=False)


def test_activate_scheduled_stage_ok_when_no_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "loan_management.scheduled_receipts.activate_scheduled_receipts_for_eod_date",
        lambda *_a, **_k: (3, []),
    )
    from eod.core import _activate_scheduled_receipts_stage

    n = _activate_scheduled_receipts_stage(date(2026, 4, 1), {}, allow_system_date_eod=False)
    assert n == 3


def test_record_repayment_rejects_non_posted_status() -> None:
    from loan_management.repayment_record import record_repayment

    with pytest.raises(ValueError, match="record_repayment only supports status='posted'"):
        record_repayment(
            loan_id=1,
            amount=1.0,
            payment_date=date(2026, 1, 1),
            source_cash_gl_account_id="x",
            status="scheduled",
            skip_loan_approval_guard=True,
            system_config={},
        )
