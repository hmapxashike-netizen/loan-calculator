"""Month-end EOD accounting: EOM REGINT and restructure amortisation use bulk_post_events."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest


def test_eom_regular_interest_income_recognition_bulk_single_call(monkeypatch: pytest.MonkeyPatch) -> None:
    from eod import core as eod_core

    captured: list[list] = []

    class FakeSvc:
        def bulk_post_events(self, items: list) -> None:
            captured.append(list(items))

    rows = [
        {"loan_id": 101, "reg_mtd": Decimal("12.34")},
        {"loan_id": 202, "reg_mtd": Decimal("0.01")},
    ]

    def fake_get_conn():
        cm = MagicMock()
        conn = MagicMock()
        cur = MagicMock()

        def exec_side(*_a, **_k):
            pass

        cur.execute.side_effect = exec_side
        cur.fetchall.return_value = rows
        conn.cursor.return_value.__enter__.return_value = cur
        conn.cursor.return_value.__exit__.return_value = None
        cm.__enter__.return_value = conn
        cm.__exit__.return_value = None
        return cm

    monkeypatch.setattr(eod_core, "_get_conn", fake_get_conn)

    from accounting.periods import normalize_accounting_period_config

    period_cfg = normalize_accounting_period_config({})
    events = {"EOM_REGULAR_INTEREST_INCOME_RECOGNITION"}
    n = eod_core._run_eom_regular_interest_income_recognition(
        date(2024, 10, 31),
        period_cfg,
        events,
        FakeSvc(),
    )
    assert n == 2
    assert len(captured) == 1
    items = captured[0]
    assert len(items) == 2
    assert items[0]["event_type"] == "EOM_REGULAR_INTEREST_INCOME_RECOGNITION"
    assert items[0]["event_id"] == "EOM-REGINT-2024-10-LOAN-101"
    assert items[0]["loan_id"] == 101
    assert items[0]["amount"] == Decimal("12.34")
    assert items[1]["event_id"] == "EOM-REGINT-2024-10-LOAN-202"


def test_eom_regular_interest_skips_when_event_not_in_run(monkeypatch: pytest.MonkeyPatch) -> None:
    from eod import core as eod_core

    calls: list = []

    class FakeSvc:
        def bulk_post_events(self, items: list) -> None:
            calls.append(items)

    n = eod_core._run_eom_regular_interest_income_recognition(
        date(2024, 10, 31),
        {},
        set(),
        FakeSvc(),
    )
    assert n == 0
    assert calls == []


def test_restructure_fee_amortisation_bulk_single_call(monkeypatch: pytest.MonkeyPatch) -> None:
    from eod import core as eod_core

    captured: list[list] = []

    class FakeSvc:
        def bulk_post_events(self, items: list) -> None:
            captured.append(list(items))

    mod_rows = [
        {
            "id": 9001,
            "loan_id": 55,
            "modification_date": date(2024, 10, 1),
            "restructure_fee_amount": Decimal("120"),
            "new_term": 12,
        },
    ]

    def fake_get_conn():
        cm = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        cur.execute.return_value = None
        cur.fetchall.return_value = mod_rows
        conn.cursor.return_value.__enter__.return_value = cur
        conn.cursor.return_value.__exit__.return_value = None
        cm.__enter__.return_value = conn
        cm.__exit__.return_value = None
        return cm

    monkeypatch.setattr(eod_core, "_get_conn", fake_get_conn)

    n = eod_core._run_restructure_fee_amortisation_month_end(
        date(2024, 10, 31),
        {"RESTRUCTURE_FEE_AMORTISATION"},
        FakeSvc(),
    )
    assert n == 1
    assert len(captured) == 1
    assert captured[0][0]["event_type"] == "RESTRUCTURE_FEE_AMORTISATION"
    assert captured[0][0]["event_id"].endswith("-RESTRUCTURE_FEE_AMORTISATION-9001")
    assert captured[0][0]["loan_id"] == 55
