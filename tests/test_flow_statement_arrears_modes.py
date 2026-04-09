from datetime import date
from decimal import Decimal

import pytest

import reporting.statement_events as se
import reporting.statements as rs
from reporting.statement_events import StatementEvent


def _mk_event(d: date, narr: str, debit: str = "0", credit: str = "0") -> StatementEvent:
    return StatementEvent(
        event_date=d,
        event_type="REGULAR_INTEREST_ACCRUAL",
        narration=narr,
        debit=Decimal(debit),
        credit=Decimal(credit),
        sort_ordinal=20,
    )


def _mk_ds(*, total_delinquency_arrears: str = "0") -> dict:
    return {"total_delinquency_arrears": Decimal(total_delinquency_arrears)}


def test_flow_arrears_mode_end_snapshot_keeps_single_eod_value(monkeypatch):
    monkeypatch.setattr(
        rs,
        "get_loan",
        lambda loan_id: {"id": loan_id, "customer_id": 1, "disbursement_date": date(2025, 1, 1)},
    )
    monkeypatch.setattr(
        rs,
        "get_loan_daily_state_balances",
        lambda loan_id, as_of_date: _mk_ds(total_delinquency_arrears="7")
        if as_of_date == date(2025, 1, 2)
        else _mk_ds(total_delinquency_arrears="3"),
    )
    monkeypatch.setattr(
        se,
        "build_merged_customer_flow_events",
        lambda loan_id, start_date, end_date: (
            [
                _mk_event(date(2025, 1, 1), "e1", debit="1"),
                _mk_event(date(2025, 1, 2), "e2", debit="1"),
            ],
            Decimal("0"),
        ),
    )
    monkeypatch.setattr(
        se,
        "reconcile_running_to_loan_daily_state",
        lambda computed_closing, loan_id, as_of_date: {"ok": True},
    )

    rows, meta = rs.generate_customer_facing_flow_statement(
        1,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        as_of_date=date(2025, 1, 2),
        arrears_mode="end_snapshot",
    )

    assert meta["arrears_mode"] == "end_snapshot"
    assert [r["Arrears"] for r in rows] == [7.0, 7.0, 7.0]


def test_flow_arrears_mode_by_row_date_tracks_daily_state_by_event_date(monkeypatch):
    monkeypatch.setattr(
        rs,
        "get_loan",
        lambda loan_id: {"id": loan_id, "customer_id": 1, "disbursement_date": date(2025, 1, 1)},
    )
    monkeypatch.setattr(
        rs,
        "get_loan_daily_state_balances",
        lambda loan_id, as_of_date: _mk_ds(total_delinquency_arrears="7")
        if as_of_date == date(2025, 1, 2)
        else _mk_ds(total_delinquency_arrears="3"),
    )
    monkeypatch.setattr(
        rs,
        "get_loan_daily_state_range",
        lambda loan_id, start_date, end_date: [
            {"as_of_date": date(2025, 1, 1), "total_delinquency_arrears": Decimal("4")},
            {"as_of_date": date(2025, 1, 2), "total_delinquency_arrears": Decimal("6")},
        ],
    )
    monkeypatch.setattr(
        se,
        "build_merged_customer_flow_events",
        lambda loan_id, start_date, end_date: (
            [
                _mk_event(date(2025, 1, 1), "e1", debit="1"),
                _mk_event(date(2025, 1, 2), "e2", debit="1"),
            ],
            Decimal("0"),
        ),
    )
    monkeypatch.setattr(
        se,
        "reconcile_running_to_loan_daily_state",
        lambda computed_closing, loan_id, as_of_date: {"ok": True},
    )

    rows, meta = rs.generate_customer_facing_flow_statement(
        1,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        as_of_date=date(2025, 1, 2),
        arrears_mode="by_row_date",
    )

    assert meta["arrears_mode"] == "by_row_date"
    assert [r["Arrears"] for r in rows] == [4.0, 6.0, 7.0]


def test_flow_arrears_mode_rejects_unsupported_value(monkeypatch):
    monkeypatch.setattr(
        rs,
        "get_loan",
        lambda loan_id: {"id": loan_id, "customer_id": 1, "disbursement_date": date(2025, 1, 1)},
    )

    with pytest.raises(ValueError, match="Unsupported arrears_mode"):
        rs.generate_customer_facing_flow_statement(
            1,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 2),
            arrears_mode="x",
        )
