"""EOD replay/backfill: refresh allocations before loan engine (backfill path only)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest


def test_run_backfill_passes_replay_refresh_allocations(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.eod_service as es

    captured: dict = {}

    def fake_run_eod(d: date, **kw: object) -> object:
        captured["date"] = d
        captured["kwargs"] = kw
        from eod.core import EODResult

        now = datetime.now(timezone.utc)
        return EODResult(
            run_id="test-run",
            as_of_date=d,
            loans_processed=0,
            started_at=now,
            finished_at=now,
            tasks_run=(),
        )

    monkeypatch.setattr(es, "run_eod_for_date", fake_run_eod)
    es.run_backfill_eod_for_date(date(2026, 3, 15))
    assert captured["kwargs"].get("replay_refresh_allocations") is True


def test_replay_refresh_allocations_runs_reallocate_then_waterfall(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "eod.core.get_repayment_ids_for_value_date",
        lambda _d: [10, 20],
    )
    monkeypatch.setattr(
        "eod.core.get_liquidation_repayment_ids_for_value_date",
        lambda _d: [99],
    )
    calls: list[tuple[str, int]] = []

    def fake_realloc(rid: int, *, system_config: dict) -> None:
        calls.append(("realloc", rid))

    def fake_wf(rid: int, *, system_config: dict) -> None:
        calls.append(("wf", rid))

    monkeypatch.setattr("eod.core.reallocate_repayment", fake_realloc)
    monkeypatch.setattr("eod.core.allocate_repayment_waterfall", fake_wf)

    from eod.core import _replay_refresh_allocations_for_date

    n = _replay_refresh_allocations_for_date(date(2026, 1, 1), {})
    assert n == 3
    assert calls == [("realloc", 10), ("realloc", 20), ("wf", 99)]


def test_replay_refresh_allocations_continues_on_single_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "eod.core.get_repayment_ids_for_value_date",
        lambda _d: [1, 2],
    )
    monkeypatch.setattr(
        "eod.core.get_liquidation_repayment_ids_for_value_date",
        lambda _d: [],
    )

    def fake_realloc(rid: int, *, system_config: dict) -> None:
        if rid == 1:
            raise RuntimeError("boom")
        return None

    monkeypatch.setattr("eod.core.reallocate_repayment", fake_realloc)
    monkeypatch.setattr("eod.core.allocate_repayment_waterfall", MagicMock())

    from eod.core import _replay_refresh_allocations_for_date

    n = _replay_refresh_allocations_for_date(date(2026, 1, 1), {})
    assert n == 1
