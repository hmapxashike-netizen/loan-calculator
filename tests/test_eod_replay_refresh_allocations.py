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
        "eod.core._clear_unapplied_liquidations_for_date",
        lambda _d: 0,
    )
    monkeypatch.setattr(
        "eod.core.get_repayment_ids_for_value_date",
        lambda _d: [10, 20],
    )
    calls: list[tuple[str, int]] = []

    def fake_realloc(rid: int, *, system_config: dict) -> None:
        calls.append(("realloc", rid))

    monkeypatch.setattr("eod.core.reallocate_repayment", fake_realloc)

    from eod.core import _replay_refresh_allocations_for_date

    n = _replay_refresh_allocations_for_date(date(2026, 1, 1), {})
    assert n == 2
    assert calls == [("realloc", 10), ("realloc", 20)]


def test_replay_refresh_allocations_continues_on_single_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "eod.core._clear_unapplied_liquidations_for_date",
        lambda _d: 0,
    )
    monkeypatch.setattr(
        "eod.core.get_repayment_ids_for_value_date",
        lambda _d: [1, 2],
    )

    def fake_realloc(rid: int, *, system_config: dict) -> None:
        if rid == 1:
            raise RuntimeError("boom")
        return None

    monkeypatch.setattr("eod.core.reallocate_repayment", fake_realloc)
    from eod.core import _replay_refresh_allocations_for_date

    n = _replay_refresh_allocations_for_date(date(2026, 1, 1), {})
    assert n == 1


def test_replay_refresh_allocations_clears_prior_liquidations_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker: dict[str, int] = {"cleared": 0}

    def fake_clear(_d: date) -> int:
        marker["cleared"] += 1
        return 3

    monkeypatch.setattr("eod.core._clear_unapplied_liquidations_for_date", fake_clear)
    monkeypatch.setattr("eod.core.get_repayment_ids_for_value_date", lambda _d: [])
    monkeypatch.setattr("eod.core.reallocate_repayment", MagicMock())

    from eod.core import _replay_refresh_allocations_for_date

    n = _replay_refresh_allocations_for_date(date(2026, 1, 1), {})
    assert n == 0
    assert marker["cleared"] == 1


def test_repost_gl_after_replay_for_date_reposts_each_loan(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cur:
        def __init__(self):
            self._rows = [(1800,), (2187,)]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return list(self._rows)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cur()

    monkeypatch.setattr("eod.core._get_conn", lambda: _Conn())
    calls: list[int] = []

    def _fake_repost(loan_id: int, start_date: date, end_date: date, *, created_by: str = "system") -> None:
        assert start_date == end_date == date(2026, 1, 1)
        assert created_by == "system"
        calls.append(loan_id)

    monkeypatch.setattr("eod.core.repost_gl_for_loan_date_range", _fake_repost)
    from eod.core import _repost_gl_after_replay_for_date

    n = _repost_gl_after_replay_for_date(date(2026, 1, 1))
    assert n == 2
    assert calls == [1800, 2187]


def test_repost_gl_after_replay_for_date_raises_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return [(1800,)]

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cur()

    monkeypatch.setattr("eod.core._get_conn", lambda: _Conn())

    def _boom(*_args, **_kwargs):
        raise RuntimeError("gl down")

    monkeypatch.setattr("eod.core.repost_gl_for_loan_date_range", _boom)
    from eod.core import _repost_gl_after_replay_for_date

    with pytest.raises(RuntimeError, match="Replay GL repost failed"):
        _repost_gl_after_replay_for_date(date(2026, 1, 1))
