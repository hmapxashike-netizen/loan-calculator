"""bulk_save_journal_entries must not insert duplicate (event_id, event_tag) in one batch."""

from __future__ import annotations

from accounting.dal import _dedupe_journal_bulk_entries


def test_dedupe_journal_bulk_entries_last_wins() -> None:
    first = {"event_id": "EOD-2024-09-09-1-X", "event_tag": "X", "lines": []}
    second = {"event_id": "EOD-2024-09-09-1-X", "event_tag": "X", "lines": [{"n": 2}]}
    out = _dedupe_journal_bulk_entries([first, second])
    assert len(out) == 1
    assert out[0]["lines"] == [{"n": 2}]


def test_dedupe_journal_bulk_entries_noop_when_unique() -> None:
    rows = [
        {"event_id": "a", "event_tag": "T1", "lines": []},
        {"event_id": "b", "event_tag": "T1", "lines": []},
    ]
    out = _dedupe_journal_bulk_entries(rows)
    assert len(out) == 2
