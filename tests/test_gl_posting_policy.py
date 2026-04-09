from __future__ import annotations

from datetime import date
from decimal import Decimal

from accounting.dal import AccountingRepository


class _FakeCursor:
    def __init__(self, *, fetchone_values: list[dict | None], fetchall_values: list[list[dict]] | None = None):
        self.fetchone_values = list(fetchone_values)
        self.fetchall_values = list(fetchall_values or [])
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql, params=None):
        self.executed.append((str(sql), params))

    def fetchone(self):
        if self.fetchone_values:
            return self.fetchone_values.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_values:
            return self.fetchall_values.pop(0)
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, cur: _FakeCursor):
        self.cur = cur
        self.commits = 0

    def cursor(self, *args, **kwargs):
        return self.cur

    def commit(self):
        self.commits += 1


def _balanced_lines() -> list[dict]:
    return [
        {"account_id": "a1", "debit": Decimal("120.00"), "credit": Decimal("0")},
        {"account_id": "a2", "debit": Decimal("0"), "credit": Decimal("120.00")},
    ]


def _last_header_insert_params(cur: _FakeCursor):
    inserts = [p for (sql, p) in cur.executed if "INSERT INTO journal_entries" in sql]
    assert inserts, "Expected journal_entries insert"
    return inserts[-1]


def test_standard_prior_month_new_post_redirects_to_anchor_month_first_day():
    cur = _FakeCursor(
        fetchone_values=[
            None,  # existing active event
            {"is_closed": False},  # period open
            {"id": 11},  # inserted header id
        ]
    )
    repo = AccountingRepository(_FakeConn(cur))
    repo.save_journal_entry(
        date(2026, 3, 20),
        "ref-1",
        "desc",
        "EV-1",
        "PAYMENT_PRINCIPAL",
        "tester",
        _balanced_lines(),
        posting_policy="standard",
        gl_anchor_date=date(2026, 4, 12),
    )
    hdr = _last_header_insert_params(cur)
    assert hdr[0] == date(2026, 4, 1)


def test_standard_prior_month_existing_posts_calendar_adjustment_not_replace():
    cur = _FakeCursor(
        fetchone_values=[
            {"id": 1, "entry_date": date(2026, 3, 10)},  # existing active header
            None,  # existing calendar adjustment
            {"id": 22},  # inserted adjustment header
        ],
        fetchall_values=[
            [
                {"account_id": "a1", "debit": Decimal("100"), "credit": Decimal("0"), "memo": "old"},
                {"account_id": "a2", "debit": Decimal("0"), "credit": Decimal("100"), "memo": "old"},
            ]
        ],
    )
    repo = AccountingRepository(_FakeConn(cur))
    repo.save_journal_entry(
        date(2026, 3, 20),
        "ref-2",
        "desc",
        "EV-2",
        "PAYMENT_PRINCIPAL",
        "tester",
        _balanced_lines(),
        posting_policy="standard",
        gl_anchor_date=date(2026, 4, 12),
    )
    hdr = _last_header_insert_params(cur)
    assert hdr[0] == date(2026, 4, 1)
    assert hdr[4] == "CALENDAR_MONTH_ADJUSTMENT"
    assert not any(
        "UPDATE journal_entries" in sql and "WHERE id = %s" in sql and params == (1,)
        for sql, params in cur.executed
    )


def test_standard_same_month_keeps_requested_entry_date_for_replace():
    cur = _FakeCursor(
        fetchone_values=[
            {"id": 3, "entry_date": date(2026, 4, 2)},  # existing
            {"is_closed": False},  # open period
            {"id": 44},  # inserted header
        ]
    )
    repo = AccountingRepository(_FakeConn(cur))
    requested = date(2026, 4, 9)
    repo.save_journal_entry(
        requested,
        "ref-3",
        "desc",
        "EV-3",
        "PAYMENT_PRINCIPAL",
        "tester",
        _balanced_lines(),
        posting_policy="standard",
        gl_anchor_date=date(2026, 4, 12),
    )
    hdr = _last_header_insert_params(cur)
    assert hdr[0] == requested


def test_eod_replay_bypasses_closed_period_branch_and_calendar_redirect():
    cur = _FakeCursor(
        fetchone_values=[
            {"id": 5, "entry_date": date(2026, 1, 15)},  # existing
            {"id": 55},  # inserted replacement header
        ]
    )
    repo = AccountingRepository(_FakeConn(cur))
    requested = date(2026, 1, 20)
    repo.save_journal_entry(
        requested,
        "ref-4",
        "desc",
        "EV-4",
        "PAYMENT_PRINCIPAL",
        "tester",
        _balanced_lines(),
        posting_policy="eod_replay",
    )
    hdr = _last_header_insert_params(cur)
    assert hdr[0] == requested
    assert not any("SELECT is_closed FROM financial_periods" in sql for sql, _ in cur.executed)


def test_calendar_adjustment_idempotency_supersedes_existing_adjustment():
    cur = _FakeCursor(
        fetchone_values=[
            {"id": 10, "entry_date": date(2026, 3, 5)},  # existing active event
            {"id": 90, "entry_date": date(2026, 4, 1)},  # existing calendar adjustment
            {"id": 91},  # new calendar adjustment
        ],
        fetchall_values=[
            [
                {"account_id": "a1", "debit": Decimal("100"), "credit": Decimal("0"), "memo": "old"},
                {"account_id": "a2", "debit": Decimal("0"), "credit": Decimal("100"), "memo": "old"},
            ]
        ],
    )
    repo = AccountingRepository(_FakeConn(cur))
    repo.save_journal_entry(
        date(2026, 3, 18),
        "ref-5",
        "desc",
        "EV-5",
        "PAYMENT_PRINCIPAL",
        "tester",
        _balanced_lines(),
        posting_policy="standard",
        gl_anchor_date=date(2026, 4, 12),
    )
    assert any(
        "UPDATE journal_entries" in sql and "superseded_by_id = %s" in sql and params == (91, 90)
        for sql, params in cur.executed
    )
