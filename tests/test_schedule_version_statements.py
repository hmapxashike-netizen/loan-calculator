"""Pure helpers for schedule version in force across recast/modification."""

from datetime import date

from loan_management.schedules import apply_schedule_version_bumps, parse_schedule_line_date


def test_apply_schedule_version_bumps_single_recast():
    bumps = [(date(2025, 4, 1), 2)]
    assert apply_schedule_version_bumps(date(2025, 3, 31), bumps) == 1
    assert apply_schedule_version_bumps(date(2025, 4, 1), bumps) == 2
    assert apply_schedule_version_bumps(date(2026, 1, 1), bumps) == 2


def test_apply_schedule_version_bumps_two_events_same_day_order_by_version():
    bumps = [(date(2025, 4, 1), 2), (date(2025, 4, 1), 3)]
    assert apply_schedule_version_bumps(date(2025, 4, 1), bumps) == 3


def test_parse_schedule_line_date_dd_mon_and_iso():
    assert parse_schedule_line_date("05-Apr-2025") == date(2025, 4, 5)
    assert parse_schedule_line_date("2025-04-05") == date(2025, 4, 5)
    assert parse_schedule_line_date("2025-04-05T00:00:00") == date(2025, 4, 5)


def test_apply_schedule_version_bumps_sequential():
    bumps = [(date(2025, 2, 1), 2), (date(2025, 6, 1), 3)]
    assert apply_schedule_version_bumps(date(2025, 1, 15), bumps) == 1
    assert apply_schedule_version_bumps(date(2025, 3, 1), bumps) == 2
    assert apply_schedule_version_bumps(date(2025, 7, 1), bumps) == 3
