"""Regression: schedule CSV/export must not truncate dd-Mon-yyyy with str[:10]."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from loan_management.schedules import schedule_date_to_iso_for_exchange


def test_iso_from_dd_mon_yyyy_string():
    assert schedule_date_to_iso_for_exchange("26-Apr-2024") == "2024-04-26"


def test_iso_from_datetime():
    assert schedule_date_to_iso_for_exchange(datetime(2024, 4, 26, 12, 0)) == "2024-04-26"


def test_iso_from_date():
    assert schedule_date_to_iso_for_exchange(date(2024, 4, 26)) == "2024-04-26"


def test_iso_from_pandas_timestamp():
    ts = pd.Timestamp("2024-04-26")
    assert schedule_date_to_iso_for_exchange(ts) == "2024-04-26"


def test_truncated_year_is_rejected_not_silently_corrupted():
    with pytest.raises(ValueError, match="Unparseable"):
        schedule_date_to_iso_for_exchange("26-Apr-202")


def test_iso_input_round_trip():
    assert schedule_date_to_iso_for_exchange("2024-04-26") == "2024-04-26"
