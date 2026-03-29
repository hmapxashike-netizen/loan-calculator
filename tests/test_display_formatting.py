"""Tests for configurable amount display and dataframe column heuristics."""
import pandas as pd

from display_formatting import (
    build_dataframe_money_column_config,
    default_display_format,
    format_display_amount,
    resolve_display_format,
)


class _FakeStColumnConfig:
    @staticmethod
    def NumberColumn(**kwargs):
        return kwargs


def test_format_display_amount_space_separator():
    s = resolve_display_format({"thousands_separator": " ", "amount_decimals": 2})
    assert format_display_amount(1234567.89, settings=s) == "1 234 567.89"


def test_resolve_merges_lists():
    base = default_display_format()
    partial = {"money_column_name_substrings": ["custom_amount"]}
    r = resolve_display_format(partial)
    assert "custom_amount" in r["money_column_name_substrings"]
    assert "principal" not in r["money_column_name_substrings"]  # replaced whole list


def test_build_column_config_new_money_column():
    df = pd.DataFrame({"loan_id": [1], "new_principal_balance": [99.5]})
    fake = _FakeStColumnConfig()
    s = resolve_display_format({})
    cfg = build_dataframe_money_column_config(
        df,
        st_column_config=fake,
        settings=s,
    )
    assert "new_principal_balance" in cfg
    assert "loan_id" not in cfg


def test_skip_rate_columns():
    df = pd.DataFrame({"annual_rate": [0.05], "principal": [1000.0]})
    fake = _FakeStColumnConfig()
    s = resolve_display_format({})
    cfg = build_dataframe_money_column_config(df, st_column_config=fake, settings=s)
    assert "principal" in cfg
    assert "annual_rate" not in cfg
