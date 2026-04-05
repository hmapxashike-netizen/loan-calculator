"""
Configurable amount display for UI labels, HTML, and Streamlit tables.

Settings live under system_config[\"display_format\"] (see default_display_format()
and System configurations → Display & numbers).
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import pandas as pd


def default_display_format() -> dict[str, Any]:
    """Defaults merged with DB config; safe to extend with new keys."""
    return {
        "amount_decimals": 2,
        "thousands_separator": ",",
        "currency_symbol": "$",
        # Streamlit NumberColumn preset: dollar | accounting | localized
        "streamlit_money_format": "dollar",
        # Column names containing any of these (case-insensitive) get money formatting in st.dataframe/data_editor.
        "money_column_name_substrings": [
            "amount",
            "principal",
            "balance",
            "payment",
            "installment",
            "debit",
            "credit",
            "fee",
            "fees",
            "total",
            "outstanding",
            "disbursed",
            "proceeds",
            "receipt",
            "value",
            "arrears",
            "allocation",
            "unapplied",
            "collateral",
            "charge",
            "valuation",
            "provision",
            "exposure",
            "due",
            "cost",
            "interest",  # schedule interest column; skipped if name also matches skip list (e.g. interest_rate)
        ],
        # If column name contains any of these (case-insensitive), do not auto-apply money format.
        "skip_column_name_substrings": [
            "_id",
            " id",
            "rate",
            "pct",
            "percent",
            "ratio",
            "number",
            "count",
            "sort",
            "order",
            "year",
            "month",
            "day",
            "period",  # schedule period number
            "code",
            "status",
            "type",
        ],
        # If True, every numeric (int/float) column is formatted as money unless skipped.
        "auto_format_all_float_columns": False,
    }


def resolve_display_format(block: dict[str, Any] | None) -> dict[str, Any]:
    """Deep-enough merge: defaults + stored block (lists replaced when provided)."""
    base = default_display_format()
    if not block or not isinstance(block, dict):
        return dict(base)
    out = {**base, **{k: v for k, v in block.items() if k not in ("money_column_name_substrings", "skip_column_name_substrings")}}
    if isinstance(block.get("money_column_name_substrings"), list):
        out["money_column_name_substrings"] = [str(x).strip().lower() for x in block["money_column_name_substrings"] if str(x).strip()]
    else:
        out["money_column_name_substrings"] = list(base["money_column_name_substrings"])
    if isinstance(block.get("skip_column_name_substrings"), list):
        out["skip_column_name_substrings"] = [str(x).strip().lower() for x in block["skip_column_name_substrings"] if str(x).strip()]
    else:
        out["skip_column_name_substrings"] = list(base["skip_column_name_substrings"])
    return out


def get_display_format_settings(*, system_config: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Resolved display_format block. If system_config is None, loads from DB (no Streamlit session).
    """
    if system_config is not None:
        return resolve_display_format(system_config.get("display_format"))
    try:
        from loan_management import load_system_config_from_db

        cfg = load_system_config_from_db() or {}
    except Exception:
        cfg = {}
    return resolve_display_format(cfg.get("display_format"))


def format_display_amount(
    value: Any,
    *,
    settings: dict[str, Any] | None = None,
    system_config: dict[str, Any] | None = None,
) -> str:
    """Grouped thousands + fixed decimals per config."""
    s = settings or get_display_format_settings(system_config=system_config)
    decimals = max(0, min(14, int(s.get("amount_decimals", 2))))
    thousands_sep = str(s.get("thousands_separator", ","))
    if value is None:
        return "—"
    try:
        if isinstance(value, Decimal):
            q = value.quantize(Decimal(10) ** -decimals, rounding=ROUND_HALF_UP)
            x = float(q)
        else:
            x = float(value)
            q = Decimal(str(x)).quantize(Decimal(10) ** -decimals, rounding=ROUND_HALF_UP)
            x = float(q)
    except (TypeError, ValueError):
        return str(value)
    if x != x:
        return "—"
    body = format(x, f",.{decimals}f")
    if thousands_sep != ",":
        body = body.replace(",", thousands_sep)
    return body


def format_display_currency(
    value: Any,
    *,
    settings: dict[str, Any] | None = None,
    system_config: dict[str, Any] | None = None,
) -> str:
    s = settings or get_display_format_settings(system_config=system_config)
    sym = str(s.get("currency_symbol", "$"))
    return f"{sym}{format_display_amount(value, settings=s)}"


def _column_is_money_column(
    col: str,
    series: pd.Series,
    settings: dict[str, Any],
) -> bool:
    if not pd.api.types.is_numeric_dtype(series):
        return False
    if pd.api.types.is_bool_dtype(series):
        return False
    cname = str(col).lower()
    for sk in settings.get("skip_column_name_substrings") or []:
        if sk and sk in cname:
            return False
    if settings.get("auto_format_all_float_columns"):
        return True
    for ms in settings.get("money_column_name_substrings") or []:
        if ms and ms in cname:
            return True
    return False


def _streamlit_money_format_key(settings: dict[str, Any]) -> str:
    fmt = str(settings.get("streamlit_money_format") or "accounting").strip().lower()
    if fmt not in ("dollar", "accounting", "localized"):
        return "accounting"
    return fmt


def build_dataframe_money_column_config(
    df: pd.DataFrame,
    *,
    st_column_config: Any,
    settings: dict[str, Any] | None = None,
    system_config: dict[str, Any] | None = None,
    overrides: dict[str, Any] | None = None,
    column_disabled: dict[str, bool] | None = None,
    step: float | None = None,
    money_column_alignment: str | None = None,
) -> dict[str, Any]:
    """
    Build column_config for st.dataframe / st.data_editor: money-like columns get NumberColumn
    with configured Streamlit preset. New numeric columns match automatically via name heuristics
    (or auto_format_all_float_columns). Explicit overrides win and are not replaced.

    money_column_alignment: optional *left* / *center* / *right* merged into each generated
    NumberColumn dict (Streamlit aligns header and cell content).
    """
    s = settings or get_display_format_settings(system_config=system_config)
    decimals = max(0, min(14, int(s.get("amount_decimals", 2))))
    if step is None:
        step = float(10 ** (-decimals)) if decimals > 0 else 1.0
    fmt = _streamlit_money_format_key(s)
    out: dict[str, Any] = dict(overrides or {})
    dis = column_disabled or {}
    for col in df.columns:
        if col in out:
            continue
        if not _column_is_money_column(col, df[col], s):
            continue
        kw: dict[str, Any] = {"format": fmt, "step": step}
        if dis.get(col):
            kw["disabled"] = True
        cfg = st_column_config.NumberColumn(**kw)
        if money_column_alignment and isinstance(cfg, dict):
            cfg = {**cfg, "alignment": money_column_alignment}
        out[col] = cfg
    return out
