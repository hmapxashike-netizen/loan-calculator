from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any


@dataclass(frozen=True)
class AccountingPeriodConfig:
    month_end_mode: str
    month_end_day: int
    fiscal_year_end_month: int


@dataclass(frozen=True)
class PeriodBounds:
    start_date: date
    end_date: date


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _month_end_for(year: int, month: int, cfg: AccountingPeriodConfig) -> date:
    if cfg.month_end_mode == "calendar":
        return date(year, month, _last_day_of_month(year, month))
    return date(year, month, min(cfg.month_end_day, _last_day_of_month(year, month)))


def _next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _previous_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def normalize_accounting_period_config(system_config: dict[str, Any] | None) -> AccountingPeriodConfig:
    cfg = (system_config or {}).get("accounting_periods") or {}
    mode_raw = str(cfg.get("month_end_mode") or "calendar").strip().lower()
    mode = "fixed_day" if mode_raw in {"fixed_day", "fixed", "date"} else "calendar"

    try:
        month_end_day = int(cfg.get("month_end_day") or 31)
    except Exception:
        month_end_day = 31
    month_end_day = max(1, min(31, month_end_day))

    try:
        fiscal_year_end_month = int(cfg.get("fiscal_year_end_month") or 12)
    except Exception:
        fiscal_year_end_month = 12
    fiscal_year_end_month = max(1, min(12, fiscal_year_end_month))

    return AccountingPeriodConfig(
        month_end_mode=mode,
        month_end_day=month_end_day,
        fiscal_year_end_month=fiscal_year_end_month,
    )


def get_month_period_bounds(as_of_date: date, cfg: AccountingPeriodConfig) -> PeriodBounds:
    this_month_end = _month_end_for(as_of_date.year, as_of_date.month, cfg)
    if as_of_date <= this_month_end:
        end_date = this_month_end
    else:
        ny, nm = _next_month(as_of_date.year, as_of_date.month)
        end_date = _month_end_for(ny, nm, cfg)

    py, pm = _previous_month(end_date.year, end_date.month)
    prev_period_end = _month_end_for(py, pm, cfg)
    start_date = prev_period_end + timedelta(days=1)
    return PeriodBounds(start_date=start_date, end_date=end_date)


def get_year_period_bounds(as_of_date: date, cfg: AccountingPeriodConfig) -> PeriodBounds:
    this_year_end = _month_end_for(as_of_date.year, cfg.fiscal_year_end_month, cfg)
    if as_of_date <= this_year_end:
        end_date = this_year_end
    else:
        end_date = _month_end_for(as_of_date.year + 1, cfg.fiscal_year_end_month, cfg)

    prev_year_end = _month_end_for(end_date.year - 1, cfg.fiscal_year_end_month, cfg)
    start_date = prev_year_end + timedelta(days=1)
    return PeriodBounds(start_date=start_date, end_date=end_date)


def is_eom(as_of_date: date, cfg: AccountingPeriodConfig) -> bool:
    return as_of_date == get_month_period_bounds(as_of_date, cfg).end_date


def is_eoy(as_of_date: date, cfg: AccountingPeriodConfig) -> bool:
    return as_of_date == get_year_period_bounds(as_of_date, cfg).end_date
