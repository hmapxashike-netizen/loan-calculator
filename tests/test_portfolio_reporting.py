"""Read-only portfolio_reporting bucket logic (no DB).

Run from project root: python -m unittest tests.test_portfolio_reporting -v
"""
from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from portfolio_reporting import (
    ARREARS_BUCKET_KEYS,
    buckets_from_daily_balance_series,
    buckets_from_daily_flow_or_balance,
    bucket_arrears_for_loan,
    bucket_maturity_for_loan,
)


def _sum_buckets(b: dict[str, Decimal]) -> Decimal:
    return sum((b[k] for k in ARREARS_BUCKET_KEYS), Decimal("0"))


def _sum_all_arrears_buckets(b: dict[str, Decimal]) -> Decimal:
    return sum(b.values(), Decimal("0"))


def test_arrears_splits_match_total_one_past_due_line():
    """All arrears on one past-due line → single DPD bucket (31–60 for May 30 vs as-of Jun 30)."""
    as_of = date(2026, 6, 30)
    lines = [
        {"Period": 1, "Date": "30-May-2026", "principal": 500, "interest": 500},
    ]
    b = bucket_arrears_for_loan(
        as_of,
        principal_arrears=Decimal("100"),
        interest_arrears=Decimal("40"),
        fees_charges=Decimal("5"),
        penalty=Decimal("3"),
        default_int=Decimal("2"),
        schedule_lines=lines,
    )
    assert _sum_buckets(b) == Decimal("150")
    assert b["bkt_31_60"] == Decimal("150")


def test_arrears_raises_when_arrears_but_no_past_due_schedule():
    try:
        bucket_arrears_for_loan(
            date(2026, 6, 30),
            principal_arrears=Decimal("10"),
            interest_arrears=Decimal("0"),
            fees_charges=Decimal("0"),
            penalty=Decimal("0"),
            default_int=Decimal("0"),
            schedule_lines=[],
        )
    except ValueError as ex:
        assert "No past-due schedule" in str(ex)
    else:
        raise AssertionError("expected ValueError")


def test_arrears_zero_total_allows_empty_past_due_schedule():
    b = bucket_arrears_for_loan(
        date(2026, 6, 30),
        principal_arrears=Decimal("0"),
        interest_arrears=Decimal("0"),
        fees_charges=Decimal("0"),
        penalty=Decimal("0"),
        default_int=Decimal("0"),
        schedule_lines=[],
    )
    assert _sum_buckets(b) == Decimal("0")


def test_arrears_fifo_two_overdue_lines():
    """Newest past-due first: Apr (newer) fills before Mar; amounts land in each line’s DPD bucket."""
    as_of = date(2026, 6, 30)
    lines = [
        {"Period": 1, "Date": "31-Mar-2026", "principal": 60, "interest": 10},
        {"Period": 2, "Date": "30-Apr-2026", "principal": 40, "interest": 10},
    ]
    b = bucket_arrears_for_loan(
        as_of,
        principal_arrears=Decimal("100"),
        interest_arrears=Decimal("20"),
        fees_charges=Decimal("0"),
        penalty=Decimal("0"),
        default_int=Decimal("0"),
        schedule_lines=lines,
    )
    assert _sum_buckets(b) == Decimal("120")
    # Apr 30 → 61 dpd (61–90); Mar 31 → 91 dpd (91–180). Apr 40+10=50, Mar 60+10=70.
    assert b["bkt_61_90"] == Decimal("50")
    assert b["bkt_91_180"] == Decimal("70")


def test_arrears_principal_newest_first_caps_then_older():
    """120 principal: newest line takes cap 100, remainder 20 on older line (not proportional 60/60)."""
    as_of = date(2026, 6, 30)
    lines = [
        {"Period": 1, "Date": "31-Mar-2026", "principal": 100, "interest": 0},
        {"Period": 2, "Date": "30-Apr-2026", "principal": 100, "interest": 0},
    ]
    b = bucket_arrears_for_loan(
        as_of,
        principal_arrears=Decimal("120"),
        interest_arrears=Decimal("0"),
        fees_charges=Decimal("0"),
        penalty=Decimal("0"),
        default_int=Decimal("0"),
        schedule_lines=lines,
    )
    assert b["bkt_61_90"] == Decimal("100")
    assert b["bkt_91_180"] == Decimal("20")


def test_arrears_interest_newest_past_due_first_partial_then_older():
    """Interest balance consumes scheduled interest caps from newest past-due instalment backward."""
    as_of = date(2026, 6, 30)
    lines = [
        {"Period": 1, "Date": "31-Mar-2026", "principal": 0, "interest": 100},
        {"Period": 2, "Date": "30-Apr-2026", "principal": 0, "interest": 32},
    ]
    b = bucket_arrears_for_loan(
        as_of,
        principal_arrears=Decimal("0"),
        interest_arrears=Decimal("62"),
        fees_charges=Decimal("0"),
        penalty=Decimal("0"),
        default_int=Decimal("0"),
        schedule_lines=lines,
    )
    assert b["bkt_61_90"] == Decimal("32")
    assert b["bkt_91_180"] == Decimal("30")


def test_maturity_scales_to_principal_not_due():
    as_of = date(2026, 1, 15)
    lines = [
        {"Period": 1, "Date": "15-Feb-2026", "principal": 50, "interest": 0},
        {"Period": 2, "Date": "15-Mar-2026", "principal": 50, "interest": 0},
    ]
    b = bucket_maturity_for_loan(as_of, principal_not_due=Decimal("1000"), schedule_lines=lines)
    assert sum(b.values(), Decimal("0")) == Decimal("1000")


def test_maturity_unallocated_when_no_future_principal():
    as_of = date(2026, 6, 1)
    lines = [
        {"Period": 1, "Date": "15-May-2026", "principal": 100, "interest": 0},
    ]
    b = bucket_maturity_for_loan(as_of, principal_not_due=Decimal("500"), schedule_lines=lines)
    from portfolio_reporting import MATURITY_BUCKET_KEYS

    assert b["bkt_unallocated"] == Decimal("500")
    assert sum(b[k] for k in MATURITY_BUCKET_KEYS if k != "bkt_unallocated") == Decimal("0")


def test_buckets_from_daily_positive_increments_scaled_to_closing():
    as_of = date(2026, 6, 30)
    series = [
        (date(2026, 6, 1), Decimal("40")),
        (date(2026, 6, 10), Decimal("100")),
    ]
    # +40 at Jun 1 (29 dpd → 1–30), +60 at Jun 10 (20 dpd → 1–30); raw 100, closing 80 → scale 0.8
    b = buckets_from_daily_balance_series(as_of, series, Decimal("80"))
    assert _sum_all_arrears_buckets(b) == Decimal("80")
    assert b["bkt_1_30"] == Decimal("80")


def test_buckets_from_daily_repayment_scales_down():
    as_of = date(2026, 6, 30)
    series = [
        (date(2026, 6, 1), Decimal("100")),
        (date(2026, 6, 20), Decimal("60")),
    ]
    b = buckets_from_daily_balance_series(as_of, series, Decimal("60"))
    assert b["bkt_1_30"] == Decimal("60")


def test_flow_primary_uses_daily_accrual_not_net_balance_change():
    """When any row has positive *_daily, vintage uses flows (scaled to closing), not balance deltas only."""
    as_of = date(2026, 6, 30)
    rows = [
        (date(2026, 6, 1), Decimal("50"), Decimal("50")),
        (date(2026, 6, 2), Decimal("60"), Decimal("30")),
    ]
    # Net balance +60 vs Jun 1; flows 50+30=80; closing 60 → scale 0.75
    b = buckets_from_daily_flow_or_balance(as_of, rows, Decimal("60"))
    assert _sum_all_arrears_buckets(b) == Decimal("60")


def test_flow_falls_back_to_balance_deltas_when_no_positive_daily():
    as_of = date(2026, 6, 30)
    rows = [
        (date(2026, 6, 1), Decimal("40"), Decimal("0")),
        (date(2026, 6, 10), Decimal("100"), Decimal("0")),
    ]
    b = buckets_from_daily_flow_or_balance(as_of, rows, Decimal("80"))
    assert _sum_all_arrears_buckets(b) == Decimal("80")
    assert b["bkt_1_30"] == Decimal("80")


def test_arrears_penalty_uses_daily_series_when_provided():
    as_of = date(2026, 6, 30)
    lines = [
        {"Period": 1, "Date": "31-Mar-2026", "principal": 100, "interest": 0},
        {"Period": 2, "Date": "30-Apr-2026", "principal": 100, "interest": 0},
    ]
    series = [
        (date(2026, 6, 1), Decimal("40"), Decimal("0")),
        (date(2026, 6, 15), Decimal("100"), Decimal("0")),
    ]
    b = bucket_arrears_for_loan(
        as_of,
        principal_arrears=Decimal("0"),
        interest_arrears=Decimal("0"),
        fees_charges=Decimal("0"),
        penalty=Decimal("90"),
        default_int=Decimal("0"),
        schedule_lines=lines,
        daily_series={"penalty": series, "default": [], "fees": []},
    )
    assert _sum_all_arrears_buckets(b) == Decimal("90")
    assert b["bkt_1_30"] == Decimal("90")


def test_arrears_fees_on_newest_past_due_when_no_pi_allocated():
    """No P/I arrears → ancillaries attach to newest past-due line (Apr vs Jan)."""
    as_of = date(2026, 6, 30)
    lines = [
        {"Period": 1, "Date": "31-Jan-2026", "principal": 100, "interest": 5},
        {"Period": 2, "Date": "30-Apr-2026", "principal": 50, "interest": 5},
    ]
    b = bucket_arrears_for_loan(
        as_of,
        principal_arrears=Decimal("0"),
        interest_arrears=Decimal("0"),
        fees_charges=Decimal("10"),
        penalty=Decimal("0"),
        default_int=Decimal("0"),
        schedule_lines=lines,
    )
    assert b["bkt_61_90"] == Decimal("10")
    assert sum(b[k] for k in ARREARS_BUCKET_KEYS) == Decimal("10")


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for fn in (
        test_arrears_splits_match_total_one_past_due_line,
        test_arrears_raises_when_arrears_but_no_past_due_schedule,
        test_arrears_zero_total_allows_empty_past_due_schedule,
        test_arrears_fifo_two_overdue_lines,
        test_arrears_principal_newest_first_caps_then_older,
        test_arrears_interest_newest_past_due_first_partial_then_older,
        test_buckets_from_daily_positive_increments_scaled_to_closing,
        test_buckets_from_daily_repayment_scales_down,
        test_flow_primary_uses_daily_accrual_not_net_balance_change,
        test_flow_falls_back_to_balance_deltas_when_no_positive_daily,
        test_arrears_penalty_uses_daily_series_when_provided,
        test_maturity_scales_to_principal_not_due,
        test_maturity_unallocated_when_no_future_principal,
        test_arrears_fees_on_newest_past_due_when_no_pi_allocated,
    ):
        suite.addTest(unittest.FunctionTestCase(fn))
    return suite
