"""Tests for creditor loan mirror helpers."""

from __future__ import annotations

from decimal import Decimal

from creditor_loans.loan_config import loan_config_from_behavior


def test_loan_config_default_waterfall_order():
    cfg = loan_config_from_behavior({})
    assert cfg.waterfall_bucket_order[0] == "interest_arrears_balance"
    assert cfg.grace_period_days == 0


def test_loan_config_reads_json_rates():
    cfg = loan_config_from_behavior({"regular_rate_per_month": "0.05", "grace_period_days": 3})
    assert cfg.regular_rate_per_month == Decimal("0.05")
    assert cfg.grace_period_days == 3
