"""Creditor schedules built from debtor engines (loans.py)."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from creditor_loans.debtor_engine_schedule import (
    build_creditor_schedule_dataframe,
    debtor_schedule_engine,
)
from loans import consumer_level_payment


def test_consumer_level_payment_matches_pmt():
    p = consumer_level_payment(10000.0, 0.01, 12)
    assert p > 0
    assert abs(p - 888.49) < 0.1  # ~ level payment at 1%/month, 12 periods


def test_debtor_schedule_engine_default():
    assert debtor_schedule_engine({}) == "term_actual_360"
    assert debtor_schedule_engine({"debtor_schedule_engine": "consumer_30_360"}) == "consumer_30_360"


def test_build_term_schedule_shape():
    disb = datetime(2025, 1, 15)
    first = datetime(2025, 2, 15)
    df = build_creditor_schedule_dataframe(
        behavior_json={"debtor_schedule_engine": "term_actual_360"},
        principal=1200.0,
        term_months=3,
        disbursement_date=disb,
        rate_pct=12.0,
        rate_basis="Per annum",
        flat_rate=False,
        use_anniversary=True,
        first_repayment_date=first,
        consumer_monthly_rate=None,
        bullet_type_label="Straight bullet (no interim payments)",
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 4
    assert "Monthly Installment" in df.columns or "Payment" in df.columns


def test_build_consumer_schedule_shape():
    disb = datetime(2025, 1, 31)
    first = datetime(2025, 2, 28)
    df = build_creditor_schedule_dataframe(
        behavior_json={"debtor_schedule_engine": "consumer_30_360"},
        principal=6000.0,
        term_months=6,
        disbursement_date=disb,
        rate_pct=0.0,
        rate_basis="Per annum",
        flat_rate=False,
        use_anniversary=False,
        first_repayment_date=first,
        consumer_monthly_rate=0.01,
        bullet_type_label="Straight bullet (no interim payments)",
    )
    assert len(df) == 7
    assert float(df.iloc[1]["Interest"] or 0) >= 0


def test_build_bullet_straight():
    disb = datetime(2025, 1, 10)
    df = build_creditor_schedule_dataframe(
        behavior_json={"debtor_schedule_engine": "bullet_actual_360"},
        principal=5000.0,
        term_months=12,
        disbursement_date=disb,
        rate_pct=6.0,
        rate_basis="Per annum",
        flat_rate=False,
        use_anniversary=True,
        first_repayment_date=None,
        consumer_monthly_rate=None,
        bullet_type_label="Straight bullet (no interim payments)",
    )
    assert len(df) == 2
    assert float(df.iloc[1]["Principal"] or 0) > 0
