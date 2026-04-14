"""Unit tests for discrete EOD statement events (slice 1 — no DB)."""

from datetime import date
from decimal import Decimal

from reporting.statement_events import (
    StatementEvent,
    _dedupe_unapplied_ledger_credits_bundled_on_receipts,
    apply_dual_running_customer_events,
    apply_running_loan_outstanding,
    build_daily_interest_accrual_events,
    build_disbursement_events_for_loan,
    build_fee_balance_delta_events,
    build_repayment_allocation_events,
    build_unapplied_ledger_statement_events,
    merge_sort_eod_flow_events,
    merge_sort_statement_events,
    rollup_flow_statement_rows_for_display,
    total_outstanding_decimal,
)


def _row(
    d: date,
    *,
    reg: str = "0",
    pen: str = "0",
    default: str = "0",
    fees: str = "0",
    reg_bal: str = "0",
    iarr_bal: str = "0",
    pen_bal: str = "0",
    default_bal: str = "0",
) -> dict:
    return {
        "as_of_date": d,
        "regular_interest_daily": reg,
        "penalty_interest_daily": pen,
        "default_interest_daily": default,
        "interest_accrued_balance": reg_bal,
        "interest_arrears_balance": iarr_bal,
        "penalty_interest_balance": pen_bal,
        "default_interest_balance": default_bal,
        "fees_charges_balance": fees,
    }


def test_total_outstanding_decimal_sums_buckets():
    ds = {
        "principal_not_due": "100",
        "principal_arrears": "0",
        "interest_accrued_balance": "1.5",
        "interest_arrears_balance": "2",
        "default_interest_balance": "0",
        "penalty_interest_balance": "0.25",
        "fees_charges_balance": "3",
    }
    assert total_outstanding_decimal(ds) == Decimal("106.75")


def test_daily_interest_emits_only_positive_dailies():
    rows = [
        _row(date(2025, 3, 29), reg="2.54", reg_bal="2.54", pen_bal="0"),
        _row(date(2025, 3, 30), reg="0", pen="0.1", reg_bal="2.54", pen_bal="0.1"),
    ]
    ev = build_daily_interest_accrual_events(rows)
    assert len(ev) == 2
    assert ev[0].event_type == "REGULAR_INTEREST_ACCRUAL"
    assert ev[0].debit == Decimal("2.54")
    assert ev[1].event_type == "PENALTY_INTEREST_ACCRUAL"


def test_daily_interest_skips_positive_daily_when_closing_bucket_unchanged():
    rows = [
        _row(date(2025, 4, 5), reg="2.5431846638", reg_bal="20.34547731", iarr_bal="0"),
        _row(date(2025, 4, 6), reg="0.0227989180", reg_bal="0", iarr_bal="0"),
    ]
    ev = build_daily_interest_accrual_events(rows)
    assert len(ev) == 1
    assert ev[0].event_type == "REGULAR_INTEREST_ACCRUAL"
    assert ev[0].event_date == date(2025, 4, 5)
    assert ev[0].debit == Decimal("2.5431846638")


def test_fee_delta_positive_and_credit():
    rows = [
        _row(date(2025, 4, 1), fees="10"),
        _row(date(2025, 4, 2), fees="12"),
        _row(date(2025, 4, 3), fees="9"),
    ]
    ev = build_fee_balance_delta_events(rows, prior_fees_balance=Decimal("10"))
    assert len(ev) == 2
    assert ev[0].debit == Decimal("2") and ev[0].credit == Decimal("0")
    assert ev[1].debit == Decimal("0") and ev[1].credit == Decimal("3")


def test_merge_sort_orders_same_day_accrual_before_fee():
    a = StatementEvent(
        date(2025, 4, 1),
        "FEES_BALANCE_DELTA",
        "fee",
        Decimal("1"),
        Decimal("0"),
        sort_ordinal=30,
    )
    b = StatementEvent(
        date(2025, 4, 1),
        "REGULAR_INTEREST_ACCRUAL",
        "reg",
        Decimal("2"),
        Decimal("0"),
        sort_ordinal=20,
    )
    m = merge_sort_eod_flow_events([a, b])
    assert m[0].event_type == "REGULAR_INTEREST_ACCRUAL"
    assert m[1].event_type == "FEES_BALANCE_DELTA"


def test_running_balance_no_snap():
    opening = Decimal("1000")
    evs = [
        StatementEvent(
            date(2025, 4, 1),
            "REGULAR_INTEREST_ACCRUAL",
            "r",
            Decimal("10"),
            Decimal("0"),
            sort_ordinal=20,
        ),
        StatementEvent(
            date(2025, 4, 2),
            "X",
            "pay",
            Decimal("0"),
            Decimal("25"),
            sort_ordinal=50,
        ),
    ]
    run = apply_running_loan_outstanding(evs, opening)
    assert run[0][1] == Decimal("1010")
    assert run[1][1] == Decimal("985")


def test_statement_event_rejects_negative_debit():
    import pytest

    with pytest.raises(ValueError):
        StatementEvent(
            date(2025, 1, 1),
            "X",
            "n",
            Decimal("-1"),
            Decimal("0"),
        )


def test_repayment_one_event_per_receipt_net_allocation():
    reps = [
        {
            "id": 9,
            "value_date": date(2025, 4, 4),
            "amount": "300",
            "customer_reference": "x",
            "alloc_principal_not_due": "200.67",
            "alloc_interest_accrued": "10",
            "alloc_interest_arrears": "0",
            "alloc_default_interest": "0",
            "alloc_penalty_interest": "0",
            "alloc_fees_charges": "0",
            "alloc_principal_arrears": "0",
            "unallocated": "0",
        },
        {
            "id": 12,
            "value_date": date(2025, 4, 6),
            "amount": "50",
            "alloc_principal_not_due": "-25",
            "alloc_principal_arrears": "0",
            "alloc_interest_accrued": "0",
            "alloc_interest_arrears": "0",
            "alloc_default_interest": "0",
            "alloc_penalty_interest": "0",
            "alloc_fees_charges": "0",
            "unallocated": "0",
        },
    ]
    ev = build_repayment_allocation_events(reps)
    assert len(ev) == 2
    assert ev[0].event_type == "PAYMENT_RECEIPT"
    assert ev[0].credit == Decimal("210.67")
    assert ev[0].debit == Decimal("0")
    assert ev[1].event_type == "PAYMENT_RECEIPT"
    assert ev[1].credit == Decimal("0")
    assert ev[1].debit == Decimal("25")


def test_disbursement_in_window():
    loan = {
        "principal": 683.97,
        "disbursement_date": date(2025, 1, 1),
        "admin_fee_amount": 0,
        "drawdown_fee_amount": 31.58,
        "arrangement_fee_amount": 21.05,
    }
    ev = build_disbursement_events_for_loan(loan, date(2024, 12, 1), date(2025, 2, 1))
    assert len(ev) == 3
    assert sum(e.debit for e in ev) == Decimal("683.97")


def test_disbursement_outside_range_empty():
    loan = {
        "principal": 1000,
        "disbursement_date": date(2025, 1, 1),
        "admin_fee_amount": 0,
        "drawdown_fee_amount": 0,
        "arrangement_fee_amount": 0,
    }
    assert build_disbursement_events_for_loan(loan, date(2025, 2, 1), date(2025, 3, 1)) == []


def test_dual_running_receipt_bundles_unapplied_delta():
    evs = [
        StatementEvent(
            date(2025, 1, 5),
            "PAYMENT_RECEIPT",
            "pay",
            Decimal("0"),
            Decimal("40"),
            repayment_id=1,
            sort_ordinal=50,
            meta={"unapplied_delta": "10"},
        ),
    ]
    d = apply_dual_running_customer_events(evs, Decimal("500"), Decimal("2"))
    assert d[0][1] == Decimal("460")
    assert d[0][2] == Decimal("12")


def test_dedupe_unapplied_credit_when_bundled_on_receipt():
    loan = [
        StatementEvent(
            date(2025, 1, 1),
            "PAYMENT_RECEIPT",
            "r",
            Decimal("0"),
            Decimal("40"),
            repayment_id=5,
            sort_ordinal=50,
            meta={"unapplied_delta": "10"},
        ),
    ]
    u_before = [
        StatementEvent(
            date(2025, 1, 1),
            "UNAPPLIED_LEDGER",
            "Unapplied credit - repayment 5",
            Decimal("0"),
            Decimal("0"),
            repayment_id=5,
            sort_ordinal=60,
            meta={"unapplied_delta": "10", "entry_kind": "credit"},
        ),
        StatementEvent(
            date(2025, 1, 2),
            "UNAPPLIED_LEDGER",
            "liq",
            Decimal("0"),
            Decimal("5"),
            repayment_id=8,
            sort_ordinal=62,
            meta={"unapplied_delta": "-5", "entry_kind": "liquidation"},
        ),
    ]
    u_after = _dedupe_unapplied_ledger_credits_bundled_on_receipts(loan, u_before)
    assert len(u_after) == 1
    assert u_after[0].event_date == date(2025, 1, 2)


def test_dedupe_unapplied_reversal_when_bundled_on_receipt_reversal():
    """Reversal allocation carries negative unallocated; ledger reversal must not double-hit running."""
    loan = [
        StatementEvent(
            date(2025, 1, 1),
            "PAYMENT_RECEIPT",
            "Reversal - Repayment id 446 (Receipt -100)",
            Decimal("60"),
            Decimal("0"),
            repayment_id=446,
            sort_ordinal=50,
            meta={"unapplied_delta": "-10"},
        ),
    ]
    u_before = [
        StatementEvent(
            date(2025, 1, 1),
            "UNAPPLIED_LEDGER",
            "Unapplied reversal - repayment 446",
            Decimal("0"),
            Decimal("0"),
            repayment_id=446,
            sort_ordinal=61,
            meta={"unapplied_delta": "-10", "entry_kind": "reversal"},
        ),
    ]
    u_after = _dedupe_unapplied_ledger_credits_bundled_on_receipts(loan, u_before)
    assert u_after == []


def test_reversal_receipt_unapplied_no_double_count_in_dual_running():
    evs = [
        StatementEvent(
            date(2025, 5, 30),
            "PAYMENT_RECEIPT",
            "Repayment id 283 (Receipt 100)",
            Decimal("0"),
            Decimal("60"),
            repayment_id=283,
            sort_ordinal=50,
            meta={"unapplied_delta": "10"},
        ),
        StatementEvent(
            date(2025, 5, 30),
            "PAYMENT_RECEIPT",
            "Reversal - Repayment id 446 (Receipt -100)",
            Decimal("60"),
            Decimal("0"),
            repayment_id=446,
            sort_ordinal=50,
            meta={"unapplied_delta": "-10"},
        ),
    ]
    d = apply_dual_running_customer_events(evs, Decimal("500"), Decimal("0"))
    assert d[0][2] == Decimal("10")
    assert d[1][2] == Decimal("0")


def test_apply_dual_running_loan_vs_unapplied():
    evs = [
        StatementEvent(
            date(2025, 1, 5),
            "REGULAR_INTEREST_ACCRUAL",
            "r",
            Decimal("10"),
            Decimal("0"),
            sort_ordinal=20,
        ),
        StatementEvent(
            date(2025, 1, 5),
            "UNAPPLIED_LEDGER",
            "u",
            Decimal("0"),
            Decimal("0"),
            sort_ordinal=60,
            meta={"unapplied_delta": "5"},
        ),
    ]
    d = apply_dual_running_customer_events(evs, Decimal("100"), Decimal("2"))
    assert d[0][1] == Decimal("110")
    assert d[0][2] == Decimal("2")
    assert d[1][1] == Decimal("110")
    assert d[1][2] == Decimal("7")


def test_unapplied_liquidation_credit_reduces_loan_balance():
    evs = [
        StatementEvent(
            date(2025, 3, 29),
            "UNAPPLIED_LEDGER",
            "Unapplied liquidation - repayment 8",
            Decimal("0"),
            Decimal("1.28"),
            sort_ordinal=62,
            meta={"unapplied_delta": "-1.28", "entry_kind": "liquidation"},
        ),
    ]
    d = apply_dual_running_customer_events(evs, Decimal("500"), Decimal("2"))
    assert d[0][1] == Decimal("498.72")
    assert d[0][2] == Decimal("0.72")


def test_build_unapplied_liquidation_emits_loan_credit_and_ascii_separator():
    rows = [
        {
            "value_date": date(2025, 3, 29),
            "unapplied_delta": "-1.28",
            "entry_kind": "liquidation",
            "repayment_id": 8,
            "liquidation_repayment_id": None,
            "parent_repayment_id": 7,
        }
    ]
    ev = build_unapplied_ledger_statement_events(rows)
    assert len(ev) == 1
    assert ev[0].credit == Decimal("1.28")
    assert ev[0].debit == Decimal("0")
    assert " - " in ev[0].narration
    assert "\u2014" not in ev[0].narration  # no unicode em dash


def test_build_unapplied_ledger_statement_events_meta_delta():
    rows = [
        {
            "value_date": date(2025, 1, 1),
            "unapplied_delta": "3.5",
            "entry_kind": "credit",
            "repayment_id": 9,
        }
    ]
    ev = build_unapplied_ledger_statement_events(rows)
    assert len(ev) == 1
    assert ev[0].meta["unapplied_delta"] == "3.5000000000"
    assert ev[0].debit == Decimal("0") and ev[0].credit == Decimal("0")


def test_merge_sort_disbursement_before_accrual_same_day():
    d = date(2025, 1, 1)
    acc = StatementEvent(
        d,
        "REGULAR_INTEREST_ACCRUAL",
        "acc",
        Decimal("1"),
        Decimal("0"),
        sort_ordinal=20,
    )
    dis = StatementEvent(
        d,
        "DISBURSEMENT",
        "draw",
        Decimal("100"),
        Decimal("0"),
        sort_ordinal=15,
    )
    m = merge_sort_statement_events([acc, dis])
    assert m[0].event_type == "DISBURSEMENT"


def test_rollup_flow_merges_adjacent_regular_accruals_and_strips_internal_keys():
    rows = [
        {
            "Due Date": date(2025, 1, 1),
            "Narration": "Regular interest",
            "Debits": 1.0,
            "Credits": 0.0,
            "Balance": 101.0,
            "Arrears": 0.0,
            "Unapplied funds": 0.0,
            "_event_type": "REGULAR_INTEREST_ACCRUAL",
        },
        {
            "Due Date": date(2025, 1, 2),
            "Narration": "Regular interest",
            "Debits": 2.0,
            "Credits": 0.0,
            "Balance": 103.0,
            "Arrears": 0.0,
            "Unapplied funds": 0.0,
            "_event_type": "REGULAR_INTEREST_ACCRUAL",
        },
        {
            "Due Date": date(2025, 1, 3),
            "Narration": "Total outstanding (flow) as at 2025-01-03",
            "Debits": 0.0,
            "Credits": 0.0,
            "Balance": 103.0,
            "Arrears": 0.0,
            "Unapplied funds": 0.0,
        },
    ]
    out = rollup_flow_statement_rows_for_display(rows)
    assert len(out) == 2
    assert "roll-up" in out[0]["Narration"]
    assert out[0]["Debits"] == 3.0
    assert out[0]["Due Date"] == date(2025, 1, 2)
    assert out[0]["Balance"] == 103.0
    assert "_event_type" not in out[0]
    assert "_event_type" not in out[1]


def _accr_row(d: date, debit: float, bal: float) -> dict:
    return {
        "Due Date": d,
        "Narration": "Regular interest",
        "Debits": debit,
        "Credits": 0.0,
        "Balance": bal,
        "Arrears": 0.0,
        "Unapplied funds": 0.0,
        "_event_type": "REGULAR_INTEREST_ACCRUAL",
    }


def test_rollup_schedule_period_two_bands_split_by_due():
    disb = date(2025, 1, 1)
    due1 = date(2025, 1, 5)
    sys_d = date(2025, 1, 10)
    rows = [
        _accr_row(date(2025, 1, 1), 1.0, 101.0),
        _accr_row(date(2025, 1, 2), 1.0, 102.0),
        _accr_row(date(2025, 1, 5), 2.0, 110.0),
        _accr_row(date(2025, 1, 6), 2.0, 112.0),
        {
            "Due Date": date(2025, 1, 10),
            "Narration": "Total outstanding (flow) as at 2025-01-10",
            "Debits": 0.0,
            "Credits": 0.0,
            "Balance": 112.0,
            "Arrears": 0.0,
            "Unapplied funds": 0.0,
        },
    ]
    out = rollup_flow_statement_rows_for_display(
        rows,
        disbursement_date=disb,
        system_business_date=sys_d,
        schedule_due_dates=[due1, date(2025, 2, 1)],
    )
    assert len(out) == 3
    assert "schedule period" in out[0]["Narration"]
    assert out[0]["Debits"] == 2.0
    assert out[1]["Debits"] == 4.0
    assert "Total outstanding (flow)" in out[2]["Narration"]


def test_rollup_schedule_period_excludes_system_date_accrual_without_receipt():
    disb = date(2025, 1, 1)
    sys_d = date(2025, 1, 5)
    rows = [
        _accr_row(date(2025, 1, 4), 1.0, 104.0),
        _accr_row(date(2025, 1, 5), 9.0, 113.0),
        {
            "Due Date": date(2025, 1, 5),
            "Narration": "Total outstanding (flow) as at 2025-01-05",
            "Debits": 0.0,
            "Credits": 0.0,
            "Balance": 113.0,
            "Arrears": 0.0,
            "Unapplied funds": 0.0,
        },
    ]
    out = rollup_flow_statement_rows_for_display(
        rows,
        disbursement_date=disb,
        system_business_date=sys_d,
        schedule_due_dates=[date(2025, 1, 31)],
    )
    assert len(out) == 3
    assert out[0]["Debits"] == 1.0
    assert out[1]["Debits"] == 9.0
    assert out[1]["Narration"] == "Regular interest"


def test_rollup_schedule_period_includes_system_date_when_receipt_same_day():
    disb = date(2025, 1, 1)
    sys_d = date(2025, 1, 5)
    rows = [
        _accr_row(date(2025, 1, 4), 1.0, 104.0),
        _accr_row(date(2025, 1, 5), 9.0, 113.0),
        {
            "Due Date": date(2025, 1, 5),
            "Narration": "p",
            "Debits": 0.0,
            "Credits": 5.0,
            "Balance": 108.0,
            "Arrears": 0.0,
            "Unapplied funds": 0.0,
            "_event_type": "PAYMENT_RECEIPT",
        },
        {
            "Due Date": date(2025, 1, 5),
            "Narration": "Total outstanding (flow) as at 2025-01-05",
            "Debits": 0.0,
            "Credits": 0.0,
            "Balance": 108.0,
            "Arrears": 0.0,
            "Unapplied funds": 0.0,
        },
    ]
    out = rollup_flow_statement_rows_for_display(
        rows,
        disbursement_date=disb,
        system_business_date=sys_d,
        schedule_due_dates=[date(2025, 1, 31)],
    )
    rolled = [r for r in out if "schedule period" in r.get("Narration", "")]
    assert len(rolled) == 1
    assert rolled[0]["Debits"] == 10.0
