"""Unit tests for loan recast orchestration (allocation + date rules)."""

from datetime import date

import pytest

from loan_management.recast_orchestration import (
    _split_recast_allocation_fifo,
    compute_recast_unapplied_allocation,
    validate_recast_effective_date,
)


def test_validate_recast_same_month_ok():
    validate_recast_effective_date(
        recast_effective_date=date(2026, 4, 1),
        system_business_date=date(2026, 4, 30),
    )


def test_validate_recast_prior_month_rejects():
    with pytest.raises(ValueError, match="same calendar month"):
        validate_recast_effective_date(
            recast_effective_date=date(2026, 3, 31),
            system_business_date=date(2026, 4, 1),
        )


def test_compute_recast_allocation_order_interest_before_principal():
    bal = {
        "fees_charges_balance": 0.0,
        "penalty_interest_balance": 0.0,
        "default_interest_balance": 0.0,
        "interest_arrears_balance": 0.0,
        "interest_accrued_balance": 40.0,
        "principal_arrears": 0.0,
        "principal_not_due": 1000.0,
    }
    alloc, unused = compute_recast_unapplied_allocation(100.0, bal)
    assert unused == 0.0
    assert alloc["alloc_interest_accrued"] == 40.0
    assert alloc["alloc_principal_not_due"] == 60.0


def test_compute_recast_unused_remainder_when_buckets_smaller():
    bal = {
        "fees_charges_balance": 0.0,
        "penalty_interest_balance": 0.0,
        "default_interest_balance": 0.0,
        "interest_arrears_balance": 0.0,
        "interest_accrued_balance": 10.0,
        "principal_arrears": 0.0,
        "principal_not_due": 20.0,
    }
    alloc, unused = compute_recast_unapplied_allocation(100.0, bal)
    assert alloc["alloc_interest_accrued"] == 10.0
    assert alloc["alloc_principal_not_due"] == 20.0
    assert unused == pytest.approx(70.0)


def test_split_recast_allocation_fifo_keeps_fifo_lineage():
    credits = [
        {"id": 10, "repayment_id": 100, "amount": 30.0, "value_date": date(2025, 4, 1)},
        {"id": 11, "repayment_id": 101, "amount": 20.0, "value_date": date(2025, 4, 2)},
    ]
    alloc = {
        "alloc_fees_charges": 0.0,
        "alloc_penalty_interest": 0.0,
        "alloc_default_interest": 0.0,
        "alloc_interest_arrears": 15.0,
        "alloc_interest_accrued": 0.0,
        "alloc_principal_arrears": 10.0,
        "alloc_principal_not_due": 25.0,
    }
    legs = _split_recast_allocation_fifo(credits, alloc)
    assert len(legs) == 2
    assert legs[0]["source_unapplied_id"] == 10
    assert legs[0]["source_repayment_id"] == 100
    assert legs[0]["alloc_total"] == pytest.approx(30.0)
    assert legs[1]["source_unapplied_id"] == 11
    assert legs[1]["source_repayment_id"] == 101
    assert legs[1]["alloc_total"] == pytest.approx(20.0)
    assert sum(l["alloc_total"] for l in legs) == pytest.approx(50.0)


def test_split_recast_allocation_fifo_raises_when_pool_insufficient():
    credits = [{"id": 10, "repayment_id": 100, "amount": 10.0, "value_date": date(2025, 4, 1)}]
    alloc = {
        "alloc_fees_charges": 0.0,
        "alloc_penalty_interest": 0.0,
        "alloc_default_interest": 0.0,
        "alloc_interest_arrears": 0.0,
        "alloc_interest_accrued": 0.0,
        "alloc_principal_arrears": 0.0,
        "alloc_principal_not_due": 11.0,
    }
    with pytest.raises(ValueError, match="could not be fully split"):
        _split_recast_allocation_fifo(credits, alloc)
