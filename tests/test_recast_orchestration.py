"""Unit tests for loan recast orchestration (allocation + date rules)."""

from datetime import date

import pytest

from loan_management.recast_orchestration import (
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
