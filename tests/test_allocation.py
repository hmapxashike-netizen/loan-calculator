"""
Tests for allocation logic (no DB). Run from project root:
  python -m pytest tests/test_allocation.py -v
  or:  python -m unittest tests.test_allocation -v
"""
from datetime import date
import unittest

# Import from project root
import sys
from pathlib import Path
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from loan_management import (
    BUCKET_TO_ALLOC,
    _get_waterfall_config,
    compute_waterfall_allocation,
)


# Standard waterfall order (fees, penalty, default, interest arrears, principal arrears; skip accrued/not_due)
STANDARD_ORDER = [
    "fees_charges_balance",
    "penalty_interest_balance",
    "default_interest_balance",
    "interest_arrears_balance",
    "principal_arrears",
]


class TestGetWaterfallConfig(unittest.TestCase):
    def test_raises_when_no_profile(self):
        with self.assertRaises(ValueError) as ctx:
            _get_waterfall_config({})
        self.assertIn("Waterfall profile is not configured", str(ctx.exception))

    def test_raises_when_empty_profiles(self):
        with self.assertRaises(ValueError):
            _get_waterfall_config({"payment_waterfall": "Standard", "waterfall_profiles": {}})

    def test_standard_profile(self):
        cfg = {
            "payment_waterfall": "Standard",
            "waterfall_profiles": {"standard": STANDARD_ORDER},
        }
        key, order = _get_waterfall_config(cfg)
        self.assertEqual(key, "standard")
        self.assertEqual(order, STANDARD_ORDER)

    def test_borrower_friendly_profile(self):
        order_bf = ["fees_charges_balance", "interest_arrears_balance", "principal_arrears"]
        cfg = {
            "payment_waterfall": "Borrower friendly",
            "waterfall_profiles": {"borrower_friendly": order_bf},
        }
        key, order = _get_waterfall_config(cfg)
        self.assertEqual(key, "borrower_friendly")
        self.assertEqual(order, order_bf)


class TestComputeWaterfallAllocation(unittest.TestCase):
    def test_allocates_to_interest_arrears_then_principal_arrears(self):
        balances = {
            "principal_not_due": 1000.0,
            "principal_arrears": 100.0,
            "interest_accrued_balance": 50.0,
            "interest_arrears_balance": 60.0,
            "default_interest_balance": 0.0,
            "penalty_interest_balance": 0.0,
            "fees_charges_balance": 0.0,
        }
        alloc, unapplied = compute_waterfall_allocation(
            80.0, balances, STANDARD_ORDER, "standard"
        )
        self.assertEqual(alloc["alloc_interest_arrears"], 60.0)
        self.assertEqual(alloc["alloc_principal_arrears"], 20.0)
        self.assertEqual(unapplied, 0.0)
        self.assertEqual(alloc["alloc_fees_charges"], 0.0)
        self.assertEqual(alloc["alloc_principal_not_due"], 0.0)

    def test_caps_at_bucket_balance(self):
        balances = {
            "principal_not_due": 500.0,
            "principal_arrears": 30.0,
            "interest_accrued_balance": 0.0,
            "interest_arrears_balance": 20.0,
            "default_interest_balance": 0.0,
            "penalty_interest_balance": 0.0,
            "fees_charges_balance": 0.0,
        }
        alloc, unapplied = compute_waterfall_allocation(
            100.0, balances, STANDARD_ORDER, "standard"
        )
        self.assertEqual(alloc["alloc_interest_arrears"], 20.0)
        self.assertEqual(alloc["alloc_principal_arrears"], 30.0)
        self.assertEqual(unapplied, 50.0)

    def test_overpayment_goes_to_unapplied(self):
        balances = {
            "principal_not_due": 0.0,
            "principal_arrears": 0.0,
            "interest_accrued_balance": 0.0,
            "interest_arrears_balance": 0.0,
            "default_interest_balance": 0.0,
            "penalty_interest_balance": 0.0,
            "fees_charges_balance": 0.0,
        }
        alloc, unapplied = compute_waterfall_allocation(
            100.0, balances, STANDARD_ORDER, "standard"
        )
        total_alloc = sum(alloc.values())
        self.assertEqual(total_alloc, 0.0)
        self.assertEqual(unapplied, 100.0)

    def test_standard_skips_interest_accrued_and_principal_not_due(self):
        balances = {
            "principal_not_due": 200.0,
            "principal_arrears": 50.0,
            "interest_accrued_balance": 10.0,
            "interest_arrears_balance": 0.0,
            "default_interest_balance": 0.0,
            "penalty_interest_balance": 0.0,
            "fees_charges_balance": 0.0,
        }
        alloc, unapplied = compute_waterfall_allocation(
            100.0, balances, STANDARD_ORDER, "standard"
        )
        self.assertEqual(alloc["alloc_principal_not_due"], 0.0)
        self.assertEqual(alloc["alloc_interest_accrued"], 0.0)
        self.assertEqual(alloc["alloc_principal_arrears"], 50.0)
        self.assertEqual(unapplied, 50.0)


if __name__ == "__main__":
    unittest.main()
