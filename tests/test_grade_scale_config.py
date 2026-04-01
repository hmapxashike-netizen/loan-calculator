"""Unit tests for loan grade scale resolution (no DB required)."""
from __future__ import annotations

import pytest

from grade_scale_config import _dpd_matches_band, format_dpd_range, resolve_loan_grade


def test_format_dpd_range():
    assert format_dpd_range(0, 0) == "0 dpd"
    assert format_dpd_range(1, 30) == "1-30 dpd"
    assert format_dpd_range(91, None) == "91+ dpd"


def test_dpd_matches_band():
    assert _dpd_matches_band(0, 0, 0) is True
    assert _dpd_matches_band(1, 0, 0) is False
    assert _dpd_matches_band(30, 1, 30) is True
    assert _dpd_matches_band(91, 91, None) is True
    assert _dpd_matches_band(500, 91, None) is True


def _default_rules():
    """Mirror seeded defaults in schema/63."""
    return [
        {
            "id": 1,
            "sort_order": 10,
            "is_active": True,
            "grade_name": "Pass",
            "performance_status": "Performing",
            "regulatory_dpd_min": 0,
            "regulatory_dpd_max": 0,
            "standard_dpd_min": 0,
            "standard_dpd_max": 0,
        },
        {
            "id": 2,
            "sort_order": 20,
            "is_active": True,
            "grade_name": "Special Mention",
            "performance_status": "Performing",
            "regulatory_dpd_min": 1,
            "regulatory_dpd_max": 30,
            "standard_dpd_min": 1,
            "standard_dpd_max": 90,
        },
        {
            "id": 3,
            "sort_order": 30,
            "is_active": True,
            "grade_name": "Sub standard",
            "performance_status": "NonPerforming",
            "regulatory_dpd_min": 31,
            "regulatory_dpd_max": 60,
            "standard_dpd_min": 91,
            "standard_dpd_max": 180,
        },
        {
            "id": 4,
            "sort_order": 40,
            "is_active": True,
            "grade_name": "Doubtful",
            "performance_status": "NonPerforming",
            "regulatory_dpd_min": 61,
            "regulatory_dpd_max": 90,
            "standard_dpd_min": 181,
            "standard_dpd_max": 360,
        },
        {
            "id": 5,
            "sort_order": 50,
            "is_active": True,
            "grade_name": "Loss",
            "performance_status": "NonPerforming",
            "regulatory_dpd_min": 91,
            "regulatory_dpd_max": None,
            "standard_dpd_min": 361,
            "standard_dpd_max": None,
        },
    ]


@pytest.fixture
def patch_rules(monkeypatch):
    def _patch():
        monkeypatch.setattr(
            "grade_scale_config.list_loan_grade_scale_rules",
            lambda active_only=True: [r for r in _default_rules() if not active_only or r["is_active"]],
        )

    return _patch


def test_resolve_regulatory(patch_rules):
    patch_rules()
    assert resolve_loan_grade(0, scale="regulatory")["grade_name"] == "Pass"
    assert resolve_loan_grade(15, scale="regulatory")["grade_name"] == "Special Mention"
    assert resolve_loan_grade(45, scale="regulatory")["grade_name"] == "Sub standard"
    assert resolve_loan_grade(75, scale="regulatory")["grade_name"] == "Doubtful"
    assert resolve_loan_grade(91, scale="regulatory")["grade_name"] == "Loss"
    assert resolve_loan_grade(400, scale="regulatory")["grade_name"] == "Loss"


def test_resolve_standard(patch_rules):
    patch_rules()
    assert resolve_loan_grade(0, scale="standard")["grade_name"] == "Pass"
    assert resolve_loan_grade(45, scale="standard")["grade_name"] == "Special Mention"
    assert resolve_loan_grade(95, scale="standard")["grade_name"] == "Sub standard"
    assert resolve_loan_grade(200, scale="standard")["grade_name"] == "Doubtful"
    assert resolve_loan_grade(361, scale="standard")["grade_name"] == "Loss"
