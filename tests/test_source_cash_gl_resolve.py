"""Source-cash GL validation accepts UUID or chart code (batch Excel)."""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest

from loan_management.cash_gl import validate_source_cash_gl_account_id_for_new_posting

_FIXED = str(uuid.uuid4())
_ENTRIES = [{"id": _FIXED, "code": "A100001-02", "name": "Test cash"}]


@patch("loan_management.cash_gl.get_cached_source_cash_account_entries", return_value=_ENTRIES)
def test_validate_accepts_canonical_uuid(_mock: object) -> None:
    assert validate_source_cash_gl_account_id_for_new_posting(_FIXED) == _FIXED


@patch("loan_management.cash_gl.get_cached_source_cash_account_entries", return_value=_ENTRIES)
def test_validate_accepts_gl_account_code(_mock: object) -> None:
    assert validate_source_cash_gl_account_id_for_new_posting("A100001-02") == _FIXED


@patch("loan_management.cash_gl.get_cached_source_cash_account_entries", return_value=_ENTRIES)
def test_validate_rejects_unknown_code(_mock: object) -> None:
    with pytest.raises(ValueError, match="not a valid UUID or a known"):
        validate_source_cash_gl_account_id_for_new_posting("X999")
