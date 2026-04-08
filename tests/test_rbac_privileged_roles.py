"""RBAC: privileged role assignment rules."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_main_helpers():
    path = Path(__file__).resolve().parent.parent / "main.py"
    spec = importlib.util.spec_from_file_location("farnda_main_rbac_test", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_privileged_roles_include_admin():
    m = _load_main_helpers()
    assert m._privileged_roles_touch("LOAN_OFFICER", "ADMIN") is True
    assert m._privileged_roles_touch("ADMIN", "LOAN_OFFICER") is True
    assert m._privileged_roles_touch("LOAN_OFFICER", "BORROWER") is False


def test_user_role_edit_superadmin_only_for_admin():
    m = _load_main_helpers()
    ok, _ = m._user_role_edit_allowed("ADMIN", "BORROWER", "ADMIN")
    assert ok is False
    ok2, _ = m._user_role_edit_allowed("SUPERADMIN", "BORROWER", "ADMIN")
    assert ok2 is True


def test_assignable_roles_filters_for_non_superadmin():
    m = _load_main_helpers()
    keys = ["ADMIN", "ANALYST", "BORROWER", "LOAN_OFFICER", "SUPERADMIN", "VENDOR"]
    # Patch list_assignable_role_keys path by calling function logic: use fallback when rbac not ready
    out = m._assignable_roles_for_ui("ADMIN")
    assert "SUPERADMIN" not in out
    assert "VENDOR" not in out
    assert "ADMIN" not in out
    assert "BORROWER" in out
