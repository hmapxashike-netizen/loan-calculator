"""Server-side checks that the current user may open a loan-app nav section (defense in depth)."""

from __future__ import annotations

from typing import Any

import streamlit as st


def user_can_open_nav_section(user: dict[str, Any] | None, nav_section: str) -> bool:
    """
    True if ``user`` may open ``nav_section`` under RBAC when tables exist.
    When RBAC is absent, returns True (legacy behaviour; menu still filters).
    """
    if not user:
        return False
    role = str(user.get("role") or "").strip().upper()
    if role == "BORROWER":
        return False
    try:
        from auth.permission_catalog import nav_permission_key_for_section
        from rbac.service import get_permission_keys_for_role_key, rbac_tables_ready

        if not rbac_tables_ready():
            return True
        pk = nav_permission_key_for_section(nav_section)
        if not pk:
            return True
        return pk in get_permission_keys_for_role_key(role)
    except Exception:
        return True


def enforce_nav_section_or_stop(user: dict[str, Any] | None, nav_section: str) -> None:
    """``st.stop()`` if the user may not open this section."""
    if not user_can_open_nav_section(user, nav_section):
        st.error("You do not have permission to open this area.")
        st.stop()
