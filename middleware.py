from __future__ import annotations

from functools import wraps
from typing import Callable, Optional, Dict, Any

import streamlit as st


def get_current_user() -> Optional[Dict[str, Any]]:
    """
    Safe, serializable user info stored in session.
    Returns None if not logged in.
    """
    return st.session_state.get("current_user")


def set_current_user(user: Any) -> None:
    """
    Store only non-sensitive user fields in session_state.
    Accepts a DAL User object or a compatible mapping.
    """
    two_fa = getattr(user, "two_factor_enabled", None) if not isinstance(user, dict) else user.get("two_factor_enabled")
    data = {
        "id": getattr(user, "id", None) if not isinstance(user, dict) else user.get("id"),
        "email": getattr(user, "email", None) if not isinstance(user, dict) else user.get("email"),
        "full_name": getattr(user, "full_name", None) if not isinstance(user, dict) else user.get("full_name"),
        "role": getattr(user, "role", None) if not isinstance(user, dict) else user.get("role"),
        "is_active": getattr(user, "is_active", None) if not isinstance(user, dict) else user.get("is_active"),
        "two_factor_enabled": bool(two_fa) if two_fa is not None else False,
    }
    last_login = getattr(user, "last_login", None) if not isinstance(user, dict) else user.get("last_login")
    if last_login is not None:
        try:
            data["last_login"] = last_login.isoformat()  # datetime
        except AttributeError:
            data["last_login"] = str(last_login)
    st.session_state["current_user"] = data


def clear_current_user() -> None:
    st.session_state.pop("current_user", None)
    st.session_state.pop("_farnda_tenant_bind_message", None)
    st.session_state.pop("_farnda_totp_pending", None)
    st.session_state.pop("_farnda_regen_backup_codes", None)
    st.session_state.pop("_farnda_enrollment_recovery_codes", None)
    st.session_state.pop("_farnda_auth_panel", None)
    try:
        from db.tenant_registry import clear_tenant_context

        clear_tenant_context()
    except Exception:
        pass


def require_login() -> Dict[str, Any]:
    """
    Gate helper for main.py and pages. If not logged in, stop the script.
    Returns current_user dict when authenticated.
    """
    user = get_current_user()
    if user is None:
        st.warning("You must be logged in to access this page.")
        st.stop()
    return user


def require_role(*allowed_roles: str) -> Callable:
    """
    Decorator to protect Streamlit page functions by role.

    Example:
        @require_role("ADMIN", "LOAN_OFFICER")
        def officer_page():
            ...
    """

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = get_current_user()
            if user is None:
                st.warning("You must be logged in to access this page.")
                st.stop()

            role = user.get("role")
            if role not in allowed_roles:
                st.error("You do not have permission to view this page.")
                st.stop()

            return fn(*args, **kwargs)

        return wrapper

    return decorator

