from __future__ import annotations

from functools import wraps
from typing import Callable, Optional, Dict, Any

import streamlit as st

from auth_dal import User


def get_current_user() -> Optional[Dict[str, Any]]:
    """
    Safe, serializable user info stored in session.
    Returns None if not logged in.
    """
    return st.session_state.get("current_user")


def set_current_user(user: User) -> None:
    """
    Store only non-sensitive fields in session_state.
    """
    st.session_state["current_user"] = {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name,
        "role": user.role,
        "is_active": user.is_active,
        "last_login": user.last_login.isoformat() if user.last_login else None,
    }


def clear_current_user() -> None:
    st.session_state.pop("current_user", None)


def require_login() -> Dict[str, Any]:
    """
    Gate helper for main.py. If not logged in, stop the script.
    Returns the current_user dict if authenticated.
    """
    user = get_current_user()
    if user is None:
        st.stop()
    return user


def require_role(*allowed_roles: str) -> Callable:
    """
    Decorator to protect individual page functions.
    Example:
        @require_role("ADMIN", "LOAN_OFFICER")
        def officer_dashboard():
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

