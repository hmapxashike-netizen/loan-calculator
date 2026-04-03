"""
Master tenant registry: resolve ``company_name`` → ``schema_name`` via ``public.tenants``.

Lookup queries use **parameter binding** and **schema-qualified** table names so they are safe
and do not depend on the current session ``search_path``.

Streamlit session state stores **only** the resolved schema name (and display company name).
**Do not** store a live SQLAlchemy :class:`~sqlalchemy.orm.Session` in ``st.session_state``;
sessions must be opened and closed per operation (or use :func:`tenant_session_scope`).
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Generator

import streamlit as st
from sqlalchemy.orm import Session

from db.tenant_session import (
    TenantSchemaValidationError,
    connect_autocommit_psycopg2,
    get_db_session,
    tenant_session_scope,
    validate_tenant_schema_name,
)

# Streamlit session_state keys (use these consistently across pages)
SESSION_TENANT_COMPANY = "tenant_company_display_name"
SESSION_TENANT_SCHEMA = "tenant_schema_name"


class TenantCompanyNotFoundError(LookupError):
    """No **active** row in ``public.tenants`` matches the given company name."""


class TenantAmbiguousCompanyError(RuntimeError):
    """More than one active row matched the company name (data integrity)."""


def get_tenant_schema(company_name: str) -> str:
    """
    Resolve an active tenant's PostgreSQL ``schema_name`` from ``public.tenants`` by company name.

    Match is **case-insensitive** on trimmed ``company_name``. Only rows with ``is_active = TRUE``
    are considered.

    Uses a direct psycopg2 autocommit connection (not the SQLAlchemy pool) to avoid
    ``set_session cannot be used inside a transaction`` with pool pre-ping.

    Returns
    -------
    str
        Validated schema identifier safe for ``SET search_path``.

    Raises
    ------
    ValueError
        If ``company_name`` is empty.
    TenantCompanyNotFoundError
        If no active tenant matches.
    TenantAmbiguousCompanyError
        If multiple active rows match (should not happen if the partial unique index is applied).
    TenantSchemaValidationError
        If the stored ``schema_name`` in the database is not a safe identifier (fix data / migration).
    """
    cn = (company_name or "").strip()
    if not cn:
        raise ValueError("company_name must be a non-empty string")

    conn = connect_autocommit_psycopg2()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trim(both from schema_name) AS schema_name
                FROM public.tenants
                WHERE lower(trim(both from company_name)) = lower(%s)
                  AND is_active IS TRUE
                LIMIT 2
                """,
                (cn,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise TenantCompanyNotFoundError(f"No active tenant found for company name: {cn!r}")
    if len(rows) > 1:
        raise TenantAmbiguousCompanyError(
            f"Multiple active tenants matched company name {cn!r}; check public.tenants data and indexes."
        )

    raw_schema = rows[0]["schema_name"]
    if raw_schema is None or str(raw_schema).strip() == "":
        raise TenantSchemaValidationError("Tenant row has empty schema_name")

    return validate_tenant_schema_name(str(raw_schema))


def list_active_tenants() -> list[dict[str, Any]]:
    """
    Return active tenants for pickers: ``[{"id", "company_name", "schema_name"}, ...]`` ordered by company.

    Uses psycopg2 autocommit (see :func:`get_tenant_schema`).
    """
    conn = connect_autocommit_psycopg2()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id,
                       trim(both from company_name) AS company_name,
                       trim(both from schema_name) AS schema_name
                FROM public.tenants
                WHERE is_active IS TRUE
                ORDER BY lower(company_name)
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def remember_tenant_context(company_name: str) -> str:
    """
    Look up ``schema_name`` and persist **company** + **schema** in ``st.session_state``.

    Returns the resolved schema name. Raises the same exceptions as :func:`get_tenant_schema`.
    """
    schema = get_tenant_schema(company_name)
    st.session_state[SESSION_TENANT_COMPANY] = (company_name or "").strip()
    st.session_state[SESSION_TENANT_SCHEMA] = schema
    return schema


def clear_tenant_context() -> None:
    """Remove tenant keys from session state (e.g. logout / switch tenant)."""
    st.session_state.pop(SESSION_TENANT_COMPANY, None)
    st.session_state.pop(SESSION_TENANT_SCHEMA, None)


def _default_tenant_company_name() -> str:
    """Resolve default company label for post-login bind: ``st.secrets['tenant']`` then env."""
    try:
        raw = st.secrets["tenant"]
        if isinstance(raw, dict):
            name = str(raw.get("default_company_name", "")).strip()
            if name:
                return name
    except Exception:
        pass
    return (os.environ.get("FARNDACRED_DEFAULT_TENANT_COMPANY") or "Farnda Demo").strip() or "Farnda Demo"


def bind_default_tenant_context_safely() -> None:
    """
    After successful login: resolve default company → schema and store in session state.

    Does not raise: records a sidebar message in ``st.session_state['_farnda_tenant_bind_message']``
    on failure (e.g. missing ``public.tenants`` row).
    """
    st.session_state.pop("_farnda_tenant_bind_message", None)
    company = _default_tenant_company_name()
    try:
        remember_tenant_context(company)
    except TenantCompanyNotFoundError:
        st.session_state["_farnda_tenant_bind_message"] = (
            f"No active tenant for company {company!r} in public.tenants. "
            "Run: python scripts/apply_tenant_registry_migration.py"
        )
    except Exception as e:
        st.session_state["_farnda_tenant_bind_message"] = f"Tenant context not set ({e}). Check .streamlit/secrets.toml [postgres]."


def get_stored_tenant_schema() -> str | None:
    """Return the active tenant schema from session state, or ``None`` if not set."""
    raw = st.session_state.get(SESSION_TENANT_SCHEMA)
    if raw is None or str(raw).strip() == "":
        return None
    return str(raw).strip()


def get_stored_tenant_company() -> str | None:
    """Return the display company name last stored, if any."""
    raw = st.session_state.get(SESSION_TENANT_COMPANY)
    if raw is None or str(raw).strip() == "":
        return None
    return str(raw).strip()


def require_tenant_schema_in_session() -> str:
    """
    Return stored tenant schema or stop the app with a Streamlit error (for gated pages).
    """
    schema = get_stored_tenant_schema()
    if not schema:
        st.error("No tenant context. Select a company on the tenant login step.")
        st.stop()
    try:
        return validate_tenant_schema_name(schema)
    except TenantSchemaValidationError as e:
        st.error(f"Invalid tenant schema in session: {e}")
        st.stop()


def open_session_for_current_tenant() -> Session:
    """
    Open a new :class:`~sqlalchemy.orm.Session` with ``search_path`` set to the stored tenant schema.

    Caller must :meth:`~sqlalchemy.orm.Session.close` the session (prefer :func:`tenant_session_scope_current`).
    """
    return get_db_session(require_tenant_schema_in_session())


@contextmanager
def tenant_session_scope_current() -> Generator[Session, None, None]:
    """
    Context manager: ``tenant_session_scope`` using the tenant schema from ``st.session_state``.
    """
    schema = require_tenant_schema_in_session()
    with tenant_session_scope(schema) as session:
        yield session


__all__ = [
    "SESSION_TENANT_COMPANY",
    "SESSION_TENANT_SCHEMA",
    "TenantCompanyNotFoundError",
    "TenantAmbiguousCompanyError",
    "bind_default_tenant_context_safely",
    "get_tenant_schema",
    "list_active_tenants",
    "remember_tenant_context",
    "clear_tenant_context",
    "get_stored_tenant_schema",
    "get_stored_tenant_company",
    "require_tenant_schema_in_session",
    "open_session_for_current_tenant",
    "tenant_session_scope_current",
]
