"""
Multi-tenant PostgreSQL access (schema-per-tenant) via SQLAlchemy + Streamlit secrets.

Architecture notes
------------------
- **One shared engine** (pooled) per Streamlit process, cached with ``@st.cache_resource``.
- **Per-request / per-call sessions** set ``search_path`` to the tenant schema.
- **Pool safety**: PostgreSQL ``search_path`` is session-scoped. Pooled connections must not
  return to the pool with a tenant schema still active, or the next borrower could query the
  wrong tenant. This module registers a pool ``reset`` listener that runs ``RESET search_path``
  when connections are returned (see SQLAlchemy ``PoolEvents.reset``).

Scaling to hundreds of tenants
-------------------------------
- Add new schemas in PostgreSQL (``CREATE SCHEMA tenant_xxx``) and migrate objects per tenant.
- Optionally introduce a **tenant registry** (table or config service) mapping auth identity →
  schema name; resolve that value and pass it to :func:`get_db_session`.
- For very large fleets, consider **read replicas**, **PgBouncer**, or **per-tenant engines**
  only where isolation or SLAs demand it; this module keeps the session API stable.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generator, Iterator

import streamlit as st
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import Pool

# PostgreSQL identifier: 1–63 chars, starts with letter or underscore, then alphanumeric/underscore.
# Unquoted identifiers are folded to lower case; this pattern matches safe unquoted names.
_TENANT_SCHEMA_RE = re.compile(r"\A[a-zA-Z_][a-zA-Z0-9_]{0,62}\Z")


class TenantDatabaseConfigError(RuntimeError):
    """Raised when Streamlit secrets or engine configuration is invalid."""


class TenantSchemaValidationError(ValueError):
    """Raised when ``tenant_schema`` is not a safe PostgreSQL identifier."""


@dataclass(frozen=True, slots=True)
class PostgresSecrets:
    """Master cluster credentials (one database, many schemas)."""

    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def sqlalchemy_url(self) -> str:
        """SQLAlchemy URL for psycopg2 driver."""
        from urllib.parse import quote_plus

        pw = quote_plus(self.password) if self.password else ""
        auth = f"{self.user}:{pw}" if pw else self.user
        return f"postgresql+psycopg2://{auth}@{self.host}:{self.port}/{self.database}"


def _read_postgres_secrets() -> PostgresSecrets:
    """Load ``[postgres]`` from ``st.secrets`` (Streamlit Cloud / local ``.streamlit/secrets.toml``)."""
    try:
        raw: Any = st.secrets["postgres"]
    except Exception as e:
        raise TenantDatabaseConfigError(
            'Missing or unreadable st.secrets["postgres"]. '
            "Define [postgres] in .streamlit/secrets.toml (see secrets.toml.example)."
        ) from e

    # Streamlit wraps TOML tables in a mapping type that is not always ``dict``.
    if not isinstance(raw, Mapping):
        raise TenantDatabaseConfigError(
            'st.secrets["postgres"] must be a table (dict-like) with keys host, port, user, password, database.'
        )

    def _req(key: str) -> str:
        if key not in raw or raw[key] in (None, ""):
            raise TenantDatabaseConfigError(f'st.secrets["postgres"] missing required key: {key!r}')
        return str(raw[key]).strip()

    host = _req("host")
    user = _req("user")
    database = _req("database")
    password = str(raw.get("password", "") or "")

    port_raw = raw.get("port", 5432)
    try:
        port = int(port_raw)
    except (TypeError, ValueError) as e:
        raise TenantDatabaseConfigError(f'Invalid postgres port: {port_raw!r}') from e

    return PostgresSecrets(host=host, port=port, user=user, password=password, database=database)


def validate_tenant_schema_name(tenant_schema: str) -> str:
    """
    Validate ``tenant_schema`` for use in ``SET search_path`` (identifier only, no injection).

    Returns the validated string unchanged. Raises :class:`TenantSchemaValidationError` if invalid.
    """
    if not isinstance(tenant_schema, str):
        raise TenantSchemaValidationError("tenant_schema must be a non-empty string")
    name = tenant_schema.strip()
    if not name:
        raise TenantSchemaValidationError("tenant_schema must be a non-empty string")
    if not _TENANT_SCHEMA_RE.fullmatch(name):
        raise TenantSchemaValidationError(
            "tenant_schema must match pattern "
            "'[a-zA-Z_][a-zA-Z0-9_]{0,62}' (PostgreSQL-safe unquoted identifier)."
        )
    return name


def _register_search_path_pool_reset(engine: Engine) -> None:
    """
    When a DBAPI connection is reset on pool return, clear ``search_path`` so the next
    checkout does not inherit another tenant's schema.
    """

    @event.listens_for(engine.pool, "reset")
    def _on_pool_reset(dbapi_connection: Any, connection_record: Any, reset_state: Any) -> None:
        if getattr(reset_state, "terminate_only", False):
            return
        if dbapi_connection is None:
            return
        try:
            # Close any open DBAPI transaction before RESET so pool_pre_ping / set_session
            # does not hit psycopg2 "set_session cannot be used inside a transaction".
            dbapi_connection.rollback()
            cur = dbapi_connection.cursor()
            cur.execute("RESET search_path")
            cur.close()
        except Exception:
            # Avoid breaking pool; connection may already be bad — let pre-ping handle it.
            pass


@st.cache_resource(show_spinner=False)
def _tenant_engine_and_session_factory() -> tuple[Engine, sessionmaker[Session]]:
    """
    Build one shared engine + session factory per Streamlit server process.

    Cached with ``st.cache_resource`` so Streamlit reruns do not create new pools.
    """
    cfg = _read_postgres_secrets()
    engine = create_engine(
        cfg.sqlalchemy_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_reset_on_return="rollback",
        future=True,
    )
    _register_search_path_pool_reset(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)
    return engine, SessionLocal


def get_tenant_engine() -> Engine:
    """Return the cached shared :class:`sqlalchemy.engine.Engine` (same pool for all tenants)."""
    engine, _ = _tenant_engine_and_session_factory()
    return engine


def connect_autocommit_psycopg2() -> Any:
    """
    psycopg2 connection with ``autocommit=True`` and :class:`~psycopg2.extras.RealDictCursor`.

    Use for **public** metadata queries (e.g. ``public.tenants``) so they do not share the
    SQLAlchemy pool. Pool checkout + ``pool_pre_ping`` can otherwise trigger psycopg2
    ``set_session cannot be used inside a transaction`` when combined with autobegin.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    cfg = _read_postgres_secrets()
    conn = psycopg2.connect(
        host=cfg.host,
        port=int(cfg.port),
        user=cfg.user,
        password=cfg.password,
        dbname=cfg.database,
        cursor_factory=RealDictCursor,
    )
    conn.autocommit = True
    return conn


def get_db_session(tenant_schema: str) -> Session:
    """
    Open a new SQLAlchemy session and set PostgreSQL ``search_path`` to the tenant schema.

    Parameters
    ----------
    tenant_schema
        Target schema name (e.g. ``tenant_acme``). Validated against a strict identifier regex.

    Returns
    -------
    sqlalchemy.orm.Session
        Caller **must** ``close()`` the session (or use :func:`tenant_session_scope`).

    Notes
    -----
    - For DDL or extensions in ``public``, you may extend this to
      ``SET search_path TO {schema}, public`` after product review.
    - Always close the session in a ``finally`` block or use a context manager to release
      connections back to the pool.
    """
    validated = validate_tenant_schema_name(tenant_schema)
    _, SessionLocal = _tenant_engine_and_session_factory()
    session = SessionLocal()
    try:
        # First statement on this connection: SET runs inside the normal autobegin transaction.
        # Avoid execution_options isolation_level AUTOCOMMIT here — it triggers psycopg2
        # "set_session cannot be used inside a transaction" with pool_pre_ping / pooled connections.
        session.execute(text(f"SET search_path TO {validated}"))
    except Exception:
        session.close()
        raise
    return session


@contextmanager
def tenant_session_scope(tenant_schema: str) -> Generator[Session, None, None]:
    """
    Context manager: yields a session with ``search_path`` set, commits on success, rolls back on error, always closes.
    """
    session = get_db_session(tenant_schema)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


__all__ = [
    "TenantDatabaseConfigError",
    "TenantSchemaValidationError",
    "PostgresSecrets",
    "validate_tenant_schema_name",
    "get_tenant_engine",
    "connect_autocommit_psycopg2",
    "get_db_session",
    "tenant_session_scope",
]
