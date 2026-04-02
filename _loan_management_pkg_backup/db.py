"""PostgreSQL connection helpers for loan_management."""

from __future__ import annotations

import contextlib

try:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor
except ImportError:
    psycopg2 = None  # type: ignore[assignment, misc]
    RealDictCursor = None  # type: ignore[assignment, misc]
    Json = None  # type: ignore[assignment, misc]

from config import get_database_url


def _get_conn():
    if psycopg2 is None:
        raise RuntimeError("Install psycopg2-binary to use loan_management.")
    return psycopg2.connect(get_database_url())


@contextlib.contextmanager
def _connection():
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
