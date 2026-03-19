"""
Agent module: brokers/referrers linked to loans (commission, TIN, tax clearance).
Agents are separate from customers; use this module for CRUD.
"""

from __future__ import annotations

import contextlib
from datetime import date
from typing import Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None

from config import get_database_url


def _get_conn():
    if psycopg2 is None:
        raise RuntimeError("Install psycopg2-binary to use the agents module.")
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


def list_agents(status: str | None = "active") -> list[dict]:
    """List agents; pass status=None for all. Returns [] if agents table does not exist (run schema/11_sectors_subsectors_agents.sql)."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if status:
                    cur.execute(
                        "SELECT * FROM agents WHERE status = %s ORDER BY name",
                        (status,),
                    )
                else:
                    cur.execute("SELECT * FROM agents ORDER BY name")
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        if psycopg2 and hasattr(psycopg2, "ProgrammingError") and isinstance(e, psycopg2.ProgrammingError):
            return []
        raise


def get_agent(agent_id: int) -> dict | None:
    """Get one agent by id."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM agents WHERE id = %s", (agent_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def create_agent(
    name: str,
    agent_type: str = "individual",
    id_number: str | None = None,
    address_line1: str | None = None,
    address_line2: str | None = None,
    city: str | None = None,
    country: str | None = None,
    phone1: str | None = None,
    phone2: str | None = None,
    email: str | None = None,
    commission_rate_pct: float | None = None,
    tin_number: str | None = None,
    tax_clearance_expiry: date | None = None,
    status: str = "active",
) -> int:
    """Create an agent. Returns agent id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agents (
                    name, agent_type, id_number, address_line1, address_line2, city, country,
                    phone1, phone2, email, commission_rate_pct, tin_number,
                    tax_clearance_expiry, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    name,
                    agent_type,
                    id_number,
                    address_line1,
                    address_line2,
                    city,
                    country,
                    phone1,
                    phone2,
                    email,
                    commission_rate_pct,
                    tin_number,
                    tax_clearance_expiry,
                    status,
                ),
            )
            return cur.fetchone()[0]


def update_agent(
    agent_id: int,
    name: str | None = None,
    id_number: str | None = None,
    address_line1: str | None = None,
    address_line2: str | None = None,
    city: str | None = None,
    country: str | None = None,
    phone1: str | None = None,
    phone2: str | None = None,
    email: str | None = None,
    commission_rate_pct: float | None = None,
    tin_number: str | None = None,
    tax_clearance_expiry: date | None = None,
    status: str | None = None,
    agent_type: str | None = None,
) -> None:
    """Update agent fields. Pass only fields to change."""
    allowed = {
        "name", "agent_type", "id_number", "address_line1", "address_line2", "city", "country",
        "phone1", "phone2", "email", "commission_rate_pct", "tin_number",
        "tax_clearance_expiry", "status",
    }
    updates = []
    vals = []
    for k, v in [
        ("name", name), ("agent_type", agent_type), ("id_number", id_number), ("address_line1", address_line1),
        ("address_line2", address_line2), ("city", city), ("country", country),
        ("phone1", phone1), ("phone2", phone2), ("email", email),
        ("commission_rate_pct", commission_rate_pct), ("tin_number", tin_number),
        ("tax_clearance_expiry", tax_clearance_expiry), ("status", status),
    ]:
        if k in allowed and v is not None:
            updates.append(f"{k} = %s")
            vals.append(v)
    if not updates:
        return
    vals.append(agent_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE agents SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s",
                vals,
            )
