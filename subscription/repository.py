"""PostgreSQL access for vendor tier catalog and tenant subscription / POP uploads."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url
from db.tenant_session import validate_tenant_schema_name

_GRACE_UNSET = object()


def _conn():
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


def ensure_public_vendor_subscription_tiers(conn) -> None:
    """Idempotent: public.vendor_subscription_tiers + seed Basic/Premium."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.vendor_subscription_tiers (
                tier_name      TEXT PRIMARY KEY,
                monthly_fee    NUMERIC(20, 10) NOT NULL DEFAULT 0,
                quarterly_fee  NUMERIC(20, 10) NOT NULL DEFAULT 0,
                is_active      BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT vendor_subscription_tiers_tier_name_nonempty
                    CHECK (length(trim(tier_name)) > 0)
            );
            """
        )
        cur.execute(
            """
            INSERT INTO public.vendor_subscription_tiers (tier_name, monthly_fee, quarterly_fee, is_active)
            VALUES ('Basic', 0, 0, TRUE), ('Premium', 0, 0, TRUE)
            ON CONFLICT (tier_name) DO NOTHING;
            """
        )
    conn.commit()


def ensure_tenant_subscription_tables(conn, tenant_schema: str) -> None:
    """Idempotent DDL in the given tenant schema (search_path)."""
    schema = validate_tenant_schema_name(tenant_schema)
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL search_path TO {schema}")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tenant_subscription (
                id                     SMALLINT PRIMARY KEY DEFAULT 1,
                tier_name              TEXT NOT NULL DEFAULT 'Basic',
                billing_cycle          TEXT NOT NULL DEFAULT 'Monthly',
                period_start           DATE,
                period_end             DATE,
                access_terminated_at   TIMESTAMPTZ,
                grace_access_until     DATE,
                notes                  TEXT,
                updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT tenant_subscription_single_row CHECK (id = 1),
                CONSTRAINT tenant_subscription_billing_cycle_chk
                    CHECK (billing_cycle IN ('Monthly', 'Quarterly')),
                CONSTRAINT tenant_subscription_tier_fk FOREIGN KEY (tier_name)
                    REFERENCES public.vendor_subscription_tiers (tier_name)
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE tenant_subscription
                ADD COLUMN IF NOT EXISTS grace_access_until DATE
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subscription_pop_uploads (
                id                     BIGSERIAL PRIMARY KEY,
                uploaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                uploaded_by            TEXT NOT NULL DEFAULT '',
                file_name              TEXT NOT NULL,
                mime_type              TEXT NOT NULL DEFAULT '',
                file_size              BIGINT NOT NULL,
                file_content           BYTEA NOT NULL,
                period_end_applied_to  DATE,
                verified               BOOLEAN NOT NULL DEFAULT FALSE
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscription_pop_uploads_uploaded_at
                ON subscription_pop_uploads (uploaded_at DESC);
            """
        )
        cur.execute(
            """
            INSERT INTO tenant_subscription (id, tier_name, billing_cycle)
            VALUES (1, 'Basic', 'Monthly')
            ON CONFLICT (id) DO NOTHING;
            """
        )
    conn.commit()


def list_vendor_tiers() -> list[dict[str, Any]]:
    conn = _conn()
    try:
        ensure_public_vendor_subscription_tiers(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tier_name, monthly_fee, quarterly_fee, is_active, updated_at
                FROM public.vendor_subscription_tiers
                ORDER BY tier_name
                """
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def upsert_vendor_tier(
    *,
    tier_name: str,
    monthly_fee: Decimal,
    quarterly_fee: Decimal,
    is_active: bool,
) -> None:
    conn = _conn()
    try:
        ensure_public_vendor_subscription_tiers(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.vendor_subscription_tiers
                    (tier_name, monthly_fee, quarterly_fee, is_active, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (tier_name) DO UPDATE SET
                    monthly_fee = EXCLUDED.monthly_fee,
                    quarterly_fee = EXCLUDED.quarterly_fee,
                    is_active = EXCLUDED.is_active,
                    updated_at = NOW()
                """,
                (tier_name.strip(), monthly_fee, quarterly_fee, is_active),
            )
        conn.commit()
    finally:
        conn.close()


def get_tenant_subscription_row(tenant_schema: str) -> dict[str, Any] | None:
    schema = validate_tenant_schema_name(tenant_schema)
    conn = _conn()
    try:
        ensure_public_vendor_subscription_tiers(conn)
        ensure_tenant_subscription_tables(conn, schema)
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL search_path TO {schema}")
            cur.execute("SELECT * FROM tenant_subscription WHERE id = 1")
            row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_tenant_subscription(
    tenant_schema: str,
    *,
    tier_name: str | None = None,
    billing_cycle: str | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    notes: str | None = None,
    clear_termination: bool = False,
    grace_access_until: date | None | object = _GRACE_UNSET,
) -> None:
    schema = validate_tenant_schema_name(tenant_schema)
    conn = _conn()
    try:
        ensure_tenant_subscription_tables(conn, schema)
        sets: list[str] = ["updated_at = NOW()"]
        params: list[Any] = []
        if tier_name is not None:
            sets.append("tier_name = %s")
            params.append(tier_name)
        if billing_cycle is not None:
            sets.append("billing_cycle = %s")
            params.append(billing_cycle)
        if period_start is not None:
            sets.append("period_start = %s")
            params.append(period_start)
        if period_end is not None:
            sets.append("period_end = %s")
            params.append(period_end)
        if notes is not None:
            sets.append("notes = %s")
            params.append(notes)
        if clear_termination:
            sets.append("access_terminated_at = NULL")
        if grace_access_until is not _GRACE_UNSET:
            sets.append("grace_access_until = %s")
            params.append(grace_access_until)
        if len(sets) == 1 and not clear_termination:
            return
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL search_path TO {schema}")
            cur.execute(
                f"UPDATE tenant_subscription SET {', '.join(sets)} WHERE id = 1",
                params,
            )
        conn.commit()
    finally:
        conn.close()


def set_tenant_access_terminated(tenant_schema: str, *, terminated: bool) -> None:
    schema = validate_tenant_schema_name(tenant_schema)
    conn = _conn()
    try:
        ensure_tenant_subscription_tables(conn, schema)
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL search_path TO {schema}")
            if terminated:
                cur.execute(
                    """
                    UPDATE tenant_subscription
                    SET access_terminated_at = NOW(), updated_at = NOW()
                    WHERE id = 1 AND access_terminated_at IS NULL
                    """
                )
            else:
                cur.execute(
                    """
                    UPDATE tenant_subscription
                    SET access_terminated_at = NULL, updated_at = NOW()
                    WHERE id = 1
                    """
                )
        conn.commit()
    finally:
        conn.close()


def insert_pop_upload(
    tenant_schema: str,
    *,
    uploaded_by: str,
    file_name: str,
    mime_type: str,
    file_size: int,
    file_content: bytes,
    period_end_applied_to: date | None = None,
) -> int:
    schema = validate_tenant_schema_name(tenant_schema)
    conn = _conn()
    try:
        ensure_tenant_subscription_tables(conn, schema)
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL search_path TO {schema}")
            cur.execute(
                """
                INSERT INTO subscription_pop_uploads (
                    uploaded_by, file_name, mime_type, file_size, file_content,
                    period_end_applied_to
                ) VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (uploaded_by, file_name, mime_type, file_size, file_content, period_end_applied_to),
            )
            row = cur.fetchone()
            new_id = row["id"] if row else None
        conn.commit()
        return int(new_id) if new_id is not None else 0
    finally:
        conn.close()


def list_pop_uploads_metadata(tenant_schema: str, *, limit: int = 50) -> list[dict[str, Any]]:
    schema = validate_tenant_schema_name(tenant_schema)
    conn = _conn()
    try:
        ensure_tenant_subscription_tables(conn, schema)
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL search_path TO {schema}")
            cur.execute(
                """
                SELECT id, uploaded_at, uploaded_by, file_name, mime_type, file_size,
                       period_end_applied_to, verified
                FROM subscription_pop_uploads
                ORDER BY uploaded_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_pop_upload_content(tenant_schema: str, upload_id: int) -> dict[str, Any] | None:
    schema = validate_tenant_schema_name(tenant_schema)
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL search_path TO {schema}")
            cur.execute(
                """
                SELECT id, file_name, mime_type, file_content
                FROM subscription_pop_uploads WHERE id = %s
                """,
                (upload_id,),
            )
            row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def row_dates(row: dict[str, Any]) -> tuple[date | None, date | None, Any]:
    """Normalize date fields from DB row."""
    ps = row.get("period_start")
    pe = row.get("period_end")
    term = row.get("access_terminated_at")
    if hasattr(ps, "date"):
        ps = ps.date()
    if hasattr(pe, "date"):
        pe = pe.date()
    return ps, pe, term


def grace_access_until_date(row: dict[str, Any]) -> date | None:
    """Normalize ``grace_access_until`` from a tenant_subscription row."""
    g = row.get("grace_access_until")
    if g is None:
        return None
    if isinstance(g, datetime):
        return g.date()
    if isinstance(g, date):
        return g
    return None
