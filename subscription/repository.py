"""PostgreSQL access for vendor tier catalog and tenant subscription / POP uploads."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url
from db.tenant_session import validate_tenant_schema_name
from subscription.nav_sections import LOAN_APP_SIDEBAR_SECTIONS

_GRACE_UNSET = object()

_NAV_ORDER: tuple[str, ...] = LOAN_APP_SIDEBAR_SECTIONS
_NAV_SET: frozenset[str] = frozenset(_NAV_ORDER)

_ALLOWED_NAV = "allowed_sidebar_sections"
_BANK_RECON = "bank_reconciliation"

_LEGACY_EXCLUDED = "excluded_sidebar_sections"
_LEGACY_LOAN_CAPTURE = "loan_capture"


def _canonical_allowed_list(candidate: list[str]) -> list[str]:
    """Preserve global nav order; drop unknown labels."""
    chosen = {str(x).strip() for x in candidate if str(x).strip() in _NAV_SET}
    return [s for s in _NAV_ORDER if s in chosen]


def merge_vendor_tier_features(raw: dict[str, Any] | None) -> dict[str, Any]:
    """
    Normalised tier entitlements stored in vendor_subscription_tiers.features:

    - ``allowed_sidebar_sections``: nav labels the tier may see (subset of LOAN_APP_SIDEBAR_SECTIONS).
      Loan Capture is available iff **Loan management** is in this list.
    - ``bank_reconciliation``: accounting bank-reconciliation tooling.

    Migrates legacy keys ``excluded_sidebar_sections`` / ``loan_capture``.
    """
    full_allow = list(_NAV_ORDER)
    default_out: dict[str, Any] = {
        _ALLOWED_NAV: list(full_allow),
        _BANK_RECON: True,
    }
    if not raw:
        return dict(default_out)

    r = dict(raw)

    # New format takes precedence when key exists (even if []).
    if _ALLOWED_NAV in r:
        raw_list = r.get(_ALLOWED_NAV)
        allowed = _canonical_allowed_list(raw_list if isinstance(raw_list, list) else [])
        bank = bool(r.get(_BANK_RECON, True))
        return {_ALLOWED_NAV: allowed, _BANK_RECON: bank}

    # Legacy: exclusions + optional loan_capture / bank flags
    excl_raw = r.get(_LEGACY_EXCLUDED)
    excluded: set[str] = set()
    if isinstance(excl_raw, list):
        excluded = {str(x).strip() for x in excl_raw if str(x).strip() in _NAV_SET}

    allowed = [s for s in _NAV_ORDER if s not in excluded]

    bank = bool(r.get(_BANK_RECON, True))
    # Legacy loan_capture=false did not remove Loan management from the sidebar list; capture used a
    # separate gate. Matrix model ties capture to Loan management — migration keeps LM allowed here.
    return {_ALLOWED_NAV: allowed, _BANK_RECON: bank}


def _conn():
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


def _normalize_vendor_catalog_single_active(cur) -> None:
    """At most one tier may be catalog-active; prefer Premium if multiple legacy TRUE rows."""
    cur.execute(
        """
        SELECT tier_name FROM public.vendor_subscription_tiers
        WHERE is_active = TRUE
        ORDER BY tier_name ASC
        """
    )
    rows = cur.fetchall()
    names = [str(r["tier_name"]) for r in rows] if rows else []
    if len(names) <= 1:
        return
    preferred = "Premium" if "Premium" in names else names[-1]
    cur.execute("UPDATE public.vendor_subscription_tiers SET is_active = FALSE")
    cur.execute(
        "UPDATE public.vendor_subscription_tiers SET is_active = TRUE WHERE tier_name = %s",
        (preferred,),
    )


def _seed_default_tier_features_if_empty(cur) -> None:
    """Legacy rows: empty JSON becomes catalog defaults for Basic / Premium (non-destructive)."""
    basic_allow = [
        s
        for s in _NAV_ORDER
        if s
        not in (
            "Notifications",
            "Document Management",
            "Portfolio reports",
        )
    ]
    basic = json.dumps(
        {
            _ALLOWED_NAV: basic_allow,
            _BANK_RECON: False,
        }
    )
    premium = json.dumps(
        {
            _ALLOWED_NAV: list(_NAV_ORDER),
            _BANK_RECON: True,
        }
    )
    cur.execute(
        """
        UPDATE public.vendor_subscription_tiers
        SET features = %s::jsonb
        WHERE lower(trim(tier_name)) = 'basic' AND features = '{}'::jsonb
        """,
        (basic,),
    )
    cur.execute(
        """
        UPDATE public.vendor_subscription_tiers
        SET features = %s::jsonb
        WHERE lower(trim(tier_name)) = 'premium' AND features = '{}'::jsonb
        """,
        (premium,),
    )


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
                features       JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT vendor_subscription_tiers_tier_name_nonempty
                    CHECK (length(trim(tier_name)) > 0)
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE public.vendor_subscription_tiers
                ADD COLUMN IF NOT EXISTS features JSONB NOT NULL DEFAULT '{}'::jsonb;
            """
        )
        cur.execute(
            """
            INSERT INTO public.vendor_subscription_tiers
                (tier_name, monthly_fee, quarterly_fee, is_active, features)
            VALUES
                ('Basic', 0, 0, FALSE, '{}'::jsonb),
                ('Premium', 0, 0, TRUE, '{}'::jsonb)
            ON CONFLICT (tier_name) DO NOTHING;
            """
        )
        _seed_default_tier_features_if_empty(cur)
        _normalize_vendor_catalog_single_active(cur)
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
                SELECT tier_name, monthly_fee, quarterly_fee, is_active, features, updated_at
                FROM public.vendor_subscription_tiers
                ORDER BY tier_name
                """
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_vendor_tier_features(tier_name: str) -> dict[str, Any]:
    """Merged entitlements for a vendor tier name (defaults to full access if tier row missing)."""
    name = str(tier_name or "").strip()
    if not name:
        return merge_vendor_tier_features(None)
    conn = _conn()
    try:
        ensure_public_vendor_subscription_tiers(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT features FROM public.vendor_subscription_tiers WHERE tier_name = %s
                """,
                (name,),
            )
            row = cur.fetchone()
        raw: dict[str, Any] | None = None
        if row:
            fv = row.get("features")
            if isinstance(fv, dict):
                raw = fv
            elif isinstance(fv, str) and fv.strip():
                try:
                    raw = json.loads(fv)
                except json.JSONDecodeError:
                    raw = {}
            elif fv is None:
                raw = {}
        return merge_vendor_tier_features(raw)
    finally:
        conn.close()


def upsert_vendor_tier(
    *,
    tier_name: str,
    monthly_fee: Decimal,
    quarterly_fee: Decimal,
    is_active: bool,
    features: dict[str, Any] | None = None,
) -> None:
    conn = _conn()
    try:
        ensure_public_vendor_subscription_tiers(conn)
        with conn.cursor() as cur:
            merged_f = merge_vendor_tier_features(features) if features is not None else None
            if merged_f is not None:
                cur.execute(
                    """
                    INSERT INTO public.vendor_subscription_tiers
                        (tier_name, monthly_fee, quarterly_fee, is_active, features, updated_at)
                    VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (tier_name) DO UPDATE SET
                        monthly_fee = EXCLUDED.monthly_fee,
                        quarterly_fee = EXCLUDED.quarterly_fee,
                        is_active = EXCLUDED.is_active,
                        features = EXCLUDED.features,
                        updated_at = NOW()
                    """,
                    (tier_name.strip(), monthly_fee, quarterly_fee, is_active, json.dumps(merged_f)),
                )
            else:
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
