"""
DB-backed configuration for IFRS-style provisioning (security subtypes, PD bands).
"""

from __future__ import annotations

from typing import Any

from psycopg2.extras import RealDictCursor

from loan_management import _connection


def provision_schema_ready() -> tuple[bool, str]:
    """
    True when migration 53 has been applied (core provisioning tables exist).
    Uses the same relation resolution as list_security_subtypes / PD band reads — not
    information_schema alone — so we never report ready when SELECT would fail.
    """
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM provision_security_subtypes LIMIT 1")
                cur.execute("SELECT 1 FROM provision_pd_bands LIMIT 1")
        return True, ""
    except Exception as e:
        err = str(e).strip()
        if "does not exist" in err.lower() or "undefinedtable" in err.lower():
            return (
                False,
                "Provisioning tables are missing. From the project root run: `python scripts/run_migration_53.py`",
            )
        return False, err


def list_security_subtypes(*, active_only: bool = True) -> list[dict[str, Any]]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = """
                SELECT id, security_type, subtype_name, typical_haircut_pct, system_notes,
                       is_active, sort_order, created_at
                FROM provision_security_subtypes
            """
            if active_only:
                q += " WHERE is_active = TRUE"
            q += " ORDER BY sort_order, security_type, subtype_name"
            cur.execute(q)
            return [dict(r) for r in cur.fetchall() or []]


def get_security_subtype(subtype_id: int) -> dict[str, Any] | None:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, security_type, subtype_name, typical_haircut_pct, system_notes,
                       is_active, sort_order, created_at
                FROM provision_security_subtypes WHERE id = %s
                """,
                (int(subtype_id),),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def insert_security_subtype(
    security_type: str,
    subtype_name: str,
    typical_haircut_pct,
    *,
    system_notes: str | None = None,
    sort_order: int = 0,
) -> int:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO provision_security_subtypes
                    (security_type, subtype_name, typical_haircut_pct, system_notes, sort_order)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (security_type.strip(), subtype_name.strip(), typical_haircut_pct, system_notes, int(sort_order)),
            )
            return int(cur.fetchone()[0])


def update_security_subtype(
    subtype_id: int,
    *,
    security_type: str | None = None,
    subtype_name: str | None = None,
    typical_haircut_pct=None,
    system_notes: str | None = None,
    is_active: bool | None = None,
    sort_order: int | None = None,
) -> None:
    fields: list[str] = []
    params: list[Any] = []
    if security_type is not None:
        fields.append("security_type = %s")
        params.append(security_type.strip())
    if subtype_name is not None:
        fields.append("subtype_name = %s")
        params.append(subtype_name.strip())
    if typical_haircut_pct is not None:
        fields.append("typical_haircut_pct = %s")
        params.append(typical_haircut_pct)
    if system_notes is not None:
        fields.append("system_notes = %s")
        params.append(system_notes)
    if is_active is not None:
        fields.append("is_active = %s")
        params.append(is_active)
    if sort_order is not None:
        fields.append("sort_order = %s")
        params.append(int(sort_order))
    if not fields:
        return
    params.append(int(subtype_id))
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE provision_security_subtypes SET {', '.join(fields)} WHERE id = %s",
                tuple(params),
            )


def delete_security_subtype_hard(subtype_id: int) -> None:
    """Delete row; loans FK will SET NULL."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM provision_security_subtypes WHERE id = %s", (int(subtype_id),))


def list_pd_bands(*, active_only: bool = True) -> list[dict[str, Any]]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = """
                SELECT id, status_label, dpd_min, dpd_max, pd_rate_pct, is_active, sort_order
                FROM provision_pd_bands
            """
            if active_only:
                q += " WHERE is_active = TRUE"
            q += " ORDER BY sort_order, dpd_min"
            cur.execute(q)
            return [dict(r) for r in cur.fetchall() or []]


def update_pd_band(
    band_id: int,
    *,
    status_label: str | None = None,
    dpd_min: int | None = None,
    dpd_max: int | None = None,
    pd_rate_pct=None,
    is_active: bool | None = None,
    sort_order: int | None = None,
) -> None:
    fields: list[str] = []
    params: list[Any] = []
    if status_label is not None:
        fields.append("status_label = %s")
        params.append(status_label.strip())
    if dpd_min is not None:
        fields.append("dpd_min = %s")
        params.append(int(dpd_min))
    if dpd_max is not None:
        fields.append("dpd_max = %s")
        params.append(dpd_max)
    if pd_rate_pct is not None:
        fields.append("pd_rate_pct = %s")
        params.append(pd_rate_pct)
    if is_active is not None:
        fields.append("is_active = %s")
        params.append(is_active)
    if sort_order is not None:
        fields.append("sort_order = %s")
        params.append(int(sort_order))
    if not fields:
        return
    params.append(int(band_id))
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE provision_pd_bands SET {', '.join(fields)} WHERE id = %s",
                tuple(params),
            )


def insert_pd_band(
    status_label: str,
    dpd_min: int,
    dpd_max: int | None,
    pd_rate_pct,
    *,
    sort_order: int = 0,
) -> int:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO provision_pd_bands (status_label, dpd_min, dpd_max, pd_rate_pct, sort_order)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (status_label.strip(), int(dpd_min), dpd_max, pd_rate_pct, int(sort_order)),
            )
            return int(cur.fetchone()[0])


def delete_pd_band_hard(band_id: int) -> None:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM provision_pd_bands WHERE id = %s", (int(band_id),))

