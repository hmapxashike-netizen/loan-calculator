"""
DB-backed loan grade scales: regulatory DPD bands vs standard (IFRS-facing) DPD bands.
"""
from __future__ import annotations

from typing import Any, Literal

from psycopg2.extras import RealDictCursor

from loan_management import _connection

ScaleKind = Literal["standard", "regulatory"]

_UNSET = object()


def _ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS loan_grade_scale_rules (
                id SERIAL PRIMARY KEY,
                sort_order INTEGER NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                grade_name VARCHAR(128) NOT NULL,
                performance_status VARCHAR(64) NOT NULL,
                regulatory_dpd_min INTEGER NOT NULL DEFAULT 0,
                regulatory_dpd_max INTEGER,
                standard_dpd_min INTEGER NOT NULL DEFAULT 0,
                standard_dpd_max INTEGER,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loan_grade_scale_rules_sort
            ON loan_grade_scale_rules (is_active, sort_order, id);
            """
        )
        cur.execute(
            """
            INSERT INTO loan_grade_scale_rules (
                sort_order, grade_name, performance_status,
                regulatory_dpd_min, regulatory_dpd_max, standard_dpd_min, standard_dpd_max
            )
            SELECT so, g, p, rmin, rmax, smin, smax
            FROM (
                VALUES
                    (10, 'Pass', 'Performing', 0, 0, 0, 0),
                    (20, 'Special Mention', 'Performing', 1, 30, 1, 90),
                    (30, 'Sub standard', 'NonPerforming', 31, 60, 91, 180),
                    (40, 'Doubtful', 'NonPerforming', 61, 90, 181, 360),
                    (50, 'Loss', 'NonPerforming', 91, NULL::int, 361, NULL::int)
            ) AS t (so, g, p, rmin, rmax, smin, smax)
            WHERE NOT EXISTS (SELECT 1 FROM loan_grade_scale_rules LIMIT 1);
            """
        )


def grade_scale_schema_ready() -> tuple[bool, str]:
    try:
        with _connection() as conn:
            _ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM loan_grade_scale_rules LIMIT 1")
        return True, ""
    except Exception as e:
        err = str(e).strip()
        if "does not exist" in err.lower() or "undefinedtable" in err.lower():
            return (
                False,
                "Table missing. From the project root run: `python scripts/run_migration_63.py`",
            )
        return False, err


def format_dpd_range(dpd_min: int, dpd_max: int | None) -> str:
    lo = int(dpd_min)
    if dpd_max is None:
        return f"{lo}+ dpd"
    hi = int(dpd_max)
    if lo == hi == 0:
        return "0 dpd"
    return f"{lo}-{hi} dpd"


def list_loan_grade_scale_rules(*, active_only: bool = False) -> list[dict[str, Any]]:
    with _connection() as conn:
        _ensure_table(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = """
                SELECT id, sort_order, is_active, grade_name, performance_status,
                       regulatory_dpd_min, regulatory_dpd_max, standard_dpd_min, standard_dpd_max,
                       created_at, updated_at
                FROM loan_grade_scale_rules
            """
            if active_only:
                q += " WHERE is_active = TRUE"
            q += " ORDER BY sort_order ASC, id ASC"
            cur.execute(q)
            return [dict(r) for r in cur.fetchall() or []]


def get_loan_grade_scale_rule(rule_id: int) -> dict[str, Any] | None:
    with _connection() as conn:
        _ensure_table(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, sort_order, is_active, grade_name, performance_status,
                       regulatory_dpd_min, regulatory_dpd_max, standard_dpd_min, standard_dpd_max,
                       created_at, updated_at
                FROM loan_grade_scale_rules WHERE id = %s
                """,
                (int(rule_id),),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def _dpd_matches_band(d: int, lo: int, hi: int | None) -> bool:
    if d < int(lo):
        return False
    if hi is not None and d > int(hi):
        return False
    return True


def resolve_loan_grade(dpd: int, *, scale: ScaleKind) -> dict[str, Any] | None:
    """
    First active rule in sort_order whose regulatory_* or standard_* band contains dpd.
    Returns dict with id, grade_name, performance_status, and scale-specific range fields.
    """
    rules = list_loan_grade_scale_rules(active_only=True)
    d = int(dpd)
    for r in rules:
        if scale == "regulatory":
            lo = int(r.get("regulatory_dpd_min") or 0)
            hi = r.get("regulatory_dpd_max")
            hi_i = int(hi) if hi is not None else None
        else:
            lo = int(r.get("standard_dpd_min") or 0)
            hi = r.get("standard_dpd_max")
            hi_i = int(hi) if hi is not None else None
        if _dpd_matches_band(d, lo, hi_i):
            out = dict(r)
            out["_scale"] = scale
            out["_dpd_range_label"] = format_dpd_range(lo, hi_i)
            return out
    return None


def insert_loan_grade_scale_rule(
    *,
    grade_name: str,
    performance_status: str,
    regulatory_dpd_min: int,
    regulatory_dpd_max: int | None,
    standard_dpd_min: int,
    standard_dpd_max: int | None,
    sort_order: int = 0,
    is_active: bool = True,
) -> int:
    gn = (grade_name or "").strip()
    ps = (performance_status or "").strip()
    if not gn:
        raise ValueError("Grade name is required.")
    if not ps:
        raise ValueError("Performance status is required.")
    with _connection() as conn:
        _ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_grade_scale_rules (
                    sort_order, is_active, grade_name, performance_status,
                    regulatory_dpd_min, regulatory_dpd_max, standard_dpd_min, standard_dpd_max
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(sort_order),
                    bool(is_active),
                    gn,
                    ps,
                    int(regulatory_dpd_min),
                    regulatory_dpd_max,
                    int(standard_dpd_min),
                    standard_dpd_max,
                ),
            )
            return int(cur.fetchone()[0])


def update_loan_grade_scale_rule(
    rule_id: int,
    *,
    grade_name: str | None = None,
    performance_status: str | None = None,
    regulatory_dpd_min: int | None = None,
    regulatory_dpd_max: int | None | object = _UNSET,
    standard_dpd_min: int | None = None,
    standard_dpd_max: int | None | object = _UNSET,
    sort_order: int | None = None,
    is_active: bool | None = None,
) -> None:
    fields: list[str] = []
    params: list[Any] = []
    if grade_name is not None:
        g = grade_name.strip()
        if not g:
            raise ValueError("Grade name cannot be empty.")
        fields.append("grade_name = %s")
        params.append(g)
    if performance_status is not None:
        p = performance_status.strip()
        if not p:
            raise ValueError("Performance status cannot be empty.")
        fields.append("performance_status = %s")
        params.append(p)
    if regulatory_dpd_min is not None:
        fields.append("regulatory_dpd_min = %s")
        params.append(int(regulatory_dpd_min))
    if regulatory_dpd_max is not _UNSET:
        fields.append("regulatory_dpd_max = %s")
        params.append(regulatory_dpd_max)
    if standard_dpd_min is not None:
        fields.append("standard_dpd_min = %s")
        params.append(int(standard_dpd_min))
    if standard_dpd_max is not _UNSET:
        fields.append("standard_dpd_max = %s")
        params.append(standard_dpd_max)
    if sort_order is not None:
        fields.append("sort_order = %s")
        params.append(int(sort_order))
    if is_active is not None:
        fields.append("is_active = %s")
        params.append(bool(is_active))
    if not fields:
        return
    fields.append("updated_at = NOW()")
    params.append(int(rule_id))
    with _connection() as conn:
        _ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE loan_grade_scale_rules SET {', '.join(fields)} WHERE id = %s",
                params,
            )
            if cur.rowcount == 0:
                raise ValueError("Rule not found.")


def delete_loan_grade_scale_rule_hard(rule_id: int) -> None:
    with _connection() as conn:
        _ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM loan_grade_scale_rules WHERE id = %s", (int(rule_id),))
            if cur.rowcount == 0:
                raise ValueError("Rule not found.")
