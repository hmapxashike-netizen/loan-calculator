"""Configurable loan purposes (capture dropdown and system config UI)."""

from __future__ import annotations

from typing import Any

from .db import _connection, psycopg2
from .schema_ddl import _ensure_loan_purposes_schema


def _fetch_loan_purposes_rows(conn: Any, *, active_only: bool) -> list[dict]:
    """Fetch loan_purposes as plain dicts (keys match SELECT aliases)."""
    where = " WHERE is_active = TRUE" if active_only else ""
    sql = f"""
        SELECT id, name, sort_order, is_active, created_at, updated_at
        FROM loan_purposes
        {where}
        ORDER BY sort_order ASC, id ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in (cur.description or [])]
        out: list[dict] = []
        for row in cur.fetchall() or []:
            out.append({str(cols[i]).lower(): row[i] for i in range(len(cols))})
        return out


def list_loan_purposes(*, active_only: bool = True) -> list[dict]:
    """Each dict: id, name, sort_order, is_active, created_at, updated_at."""
    if psycopg2 is None:
        return []
    with _connection() as conn:
        _ensure_loan_purposes_schema(conn)
        return _fetch_loan_purposes_rows(conn, active_only=active_only)


def count_loan_purposes_rows() -> int:
    """Row count in loan_purposes (for diagnostics vs list_loan_purposes)."""
    if psycopg2 is None:
        return 0
    with _connection() as conn:
        _ensure_loan_purposes_schema(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM loan_purposes")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0


def clear_all_loan_purposes() -> tuple[int, int]:
    """
    DELETE all rows from loan_purposes; set loans.loan_purpose_id to NULL first.
    Returns (loans_updated, purposes_deleted).
    """
    with _connection() as conn:
        _ensure_loan_purposes_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE loans SET loan_purpose_id = NULL WHERE loan_purpose_id IS NOT NULL"
            )
            loans_n = cur.rowcount
            cur.execute("DELETE FROM loan_purposes")
            pur_n = cur.rowcount
    return int(loans_n), int(pur_n)


def get_loan_purpose_by_id(purpose_id: int) -> dict | None:
    if psycopg2 is None:
        return None
    try:
        with _connection() as conn:
            _ensure_loan_purposes_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, sort_order, is_active, created_at, updated_at
                    FROM loan_purposes WHERE id = %s
                    """,
                    (int(purpose_id),),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in (cur.description or [])]
                return {str(cols[i]).lower(): row[i] for i in range(len(cols))}
    except Exception:
        return None


def create_loan_purpose(name: str, sort_order: int = 0) -> int:
    nm = (name or "").strip()
    if not nm:
        raise ValueError("Loan purpose name is required.")
    try:
        so = int(sort_order)
    except (TypeError, ValueError):
        so = 0
    with _connection() as conn:
        _ensure_loan_purposes_schema(conn)
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO loan_purposes (name, sort_order, is_active)
                    VALUES (%s, %s, TRUE)
                    RETURNING id
                    """,
                    (nm, so),
                )
                return int(cur.fetchone()[0])
            except Exception as e:
                err = str(e).lower()
                if "unique" in err or "duplicate" in err:
                    raise ValueError(
                        "A loan purpose with this name already exists (names are unique, case-insensitive)."
                    ) from e
                raise


def set_loan_purpose_active(purpose_id: int, is_active: bool) -> None:
    with _connection() as conn:
        _ensure_loan_purposes_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_purposes
                SET is_active = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (bool(is_active), int(purpose_id)),
            )
            if cur.rowcount == 0:
                raise ValueError("Loan purpose not found.")


def update_loan_purpose(
    purpose_id: int,
    *,
    name: str | None = None,
    sort_order: int | None = None,
) -> None:
    updates: list[str] = []
    args: list[Any] = []
    if name is not None:
        nm = name.strip()
        if not nm:
            raise ValueError("Loan purpose name cannot be empty.")
        updates.append("name = %s")
        args.append(nm)
    if sort_order is not None:
        updates.append("sort_order = %s")
        args.append(int(sort_order))
    if not updates:
        return
    args.append(int(purpose_id))
    with _connection() as conn:
        _ensure_loan_purposes_schema(conn)
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    UPDATE loan_purposes
                    SET {", ".join(updates)}, updated_at = NOW()
                    WHERE id = %s
                    """,
                    args,
                )
            except Exception as e:
                err = str(e).lower()
                if "unique" in err or "duplicate" in err:
                    raise ValueError(
                        "A loan purpose with this name already exists (names are unique, case-insensitive)."
                    ) from e
                raise
            if cur.rowcount == 0:
                raise ValueError("Loan purpose not found.")


def ensure_loan_purpose_rows(
    definitions: list[tuple[str, int]],
) -> tuple[int, int]:
    """
    Insert each (name, sort_order) when no row matches the name case-insensitively.
    Idempotent: safe to re-run after migration 62 or manual inserts.

    Returns (inserted_count, skipped_count).
    """
    inserted = 0
    skipped = 0
    with _connection() as conn:
        _ensure_loan_purposes_schema(conn)
        with conn.cursor() as cur:
            for name, sort_order in definitions:
                nm = (name or "").strip()
                if not nm:
                    continue
                try:
                    so = int(sort_order)
                except (TypeError, ValueError):
                    so = 0
                cur.execute(
                    """
                    SELECT 1 FROM loan_purposes
                    WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))
                    LIMIT 1
                    """,
                    (nm,),
                )
                if cur.fetchone():
                    skipped += 1
                    continue
                cur.execute(
                    """
                    INSERT INTO loan_purposes (name, sort_order, is_active)
                    VALUES (%s, %s, TRUE)
                    """,
                    (nm, so),
                )
                inserted += 1
    return inserted, skipped
