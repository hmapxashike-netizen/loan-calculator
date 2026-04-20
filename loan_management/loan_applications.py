"""Loan applications (prospect pipeline): CRUD, reference numbers, supersede, soft-delete."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from .db import Json, RealDictCursor, _connection
from .schema_ddl import _ensure_loan_applications_schema

# --- Status constants (application lifecycle; drafts use loan_approval_drafts separately) ---

STATUS_PROSPECT = "PROSPECT"
STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_SENT_FOR_APPROVAL = "SENT_FOR_APPROVAL"
STATUS_BOOKED = "BOOKED"
STATUS_DECLINED = "DECLINED"
STATUS_WITHDRAWN = "WITHDRAWN"
STATUS_SUPERSEDED = "SUPERSEDED"

_TERMINAL_STATUSES = frozenset(
    {STATUS_BOOKED, STATUS_DECLINED, STATUS_WITHDRAWN, STATUS_SUPERSEDED}
)


def _is_terminal_status(code: object) -> bool:
    try:
        from loan_management.loan_pipeline_config import is_terminal_application_status

        return is_terminal_application_status(code)
    except ImportError:
        return str(code or "").strip().upper() in _TERMINAL_STATUSES


def agent_surname_prefix(agent_name: str | None, *, width: int = 3) -> str:
    """
    First `width` letters for reference prefix (A–Z), padded with X.
    Uses last whitespace-separated token as surname when multiple tokens exist.
    """
    raw = (agent_name or "").strip()
    if not raw:
        return "NON"[:width].ljust(width, "X")[:width]
    parts = raw.split()
    surname = parts[-1] if len(parts) >= 2 else parts[0]
    letters = re.sub(r"[^A-Za-z]", "", surname).upper()
    if not letters:
        return "NON"[:width].ljust(width, "X")[:width]
    pad = letters + ("X" * width)
    return pad[:width]


def _allocate_reference_number(conn, agent_id: int | None) -> str:
    """Next reference: {prefix}{8-digit-seq} using loan_application_ref_sequences."""
    from agents import get_agent

    if agent_id is None:
        prefix = "NON"
    else:
        ag = get_agent(int(agent_id))
        prefix = agent_surname_prefix((ag or {}).get("name"), width=3)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO loan_application_ref_sequences (prefix, next_num)
            VALUES (%s, 1)
            ON CONFLICT (prefix) DO UPDATE
            SET next_num = loan_application_ref_sequences.next_num + 1
            RETURNING next_num
            """,
            (prefix,),
        )
        row = cur.fetchone()
        seq = int(row[0]) if row else 1
        return f"{prefix}{seq:08d}"


def create_loan_application(
    *,
    customer_id: int | None = None,
    agent_id: int | None = None,
    national_id: str | None = None,
    requested_principal: Decimal | float | None = None,
    product_code: str | None = None,
    metadata: dict[str, Any] | None = None,
    status: str = STATUS_PROSPECT,
    created_by: str | None = None,
    reference_number: str | None = None,
) -> int:
    """Insert a new application row. Allocates ``reference_number`` when not provided."""
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        if reference_number is None:
            reference_number = _allocate_reference_number(conn, agent_id)
        rp = as_10dp(requested_principal) if requested_principal is not None else None
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_applications (
                    reference_number, customer_id, agent_id, national_id, requested_principal,
                    product_code, status, metadata, created_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    reference_number,
                    customer_id,
                    agent_id,
                    national_id.strip() if national_id else None,
                    float(rp) if rp is not None else None,
                    (product_code or "").strip() or None,
                    (status or STATUS_PROSPECT).strip().upper(),
                    Json(metadata or {}),
                    created_by,
                ),
            )
            return int(cur.fetchone()[0])


def get_loan_application(application_id: int) -> dict | None:
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM loan_applications WHERE id = %s AND deleted_at IS NULL",
                (int(application_id),),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def list_loan_applications(
    *,
    status: str | None = None,
    agent_id: int | None = None,
    customer_id: int | None = None,
    include_superseded: bool = False,
    limit: int = 500,
) -> list[dict]:
    clauses: list[str] = ["deleted_at IS NULL"]
    params: list[Any] = []
    if not include_superseded:
        clauses.append("superseded_by_id IS NULL")
    if status:
        clauses.append("status = %s")
        params.append(status.strip().upper())
    if agent_id is not None:
        clauses.append("agent_id = %s")
        params.append(int(agent_id))
    if customer_id is not None:
        clauses.append("customer_id = %s")
        params.append(int(customer_id))
    where = " AND ".join(clauses)
    params.append(min(max(limit, 1), 5000))
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT * FROM loan_applications
                WHERE {where}
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            return [dict(r) for r in cur.fetchall()]


def update_loan_application(
    application_id: int,
    *,
    customer_id: int | None = None,
    agent_id: int | None = None,
    national_id: str | None = None,
    requested_principal: Decimal | float | None = None,
    product_code: str | None = None,
    metadata: dict[str, Any] | None = None,
    status: str | None = None,
) -> None:
    """Patch fields on a non-terminal, non-deleted application."""
    app = get_loan_application(application_id)
    if not app:
        raise ValueError(f"loan_applications id={application_id} not found or deleted.")
    if app.get("superseded_by_id"):
        raise ValueError("Cannot update superseded application.")
    if _is_terminal_status(app.get("status")):
        raise ValueError("Cannot update terminal application.")

    sets: list[str] = ["updated_at = NOW()"]
    params: list[Any] = []
    if customer_id is not None:
        sets.append("customer_id = %s")
        params.append(customer_id)
    if agent_id is not None:
        sets.append("agent_id = %s")
        params.append(agent_id)
    if national_id is not None:
        sets.append("national_id = %s")
        params.append(national_id.strip() or None)
    if requested_principal is not None:
        sets.append("requested_principal = %s")
        params.append(float(as_10dp(requested_principal)))
    if product_code is not None:
        sets.append("product_code = %s")
        params.append(product_code.strip() or None)
    if metadata is not None:
        sets.append("metadata = %s")
        params.append(Json(metadata))
    if status is not None:
        sets.append("status = %s")
        params.append(status.strip().upper())

    if len(sets) <= 1:
        return
    params.append(int(application_id))
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE loan_applications SET {', '.join(sets)} WHERE id = %s AND deleted_at IS NULL",
                tuple(params),
            )


def supersede_loan_application(
    old_application_id: int,
    *,
    created_by: str | None = None,
) -> int:
    """Create a new application row; mark old as SUPERSEDED with supersede chain."""
    old = get_loan_application(old_application_id)
    if not old:
        raise ValueError(f"loan_applications id={old_application_id} not found.")
    if old.get("loan_id"):
        raise ValueError("Cannot supersede a booked application.")
    if (old.get("status") or "").upper() == STATUS_SUPERSEDED:
        raise ValueError("Application already superseded.")

    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        ref = _allocate_reference_number(conn, old.get("agent_id"))
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_applications (
                    reference_number, customer_id, agent_id, national_id, requested_principal,
                    product_code, status, metadata, created_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    ref,
                    old.get("customer_id"),
                    old.get("agent_id"),
                    old.get("national_id"),
                    old.get("requested_principal"),
                    old.get("product_code"),
                    STATUS_PROSPECT,
                    Json(old.get("metadata") or {}),
                    created_by,
                ),
            )
            new_id = int(cur.fetchone()[0])
            cur.execute(
                """
                UPDATE loan_applications
                SET superseded_at = NOW(),
                    superseded_by_id = %s,
                    status = %s,
                    updated_at = NOW()
                WHERE id = %s AND deleted_at IS NULL
                """,
                (new_id, STATUS_SUPERSEDED, int(old_application_id)),
            )
    return new_id


def soft_delete_loan_application(application_id: int, *, deleted_by: str | None = None) -> None:
    """Soft-delete. Refuses booked applications."""
    app = get_loan_application(application_id)
    if not app:
        raise ValueError(f"loan_applications id={application_id} not found.")
    if app.get("loan_id"):
        raise ValueError("Cannot soft-delete a booked application (loan exists).")

    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_applications
                SET deleted_at = NOW(), deleted_by = %s, updated_at = NOW()
                WHERE id = %s AND deleted_at IS NULL
                """,
                (deleted_by, int(application_id)),
            )


def set_loan_approval_draft_application_id(draft_id: int, application_id: int) -> None:
    """Attach pipeline application to an approval draft (additive; does not change draft save APIs)."""
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_approval_drafts
                SET application_id = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (int(application_id), int(draft_id)),
            )


def update_application_status(application_id: int, status: str) -> None:
    app = get_loan_application(application_id)
    if not app:
        raise ValueError(f"loan_applications id={application_id} not found or deleted.")
    if _is_terminal_status(app.get("status")):
        raise ValueError("Cannot change status: application is already in a terminal state.")
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_applications
                SET status = %s, updated_at = NOW()
                WHERE id = %s AND deleted_at IS NULL
                """,
                (status.strip().upper(), int(application_id)),
            )
