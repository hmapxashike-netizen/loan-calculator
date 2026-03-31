"""
Customer and agent approval drafts (e.g. regulated name changes with supporting document).

Requires schema migration:
    python scripts/run_migration_61.py
"""

from __future__ import annotations

import json
from typing import Any

from psycopg2 import ProgrammingError
from psycopg2.extras import Json, RealDictCursor

from agents import get_agent, update_agent
from customers import _connection, get_customer, update_corporate, update_individual


def _row_details(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}


def save_approval_draft(
    *,
    entity_type: str,
    entity_id: int,
    action_type: str,
    old_details: dict[str, Any],
    new_details: dict[str, Any],
    requested_by: str,
    supporting_document: str | None = None,
) -> int:
    """Insert a PENDING draft. Returns new draft id."""
    et = (entity_type or "").strip().lower()
    if et not in {"customer", "agent"}:
        raise ValueError("entity_type must be 'customer' or 'agent'.")
    act = (action_type or "").strip().upper()
    if not act:
        raise ValueError("action_type is required.")
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO customer_agent_approval_drafts (
                    entity_type, entity_id, action_type, status,
                    old_details, new_details, supporting_document, requested_by
                )
                VALUES (%s, %s, %s, 'PENDING', %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    et,
                    int(entity_id),
                    act,
                    Json(old_details or {}),
                    Json(new_details or {}),
                    (supporting_document or "").strip() or None,
                    (requested_by or "").strip() or None,
                ),
            )
            rid = cur.fetchone()
            return int(rid[0])


def _fetch_draft_row(cur, draft_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT id, entity_type, entity_id, action_type, status,
               old_details, new_details, supporting_document, requested_by,
               reviewer_note, reviewed_by, submitted_at, reviewed_at
        FROM customer_agent_approval_drafts
        WHERE id = %s
        """,
        (int(draft_id),),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_draft(draft_id: int) -> dict[str, Any] | None:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, entity_type, entity_id, action_type, status,
                       old_details, new_details, supporting_document, requested_by,
                       reviewer_note, reviewed_by, submitted_at, reviewed_at
                FROM customer_agent_approval_drafts
                WHERE id = %s
                """,
                (int(draft_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            d["old_details"] = _row_details(d.get("old_details"))
            d["new_details"] = _row_details(d.get("new_details"))
            return d


def list_pending_drafts() -> list[dict[str, Any]]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT id, entity_type, entity_id, action_type, status,
                           old_details, new_details, supporting_document, requested_by,
                           submitted_at
                    FROM customer_agent_approval_drafts
                    WHERE status = 'PENDING'
                    ORDER BY submitted_at ASC, id ASC
                    """
                )
                rows = cur.fetchall() or []
            except ProgrammingError as e:
                raise RuntimeError(
                    "customer_agent_approval_drafts table missing. Run: python scripts/run_migration_61.py"
                ) from e
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["old_details"] = _row_details(d.get("old_details"))
        d["new_details"] = _row_details(d.get("new_details"))
        out.append(d)
    return out


def _apply_name_change(draft: dict[str, Any]) -> None:
    nd = draft.get("new_details") or {}
    name = (nd.get("name") or "").strip()
    if not name:
        raise ValueError("Approved draft has no new name in new_details.")
    et = (draft.get("entity_type") or "").strip().lower()
    eid = int(draft["entity_id"])
    action = (draft.get("action_type") or "").strip().upper()
    if action != "NAME_CHANGE":
        raise ValueError(f"Unsupported action_type for apply: {action!r}")

    if et == "agent":
        ag = get_agent(eid)
        if not ag:
            raise ValueError(f"Agent {eid} not found.")
        update_agent(eid, name=name)
        return

    if et == "customer":
        cust = get_customer(eid)
        if not cust:
            raise ValueError(f"Customer {eid} not found.")
        ctype = (cust.get("type") or "").strip().lower()
        if ctype == "individual":
            update_individual(eid, name=name)
            return
        if ctype == "corporate":
            update_corporate(eid, legal_name=name)
            return
        raise ValueError(f"Unknown customer type {ctype!r}.")

    raise ValueError(f"Unknown entity_type {et!r}.")


def approve_draft(draft_id: int, *, approved_by: str) -> None:
    actor = (approved_by or "").strip() or "system"
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            draft = _fetch_draft_row(cur, draft_id)
            if not draft:
                raise ValueError(f"Draft {draft_id} not found.")
            if (draft.get("status") or "").upper() != "PENDING":
                raise ValueError(f"Draft {draft_id} is not pending (status={draft.get('status')!r}).")
            draft["old_details"] = _row_details(draft.get("old_details"))
            draft["new_details"] = _row_details(draft.get("new_details"))
    _apply_name_change(draft)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE customer_agent_approval_drafts
                SET status = 'APPROVED',
                    reviewed_by = %s,
                    reviewer_note = NULL,
                    reviewed_at = NOW()
                WHERE id = %s AND status = 'PENDING'
                """,
                (actor, int(draft_id)),
            )
            if cur.rowcount == 0:
                raise ValueError(f"Draft {draft_id} could not be finalized (concurrent update?).")


def rework_draft(draft_id: int, note: str, *, reworked_by: str) -> None:
    actor = (reworked_by or "").strip() or "system"
    note = (note or "").strip()
    if not note:
        raise ValueError("Note is required for rework.")
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE customer_agent_approval_drafts
                SET status = 'REWORK',
                    reviewer_note = %s,
                    reviewed_by = %s,
                    reviewed_at = NOW()
                WHERE id = %s AND status = 'PENDING'
                """,
                (note, actor, int(draft_id)),
            )
            if cur.rowcount == 0:
                raise ValueError(f"Draft {draft_id} not found or not pending.")


def dismiss_draft(draft_id: int, note: str, *, dismissed_by: str) -> None:
    actor = (dismissed_by or "").strip() or "system"
    note = (note or "").strip()
    if not note:
        raise ValueError("Note is required for dismiss.")
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE customer_agent_approval_drafts
                SET status = 'DISMISSED',
                    reviewer_note = %s,
                    reviewed_by = %s,
                    reviewed_at = NOW()
                WHERE id = %s AND status = 'PENDING'
                """,
                (note, actor, int(draft_id)),
            )
            if cur.rowcount == 0:
                raise ValueError(f"Draft {draft_id} not found or not pending.")
