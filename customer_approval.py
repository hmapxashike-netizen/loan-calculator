"""
Customer & Agent Approval Module: Handle Maker-Checker for name changes, etc.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from config import get_database_url


def _get_conn():
    return psycopg2.connect(get_database_url())

def save_approval_draft(
    entity_type: str,
    entity_id: int,
    action_type: str,
    old_details: dict[str, Any] | None,
    new_details: dict[str, Any],
    requested_by: str | None = None,
    supporting_document: str | None = None,
) -> int:
    """Save a draft for an entity (customer/agent) update needing approval."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO customer_approval_drafts 
                (entity_type, entity_id, action_type, old_details, new_details, requested_by, supporting_document)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    entity_type,
                    entity_id,
                    action_type,
                    json.dumps(old_details) if old_details else None,
                    json.dumps(new_details),
                    requested_by,
                    supporting_document
                )
            )
            draft_id = cur.fetchone()[0]
            conn.commit()
            return draft_id

def list_pending_drafts() -> list[dict]:
    """List all pending drafts for customers and agents."""
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM customer_approval_drafts WHERE status = 'PENDING' ORDER BY submitted_at DESC")
            return [dict(r) for r in cur.fetchall()]

def get_draft(draft_id: int) -> dict | None:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM customer_approval_drafts WHERE id = %s", (draft_id,))
            row = cur.fetchone()
            return dict(row) if row else None

def approve_draft(draft_id: int, approved_by: str | None = None) -> None:
    """Approve a draft. If it's a name change, update the main table and log history."""
    draft = get_draft(draft_id)
    if not draft:
        raise ValueError(f"Draft {draft_id} not found")
    if draft["status"] != "PENDING":
        raise ValueError(f"Draft {draft_id} is not PENDING")

    entity_type = draft["entity_type"]
    entity_id = draft["entity_id"]
    action_type = draft["action_type"]
    new_details = draft["new_details"]
    old_details = draft.get("old_details") or {}
    
    with _get_conn() as conn:
        with conn.cursor() as cur:
            if action_type == "NAME_CHANGE":
                old_name = old_details.get("name", "")
                new_name = new_details.get("name", "")
                
                # Update main table
                if entity_type == "customer":
                    # For customer, we have to find if it's individual or corporate
                    cur.execute("SELECT type FROM customers WHERE id = %s", (entity_id,))
                    c_type_row = cur.fetchone()
                    if not c_type_row:
                        raise ValueError("Customer not found")
                    c_type = c_type_row[0]
                    
                    if c_type == "individual":
                        cur.execute("UPDATE individuals SET name = %s WHERE customer_id = %s", (new_name, entity_id))
                    elif c_type == "corporate":
                        cur.execute("UPDATE corporates SET legal_name = %s WHERE customer_id = %s", (new_name, entity_id))
                elif entity_type == "agent":
                    cur.execute("UPDATE agents SET name = %s WHERE id = %s", (new_name, entity_id))
                else:
                    raise ValueError("Unknown entity_type")

                # Insert into history
                customer_id = entity_id if entity_type == "customer" else None
                agent_id = entity_id if entity_type == "agent" else None
                
                cur.execute(
                    """
                    INSERT INTO customer_name_history
                    (customer_id, agent_id, old_name, new_name, requested_by, approved_by, supporting_document)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        customer_id,
                        agent_id,
                        old_name,
                        new_name,
                        draft["requested_by"],
                        approved_by,
                        draft["supporting_document"]
                    )
                )

            # Mark draft as approved
            cur.execute(
                """
                UPDATE customer_approval_drafts 
                SET status = 'APPROVED', approved_by = %s, approved_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (approved_by, draft_id)
            )
            conn.commit()

def dismiss_draft(draft_id: int, note: str, dismissed_by: str | None = None) -> None:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE customer_approval_drafts 
                SET status = 'DISMISSED', dismissed_note = %s, dismissed_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (note, draft_id)
            )
            conn.commit()

def rework_draft(draft_id: int, note: str, reworked_by: str | None = None) -> None:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE customer_approval_drafts 
                SET status = 'REWORK', rework_note = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (note, draft_id)
            )
            conn.commit()

def init_approval_schema():
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS customer_approval_drafts (
                id SERIAL PRIMARY KEY,
                entity_type VARCHAR(50) NOT NULL,
                entity_id INTEGER NOT NULL,
                action_type VARCHAR(50) NOT NULL,
                old_details JSONB,
                new_details JSONB NOT NULL,
                requested_by VARCHAR(100),
                approved_by VARCHAR(100),
                supporting_document TEXT,
                status VARCHAR(50) DEFAULT 'PENDING',
                submitted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                approved_at TIMESTAMP WITH TIME ZONE,
                dismissed_note TEXT,
                dismissed_at TIMESTAMP WITH TIME ZONE,
                rework_note TEXT,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS customer_name_history (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
                agent_id INTEGER REFERENCES agents(id) ON DELETE CASCADE,
                old_name VARCHAR(255) NOT NULL,
                new_name VARCHAR(255) NOT NULL,
                requested_by VARCHAR(100),
                approved_by VARCHAR(100),
                supporting_document TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                CHECK (customer_id IS NOT NULL OR agent_id IS NOT NULL)
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_customer_name_history_customer_id ON customer_name_history(customer_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_customer_name_history_agent_id ON customer_name_history(agent_id);")
        conn.commit()

init_approval_schema()
