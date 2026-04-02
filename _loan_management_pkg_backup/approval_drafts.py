"""Loan approval draft queue: staged capture, pending review, approve/dismiss."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .db import Json, RealDictCursor, _connection
from .serialization import _json_safe


def _ensure_loan_approval_drafts_table(conn: Any) -> None:
    """Create approval draft queue table if it does not yet exist."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS loan_approval_drafts (
                id BIGSERIAL PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                loan_type VARCHAR(64) NOT NULL,
                product_code VARCHAR(64),
                details_json JSONB NOT NULL,
                schedule_json JSONB NOT NULL,
                assigned_approver_id VARCHAR(128),
                status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                created_by VARCHAR(128),
                approved_by VARCHAR(128),
                rework_note TEXT,
                dismissed_note TEXT,
                loan_id INTEGER,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                approved_at TIMESTAMPTZ,
                dismissed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_loan_approval_drafts_status ON loan_approval_drafts(status)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_loan_approval_drafts_assignee ON loan_approval_drafts(assigned_approver_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS ix_loan_approval_drafts_customer ON loan_approval_drafts(customer_id)"
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'loan_approval_drafts'
                      AND column_name = 'assigned_approver_id'
                      AND data_type IN ('integer', 'bigint', 'smallint')
                ) THEN
                    ALTER TABLE loan_approval_drafts
                    ALTER COLUMN assigned_approver_id TYPE VARCHAR(128)
                    USING assigned_approver_id::text;
                END IF;
            END $$;
            """
        )


def save_loan_approval_draft(
    customer_id: int,
    loan_type: str,
    details: dict[str, Any],
    schedule_df: pd.DataFrame | None,
    *,
    product_code: str | None = None,
    assigned_approver_id: str | None = None,
    created_by: str | None = None,
    status: str = "PENDING",
    loan_id: int | None = None,
) -> int:
    """Persist a loan draft for approval queue (no loan tables/GL posting)."""
    st_val = (status or "PENDING").strip().upper() or "PENDING"
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_approval_drafts (
                    customer_id, loan_type, product_code, details_json, schedule_json,
                    assigned_approver_id, status, created_by, submitted_at, updated_at, loan_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
                RETURNING id
                """,
                (
                    int(customer_id),
                    str(loan_type),
                    product_code,
                    Json(_json_safe(details or {})),
                    Json(
                        _json_safe(
                            schedule_df.to_dict(orient="records")
                            if schedule_df is not None
                            else []
                        )
                    ),
                    str(assigned_approver_id) if assigned_approver_id is not None else None,
                    st_val,
                    created_by,
                    int(loan_id) if loan_id is not None else None,
                ),
            )
            return int(cur.fetchone()[0])


def update_loan_approval_draft_staged(
    draft_id: int,
    customer_id: int,
    loan_type: str,
    details: dict[str, Any],
    schedule_df: pd.DataFrame,
    *,
    product_code: str | None = None,
    assigned_approver_id: str | None = None,
) -> None:
    """Update a STAGED (incomplete capture) draft in place; no status change."""
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_approval_drafts
                SET customer_id = %s,
                    loan_type = %s,
                    product_code = %s,
                    details_json = %s,
                    schedule_json = %s,
                    assigned_approver_id = %s,
                    updated_at = NOW()
                WHERE id = %s AND UPPER(status) = 'STAGED'
                """,
                (
                    int(customer_id),
                    str(loan_type),
                    product_code,
                    Json(_json_safe(details or {})),
                    Json(_json_safe(schedule_df.to_dict(orient="records"))),
                    str(assigned_approver_id) if assigned_approver_id is not None else None,
                    int(draft_id),
                ),
            )
            if cur.rowcount == 0:
                raise ValueError(f"Draft #{draft_id} not found or is not STAGED (cannot update).")


def resubmit_loan_approval_draft(
    draft_id: int,
    customer_id: int,
    loan_type: str,
    details: dict[str, Any],
    schedule_df: pd.DataFrame,
    *,
    product_code: str | None = None,
    assigned_approver_id: str | None = None,
    created_by: str | None = None,
) -> int:
    """Update an existing draft and place it back in PENDING for approval."""
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_approval_drafts
                SET customer_id = %s,
                    loan_type = %s,
                    product_code = %s,
                    details_json = %s,
                    schedule_json = %s,
                    assigned_approver_id = %s,
                    status = 'PENDING',
                    created_by = %s,
                    rework_note = NULL,
                    dismissed_note = NULL,
                    dismissed_at = NULL,
                    submitted_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id
                """,
                (
                    int(customer_id),
                    str(loan_type),
                    product_code,
                    Json(_json_safe(details or {})),
                    Json(_json_safe(schedule_df.to_dict(orient="records"))),
                    str(assigned_approver_id) if assigned_approver_id is not None else None,
                    created_by,
                    int(draft_id),
                ),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Draft #{draft_id} was not found for resubmission.")
            return int(row[0])


def list_loan_approval_drafts(
    *,
    status: str = "PENDING",
    search: str | None = None,
    assigned_approver_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """List loan approval drafts for inbox/review."""
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where = ["TRUE"]
            params: list[Any] = []
            if status:
                where.append("d.status = %s")
                params.append(status)
            if assigned_approver_id is not None:
                where.append("d.assigned_approver_id = %s")
                params.append(str(assigned_approver_id))
            if search:
                where.append(
                    "("
                    "CAST(d.id AS TEXT) ILIKE %s OR "
                    "CAST(d.customer_id AS TEXT) ILIKE %s OR "
                    "COALESCE(d.product_code, '') ILIKE %s OR "
                    "COALESCE(d.loan_type, '') ILIKE %s"
                    ")"
                )
                like = f"%{search.strip()}%"
                params.extend([like, like, like, like])
            params.append(int(limit))
            cur.execute(
                f"""
                SELECT
                    d.id, d.customer_id, d.loan_type, d.product_code, d.assigned_approver_id,
                    d.status, d.created_by, d.submitted_at, d.approved_at, d.dismissed_at, d.loan_id
                FROM loan_approval_drafts d
                WHERE {' AND '.join(where)}
                ORDER BY d.submitted_at DESC, d.id DESC
                LIMIT %s
                """,
                tuple(params),
            )
            return list(cur.fetchall() or [])


def get_loan_approval_draft(draft_id: int) -> dict[str, Any] | None:
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM loan_approval_drafts
                WHERE id = %s
                """,
                (int(draft_id),),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def terminate_loan(loan_id: int, terminated_by: str | None = None) -> None:
    """Soft-deletes a loan and inactivates its related GL journals."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE loans SET status = 'terminated', updated_at = NOW() WHERE id = %s",
                (loan_id,),
            )

            cur.execute("SELECT id FROM loan_repayments WHERE loan_id = %s", (loan_id,))
            rep_ids = [r[0] for r in cur.fetchall()]

            cur.execute(
                "UPDATE journal_entries SET is_active = FALSE WHERE event_id = %s AND event_type = 'LOAN_APPROVAL'",
                (str(loan_id),),
            )

            cur.execute(
                "UPDATE journal_entries SET is_active = FALSE WHERE event_id LIKE 'EOD-%%-' || %s || '-%%'",
                (str(loan_id),),
            )
            cur.execute(
                "UPDATE journal_entries SET is_active = FALSE WHERE event_id LIKE 'EOM%%-' || %s || '-%%'",
                (str(loan_id),),
            )

            for rid in rep_ids:
                s_rid = str(rid)
                cur.execute(
                    """
                    UPDATE journal_entries
                    SET is_active = FALSE
                    WHERE event_id LIKE 'REPAY-' || %s || '-%%'
                       OR event_id LIKE 'REV-REPAY-' || %s || '-%%'
                       OR event_id = 'OP-' || %s
                       OR event_id LIKE 'LIQ-' || %s || '-%%'
                       OR event_id LIKE 'REV-LIQ-' || %s || '-%%'
                       OR event_id LIKE 'REV-RCPT-' || %s || '-%%'
                    """,
                    (s_rid, s_rid, s_rid, s_rid, s_rid, s_rid),
                )


def approve_loan_approval_draft(
    draft_id: int,
    *,
    approved_by: str | None = None,
) -> int:
    """Approve a pending draft and create the actual loan + schedule + GL."""
    draft = get_loan_approval_draft(draft_id)
    if not draft:
        raise ValueError(f"Draft #{draft_id} was not found.")
    if str(draft.get("status") or "").upper() != "PENDING":
        raise ValueError(f"Draft #{draft_id} is not pending (status={draft.get('status')}).")

    details = dict(draft.get("details_json") or {})

    if details.get("approval_action") == "TERMINATE":
        existing_loan_id = draft.get("loan_id")
        if not existing_loan_id:
            raise ValueError("Termination draft missing loan_id.")
        terminate_loan(existing_loan_id, terminated_by=approved_by)
        loan_id = existing_loan_id
    else:
        from . import _legacy

        save_loan = _legacy.save_loan
        details["status"] = "active"
        schedule_rows = draft.get("schedule_json") or []
        schedule_df = pd.DataFrame(schedule_rows)
        loan_id = save_loan(
            int(draft["customer_id"]),
            str(draft["loan_type"]),
            details,
            schedule_df,
            product_code=draft.get("product_code"),
        )

    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_approval_drafts
                SET status = 'APPROVED',
                    approved_at = NOW(),
                    approved_by = %s,
                    loan_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (approved_by, int(loan_id), int(draft_id)),
            )
    return int(loan_id)


def send_back_loan_approval_draft(
    draft_id: int,
    *,
    note: str | None = None,
    actor: str | None = None,
) -> None:
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_approval_drafts
                SET status = 'REWORK',
                    rework_note = %s,
                    approved_by = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (note, actor, int(draft_id)),
            )


def dismiss_loan_approval_draft(
    draft_id: int,
    *,
    note: str | None = None,
    actor: str | None = None,
) -> None:
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE loan_approval_drafts
                SET status = 'DISMISSED',
                    dismissed_note = %s,
                    approved_by = %s,
                    dismissed_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (note, actor, int(draft_id)),
            )
