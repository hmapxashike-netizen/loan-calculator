"""Loan approval draft queue and approve/terminate wiring."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from .db import Json, RealDictCursor, _connection
from .modification_gl import (
    post_modification_topup_disbursement,
    post_principal_writeoff_for_loan,
)
from .save_loan import save_loan
from .schema_ddl import _ensure_loan_approval_drafts_table
from .serialization import _json_safe


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
    schedule_df_secondary: pd.DataFrame | None = None,
) -> int:
    """Persist a loan draft for approval queue (no loan tables/GL posting)."""
    st_val = (status or "PENDING").strip().upper() or "PENDING"
    sec_rows = (
        _json_safe(schedule_df_secondary.to_dict(orient="records"))
        if schedule_df_secondary is not None
        else []
    )
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_approval_drafts (
                    customer_id, loan_type, product_code, details_json, schedule_json,
                    schedule_json_secondary,
                    assigned_approver_id, status, created_by, submitted_at, updated_at, loan_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
                RETURNING id
                """,
                (
                    int(customer_id),
                    str(loan_type),
                    product_code,
                    Json(_json_safe(details or {})),
                    Json(_json_safe(schedule_df.to_dict(orient="records") if schedule_df is not None else [])),
                    Json(sec_rows),
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
    schedule_df_secondary: pd.DataFrame | None = None,
) -> None:
    """Update a STAGED (incomplete capture) draft in place; no status change."""
    sec_rows = (
        _json_safe(schedule_df_secondary.to_dict(orient="records"))
        if schedule_df_secondary is not None
        else []
    )
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
                    schedule_json_secondary = %s,
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
                    Json(sec_rows),
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
    schedule_df_secondary: pd.DataFrame | None = None,
) -> int:
    """Update an existing draft and place it back in PENDING for approval."""
    sec_rows = (
        _json_safe(schedule_df_secondary.to_dict(orient="records"))
        if schedule_df_secondary is not None
        else []
    )
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
                    schedule_json_secondary = %s,
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
                    Json(sec_rows),
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
    status: str | None = "PENDING",
    statuses: list[str] | None = None,
    search: str | None = None,
    assigned_approver_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """List loan approval drafts for inbox/review.

    If ``statuses`` is non-empty, filter with ``IN (...)`` (takes precedence over ``status``).
    Otherwise, if ``status`` is set, filter to that single status. If both are unset/empty,
    no status filter is applied.
    """
    with _connection() as conn:
        _ensure_loan_approval_drafts_table(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where = ["TRUE"]
            params: list[Any] = []
            if statuses:
                placeholders = ", ".join(["%s"] * len(statuses))
                where.append(f"d.status IN ({placeholders})")
                params.extend(statuses)
            elif status:
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
                "UPDATE journal_entries SET is_active = FALSE WHERE event_id LIKE 'EOM-%%-LOAN-' || %s || '-%%'",
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
    action = str(details.get("approval_action") or "").strip().upper()
    actor = approved_by or "approver"

    def _parse_rd(raw: object) -> date:
        if isinstance(raw, date):
            return raw
        if isinstance(raw, str) and raw.strip():
            return date.fromisoformat(raw.strip()[:10])
        raise ValueError("Modification draft missing valid restructure_date.")

    if action == "TERMINATE":
        existing_loan_id = draft.get("loan_id")
        if not existing_loan_id:
            raise ValueError("Termination draft missing loan_id.")
        terminate_loan(existing_loan_id, terminated_by=approved_by)
        loan_id = int(existing_loan_id)
    elif action == "LOAN_MODIFICATION":
        from loan_management import get_loan
        from reamortisation import apply_loan_modification_from_approval_schedule

        source_loan_id = int(draft.get("loan_id") or 0)
        if not source_loan_id:
            raise ValueError("Modification draft missing loan_id.")
        rd = _parse_rd(details.get("restructure_date"))
        wo = float(details.get("writeoff_amount") or 0)
        tu = float(details.get("topup_amount") or 0)
        suf = str(int(draft_id))
        if wo > 0:
            post_principal_writeoff_for_loan(
                source_loan_id, wo, entry_date=rd, created_by=actor, unique_suffix=suf
            )
        if tu > 0:
            post_modification_topup_disbursement(
                source_loan_id, tu, entry_date=rd, created_by=actor, unique_suffix=suf
            )
        schedule_df = pd.DataFrame(draft.get("schedule_json") or [])
        mod_det = dict(details.get("modification_loan_details") or {})
        src = get_loan(source_loan_id)
        if src and mod_det.get("cash_gl_account_id") in (None, "") and src.get("cash_gl_account_id"):
            mod_det["cash_gl_account_id"] = str(src.get("cash_gl_account_id"))
        apply_loan_modification_from_approval_schedule(
            source_loan_id,
            rd,
            schedule_df,
            mod_det,
            str(draft["loan_type"]),
            outstanding_interest_treatment=str(details.get("outstanding_interest_treatment") or "capitalise"),
            notes=str(details.get("modification_notes") or "") or None,
        )
        if tu > 0:
            from .loan_records import update_loan_restructure_flags

            update_loan_restructure_flags(
                int(source_loan_id),
                modification_topup_applied=True,
            )
        loan_id = int(source_loan_id)
    elif action == "LOAN_MODIFICATION_SPLIT":
        from loan_management import get_loan

        source_loan_id = int(draft.get("loan_id") or 0)
        if not source_loan_id:
            raise ValueError("Split modification draft missing loan_id.")
        rd = _parse_rd(details.get("restructure_date"))
        wo = float(details.get("writeoff_amount") or 0)
        tu = float(details.get("topup_amount") or 0)
        suf = str(int(draft_id))
        if wo > 0:
            post_principal_writeoff_for_loan(
                source_loan_id, wo, entry_date=rd, created_by=actor, unique_suffix=suf
            )
        if tu > 0:
            post_modification_topup_disbursement(
                source_loan_id, tu, entry_date=rd, created_by=actor, unique_suffix=suf
            )
        src = get_loan(source_loan_id)
        cash_gl = str(src.get("cash_gl_account_id") or "").strip() if src else ""
        terminate_loan(source_loan_id, terminated_by=approved_by)

        leg_details_list = details.get("split_loan_details_list")
        if not isinstance(leg_details_list, list) or len(leg_details_list) < 2:
            leg_details_list = [
                dict(details.get("split_loan_details_a") or {}),
                dict(details.get("split_loan_details_b") or {}),
            ]
        n_legs = int(details.get("split_leg_count") or len(leg_details_list))
        n_legs = max(2, min(n_legs, 4))
        leg_details_list = [dict(leg_details_list[i]) for i in range(min(len(leg_details_list), n_legs))]
        while len(leg_details_list) < n_legs:
            leg_details_list.append({})

        schedule_rows_list: list[list[Any]] = [
            list(draft.get("schedule_json") or []),
            list(draft.get("schedule_json_secondary") or []),
        ]
        extras = details.get("split_schedules_extra")
        if isinstance(extras, list):
            for block in extras:
                if isinstance(block, list):
                    schedule_rows_list.append(block)
                else:
                    schedule_rows_list.append([])
        while len(schedule_rows_list) < n_legs:
            schedule_rows_list.append([])

        pc_list = details.get("split_product_codes")
        if not isinstance(pc_list, list) or len(pc_list) < n_legs:
            pc_list = [draft.get("product_code")] + [details.get("split_product_code_b") or draft.get("product_code")]
            while len(pc_list) < n_legs:
                pc_list.append(pc_list[-1] if pc_list else draft.get("product_code"))
        pc_list = [str(pc_list[i] if i < len(pc_list) else draft.get("product_code")) for i in range(n_legs)]

        lt_list = details.get("split_loan_types")
        if not isinstance(lt_list, list) or len(lt_list) < n_legs:
            lt_base = str(draft["loan_type"])
            lt_b = str(details.get("split_loan_type_b") or lt_base)
            lt_list = [lt_base, lt_b] + [lt_b] * max(0, n_legs - 2)
        lt_list = [str(lt_list[i] if i < len(lt_list) else draft["loan_type"]) for i in range(n_legs)]

        _split_topup = tu > 0
        created_ids: list[int] = []
        for i in range(n_legs):
            d_i = leg_details_list[i]
            if cash_gl:
                d_i.setdefault("cash_gl_account_id", cash_gl)
            d_i["status"] = "active"
            df_i = pd.DataFrame(schedule_rows_list[i] if i < len(schedule_rows_list) else [])
            if df_i.empty:
                raise ValueError(
                    f"Split modification draft missing schedule rows for leg {chr(ord('A') + i)}."
                )
            new_id = save_loan(
                int(draft["customer_id"]),
                lt_list[i],
                d_i,
                df_i,
                product_code=pc_list[i],
                originated_from_split=True,
                modification_topup_applied=_split_topup,
            )
            created_ids.append(int(new_id))

        details["split_created_loan_ids"] = created_ids
        if len(created_ids) >= 2:
            details["split_created_loan_id_b"] = int(created_ids[1])
        loan_id = int(created_ids[0])
        with _connection() as conn:
            _ensure_loan_approval_drafts_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE loan_approval_drafts SET details_json = %s, updated_at = NOW() WHERE id = %s",
                    (Json(_json_safe(details)), int(draft_id)),
                )
    else:
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
