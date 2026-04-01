"""
Loan capture: validation and orchestration without Streamlit.

UI owns session state and rendering; this module owns deterministic rules and DB calls
for staged drafts and the approval queue.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from loan_management import (
    resubmit_loan_approval_draft,
    save_loan_approval_draft,
    update_loan_approval_draft_staged,
)


def validate_source_cash_gl_selection(
    cash_gl_account_id: str | None,
    cached_entries: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    """
    When the source-cash cache is non-empty, require a non-empty selection that appears in the cache.

    Returns (ok, error_message). error_message is None when ok is True.
    """
    if not cached_entries:
        return True, None
    if cash_gl_account_id is None or str(cash_gl_account_id).strip() == "":
        return (
            False,
            "Select **Source cash / bank GL** below. When the **source cash account cache** is configured, "
            "this operating account is required for disbursement and **LOAN_CAPTURE** on `cash_operating`. "
            "Rebuild the cache under **System configurations → Accounting configurations → "
            "Maintenance — source cash account cache** if the list is empty.",
        )
    allowed = {str(e.get("id")) for e in cached_entries if e.get("id")}
    cid_norm = str(cash_gl_account_id).strip()
    if cid_norm not in allowed:
        return (
            False,
            "The selected cash GL is not in the source cash cache. Rebuild the cache after chart changes, "
            "or pick an account from the dropdown.",
        )
    return True, None


def merge_details_with_stage1(details: dict[str, Any], stage1: dict[str, Any]) -> dict[str, Any]:
    """
    Merge persisted capture_loan_details with stage-1 session fields for draft save / approval queue.

    Preserves and shallow-merges metadata under ``metadata``.
    """
    out = dict(details or {})
    base_meta = dict(out.get("metadata") or {})
    return {**out, **dict(stage1 or {}), "metadata": base_meta}


def validate_staged_save_prerequisites(
    *,
    rework_source_draft_id: Any,
    customer_id: Any,
    loan_type: Any,
) -> str | None:
    """Return user-facing error message or None if staged save may proceed."""
    if rework_source_draft_id is not None:
        return (
            "This session is a **rework** from **Approve loans**. Finish edits and use **Send for approval** — "
            "staged save is not used for rework."
        )
    if not customer_id or not loan_type:
        return "Complete **Details** (customer and product) in capture first."
    return None


def validate_send_for_approval_prerequisites(
    details: dict[str, Any],
    df_schedule: Any,
    customer_id: Any,
    loan_type: Any,
) -> str | None:
    """Return user-facing error message or None if send-for-approval may proceed."""
    if not details or df_schedule is None or not customer_id or not loan_type:
        return "Draft is incomplete. Please rebuild schedule first."
    return None


@dataclass(frozen=True)
class StagedDraftSaveResult:
    ok: bool
    error: str | None = None
    new_stage1_draft_id: int | None = None
    flash_message: str | None = None


def persist_staged_capture_draft(
    *,
    customer_id: int,
    loan_type: str,
    product_code: str | None,
    details_to_save: dict[str, Any],
    schedule_df: pd.DataFrame | None,
    existing_stage1_draft_id: int | None,
) -> StagedDraftSaveResult:
    """
    Insert or update a STAGED approval draft (same behavior as prior capture UI).
    """
    df = schedule_df if schedule_df is not None else pd.DataFrame()
    try:
        if existing_stage1_draft_id is not None:
            sid = int(existing_stage1_draft_id)
            update_loan_approval_draft_staged(
                sid,
                int(customer_id),
                str(loan_type),
                details_to_save,
                df,
                product_code=product_code,
                assigned_approver_id=None,
            )
            return StagedDraftSaveResult(
                ok=True,
                flash_message=(
                    f"Updated staged draft #{sid} (key details + schedule). "
                    f"Resume from **Resume capture draft**."
                ),
            )
        new_sid = save_loan_approval_draft(
            int(customer_id),
            str(loan_type),
            details_to_save,
            df,
            product_code=product_code,
            assigned_approver_id=None,
            created_by="capture_ui",
            status="STAGED",
        )
        nid = int(new_sid)
        return StagedDraftSaveResult(
            ok=True,
            new_stage1_draft_id=nid,
            flash_message=(
                f"Saved draft **#{nid}** (key details + schedule). Resume from **Resume capture draft**."
            ),
        )
    except Exception as ex:
        return StagedDraftSaveResult(ok=False, error=str(ex))


def resolve_and_submit_approval_draft(
    *,
    customer_id: int,
    loan_type: str,
    product_code: str | None,
    details_to_queue: dict[str, Any],
    df_schedule: pd.DataFrame,
    source_rework_draft_id: int | None,
    stage1_draft_id: int | None,
) -> int:
    """
    Create or resubmit a draft into PENDING (same branching as prior capture UI).
    """
    cid = int(customer_id)
    lt = str(loan_type)
    if source_rework_draft_id is not None:
        return resubmit_loan_approval_draft(
            int(source_rework_draft_id),
            cid,
            lt,
            details_to_queue,
            df_schedule,
            product_code=product_code,
            assigned_approver_id=None,
            created_by="capture_ui",
        )
    if stage1_draft_id is not None:
        return resubmit_loan_approval_draft(
            int(stage1_draft_id),
            cid,
            lt,
            details_to_queue,
            df_schedule,
            product_code=product_code,
            assigned_approver_id=None,
            created_by="capture_ui",
        )
    return save_loan_approval_draft(
        cid,
        lt,
        details_to_queue,
        df_schedule,
        product_code=product_code,
        assigned_approver_id=None,
        created_by="capture_ui",
    )


def attach_loan_draft_documents_from_staging(
    draft_id: int,
    staged_loan_docs: list[dict[str, Any]],
    *,
    upload_document_fn: Callable[..., None] | None,
) -> tuple[int, list[str]]:
    """
    Upload staged files to the draft. Returns (success_count, error_messages for UI).
    """
    if not upload_document_fn or not staged_loan_docs:
        return 0, []
    errors: list[str] = []
    count = 0
    for row in staged_loan_docs:
        cat_id = row["category_id"]
        f = row["file"]
        notes = row.get("notes") or ""
        try:
            upload_document_fn(
                "loan_approval_draft",
                int(draft_id),
                int(cat_id),
                f.name,
                f.type,
                f.size,
                f.getvalue(),
                uploaded_by="System User",
                notes=notes,
            )
            count += 1
        except Exception as de:
            errors.append(f"Failed to attach {f.name}: {de}")
    return count, errors


def build_approval_flash_after_submit(
    *,
    had_source_rework: bool,
    draft_id: int,
    doc_count: int,
) -> str:
    if had_source_rework:
        return (
            f"Draft #{draft_id} re-submitted for approval. "
            f"Attached documents: {doc_count}."
        )
    return (
        f"Draft sent for approval. Draft ID: {draft_id}. "
        f"Attached documents: {doc_count}."
    )
