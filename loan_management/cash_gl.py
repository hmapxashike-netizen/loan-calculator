"""Source-cash GL validation, payload merge, and accounting post_event wiring."""

from __future__ import annotations

import uuid
from typing import Any

from .product_catalog import load_system_config_from_db


SOURCE_CASH_ACCOUNT_CACHE_KEY = "source_cash_account_cache"
SOURCE_CASH_TREE_ROOT_CODE = "A100000"


def get_cached_source_cash_account_entries(*, system_config: dict | None = None) -> list[dict]:
    """Snapshot rows ``{id, code, name}`` from system config; empty if cache never built."""
    cfg = system_config if system_config is not None else (load_system_config_from_db() or {})
    block = cfg.get(SOURCE_CASH_ACCOUNT_CACHE_KEY) or {}
    entries = block.get("entries") or []
    if not isinstance(entries, list):
        return []
    return [dict(e) for e in entries]


def validate_source_cash_gl_account_id_for_new_posting(
    account_uuid: str | None,
    *,
    field_label: str = "Cash account",
    system_config: dict | None = None,
) -> str:
    """
    Require a populated source-cash cache and a reference that appears in it.

    Accepts either the account **UUID** (as stored in the database) or the account **code**
    (e.g. from the chart or Excel), matched exactly against the cached entries.

    Returns canonical UUID string.
    """
    entries = get_cached_source_cash_account_entries(system_config=system_config)
    if not entries:
        raise ValueError(
            f"{field_label}: the source cash list has not been built. "
            "Go to **System configurations → Accounting configurations**, open "
            "**Maintenance — source cash account cache**, and rebuild it (administrators only)."
        )
    if account_uuid is None or str(account_uuid).strip() == "":
        raise ValueError(f"{field_label} is required.")
    raw = str(account_uuid).strip()
    allowed_ids = {str(e.get("id")) for e in entries if e.get("id")}

    parsed_uuid: str | None = None
    try:
        parsed_uuid = str(uuid.UUID(raw))
    except ValueError:
        pass

    if parsed_uuid is not None:
        if parsed_uuid in allowed_ids:
            return parsed_uuid
        raise ValueError(
            f"{field_label} is not in the allowed list (posting leaves under **{SOURCE_CASH_TREE_ROOT_CODE}** per branch). "
            "Pick an account from the dropdown or rebuild the cache after chart changes."
        )

    code_matches = [
        e for e in entries if str(e.get("code") or "").strip() == raw
    ]
    if len(code_matches) > 1:
        raise ValueError(
            f"{field_label}: ambiguous GL code {raw!r} ({len(code_matches)} cache entries). "
            "Fix duplicate codes under the source-cash tree or use the account UUID."
        )
    if len(code_matches) == 1:
        mid = code_matches[0].get("id")
        if mid:
            return str(uuid.UUID(str(mid)))

    raise ValueError(
        f"{field_label}: not a valid UUID or a known source-cash account **code** under **{SOURCE_CASH_TREE_ROOT_CODE}**. "
        "Use the exact code from the chart (or the UUID from the Teller dropdown), and rebuild the cache after chart changes."
    )


def _merge_cash_gl_into_payload(
    loan_id: int | None,
    repayment_id: int | None,
    payload: dict | None,
) -> dict:
    """
    If payload does not already set account_overrides['cash_operating'], fill from:
    1) loan_repayments.source_cash_gl_account_id when repayment_id is set, else
    2) loans.cash_gl_account_id for the loan.
    Explicit account_overrides from the caller always win.
    """
    payload = dict(payload or {})
    ao = dict(payload.get("account_overrides") or {})
    if "cash_operating" in ao:
        return payload
    if loan_id is None:
        return payload
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor as _RDC

        from config import get_database_url as _gdb
    except ImportError:
        return payload
    src: str | None = None
    conn = psycopg2.connect(_gdb())
    try:
        with conn.cursor(cursor_factory=_RDC) as cur:
            if repayment_id is not None:
                cur.execute(
                    """
                    SELECT source_cash_gl_account_id FROM loan_repayments WHERE id = %s
                    """,
                    (int(repayment_id),),
                )
                r = cur.fetchone()
                if r and r.get("source_cash_gl_account_id"):
                    src = str(r["source_cash_gl_account_id"])
            if src is None:
                cur.execute(
                    "SELECT cash_gl_account_id FROM loans WHERE id = %s",
                    (int(loan_id),),
                )
                lr = cur.fetchone()
                if lr and lr.get("cash_gl_account_id"):
                    src = str(lr["cash_gl_account_id"])
    except Exception:
        src = None
    finally:
        conn.close()
    if src:
        ao["cash_operating"] = src
        payload["account_overrides"] = ao
    return payload


def _merge_creditor_cash_gl_into_payload(
    creditor_drawdown_id: int | None,
    repayment_id: int | None,
    payload: dict | None,
) -> dict:
    """
    If payload does not already set account_overrides['cash_operating'], fill from:
    1) creditor_repayments.source_cash_gl_account_id when repayment_id is set, else
    2) creditor_drawdowns.cash_gl_account_id for the drawdown.
    """
    payload = dict(payload or {})
    ao = dict(payload.get("account_overrides") or {})
    if "cash_operating" in ao:
        return payload
    if creditor_drawdown_id is None:
        return payload
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor as _RDC

        from config import get_database_url as _gdb
    except ImportError:
        return payload
    src: str | None = None
    conn = psycopg2.connect(_gdb())
    try:
        with conn.cursor(cursor_factory=_RDC) as cur:
            if repayment_id is not None:
                cur.execute(
                    """
                    SELECT source_cash_gl_account_id FROM creditor_repayments WHERE id = %s
                    """,
                    (int(repayment_id),),
                )
                r = cur.fetchone()
                if r and r.get("source_cash_gl_account_id"):
                    src = str(r["source_cash_gl_account_id"])
            if src is None:
                cur.execute(
                    "SELECT cash_gl_account_id FROM creditor_drawdowns WHERE id = %s",
                    (int(creditor_drawdown_id),),
                )
                lr = cur.fetchone()
                if lr and lr.get("cash_gl_account_id"):
                    src = str(lr["cash_gl_account_id"])
    except Exception:
        src = None
    finally:
        conn.close()
    if src:
        ao["cash_operating"] = src
        payload["account_overrides"] = ao
    return payload


def _post_event_for_loan(
    svc,
    loan_id: int | None,
    *,
    repayment_id: int | None = None,
    posting_policy: str | None = None,
    **kwargs,
) -> None:
    """
    Post with loan_id / repayment_id so AccountingService.post_event applies the same
    cash GL merge as Teller (receipt) and loan capture (loan row).
    """
    kw = dict(kwargs)
    if loan_id is not None:
        kw["loan_id"] = int(loan_id)
    if repayment_id is not None:
        kw["repayment_id"] = int(repayment_id)
    if posting_policy is not None:
        kw["posting_policy"] = posting_policy
    svc.post_event(**kw)
