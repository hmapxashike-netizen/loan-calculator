"""Source-cash GL validation, payload merge, and accounting post_event wiring."""

from __future__ import annotations

import uuid
from typing import Any


def _parse_optional_uuid_str(val: Any) -> str | None:
    """Return canonical UUID string or None; raises ValueError if non-empty but invalid."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return str(uuid.UUID(s))
    except ValueError as e:
        raise ValueError(f"Invalid UUID for GL account reference: {val!r}") from e


SOURCE_CASH_ACCOUNT_CACHE_KEY = "source_cash_account_cache"
SOURCE_CASH_TREE_ROOT_CODE = "A100000"


def get_cached_source_cash_account_entries() -> list[dict]:
    """Snapshot rows ``{id, code, name}`` from system config; empty if cache never built."""
    from . import _legacy as _lm

    cfg = _lm.load_system_config_from_db() or {}
    block = cfg.get(SOURCE_CASH_ACCOUNT_CACHE_KEY) or {}
    entries = block.get("entries") or []
    if not isinstance(entries, list):
        return []
    return [dict(e) for e in entries]


def validate_source_cash_gl_account_id_for_new_posting(
    account_uuid: str | None,
    *,
    field_label: str = "Cash account",
) -> str:
    """
    Require a populated source-cash cache and an account id that appears in it.
    Returns canonical UUID string.
    """
    entries = get_cached_source_cash_account_entries()
    if not entries:
        raise ValueError(
            f"{field_label}: the source cash list has not been built. "
            "Go to **System configurations → Accounting configurations**, open "
            "**Maintenance — source cash account cache**, and rebuild it (administrators only)."
        )
    if account_uuid is None or str(account_uuid).strip() == "":
        raise ValueError(f"{field_label} is required.")
    canonical = _parse_optional_uuid_str(account_uuid)
    if canonical is None:
        raise ValueError(f"Invalid {field_label} UUID.")
    allowed = {str(e.get("id")) for e in entries if e.get("id")}
    if canonical not in allowed:
        raise ValueError(
            f"{field_label} is not in the allowed list (posting leaves under **{SOURCE_CASH_TREE_ROOT_CODE}** per branch). "
            "Pick an account from the dropdown or rebuild the cache after chart changes."
        )
    return canonical


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


def _post_event_for_loan(
    svc,
    loan_id: int | None,
    *,
    repayment_id: int | None = None,
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
    svc.post_event(**kw)
