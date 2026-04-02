"""Optional allocation_audit_log writes (no-op if table missing)."""

from __future__ import annotations

from datetime import date
from typing import Any

from .db import Json, _connection


def _log_allocation_audit(
    event_type: str,
    loan_id: int,
    as_of_date: date,
    *,
    repayment_id: int | None = None,
    original_repayment_id: int | None = None,
    narration: str | None = None,
    details: dict | None = None,
    conn: Any = None,
) -> None:
    """Write to allocation_audit_log for reversal add-back and system reallocation. No-op if table missing."""
    try:
        def _do_insert(c: Any) -> None:
            with c.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO allocation_audit_log
                    (event_type, loan_id, as_of_date, repayment_id, original_repayment_id, narration, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event_type,
                        loan_id,
                        as_of_date,
                        repayment_id,
                        original_repayment_id,
                        narration,
                        Json(details) if details else None,
                    ),
                )

        if conn is not None:
            _do_insert(conn)
        else:
            with _connection() as c:
                _do_insert(c)
    except Exception:
        pass  # Table may not exist; do not fail the main operation
