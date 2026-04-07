"""Loan schedule versions and ``schedule_lines`` persistence."""

from __future__ import annotations

from datetime import date, datetime
from typing import Sequence

import pandas as pd

from decimal_utils import as_10dp

from .db import RealDictCursor, _connection

_SCHEDULE_DATE_STORAGE_FMT = "%d-%b-%Y"


def get_latest_schedule_version(loan_id: int) -> int:
    """Return the latest schedule version number for a loan (1 = original)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(version), 1) FROM loan_schedules WHERE loan_id = %s",
                (loan_id,),
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] else 1


def get_schedule_lines(loan_id: int, schedule_version: int | None = None) -> list[dict]:
    """Fetch schedule lines for a loan. If schedule_version is None, use latest."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if schedule_version is None:
                cur.execute(
                    "SELECT id FROM loan_schedules WHERE loan_id = %s ORDER BY version DESC LIMIT 1",
                    (loan_id,),
                )
                row = cur.fetchone()
                if not row:
                    return []
                cur.execute(
                    'SELECT * FROM schedule_lines WHERE loan_schedule_id = %s ORDER BY "Period"',
                    (row["id"],),
                )
                return [dict(r) for r in cur.fetchall()]
            cur.execute(
                """
                SELECT sl.* FROM schedule_lines sl
                JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
                WHERE ls.loan_id = %s AND ls.version = %s
                ORDER BY sl."Period"
                """,
                (loan_id, schedule_version),
            )
            return [dict(r) for r in cur.fetchall()]


def _parse_line_date(raw: object) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw
    if hasattr(raw, "date"):
        return raw.date() if callable(getattr(raw, "date", None)) else None  # type: ignore[union-attr]
    if isinstance(raw, str):
        s = raw[:32].strip()
        for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
            try:
                chunk = s if fmt == "%d-%b-%Y" else s[:10]
                return datetime.strptime(chunk, fmt).date()
            except ValueError:
                continue
        return None
    return None


def parse_schedule_line_date(raw: object) -> date | None:
    """Parse a schedule line ``Date`` from DB, UI, or export (dd-Mon-yyyy, ISO, date/datetime)."""
    return _parse_line_date(raw)


def format_schedule_date_for_storage(raw: object) -> str | None:
    """
    Normalize a schedule line date for ``schedule_lines.\"Date\"`` (VARCHAR, canonical dd-Mon-yyyy).

    Rejects values that cannot be parsed. Callers should run migration **76_schedule_lines_date_varchar32.sql**
    if the column was ever VARCHAR(10), or inserts will truncate 4-digit years.
    """
    if raw is None:
        return None
    if isinstance(raw, float) and pd.isna(raw):
        return None
    if isinstance(raw, datetime):
        return raw.date().strftime(_SCHEDULE_DATE_STORAGE_FMT)
    if isinstance(raw, date):
        return raw.strftime(_SCHEDULE_DATE_STORAGE_FMT)
    s = str(raw).strip()
    if not s:
        return None
    if len(s) > 32:
        s = s[:32].strip()
    parsed = _parse_line_date(s)
    if parsed is not None:
        return parsed.strftime(_SCHEDULE_DATE_STORAGE_FMT)
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.notna(ts):
            return ts.date().strftime(_SCHEDULE_DATE_STORAGE_FMT)
    except Exception:
        pass
    raise ValueError(
        f"Unparseable schedule Date {raw!r}. Use YYYY-MM-DD or dd-Mon-yyyy with a **four-digit** year "
        "(e.g. 01-Jan-2024). If the DB column is VARCHAR(10), run schema migration 76 to widen it."
    )


def schedule_date_to_iso_for_exchange(raw: object) -> str:
    """
    Normalize a schedule ``Date`` cell to ``YYYY-MM-DD`` for CSV/Excel generators and stable interchange.

    Do **not** use ``str(value)[:10]`` when values may be ``dd-Mon-yyyy`` (11 characters): that truncates
    the year (e.g. ``26-Apr-2024`` → ``26-Apr-202``), which is unparseable and matched legacy VARCHAR(10) damage.

    Persistence still uses :func:`format_schedule_date_for_storage` (canonical dd-Mon-yyyy in the DB).
    """
    if raw is None:
        raise ValueError("schedule date is missing")
    if isinstance(raw, float) and pd.isna(raw):
        raise ValueError("schedule date is missing")
    if isinstance(raw, datetime):
        return raw.date().isoformat()
    if isinstance(raw, date):
        return raw.isoformat()
    # pandas.Timestamp and similar
    if hasattr(raw, "to_pydatetime") and callable(getattr(raw, "to_pydatetime", None)):
        try:
            return raw.to_pydatetime().date().isoformat()
        except Exception:
            pass
    s = str(raw).strip()
    if not s:
        raise ValueError("schedule date is missing")
    if len(s) > 32:
        s = s[:32].strip()
    parsed = _parse_line_date(s)
    if parsed is not None:
        return parsed.isoformat()
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.notna(ts):
            return ts.date().isoformat()
    except Exception:
        pass
    raise ValueError(
        f"Unparseable schedule Date {raw!r}. Use YYYY-MM-DD or dd-Mon-yyyy with a **four-digit** year "
        "(e.g. 01-Jan-2024). If the DB column is VARCHAR(10), run schema migration 76 to widen it."
    )


def _period_date_cell(raw: object) -> str | None:
    """INSERT helper: normalized date string or None."""
    if raw is None:
        return None
    try:
        if pd.isna(raw):
            return None
    except (ValueError, TypeError):
        return None
    return format_schedule_date_for_storage(raw)


def get_max_schedule_due_date_on_or_before(loan_id: int, on_or_before: date) -> date | None:
    """
    Latest instalment date stored on any saved schedule version for the loan that is
    on or before ``on_or_before``.

    Used for statement accrual windows after a new schedule version is saved: the latest
    version may not list historical due dates (e.g. pre-recast March due), but EOD
    ``regular_interest_period_to_date`` still resets on those historical boundaries.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sl."Date" AS d
                FROM schedule_lines sl
                INNER JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
                WHERE ls.loan_id = %s
                """,
                (loan_id,),
            )
            rows = cur.fetchall()
    best: date | None = None
    for r in rows:
        raw = r[0] if not isinstance(r, dict) else r.get("d")
        pd = _parse_line_date(raw)
        if pd is None or pd > on_or_before:
            continue
        if best is None or pd > best:
            best = pd
    return best


def replace_schedule_lines(loan_schedule_id: int, schedule_df: pd.DataFrame) -> None:
    """Replace all schedule_lines for a schedule with new values (e.g. after 10dp correction)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule_lines WHERE loan_schedule_id = %s", (loan_schedule_id,))
        with conn.cursor() as cur:
            for _, row in schedule_df.iterrows():
                period = int(row.get("Period", row.get("period", 0)))
                period_date = _period_date_cell(row.get("Date"))
                payment = float(as_10dp(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0))))) if pd.notna(row.get("Payment", row.get("Monthly Installment", 0))) else 0.0
                principal = float(as_10dp(row.get("Principal", row.get("principal", 0)))) if pd.notna(row.get("Principal")) else 0.0
                interest = float(as_10dp(row.get("Interest", row.get("interest", 0)))) if pd.notna(row.get("Interest")) else 0.0
                principal_balance = float(as_10dp(row.get("Principal Balance", row.get("principal_balance", 0)))) if pd.notna(row.get("Principal Balance")) else 0.0
                total_outstanding = float(as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0)))) if pd.notna(row.get("Total Outstanding")) else 0.0
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (loan_schedule_id, period, period_date, payment, principal, interest, principal_balance, total_outstanding),
                )


def save_new_schedule_version(loan_id: int, schedule_df: pd.DataFrame, version: int) -> int:
    """Insert a new schedule version and its lines. Returns the new loan_schedules.id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO loan_schedules (loan_id, version) VALUES (%s, %s) RETURNING id",
                (loan_id, version),
            )
            schedule_id = cur.fetchone()[0]
        with conn.cursor() as cur:
            for _, row in schedule_df.iterrows():
                period = int(row.get("Period", row.get("period", 0)))
                period_date = _period_date_cell(row.get("Date"))
                payment = float(as_10dp(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0))))) if pd.notna(row.get("Payment", row.get("Monthly Installment", 0))) else 0.0
                principal = float(as_10dp(row.get("Principal", row.get("principal", 0)))) if pd.notna(row.get("Principal")) else 0.0
                interest = float(as_10dp(row.get("Interest", row.get("interest", 0)))) if pd.notna(row.get("Interest")) else 0.0
                principal_balance = float(as_10dp(row.get("Principal Balance", row.get("principal_balance", 0)))) if pd.notna(row.get("Principal Balance")) else 0.0
                total_outstanding = float(as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0)))) if pd.notna(row.get("Total Outstanding")) else 0.0
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (schedule_id, period, period_date, payment, principal, interest, principal_balance, total_outstanding),
                )
    return schedule_id


def apply_schedule_version_bumps(on_date: date, bumps: Sequence[tuple[date, int]]) -> int:
    """Pure helper: schedule version in force on ``on_date`` after ordered bump events."""
    v = 1
    for d, nv in sorted(bumps, key=lambda x: (x[0], x[1])):
        if on_date >= d:
            v = nv
    return v


def list_schedule_bumping_events(loan_id: int) -> list[tuple[date, int]]:
    """
    Chronological recast + modification events that advance ``loan_schedules.version``.

    Statement instalment rows for date D must use lines from the version effective on D,
    not the latest schedule (post-recast lines change principal/interest splits for history).
    """
    raw: list[tuple[date, int, int]] = []
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT recast_date AS d, new_schedule_version AS nv, id AS sid
                FROM loan_recasts
                WHERE loan_id = %s
                """,
                (loan_id,),
            )
            for r in cur.fetchall():
                pd = _parse_line_date(r.get("d"))
                if pd is not None:
                    raw.append((pd, int(r["nv"]), int(r["sid"])))
            try:
                cur.execute(
                    """
                    SELECT modification_date AS d, new_schedule_version AS nv, id AS sid
                    FROM loan_modifications
                    WHERE loan_id = %s
                    """,
                    (loan_id,),
                )
                for r in cur.fetchall():
                    pd = _parse_line_date(r.get("d"))
                    if pd is not None:
                        raw.append((pd, int(r["nv"]), 1_000_000_000 + int(r["sid"])))
            except Exception:
                pass
    raw.sort(key=lambda x: (x[0], x[2]))
    return [(d, nv) for d, nv, _ in raw]


def schedule_version_effective_on(loan_id: int, on_date: date) -> int:
    """Return ``loan_schedules.version`` that governs contractual schedule on ``on_date``."""
    return apply_schedule_version_bumps(on_date, list_schedule_bumping_events(loan_id))


def collect_due_dates_in_range_all_schedule_versions(
    loan_id: int,
    start: date,
    end: date,
) -> list[date]:
    """Union of instalment dates from every saved version intersecting [start, end]."""
    latest = get_latest_schedule_version(loan_id)
    seen: set[date] = set()
    for ver in range(1, latest + 1):
        for sl in get_schedule_lines(loan_id, ver):
            pd = _parse_line_date(sl.get("Date"))
            if pd is not None and start <= pd <= end:
                seen.add(pd)
    return sorted(seen)


def get_schedule_line_on_version_for_date(
    loan_id: int,
    schedule_version: int,
    due_date: date,
) -> dict | None:
    for sl in get_schedule_lines(loan_id, schedule_version):
        if _parse_line_date(sl.get("Date")) == due_date:
            return dict(sl)
    return None


def get_original_facility_for_statements(loan_id: int, loan: dict | None = None) -> float | None:
    """
    Original contractual facility for drawdown lines and fee bases on statements.

    Recast updates ``loans.principal`` to the new balance; drawdown must still reflect the
    original facility. Prefer legacy ``loans.facility`` when the column exists and is set,
    else the first positive ``total_outstanding`` (or ``principal_balance``) on version 1.
    """
    if loan is not None:
        fac = loan.get("facility")
        if fac is not None and float(fac or 0) > 0:
            return float(as_10dp(fac))
    lines = get_schedule_lines(loan_id, 1)
    if not lines:
        return None

    def _pnum(sl: dict) -> int:
        try:
            return int(sl.get("Period") or sl.get("period") or 0)
        except (TypeError, ValueError):
            return 0

    for sl in sorted(lines, key=_pnum):
        to = float(sl.get("total_outstanding") or 0)
        if to > 1e-12:
            return float(as_10dp(to))
        pb = float(sl.get("principal_balance") or 0)
        if pb > 1e-12:
            return float(as_10dp(pb))
    return None
