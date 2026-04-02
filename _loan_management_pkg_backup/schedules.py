"""Loan schedule versions and schedule_lines persistence."""

from __future__ import annotations

import pandas as pd

from decimal_utils import as_10dp

from .db import RealDictCursor, _connection


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


def replace_schedule_lines(loan_schedule_id: int, schedule_df: pd.DataFrame) -> None:
    """Replace all schedule_lines for a schedule with new values (e.g. after 10dp correction)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule_lines WHERE loan_schedule_id = %s", (loan_schedule_id,))
        with conn.cursor() as cur:
            for _, row in schedule_df.iterrows():
                period = int(row.get("Period", row.get("period", 0)))
                period_date = str(row.get("Date", row.get("Date", "")))[:32] if pd.notna(row.get("Date")) else None
                payment = (
                    float(as_10dp(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0)))))
                    if pd.notna(row.get("Payment", row.get("Monthly Installment", 0)))
                    else 0.0
                )
                principal = (
                    float(as_10dp(row.get("Principal", row.get("principal", 0))))
                    if pd.notna(row.get("Principal"))
                    else 0.0
                )
                interest = (
                    float(as_10dp(row.get("Interest", row.get("interest", 0))))
                    if pd.notna(row.get("Interest"))
                    else 0.0
                )
                principal_balance = (
                    float(as_10dp(row.get("Principal Balance", row.get("principal_balance", 0))))
                    if pd.notna(row.get("Principal Balance"))
                    else 0.0
                )
                total_outstanding = (
                    float(as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0))))
                    if pd.notna(row.get("Total Outstanding"))
                    else 0.0
                )
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        loan_schedule_id,
                        period,
                        period_date,
                        payment,
                        principal,
                        interest,
                        principal_balance,
                        total_outstanding,
                    ),
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
                period_date = str(row.get("Date", row.get("Date", "")))[:32] if pd.notna(row.get("Date")) else None
                payment = (
                    float(as_10dp(row.get("Payment", row.get("Monthly Installment", row.get("payment", 0)))))
                    if pd.notna(row.get("Payment", row.get("Monthly Installment", 0)))
                    else 0.0
                )
                principal = (
                    float(as_10dp(row.get("Principal", row.get("principal", 0))))
                    if pd.notna(row.get("Principal"))
                    else 0.0
                )
                interest = (
                    float(as_10dp(row.get("Interest", row.get("interest", 0))))
                    if pd.notna(row.get("Interest"))
                    else 0.0
                )
                principal_balance = (
                    float(as_10dp(row.get("Principal Balance", row.get("principal_balance", 0))))
                    if pd.notna(row.get("Principal Balance"))
                    else 0.0
                )
                total_outstanding = (
                    float(as_10dp(row.get("Total Outstanding", row.get("total_outstanding", 0))))
                    if pd.notna(row.get("Total Outstanding"))
                    else 0.0
                )
                cur.execute(
                    """INSERT INTO schedule_lines (loan_schedule_id, "Period", "Date", payment, principal, interest, principal_balance, total_outstanding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        schedule_id,
                        period,
                        period_date,
                        payment,
                        principal,
                        interest,
                        principal_balance,
                        total_outstanding,
                    ),
                )
    return schedule_id
