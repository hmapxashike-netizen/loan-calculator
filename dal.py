from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url


def get_conn():
    """Create a new psycopg2 connection using config.get_database_url()."""
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


@dataclass
class User:
    id: str
    email: str
    password_hash: str
    full_name: str
    role: str
    is_active: bool
    failed_login_attempts: int
    locked_until: Optional[datetime]
    last_login: Optional[datetime]
    created_at: datetime


class UserRepository:
    def __init__(self, conn):
        self.conn = conn

    def _row_to_user(self, row) -> User:
        return User(
            id=str(row["id"]),
            email=row["email"],
            password_hash=row["password_hash"],
            full_name=row["full_name"],
            role=row["role"],
            is_active=row["is_active"],
            failed_login_attempts=row["failed_login_attempts"],
            locked_until=row["locked_until"],
            last_login=row["last_login"],
            created_at=row["created_at"],
        )

    def get_by_email(self, email: str) -> Optional[User]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            row = cur.fetchone()
        return self._row_to_user(row) if row else None

    def list_users(self) -> list[User]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM users
                ORDER BY created_at DESC
                """
            )
            rows = cur.fetchall()
        return [self._row_to_user(row) for row in rows]

    def create_user(self, email: str, password_hash: str, full_name: str, role: str) -> User:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (email, password_hash, full_name, role)
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (email, password_hash, full_name, role),
            )
            row = cur.fetchone()
        self.conn.commit()
        return self._row_to_user(row)

    def update_role(self, user_id: str, new_role: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET role = %s WHERE id = %s",
                (new_role, user_id),
            )
        self.conn.commit()

    def set_active(self, user_id: str, is_active: bool) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET is_active = %s WHERE id = %s",
                (is_active, user_id),
            )
        self.conn.commit()

    def unlock_account(self, user_id: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users
                SET failed_login_attempts = 0,
                    locked_until = NULL
                WHERE id = %s
                """,
                (user_id,),
            )
        self.conn.commit()

    def update_password(self, user_id: str, password_hash: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users
                SET password_hash = %s,
                    failed_login_attempts = 0,
                    locked_until = NULL
                WHERE id = %s
                """,
                (password_hash, user_id),
            )
        self.conn.commit()

    def increment_failed_attempts(self, user_id: str, *, lockout: bool) -> None:
        with self.conn.cursor() as cur:
            if lockout:
                locked_until = datetime.now(timezone.utc) + timedelta(minutes=15)
                cur.execute(
                    """
                    UPDATE users
                    SET failed_login_attempts = failed_login_attempts + 1,
                        locked_until = %s
                    WHERE id = %s
                    """,
                    (locked_until, user_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE users
                    SET failed_login_attempts = failed_login_attempts + 1
                    WHERE id = %s
                    """,
                    (user_id,),
                )
        self.conn.commit()

    def reset_failed_attempts_and_update_last_login(self, user_id: str) -> None:
        now = datetime.now(timezone.utc)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users
                SET failed_login_attempts = 0,
                    locked_until = NULL,
                    last_login = %s
                WHERE id = %s
                """,
                (now, user_id),
            )
        self.conn.commit()


class SecurityAuditLogRepository:
    def __init__(self, conn):
        self.conn = conn

    def log_login_attempt(
        self,
        *,
        user_id: Optional[str],
        email_used: Optional[str],
        success: bool,
        ip_address: Optional[str],
        user_agent: Optional[str],
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO security_audit_log
                    (user_id, email_used, success, ip_address, user_agent, event_type)
                VALUES (%s, %s, %s, %s::inet, %s, 'LOGIN')
                """,
                (user_id, email_used, success, ip_address, user_agent),
            )
        self.conn.commit()

    def list_recent(self, limit: int = 100) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM security_audit_log
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return list(rows)

