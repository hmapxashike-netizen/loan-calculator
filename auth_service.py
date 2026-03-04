from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import bcrypt

from auth_dal import UserRepository, SecurityAuditLogRepository, User


MAX_FAILED_ATTEMPTS = 5
LOCKOUT_MINUTES = 15


class AuthService:
    """
    Authentication and account lockout logic.
    """

    def __init__(self, conn):
        self.users = UserRepository(conn)
        self.audit = SecurityAuditLogRepository(conn)

    # ----- Password hashing -----

    @staticmethod
    def hash_password(plain_password: str) -> str:
        salt = bcrypt.gensalt(rounds=12)
        hashed = bcrypt.hashpw(plain_password.encode("utf-8"), salt)
        return hashed.decode("utf-8")

    @staticmethod
    def verify_password(plain_password: str, password_hash: str) -> bool:
        try:
            return bcrypt.checkpw(
                plain_password.encode("utf-8"),
                password_hash.encode("utf-8"),
            )
        except Exception:
            return False

    # ----- Lockout & login flow -----

    @staticmethod
    def _is_locked(user: User) -> bool:
        if user.locked_until is None:
            return False
        now = datetime.now(timezone.utc)
        return user.locked_until > now

    def _handle_failed_login(
        self,
        user: Optional[User],
        email_used: str,
        ip: Optional[str],
        user_agent: Optional[str],
    ) -> None:
        if user:
            will_lock = user.failed_login_attempts + 1 >= MAX_FAILED_ATTEMPTS
            self.users.increment_failed_attempts(
                user.id,
                lockout_minutes=LOCKOUT_MINUTES if will_lock else None,
            )
            self.audit.log_login_attempt(
                user_id=user.id,
                email_used=email_used,
                success=False,
                ip_address=ip,
                user_agent=user_agent,
            )
        else:
            # Unknown user: log anonymous failure to avoid enumeration.
            self.audit.log_login_attempt(
                user_id=None,
                email_used=email_used,
                success=False,
                ip_address=ip,
                user_agent=user_agent,
            )

    def _handle_successful_login(
        self,
        user: User,
        ip: Optional[str],
        user_agent: Optional[str],
    ) -> None:
        self.users.reset_failed_attempts_and_update_last_login(user.id)
        self.audit.log_login_attempt(
            user_id=user.id,
            email_used=user.email,
            success=True,
            ip_address=ip,
            user_agent=user_agent,
        )

    def authenticate(
        self,
        email: str,
        password: str,
        ip: Optional[str],
        user_agent: Optional[str],
    ) -> Optional[User]:
        """
        Returns User on success, None on failure.
        Always logs an audit record, but never reveals whether the account exists.
        """
        user = self.users.get_by_email(email)

        if user and self._is_locked(user):
            # locked accounts return generic failure
            self._handle_failed_login(user, email_used=email, ip=ip, user_agent=user_agent)
            return None

        valid = bool(user and user.is_active and self.verify_password(password, user.password_hash))

        if not valid:
            self._handle_failed_login(user, email_used=email, ip=ip, user_agent=user_agent)
            return None

        self._handle_successful_login(user, ip=ip, user_agent=user_agent)
        return user

