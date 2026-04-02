from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import bcrypt

from dal import UserRepository, SecurityAuditLogRepository, User


MAX_FAILED_ATTEMPTS = 5
LOCKOUT_MINUTES = 15


class AuthService:
    """
    Authentication and account lockout logic.

    NOTE: Ensure `bcrypt` is installed (e.g. add `bcrypt` to requirements.txt).
    """

    def __init__(self, conn):
        self.users = UserRepository(conn)
        self.audit = SecurityAuditLogRepository(conn)

    # ------------- password hashing -------------

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

    # ------------- lockout helpers -------------

    @staticmethod
    def check_account_lockout(user: User) -> bool:
        """
        Returns True if the account is currently locked.
        """
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
            self.users.increment_failed_attempts(user.id, lockout=will_lock)
            self.audit.log_login_attempt(
                user_id=user.id,
                email_used=email_used,
                success=False,
                ip_address=ip,
                user_agent=user_agent,
            )
        else:
            # Unknown user: still log to prevent enumeration.
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

    # ------------- main authenticate API -------------

    def authenticate(
        self,
        email: str,
        password: str,
        ip: Optional[str],
        user_agent: Optional[str],
    ) -> tuple[Optional[User], str]:
        """
        Returns (user, status):
          - (User, "ok") on success
          - (None, "locked") if the account is locked
          - (None, "invalid") for any other failure
        All attempts are logged. This allows the UI to show a clear
        message when an account is locked while still avoiding
        enumeration across accounts.
        """
        user = self.users.get_by_email(email)

        # If user exists and is locked, log but do not increment further.
        if user and self.check_account_lockout(user):
            self.audit.log_login_attempt(
                user_id=user.id,
                email_used=email,
                success=False,
                ip_address=ip,
                user_agent=user_agent,
            )
            return None, "locked"

        if user and user.is_active:
            valid = self.verify_password(password, user.password_hash)
        else:
            valid = False

        if not valid:
            self._handle_failed_login(user, email_used=email, ip=ip, user_agent=user_agent)
            return None, "invalid"

        self._handle_successful_login(user, ip=ip, user_agent=user_agent)
        return user, "ok"

