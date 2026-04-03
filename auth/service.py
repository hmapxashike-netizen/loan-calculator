from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import bcrypt

from auth.totp import (
    TOTP_MANDATORY_ROLES,
    generate_backup_codes_plain,
    normalize_backup_code,
    verify_totp,
)
from dal import UserRepository, SecurityAuditLogRepository, User


MAX_FAILED_ATTEMPTS = 5
LOCKOUT_MINUTES = 15

_MIN_RECOVERY_PASSWORD_LEN = 10


def totp_issuer_name() -> str:
    return (os.environ.get("FARNDACRED_TOTP_ISSUER") or "FarndaCred").strip() or "FarndaCred"


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

    # ------------- TOTP (SUPERADMIN / VENDOR) -------------

    @staticmethod
    def role_requires_totp_enrollment(role: str) -> bool:
        return role in TOTP_MANDATORY_ROLES

    def _totp_fully_configured(self, user: User) -> bool:
        return bool(user.two_factor_enabled and (user.two_factor_secret or "").strip())

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
          - (User, "ok") on success (full login, no TOTP or not applicable)
          - (User, "totp_required") password ok; need authenticator or backup code (do not set session yet)
          - (User, "setup_totp_required") password ok; must enroll TOTP before session
          - (None, "locked") if the account is locked
          - (None, "invalid") for any other failure
        """
        user = self.users.get_by_email(email)

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

        assert user is not None

        if self.role_requires_totp_enrollment(user.role):
            if self._totp_fully_configured(user):
                return user, "totp_required"
            return user, "setup_totp_required"

        self._handle_successful_login(user, ip=ip, user_agent=user_agent)
        return user, "ok"

    def complete_totp_login(
        self,
        user_id: str,
        code: str,
        ip: Optional[str],
        user_agent: Optional[str],
    ) -> tuple[Optional[User], str]:
        """
        After password success, verify TOTP or consume one backup code, then finish login.
        """
        user = self.users.get_by_id(user_id)
        if not user or not user.is_active:
            return None, "invalid"
        if not self.role_requires_totp_enrollment(user.role) or not self._totp_fully_configured(user):
            return None, "invalid"

        secret = (user.two_factor_secret or "").strip()
        raw = (code or "").strip()
        ok = False
        if secret and verify_totp(secret, raw):
            ok = True
        else:
            norm = normalize_backup_code(raw)
            if norm and self.users.try_consume_backup_code(user_id, norm):
                ok = True

        if not ok:
            self._handle_failed_login(user, email_used=user.email, ip=ip, user_agent=user_agent)
            return None, "invalid"

        self._handle_successful_login(user, ip=ip, user_agent=user_agent)
        return user, "ok"

    def finalize_totp_enrollment(
        self,
        user_id: str,
        secret: str,
        verification_code: str,
        ip: Optional[str],
        user_agent: Optional[str],
    ) -> tuple[bool, list[str], str]:
        """
        Confirm scanned secret with a valid 6-digit code, enable 2FA, issue backup codes.
        Returns (success, plain_backup_codes_to_show_once, error_message).
        """
        user = self.users.get_by_id(user_id)
        if not user or not user.is_active:
            return False, [], "Invalid session."
        if not self.role_requires_totp_enrollment(user.role):
            return False, [], "Two-step verification is not required for this account."
        if not verify_totp(secret, verification_code):
            return False, [], "That code does not match. Check the time on your phone."

        plain = generate_backup_codes_plain()
        norms = [normalize_backup_code(p) for p in plain]
        try:
            self.users.set_two_factor(user_id, secret=secret.strip(), enabled=True)
            self.users.replace_totp_backup_codes(user_id, norms)
        except Exception as e:
            return False, [], str(e)

        return True, plain, ""

    def complete_session_after_totp_setup(self, user_id: str, ip: Optional[str], user_agent: Optional[str]) -> Optional[User]:
        """Call after the user has saved recovery codes: audit login + last_login."""
        user = self.users.get_by_id(user_id)
        if not user or not user.is_active:
            return None
        self._handle_successful_login(user, ip=ip, user_agent=user_agent)
        return user

    def regenerate_backup_codes(self, user_id: str, totp_code: str) -> tuple[bool, list[str], str]:
        """
        Logged-in SUPERADMIN/VENDOR: mint new recovery codes after a valid TOTP check.
        Replaces all unused backup hashes; show returned plaintext once to the user.
        """
        user = self.users.get_by_id(user_id)
        if not user or not user.is_active:
            return False, [], "Invalid user."
        if not self.role_requires_totp_enrollment(user.role):
            return False, [], "Recovery codes apply only to vendor or super-admin accounts."
        if not self._totp_fully_configured(user):
            return False, [], "Two-step verification is not fully enabled."
        secret = (user.two_factor_secret or "").strip()
        if not verify_totp(secret, totp_code):
            return False, [], "Invalid authenticator code."
        plain = generate_backup_codes_plain()
        norms = [normalize_backup_code(p) for p in plain]
        try:
            self.users.replace_totp_backup_codes(user_id, norms)
        except Exception as e:
            return False, [], str(e)
        return True, plain, ""

    def recover_password_with_backup_code(
        self,
        *,
        email: str,
        backup_code: str,
        new_password: str,
        ip: Optional[str],
        user_agent: Optional[str],
    ) -> tuple[bool, str]:
        """
        SUPERADMIN/VENDOR: one backup code + new password when the password is forgotten.
        Consumes the backup code. Does not log the user in.
        """
        email = (email or "").strip()
        if len(new_password) < _MIN_RECOVERY_PASSWORD_LEN:
            return False, f"Password must be at least {_MIN_RECOVERY_PASSWORD_LEN} characters."

        user = self.users.get_by_email(email)
        _fail_msg = (
            "Unable to reset password. Check email, recovery code, password length (10+ characters), "
            "and that the account uses two-step verification."
        )

        if not user or not user.is_active:
            self.audit.log_login_attempt(
                user_id=None,
                email_used=email,
                success=False,
                ip_address=ip,
                user_agent=user_agent,
                event_type="PASSWORD_RESET_BACKUP",
            )
            return False, _fail_msg

        if not self.role_requires_totp_enrollment(user.role) or not user.two_factor_enabled:
            self.audit.log_login_attempt(
                user_id=user.id,
                email_used=email,
                success=False,
                ip_address=ip,
                user_agent=user_agent,
                event_type="PASSWORD_RESET_BACKUP",
            )
            return False, _fail_msg

        norm = normalize_backup_code(backup_code)
        if not norm or not self.users.try_consume_backup_code(user.id, norm):
            self.audit.log_login_attempt(
                user_id=user.id,
                email_used=email,
                success=False,
                ip_address=ip,
                user_agent=user_agent,
                event_type="PASSWORD_RESET_BACKUP",
            )
            return False, _fail_msg

        pw_hash = self.hash_password(new_password)
        self.users.update_password(user.id, pw_hash)
        self.audit.log_login_attempt(
            user_id=user.id,
            email_used=email,
            success=True,
            ip_address=ip,
            user_agent=user_agent,
            event_type="PASSWORD_RESET_BACKUP",
        )
        return True, "Password updated. Sign in with your new password and authenticator."
