"""TOTP (RFC 6238) helpers for authenticator apps (Google Authenticator, etc.)."""

from __future__ import annotations

import io
import re
import secrets
import pyotp
import qrcode

# Roles that must use two-step verification (enrolled at first sign-in).
TOTP_MANDATORY_ROLES = frozenset({"SUPERADMIN", "VENDOR"})

BACKUP_CODE_COUNT = 10
BACKUP_CODE_GROUP_LEN = 4


def random_totp_secret() -> str:
    return pyotp.random_base32()


def provisioning_uri(*, secret: str, email: str, issuer: str) -> str:
    totp = pyotp.TOTP(secret)
    return totp.provisioning_uri(name=email, issuer_name=issuer)


def verify_totp(secret: str, code: str, *, valid_window: int = 1) -> bool:
    if not secret or not code:
        return False
    digits = re.sub(r"\s+", "", code.strip())
    if not digits.isdigit() or len(digits) != 6:
        return False
    totp = pyotp.TOTP(secret)
    return bool(totp.verify(digits, valid_window=valid_window))


def qr_png_bytes(uri: str, *, box_size: int = 6) -> bytes:
    img = qrcode.make(uri, image_factory=None)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def normalize_backup_code(raw: str) -> str:
    s = (raw or "").upper().replace(" ", "").replace("-", "")
    return s


def generate_backup_codes_plain(*, count: int = BACKUP_CODE_COUNT) -> list[str]:
    """Human-readable groups, e.g. A3F2-9K1L (alphanumeric uppercase)."""
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    out: list[str] = []
    for _ in range(count):
        parts = []
        for _g in range(2):
            chunk = "".join(secrets.choice(alphabet) for _ in range(BACKUP_CODE_GROUP_LEN))
            parts.append(chunk)
        out.append("-".join(parts))
    return out
