from __future__ import annotations

"""
ID generation utilities for FarndaCred.

Why not just use auto‑incrementing integers?
--------------------------------------------
Relying only on sequential integers (1, 2, 3, ...) for customer and loan
identifiers has a few drawbacks in a production FarndaCred deployment:

- Security / privacy:
  - Sequential IDs are easy to guess and enumerate (e.g. /loan/1, /loan/2, ...),
    which makes it trivial for a malicious user to probe for other customers'
    or loans' data if any endpoint is misconfigured.
- Scalability and distribution:
  - Auto‑incrementing IDs are typically tied to a single database sequence,
    which becomes a coordination point when scaling out horizontally or
    across regions.
- Auditability and UX:
  - Business‑facing staff often benefit from stable, opaque reference codes
    (e.g. "Customer C29X‑K7L‑P92" or a sortable LoanID) that can be quoted
    in emails and reports without revealing how many records exist.

This module provides *public* identifiers that can sit alongside the internal
integer primary keys in the database. Internal IDs remain useful for joins,
but external APIs, documents and screens should prefer these opaque IDs.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import secrets
from typing import Final


# Customer ID generation
# ----------------------
#
# Format (display form): CXXX-XXX-XXX
# - Total length 12 characters including hyphens.
# - The first character is the fixed prefix "C".
# - The remaining 9 characters are random, drawn from an alphabet that
#   intentionally excludes ambiguous characters like I, L, O, 0 and 1.
#
# Example: C29X-K7L-P92
#
# This keeps the identifier short, human-friendly, and resistant to
# mis-reading while still providing a very large random space.

_CUSTOMER_ID_PREFIX: Final[str] = "C"
_CUSTOMER_ID_RANDOM_LEN: Final[int] = 9  # random characters after "C"
_CUSTOMER_ID_DISPLAY_LEN: Final[int] = 12  # e.g. CXXX-XXX-XXX
_CUSTOMER_ID_ALPHABET: Final[str] = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
# Excludes: I, L, O, U, 0, 1 to avoid confusion.


# Loan ID generation (K-sortable)
# -------------------------------
#
# We generate a 16-character base32 string that is K-sortable:
# - The high bits encode the UTC timestamp in milliseconds (48 bits).
# - The low bits are cryptographically secure random bits (32 bits).
# - Encoded big-endian using a Crockford-style Base32 alphabet.
#
# This gives:
# - Chronological ordering when sorting as strings.
# - High uniqueness without central coordination, suitable for
#   distributed systems or future multi-node deployments.

_BASE32_ALPHABET: Final[str] = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_LOAN_ID_LENGTH: Final[int] = 16  # 16 * 5 = 80 bits (48 time + 32 random)
_TIMESTAMP_BITS: Final[int] = 48
_RANDOM_BITS: Final[int] = 32


def _encode_base32(value: int, length: int) -> str:
    """Encode an integer to a fixed-length Base32 string (big-endian)."""
    chars = ["0"] * length
    for i in range(length - 1, -1, -1):
        chars[i] = _BASE32_ALPHABET[value & 31]
        value >>= 5
    return "".join(chars)


def _decode_base32(value: str) -> int:
    """Decode a Base32 string (using our alphabet) back to an integer."""
    acc = 0
    for ch in value:
        acc = (acc << 5) | _BASE32_ALPHABET.index(ch)
    return acc


@dataclass(frozen=True)
class IDService:
    """
    Utility service for generating and validating external Customer and Loan IDs.

    These IDs are designed to be:
    - Hard to guess (cryptographically strong randomness).
    - Safe to expose in URLs and documents.
    - Stable even if internal integer primary keys are restructured.
    """

    @staticmethod
    def generate_customer_id() -> str:
        """
        Generate a new customer ID in the form CXXX-XXX-XXX.

        Uses Python's `secrets` module (cryptographically secure) and an
        alphabet without ambiguous characters to reduce transcription errors.
        """
        # Generate the random body (after the 'C' prefix).
        random_chars = "".join(
            secrets.choice(_CUSTOMER_ID_ALPHABET) for _ in range(_CUSTOMER_ID_RANDOM_LEN)
        )
        raw = _CUSTOMER_ID_PREFIX + random_chars  # length 10

        # Group as CXXX-XXX-XXX for readability.
        # raw[0:4] -> CXXX, raw[4:7] -> XXX, raw[7:10] -> XXX
        return f"{raw[0:4]}-{raw[4:7]}-{raw[7:10]}"

    @staticmethod
    def validate_customer_id(value: str) -> bool:
        """
        Validate a customer ID string.

        Rules:
        - Format: CXXX-XXX-XXX (length 12 with hyphens).
        - 'C' prefix.
        - Only allowed alphabet characters (no I, L, O, U, 0, 1) in the body.
        """
        if not isinstance(value, str):
            return False
        if len(value) != _CUSTOMER_ID_DISPLAY_LEN:
            return False
        if value[0] != _CUSTOMER_ID_PREFIX:
            return False
        if value[4] != "-" or value[8] != "-":
            return False

        # Strip hyphens and validate characters.
        body = value.replace("-", "")
        if len(body) != 10:
            return False
        # First char is the prefix 'C'; the remaining must be in the alphabet.
        if body[0] != _CUSTOMER_ID_PREFIX:
            return False
        for ch in body[1:]:
            if ch not in _CUSTOMER_ID_ALPHABET:
                return False
        return True

    @staticmethod
    def generate_loan_id(now: datetime | None = None) -> str:
        """
        Generate a K-sortable 16-character loan ID.

        The resulting ID:
        - Sorts lexicographically in the same order as creation time.
        - Encodes the UTC timestamp in milliseconds plus 32 bits of random data.
        - Uses a Crockford-style Base32 alphabet (safe for URLs and case-insensitive systems).
        """
        if now is None:
            now = datetime.now(timezone.utc)
        # 48-bit timestamp in milliseconds since Unix epoch.
        time_ms = int(now.timestamp() * 1000) & ((1 << _TIMESTAMP_BITS) - 1)
        rnd = secrets.randbits(_RANDOM_BITS)
        combined = (time_ms << _RANDOM_BITS) | rnd
        return _encode_base32(combined, _LOAN_ID_LENGTH)

    @staticmethod
    def validate_loan_id(value: str, *, allow_future: bool = True) -> bool:
        """
        Validate a loan ID string.

        Basic rules:
        - Exactly 16 characters.
        - Characters must come from the Base32 alphabet (no ambiguous glyphs).
        - Optionally checks that the embedded timestamp is not too far in the past,
          and (unless `allow_future` is True) not unreasonably far in the future.
        """
        if not isinstance(value, str):
            return False
        if len(value) != _LOAN_ID_LENGTH:
            return False
        if any(ch not in _BASE32_ALPHABET for ch in value):
            return False

        # Decode timestamp sanity check (not strictly required for format validity,
        # but helps catch obvious corruption).
        try:
            combined = _decode_base32(value)
        except ValueError:
            return False

        time_ms = combined >> _RANDOM_BITS
        # Rough sanity window: years 2000–2100.
        epoch_ms_2000 = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        epoch_ms_2100 = int(datetime(2100, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        if not (epoch_ms_2000 <= time_ms <= epoch_ms_2100):
            return False

        if not allow_future:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            if time_ms > now_ms + 60_000:  # allow small clock skew
                return False

        return True


__all__ = ["IDService"]

