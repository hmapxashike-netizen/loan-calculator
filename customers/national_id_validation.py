"""National ID format: 7–9 digits, one alphabetic check letter, two trailing digits."""

from __future__ import annotations

import re

# After normalization (check letter uppercased): 7–9 digits + [A-Z] + 2 digits → 10–12 characters.
_NATIONAL_ID_RE = re.compile(r"^\d{7,9}[A-Z]\d{2}$")

_MATCH_INPUT = re.compile(r"^(\d{7,9})([A-Za-z])(\d{2})$")

NATIONAL_ID_FORMAT_HELP = "7–9 digits, one check letter (A–Z), then 2 digits — e.g. 1234567A12 or 12345678A12"


def normalize_national_id_input(raw: str) -> str:
    """Strip spaces; uppercase check letter when the full pattern matches."""
    s = (raw or "").strip().replace(" ", "")
    m = _MATCH_INPUT.fullmatch(s)
    if m:
        return m.group(1) + m.group(2).upper() + m.group(3)
    return s


def is_valid_national_id_format(raw: str) -> bool:
    """True if value matches ``[7–9 digits][check letter][2 digits]`` after normalization."""
    s = normalize_national_id_input(raw)
    return bool(s) and _NATIONAL_ID_RE.match(s) is not None
