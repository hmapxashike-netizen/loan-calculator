"""
Default loan purpose labels for idempotent DB seeding.

Edit **this file** to match your institution’s list, then run:
    python scripts/seed_loan_purposes.py

Re-running skips any name that already exists in ``loan_purposes`` (case-insensitive).
System **config UI** only reads/writes the same table — it does not duplicate this list.
"""
from __future__ import annotations

# (display name, sort_order). Adjust names/order for your environment.
DEFAULT_LOAN_PURPOSES: list[tuple[str, int]] = [
    ("Working capital", 10),
    ("Asset purchase / equipment", 20),
    ("Vehicle finance", 30),
    ("Property / mortgage", 40),
    ("Agriculture", 50),
    ("Trade / inventory", 60),
    ("Personal / consumer", 70),
    ("Other", 90),
]


def run_default_seed() -> tuple[int, int]:
    """Apply ``DEFAULT_LOAN_PURPOSES`` via ``loan_management.ensure_loan_purpose_rows``."""
    from loan_management import ensure_loan_purpose_rows

    return ensure_loan_purpose_rows(DEFAULT_LOAN_PURPOSES)
