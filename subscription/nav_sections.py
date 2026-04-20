"""Top-level loan app sidebar section titles.

Must stay aligned with ``LOAN_APP_SECTIONS`` in ``app.py`` (same strings, same order).
"""

from __future__ import annotations

# Extra matrix row (not a sidebar section): gated separately in subscription.access.
BANK_RECONCILIATION_ROW_LABEL = "Bank reconciliation (accounting)"

# Keep in sync with app.LOAN_APP_SECTIONS — used for tier sidebar exclusions in vendor config.
LOAN_APP_SIDEBAR_SECTIONS: tuple[str, ...] = (
    "Customers",
    "Loan pipeline",
    "Loan management",
    "Creditor loans",
    "Portfolio reports",
    "Teller",
    "Reamortisation",
    "Statements",
    "Accounting",
    "Journals",
    "Notifications",
    "Document Management",
    "End of day",
    "System configurations",
    "Subscription",
)


def tier_entitlement_matrix_row_labels() -> tuple[str, ...]:
    """Sidebar sections plus one non-nav capability row for the vendor tier matrix."""
    return LOAN_APP_SIDEBAR_SECTIONS + (BANK_RECONCILIATION_ROW_LABEL,)
