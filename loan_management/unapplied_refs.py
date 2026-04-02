"""Deterministic GL / statement reference strings for repayments and unapplied funds."""

from __future__ import annotations

from datetime import date


def _repayment_journal_reference(loan_id: int, repayment_id: int) -> str:
    """Journal header reference for receipt allocation GL: always names loan and repayment."""
    return f"Loan {loan_id}, Repayment id {repayment_id}"


def _unapplied_original_reference(
    entry_kind: str,
    *,
    loan_id: int,
    repayment_id: int,
    value_date: date,
) -> str:
    """
    Deterministic reference for unapplied funds lifecycle (PDF-driven).

    Reversal references are built as: `REV-` + this string.
    """
    return f"{entry_kind}: Repayment ID {repayment_id} on LoanID {loan_id} - {value_date.isoformat()}"


def _unapplied_reversal_reference(original_reference: str) -> str:
    """Prefix original reference with REV- per PDF reversal rule."""
    return f"REV-{original_reference}"
