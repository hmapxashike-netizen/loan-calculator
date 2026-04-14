"""Unit tests for LOAN_APPROVAL deferral guard (no DB)."""

from __future__ import annotations

from datetime import date


def test_should_defer_when_disbursement_after_system_date(monkeypatch):
    from loan_management import loan_approval_gl_guard as g

    monkeypatch.setattr(g, "system_business_date_for_guard", lambda: date(2026, 1, 15))
    details = {"disbursement_date": date(2026, 2, 1)}
    assert g.should_defer_loan_approval_gl_at_capture(details) is True


def test_should_not_defer_when_disbursement_on_or_before_system_date(monkeypatch):
    from loan_management import loan_approval_gl_guard as g

    monkeypatch.setattr(g, "system_business_date_for_guard", lambda: date(2026, 1, 15))
    assert g.should_defer_loan_approval_gl_at_capture({"disbursement_date": date(2026, 1, 15)}) is False
    assert g.should_defer_loan_approval_gl_at_capture({"disbursement_date": date(2026, 1, 10)}) is False
