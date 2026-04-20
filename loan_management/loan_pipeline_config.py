"""Configurable loan application pipeline labels and business facility subtypes (system_config)."""

from __future__ import annotations

from typing import Any

from loan_management.product_catalog import load_system_config_from_db

_STATUS_BOOKED = "BOOKED"
_STATUS_DECLINED = "DECLINED"
_STATUS_IN_PROGRESS = "IN_PROGRESS"
_STATUS_PROSPECT = "PROSPECT"
_STATUS_SENT_FOR_APPROVAL = "SENT_FOR_APPROVAL"
_STATUS_SUPERSEDED = "SUPERSEDED"
_STATUS_WITHDRAWN = "WITHDRAWN"

_DEFAULT_STATUSES: list[dict[str, Any]] = [
    # `label` = imperative button text; `display_label` = past-tense wording for banners (DB `status` stays *_GRANTED etc.).
    # BOOKED/Disbursed has no button — use Loan Capture + Link booked loan; BOOKED skipped in SKIP_PIPELINE_BUTTON_CODES.
    {
        "code": "CREDIT_APPROVAL_GRANTED",
        "label": "Grant Credit Approval",
        "display_label": "Credit Approval Granted",
        "terminal": False,
    },
    {
        "code": "PAYROLL_DEDUCTION_APPROVAL_GRANTED",
        "label": "Grant Payroll Deduction Approval",
        "display_label": "Payroll Deduction Approval Granted",
        "terminal": False,
    },
    {
        "code": "TREASURY_APPROVAL_GRANTED",
        "label": "Grant Treasury Approval",
        "display_label": "Treasury Approval Granted",
        "terminal": False,
    },
    {"code": "WITHDRAWN", "label": "Withdraw", "display_label": "Withdrawn", "terminal": True},
    {"code": "DECLINED", "label": "Decline", "display_label": "Declined", "terminal": True},
    {"code": "SOFT_DELETE", "label": "Delete", "display_label": "Deleted", "terminal": True, "action": "soft_delete"},
    {
        "code": "SUPERSEDE",
        "label": "Supersede with New Application",
        "display_label": "Superseded by a New Application",
        "terminal": False,
        "action": "supersede",
    },
]

_LEGACY_STATUS_LABELS: dict[str, str] = {
    _STATUS_PROSPECT: "Application Submitted",
    _STATUS_IN_PROGRESS: "In progress",
    _STATUS_SENT_FOR_APPROVAL: "Sent for approval",
    _STATUS_BOOKED: "Disbursed",
    _STATUS_DECLINED: "Declined",
    _STATUS_WITHDRAWN: "Withdrawn",
    _STATUS_SUPERSEDED: "Superseded",
}


def default_loan_application_statuses() -> list[dict[str, Any]]:
    """Return built-in defaults (copied)."""
    return [dict(row) for row in _DEFAULT_STATUSES]


def default_business_facility_subtypes() -> list[str]:
    return [
        "Order Finance",
        "Invoice Discounting",
        "General Working Capital",
        "Asset Finance",
    ]


def _merged_system_config_for_pipeline() -> dict[str, Any]:
    """Merge DB system_config with defaults for pipeline keys (no Streamlit)."""
    raw = load_system_config_from_db() or {}
    out = dict(raw)
    if not out.get("loan_application_statuses"):
        out["loan_application_statuses"] = default_loan_application_statuses()
    if not out.get("business_facility_subtypes"):
        out["business_facility_subtypes"] = default_business_facility_subtypes()
    return out


# Codes never shown as pipeline buttons (legacy rows may still exist in saved config).
SKIP_PIPELINE_BUTTON_CODES = frozenset({"APPLICATION_SUBMITTED", "BOOKED"})


def effective_loan_application_statuses(cfg: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Resolved status definitions: explicit cfg, else DB merge, else defaults."""
    if cfg is not None:
        rows = cfg.get("loan_application_statuses")
        if isinstance(rows, list) and rows:
            return [dict(r) for r in rows if isinstance(r, dict)]
    mc = _merged_system_config_for_pipeline()
    rows = mc.get("loan_application_statuses")
    if isinstance(rows, list) and rows:
        return [dict(r) for r in rows if isinstance(r, dict)]
    return default_loan_application_statuses()


def effective_business_facility_subtypes(cfg: dict[str, Any] | None = None) -> list[str]:
    if cfg is not None:
        subs = cfg.get("business_facility_subtypes")
        if isinstance(subs, list) and subs:
            return [str(x).strip() for x in subs if str(x).strip()]
    mc = _merged_system_config_for_pipeline()
    subs = mc.get("business_facility_subtypes")
    if isinstance(subs, list) and subs:
        return [str(x).strip() for x in subs if str(x).strip()]
    return default_business_facility_subtypes()


def status_label_for_code(code: str | None, cfg: dict[str, Any] | None = None) -> str:
    """Human-readable label for a stored status code."""
    c = (code or "").strip().upper()
    if not c:
        return "—"
    for row in effective_loan_application_statuses(cfg):
        rc = str(row.get("code") or "").strip().upper()
        if rc == c:
            disp = str(row.get("display_label") or "").strip()
            lab = str(row.get("label") or "").strip()
            return disp or lab or c
    return _LEGACY_STATUS_LABELS.get(c, c.replace("_", " ").title())


def pipeline_action_for_code(code: str | None, cfg: dict[str, Any] | None = None) -> str | None:
    c = (code or "").strip().upper()
    for row in effective_loan_application_statuses(cfg):
        rc = str(row.get("code") or "").strip().upper()
        if rc == c:
            act = row.get("action")
            return str(act).strip().lower() if act else None
    return None


def is_terminal_application_status(code: str | None, cfg: dict[str, Any] | None = None) -> bool:
    """True if pipeline marks this code terminal, or legacy terminal status."""
    c = (code or "").strip().upper()
    if not c:
        return False
    if c in (
        _STATUS_BOOKED,
        _STATUS_DECLINED,
        _STATUS_WITHDRAWN,
        _STATUS_SUPERSEDED,
    ):
        return True
    for row in effective_loan_application_statuses(cfg):
        rc = str(row.get("code") or "").strip().upper()
        if rc == c and row.get("terminal"):
            return True
    return False


def non_terminal_for_submit(code: str | None, cfg: dict[str, Any] | None = None) -> bool:
    """Applications in this status may still be edited / sent for approval (not terminal)."""
    return not is_terminal_application_status(code, cfg)


def statuses_eligible_for_status_update_buttons(cfg: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Rows shown as primary status buttons (excludes duplicate legacy-only rows)."""
    return [r for r in effective_loan_application_statuses(cfg) if not r.get("action")]

