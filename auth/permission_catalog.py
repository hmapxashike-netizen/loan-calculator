"""
Human-oriented permission catalog (Phase 1: sidebar sections + dashboards).

Stable ``permission_key`` values are used in code and DB; labels and prose are
shown to admins when assigning access.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PermissionRecord:
    permission_key: str
    label: str
    category: str
    summary: str
    grants_md: str
    risk_tag: str  # standard | sensitive | financial
    grant_restricted_to_superadmin: bool
    #: When set, maps this nav permission to the loan app section title (must match LOAN_APP_SECTIONS).
    nav_section: str | None = None


def _md_bullets(lines: tuple[str, ...]) -> str:
    return "\n".join(f"- {line}" for line in lines)


# Dashboard keys (not LOAN_APP_SECTIONS)
PERMISSION_DASHBOARD_OFFICER = "dashboard.officer"
PERMISSION_DASHBOARD_ADMIN = "dashboard.admin"

RESERVED_SUPERADMIN_MARKER = "reserved.superadmin_only_marker"


def all_permission_records() -> tuple[PermissionRecord, ...]:
    nav_standard = (
        PermissionRecord(
            permission_key="nav.customers",
            label="Customers",
            category="Navigation",
            summary="Work with customer records and related onboarding or KYC context.",
            grants_md=_md_bullets(
                (
                    "Open the Customers area from the sidebar.",
                    "View and maintain customer data allowed by your organisation’s processes.",
                )
            ),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section="Customers",
        ),
        PermissionRecord(
            permission_key="nav.loan_management",
            label="Loan management",
            category="Navigation",
            summary="Origination, schedules, approvals, and loan maintenance.",
            grants_md=_md_bullets(
                (
                    "Access loan capture, batch capture, schedules, calculators, updates, suspense, and approvals.",
                    "Subscription may still hide some sub-areas (e.g. capture on basic tier).",
                )
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section="Loan management",
        ),
        PermissionRecord(
            permission_key="nav.portfolio_reports",
            label="Portfolio reports",
            category="Navigation",
            summary="Portfolio-level reporting and analytics.",
            grants_md=_md_bullets(
                ("Open portfolio reporting tools for the lending book.",),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section="Portfolio reports",
        ),
        PermissionRecord(
            permission_key="nav.teller",
            label="Teller",
            category="Navigation",
            summary="Teller-facing receipt and posting flows.",
            grants_md=_md_bullets(
                ("Use teller workflows for collections and receipts.",),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section="Teller",
        ),
        PermissionRecord(
            permission_key="nav.reamortisation",
            label="Reamortisation",
            category="Navigation",
            summary="Re-amortisation and related restructuring workflows.",
            grants_md=_md_bullets(
                ("Access reamortisation tools subject to product configuration.",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section="Reamortisation",
        ),
        PermissionRecord(
            permission_key="nav.statements",
            label="Statements",
            category="Navigation",
            summary="Borrower and loan statements.",
            grants_md=_md_bullets(
                ("Generate or view statements as permitted by configuration.",),
            ),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section="Statements",
        ),
        PermissionRecord(
            permission_key="nav.accounting",
            label="Accounting",
            category="Navigation",
            summary="Chart of accounts, journals, reports, and accounting configuration.",
            grants_md=_md_bullets(
                (
                    "Open the Accounting module (COA, templates, receipt mapping, journals, reports).",
                    "Financial data exposure: assign only to trusted finance users.",
                )
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section="Accounting",
        ),
        PermissionRecord(
            permission_key="nav.journals",
            label="Journals",
            category="Navigation",
            summary="Journal-centric views and postings outside the full Accounting landing.",
            grants_md=_md_bullets(
                ("Access journal-related screens in the loan application shell.",),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section="Journals",
        ),
        PermissionRecord(
            permission_key="nav.notifications",
            label="Notifications",
            category="Navigation",
            summary="Notification configuration and monitoring.",
            grants_md=_md_bullets(
                ("Manage or review notification settings and activity.",),
            ),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section="Notifications",
        ),
        PermissionRecord(
            permission_key="nav.document_management",
            label="Document Management",
            category="Navigation",
            summary="Document storage and categories for loans and customers.",
            grants_md=_md_bullets(
                ("Upload, classify, and retrieve documents per policy.",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section="Document Management",
        ),
        PermissionRecord(
            permission_key="nav.end_of_day",
            label="End of day",
            category="Navigation",
            summary="EOD processing, system business date, and related maintenance.",
            grants_md=_md_bullets(
                (
                    "Run or review end-of-day; may advance system date and post accruals.",
                    "High operational impact — typically restricted to senior operations or admin.",
                )
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section="End of day",
        ),
        PermissionRecord(
            permission_key="nav.system_configurations",
            label="System configurations",
            category="Navigation",
            summary="Global product, EOD, accounting period, display, and related settings.",
            grants_md=_md_bullets(
                (
                    "Change settings that affect the whole organisation: products, EOD, IFRS, display, etc.",
                    "Powerful: misuse can affect bookings, statements, and fees.",
                )
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section="System configurations",
        ),
        PermissionRecord(
            permission_key="nav.subscription",
            label="Subscription",
            category="Navigation",
            summary="Organisation subscription status, limits, and (for vendor roles) platform tools.",
            grants_md=_md_bullets(
                (
                    "View or manage subscription context; vendor-facing tools depend on role and tenant.",
                    "Combined with role VENDOR or SUPERADMIN, exposes vendor subscription operations.",
                )
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section="Subscription",
        ),
    )

    dashboards = (
        PermissionRecord(
            permission_key=PERMISSION_DASHBOARD_OFFICER,
            label="Officer Dashboard",
            category="Dashboards",
            summary="Landing dashboard for lending staff.",
            grants_md=_md_bullets(
                ("Shows the officer home page when this is the first menu item.",),
            ),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key=PERMISSION_DASHBOARD_ADMIN,
            label="Admin Dashboard",
            category="Dashboards",
            summary="Landing dashboard for organisation administrators.",
            grants_md=_md_bullets(
                ("Shows the admin home page when this is the first menu item.",),
            ),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
    )

    security = (
        PermissionRecord(
            permission_key=RESERVED_SUPERADMIN_MARKER,
            label="Reserved: superadmin-only capability marker",
            category="Security",
            summary="Illustrates permissions only a superadmin may assign; does not add sidebar items.",
            grants_md=_md_bullets(
                (
                    "Used to validate RBAC save rules.",
                    "Only SUPERADMIN can assign this permission to a role.",
                    "Seeded on the SUPERADMIN role only; safe to leave off other roles.",
                )
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=True,
            nav_section=None,
        ),
    )

    return nav_standard + dashboards + security


def permission_by_key() -> dict[str, PermissionRecord]:
    return {p.permission_key: p for p in all_permission_records()}


def nav_permission_key_for_section(section_title: str) -> str | None:
    for p in all_permission_records():
        if p.nav_section == section_title:
            return p.permission_key
    return None


def all_nav_section_titles() -> tuple[str, ...]:
    return tuple(
        p.nav_section
        for p in all_permission_records()
        if p.nav_section is not None
    )
