"""
Human-oriented permission catalog (Phase 1: sidebar sections + feature keys).

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


# Dashboard keys (not LOAN_APP_SECTIONS). Officer dashboard removed — use sidebar nav only.
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
                    "Access loan capture, schedules, calculators, updates, suspense, and approvals.",
                    "**Batch loan capture (migration)** is a separate permission (see **Loan management — batch loan capture**).",
                    "Subscription may still hide some sub-areas (e.g. capture on basic tier).",
                )
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section="Loan management",
        ),
        PermissionRecord(
            permission_key="nav.creditor_loans",
            label="Creditor loans",
            category="Navigation",
            summary="Borrowing / liability mirror facilities (separate from debtor loans).",
            grants_md=_md_bullets(
                (
                    "Open creditor capture, counterparties, receipts, and write-offs.",
                    "Requires feature permissions (creditor_loans.*) for sub-areas.",
                )
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section="Creditor loans",
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
            permission_key="teller.scheduled_receipts",
            label="Scheduled receipts (data take-on)",
            category="Teller",
            summary="Capture future value-dated receipts that post on value date; cancel before value date.",
            grants_md=_md_bullets(
                (
                    "Use the Scheduled receipts area under Teller (batch upload, list, cancel).",
                    "Does not replace nav.teller; assign both where operators need both flows.",
                )
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=True,
            nav_section=None,
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
                (
                    "Open the **Journals** sidebar section.",
                    "Sub-areas (manual / balance / approvals) use **journals.*** feature keys below.",
                ),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section="Journals",
        ),
        PermissionRecord(
            permission_key="journals.manual",
            label="Journals — manual journal",
            category="Journals",
            summary="Template-based manual journal posting under Journals.",
            grants_md=_md_bullets(
                ("**Manual Journals** horizontal tab (template posting, reversals).",),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="journals.balance_adjustment",
            label="Journals — balance adjustment",
            category="Journals",
            summary="One-off debit/credit balance adjustment between posting accounts.",
            grants_md=_md_bullets(
                ("**Balance Adjustments** horizontal tab under Journals.",),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="journals.approvals",
            label="Journals — journal approvals",
            category="Journals",
            summary="Review and approve journal workflows (reserved for policy-driven queues).",
            grants_md=_md_bullets(
                (
                    "**Journal approvals** tab; wire to approval queues when enabled.",
                    "Assign with **Open Journals** where supervisors approve postings.",
                ),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
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
        PermissionRecord(
            permission_key="creditor_loans.view",
            label="Creditor loans — view",
            category="Creditor loans",
            summary="View creditor facilities, schedules, and mirror daily state.",
            grants_md=_md_bullets(("View creditor loan list and schedules.",)),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="creditor_loans.capture",
            label="Creditor loans — capture",
            category="Creditor loans",
            summary="Create creditor (borrowing) facilities.",
            grants_md=_md_bullets(("Capture new creditor facilities and schedules.",)),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="creditor_loans.receipts",
            label="Creditor loans — receipts",
            category="Creditor loans",
            summary="Record payments to lenders.",
            grants_md=_md_bullets(("Post creditor repayments with allocation and GL.",)),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="creditor_loans.writeoff",
            label="Creditor loans — write-off",
            category="Creditor loans",
            summary="Post creditor-specific write-off journals.",
            grants_md=_md_bullets(("Principal or interest write-off on creditor facilities.",)),
            risk_tag="financial",
            grant_restricted_to_superadmin=True,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="creditor_loans.counterparties",
            label="Creditor loans — counterparties",
            category="Creditor loans",
            summary="Maintain lender / financier master data.",
            grants_md=_md_bullets(("Add or edit creditor counterparties.",)),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="loan_management.approve_loans",
            label="Loan management — approve loans",
            category="Loan management",
            summary="Approve loan approval drafts (commit to book) in Loan management.",
            grants_md=_md_bullets(
                (
                    "Shows the **Approve Loans** tab under Loan management.",
                    "Loan officers capture; supervisors or admins typically hold this permission.",
                )
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="loan_management.schedules_repayments",
            label="Loan management — schedules & repayments (view)",
            category="Loan management",
            summary="View stored amortisation schedules and repayment lines for existing loans.",
            grants_md=_md_bullets(
                (
                    "Shows the **Schedules & repayments** horizontal tab under Loan management.",
                    "Usually granted together with **Open Loan management**; can be revoked for read-only capture roles.",
                ),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="loan_management.batch_capture",
            label="Loan management — batch loan capture (migration)",
            category="Loan management",
            summary="CSV batch import of loans (migration / data take-on) without the approval queue.",
            grants_md=_md_bullets(
                (
                    "Shows the **Batch Capture** horizontal tab under Loan management.",
                    "High risk: commits loans directly; assign only to trusted migration operators.",
                ),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=True,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="accounting.chart_templates_mapping",
            label="Accounting — chart, templates & receipt mapping",
            category="Accounting",
            summary="Chart of accounts, transaction templates, receipt → GL mapping, and manual journals.",
            grants_md=_md_bullets(
                (
                    "**Chart of Accounts**, **Transaction Templates**, **Receipt → GL Mapping**, **Manual Journals**.",
                ),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="accounting.financial_reports",
            label="Accounting — financial reports",
            category="Accounting",
            summary="IFRS-style financial reports and related views in Accounting.",
            grants_md=_md_bullets(("**Financial Reports** tab in Accounting.",)),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="accounting.bank_reconciliation",
            label="Accounting — bank reconciliation",
            category="Accounting",
            summary="Bank reconciliation workspace (when enabled for the subscription).",
            grants_md=_md_bullets(
                ("**Bank reconciliation** tab; may still be hidden when the tenant is not on Premium bank recon.",),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="notifications.send",
            label="Notifications — send",
            category="Notifications",
            summary="Send ad-hoc SMS / email / in-app notifications.",
            grants_md=_md_bullets(("**Send Notification** tab.",)),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="notifications.history",
            label="Notifications — history",
            category="Notifications",
            summary="View sent and failed notification history.",
            grants_md=_md_bullets(("**History** tab.",)),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="notifications.templates",
            label="Notifications — templates",
            category="Notifications",
            summary="Manage reusable notification templates.",
            grants_md=_md_bullets(("**Templates** tab.",)),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="reamortisation.approve_modifications",
            label="Reamortisation — approve modifications",
            category="Reamortisation",
            summary="Approve Modifications tab (draft approvals, send back, dismiss).",
            grants_md=_md_bullets(
                ("Use the **Approve Modifications** horizontal tab in Reamortisation.",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="reamortisation.direct_principal",
            label="Reamortisation — direct principal (admin recast)",
            category="Reamortisation",
            summary="Direct principal recast tab without unapplied-funds path.",
            grants_md=_md_bullets(
                ("Shows **Direct principal (admin)** in Reamortisation; high impact on principal balance.",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="reamortisation.general_workspace",
            label="Reamortisation — loan modification, recast & unapplied",
            category="Reamortisation",
            summary="Loan Modification, Loan Recast, and Unapplied Funds tabs.",
            grants_md=_md_bullets(
                (
                    "Standard reamortisation workspace (capture, recast, unapplied).",
                    "Assign with **Open Reamortisation** sidebar permission where users need the module.",
                ),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="statements.debtor_loans",
            label="Statements — debtor (customer) loan statements",
            category="Statements",
            summary="Customer loan statement generator and related debtor views.",
            grants_md=_md_bullets(("**Customer loan statement** area under Statements.",)),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="statements.creditor_loans",
            label="Statements — creditor / borrowing statements",
            category="Statements",
            summary="Creditor-side statement views under Statements (when enabled).",
            grants_md=_md_bullets(
                ("**Creditor loan statement** area; complements **Creditor loans** schedules.",),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="statements.gl",
            label="Statements — general ledger",
            category="Statements",
            summary="GL trial-style extracts and exports from Statements.",
            grants_md=_md_bullets(("**General Ledger** horizontal tab under Statements.",)),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="document_management.view",
            label="Document management — view & download",
            category="Document Management",
            summary="Browse uploaded and generated documents; download only.",
            grants_md=_md_bullets(
                ("**All Documents** and **Generated Documents** read-only use.",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="document_management.edit",
            label="Document management — configure & upload",
            category="Document Management",
            summary="Document classes, categories, and uploads that change master data.",
            grants_md=_md_bullets(
                ("**Document Classes** and **Document Categories** configuration and upload flows.",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="eod.advance_system_date",
            label="End of day — run / advance system date",
            category="End of day",
            summary="EOD Date advance tab (run EOD, advance business date).",
            grants_md=_md_bullets(("**EOD Date advance** tab under End of day.",)),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="eod.fix_issues",
            label="End of day — fix EOD issues",
            category="End of day",
            summary="Backfill, reallocation, and repair tools after EOD problems.",
            grants_md=_md_bullets(("**Fix EOD issues** tab (recompute, repair, diagnostics).",)),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="subscription.tenant_account",
            label="Subscription — organisation (tenant) view",
            category="Subscription",
            summary="Proof-of-payment uploads and subscription summary for your organisation.",
            grants_md=_md_bullets(
                ("Tenant-facing subscription tab (POP, period, tier as exposed to the client org).",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="subscription.vendor_console",
            label="Subscription — vendor console",
            category="Subscription",
            summary="Cross-tenant subscription maintenance for platform vendors.",
            grants_md=_md_bullets(
                ("Vendor tools: working-tenant switcher, tier/cycle, grace, terminations.",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="subscription.platform_admin",
            label="Subscription — platform superadmin",
            category="Subscription",
            summary="Superadmin dual view (vendor tools plus organisation account).",
            grants_md=_md_bullets(
                ("Allows the **Vendor** tab alongside organisation subscription for SUPERADMIN.",),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=True,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="teller.single_receipt",
            label="Teller — single repayment",
            category="Teller",
            summary="Single repayment receipt posting.",
            grants_md=_md_bullets(("**Single repayment** horizontal section under Teller.",)),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="teller.batch_and_reverse",
            label="Teller — batch, reverse & written-off recovery",
            category="Teller",
            summary="Batch payments, reverse receipt, and written-off loan recovery flows.",
            grants_md=_md_bullets(
                (
                    "**Batch payments**, **Reverse receipt**, and **Receipt from fully written-off loan**.",
                ),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="portfolio_reports.view_reports",
            label="Portfolio reports — view analyses",
            category="Portfolio reports",
            summary="Groups 1–5 portfolio and regulatory reports (read-only analytics).",
            grants_md=_md_bullets(
                ("All standard portfolio report pickers except **Data export**.",),
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="portfolio_reports.data_exports",
            label="Portfolio reports — data exports",
            category="Portfolio reports",
            summary="Bulk CSV / ZIP export of loan tables (group 6).",
            grants_md=_md_bullets(("**Data export** group under Portfolio reports.",)),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="customers.approve",
            label="Customers — approvals",
            category="Customers",
            summary="Approvals queue for customer / agent changes.",
            grants_md=_md_bullets(("**Approvals** section under Customers.",)),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="customers.view_only",
            label="Customers — view & manage (read-oriented)",
            category="Customers",
            summary="View & Manage list and profile review without capture tabs.",
            grants_md=_md_bullets(
                (
                    "**View & Manage** for read-oriented staff (no Add / Batch unless also granted).",
                    "Pair with **workspace** for capture if your policy splits duties.",
                ),
            ),
            risk_tag="standard",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="customers.workspace",
            label="Customers — capture & agents",
            category="Customers",
            summary="Add Individual, Add Corporate, Agents, and batch capture entry points.",
            grants_md=_md_bullets(
                (
                    "**Add Individual**, **Add Corporate**, **Agents**, and **Batch Capture**.",
                ),
            ),
            risk_tag="sensitive",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
        PermissionRecord(
            permission_key="accounting.supervise",
            label="Accounting — supervise",
            category="Accounting",
            summary="Senior accounting oversight (reserved for future journal/period gates).",
            grants_md=_md_bullets(
                (
                    "Assign to accounts supervisors alongside Accounting and Journals nav as your policy requires.",
                    "Seeded on supervisor-style roles; wire feature checks where your organisation needs them.",
                )
            ),
            risk_tag="financial",
            grant_restricted_to_superadmin=False,
            nav_section=None,
        ),
    )

    dashboards = (
        PermissionRecord(
            permission_key=PERMISSION_DASHBOARD_ADMIN,
            label="Admin Dashboard",
            category="System configurations",
            summary="Landing dashboard for organisation administrators (same sidebar rules as other areas).",
            grants_md=_md_bullets(
                (
                    "Shows **Admin Dashboard** in the main menu when granted.",
                    "Only a **super administrator** may assign this permission to a role.",
                    "Typically granted to **Administrator** and **Super administrator** only.",
                ),
            ),
            risk_tag="standard",
            grant_restricted_to_superadmin=True,
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
