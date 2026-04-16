"""
Seed rbac_roles, rbac_permissions, and rbac_role_permissions to match pre-RBAC menu behaviour.
"""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from auth.permission_catalog import (
    PERMISSION_DASHBOARD_ADMIN,
    RESERVED_SUPERADMIN_MARKER,
    all_permission_records,
    nav_permission_key_for_section,
)


def _nav_keys_all() -> set[str]:
    return {
        p.permission_key
        for p in all_permission_records()
        if p.nav_section is not None
    }


def _creditor_loans_action_keys() -> set[str]:
    """Feature keys for creditor module (nav is included via ``all_nav``)."""
    return {
        "creditor_loans.view",
        "creditor_loans.capture",
        "creditor_loans.receipts",
        "creditor_loans.counterparties",
    }


def _accounts_officer_keys() -> set[str]:
    """Navigation + creditor feature permissions for Accounts Officer (no creditor write-off)."""
    nav_acct = nav_permission_key_for_section("Accounting")
    nav_cl = nav_permission_key_for_section("Creditor loans")
    keys: set[str] = set()
    if nav_acct:
        keys.add(nav_acct)
    if nav_cl:
        keys.add(nav_cl)
    keys |= _creditor_loans_action_keys()
    return keys


def _accounts_supervisor_keys() -> set[str]:
    """Accounts Officer scope plus Journals and accounting supervise (for policy / future gates)."""
    keys = set(_accounts_officer_keys())
    nav_j = nav_permission_key_for_section("Journals")
    if nav_j:
        keys.add(nav_j)
    keys.add("accounting.supervise")
    return keys


def _viewer_keys() -> set[str]:
    """Read-only portfolio and statements navigation only."""
    keys: set[str] = set()
    ns = nav_permission_key_for_section("Statements")
    np = nav_permission_key_for_section("Portfolio reports")
    if ns:
        keys.add(ns)
    if np:
        keys.add(np)
    return keys


def expand_granular_feature_keys(role_key: str, keys: set[str]) -> set[str]:
    """
    When a role has a parent ``nav.*`` key from older seeds, ensure matching granular
    feature keys exist so behaviour matches pre-split RBAC (until admins revoke ticks).
    """
    k = set(keys)
    rk = role_key.strip().upper()
    if "nav.reamortisation" in k:
        k.add("reamortisation.approve_modifications")
        k.add("reamortisation.general_workspace")
        if rk in ("ADMIN", "SUPERADMIN"):
            k.add("reamortisation.direct_principal")
    if "nav.statements" in k:
        k.add("statements.debtor_loans")
        k.add("statements.gl")
        if rk != "VIEWER":
            k.add("statements.creditor_loans")
    if "nav.document_management" in k:
        k.add("document_management.view")
        k.add("document_management.edit")
    if "nav.end_of_day" in k:
        k.add("eod.advance_system_date")
        k.add("eod.fix_issues")
    if "nav.subscription" in k:
        if rk == "VENDOR":
            k.add("subscription.vendor_console")
        elif rk == "SUPERADMIN":
            k.update(
                {
                    "subscription.tenant_account",
                    "subscription.vendor_console",
                    "subscription.platform_admin",
                }
            )
        else:
            k.add("subscription.tenant_account")
    if "nav.teller" in k:
        k.add("teller.single_receipt")
        k.add("teller.batch_and_reverse")
        k.add("teller.scheduled_receipts")
    if "nav.portfolio_reports" in k:
        k.add("portfolio_reports.view_reports")
        if rk != "VIEWER":
            k.add("portfolio_reports.data_exports")
    if "nav.customers" in k:
        k.add("customers.approve")
        k.add("customers.view_only")
        k.add("customers.workspace")
    if "nav.accounting" in k:
        k.update(
            {
                "accounting.chart_templates_mapping",
                "accounting.financial_reports",
                "accounting.bank_reconciliation",
            }
        )
    if "nav.notifications" in k:
        k.update(
            {
                "notifications.send",
                "notifications.history",
                "notifications.templates",
            }
        )
    if "nav.loan_management" in k:
        k.add("loan_management.schedules_repayments")
    if "nav.journals" in k:
        k.add("journals.manual")
        k.add("journals.balance_adjustment")
        k.add("journals.approvals")
    return k


def seed_rbac_defaults(conn) -> None:
    from rbac.repository import RbacRepository

    repo = RbacRepository(conn)
    repo.sync_permissions_from_catalog()

    system_roles = (
        ("SUPERADMIN", "Super administrator", True),
        ("ADMIN", "Administrator", True),
        ("LOAN_OFFICER", "Loan officer", True),
        ("LOAN_SUPERVISOR", "Loan Supervisor", True),
        ("ACCOUNTS_OFFICER", "Accounts Officer", True),
        ("ACCOUNTS_SUPERVISOR", "Accounts Supervisor", True),
        ("VIEWER", "Viewer", True),
        ("BORROWER", "Borrower", True),
        ("VENDOR", "Vendor", True),
    )

    role_ids: dict[str, int] = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for role_key, display_name, is_system in system_roles:
            cur.execute(
                """
                INSERT INTO rbac_roles (role_key, display_name, is_system)
                VALUES (%s, %s, %s)
                ON CONFLICT (role_key) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    is_system = EXCLUDED.is_system
                RETURNING id
                """,
                (role_key, display_name, is_system),
            )
            row = cur.fetchone()
            role_ids[role_key] = int(row["id"])

        all_nav = _nav_keys_all()
        sys_cfg_key = nav_permission_key_for_section("System configurations")
        officer_nav = all_nav - {sys_cfg_key} if sys_cfg_key else all_nav
        creditor_keys = _creditor_loans_action_keys()
        accounts_officer_keys = _accounts_officer_keys()
        accounts_supervisor_keys = _accounts_supervisor_keys()
        viewer_keys = _viewer_keys()
        loan_supervisor_keys = officer_nav | creditor_keys | {"loan_management.approve_loans"}
        admin_extra = {"loan_management.approve_loans", "accounting.supervise"}

        grants_spec = (
            ("BORROWER", set()),
            (
                "VENDOR",
                {nav_permission_key_for_section("Subscription") or "nav.subscription"},
            ),
            ("LOAN_OFFICER", officer_nav | creditor_keys),
            ("LOAN_SUPERVISOR", loan_supervisor_keys),
            ("ACCOUNTS_OFFICER", accounts_officer_keys),
            ("ACCOUNTS_SUPERVISOR", accounts_supervisor_keys),
            ("VIEWER", viewer_keys),
            ("ADMIN", {PERMISSION_DASHBOARD_ADMIN} | all_nav | creditor_keys | admin_extra),
            (
                "SUPERADMIN",
                {PERMISSION_DASHBOARD_ADMIN}
                | all_nav
                | creditor_keys
                | {"creditor_loans.writeoff", "loan_management.batch_capture"}
                | {RESERVED_SUPERADMIN_MARKER}
                | admin_extra,
            ),
        )

        for role_key, keys in grants_spec:
            rid = role_ids[role_key]
            expanded = expand_granular_feature_keys(role_key, set(keys))
            cur.execute("DELETE FROM rbac_role_permissions WHERE role_id = %s", (rid,))
            for pk in sorted(expanded):
                cur.execute(
                    "INSERT INTO rbac_role_permissions (role_id, permission_key) VALUES (%s, %s)",
                    (rid, pk),
                )

    conn.commit()
