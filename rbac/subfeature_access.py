"""Fine-grained RBAC helpers: explicit keys win; parent ``nav.*`` grants legacy full access."""

from __future__ import annotations

from typing import Any


def _has(keys: frozenset[str], feat: str, *, nav: str | None = None) -> bool:
    if feat in keys:
        return True
    if nav and nav in keys:
        return True
    return False


def reamort_can_general(user: dict[str, Any] | None = None) -> bool:
    from rbac.service import rbac_tables_ready

    if not rbac_tables_ready():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "reamortisation.general_workspace", nav="nav.reamortisation")


def reamort_can_approve_modifications(user: dict[str, Any] | None = None) -> bool:
    from rbac.service import rbac_tables_ready

    if not rbac_tables_ready():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "reamortisation.approve_modifications", nav="nav.reamortisation")


def reamort_can_direct_principal(user: dict[str, Any] | None = None) -> bool:
    from rbac.service import rbac_tables_ready

    if not rbac_tables_ready():
        role, _ = _keys_from(user)
        return role in ("ADMIN", "SUPERADMIN")
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    if "reamortisation.direct_principal" in keys:
        return True
    if role == "ADMIN" and "nav.reamortisation" in keys:
        return True
    return False


def _keys_from(user: dict[str, Any] | None) -> tuple[str, frozenset[str]]:
    from middleware import get_current_user
    from rbac.service import get_permission_keys_for_role_key, rbac_tables_ready

    if user is None:
        user = get_current_user() or {}
    role = str(user.get("role") or "").strip().upper()
    if not rbac_tables_ready():
        return role, frozenset()
    return role, get_permission_keys_for_role_key(role)


def _rbac_off() -> bool:
    from rbac.service import rbac_tables_ready

    return not rbac_tables_ready()


def statements_can_debtor(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "statements.debtor_loans", nav="nav.statements")


def statements_can_creditor(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return False
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    if "statements.creditor_loans" in keys:
        return True
    if "nav.statements" in keys and role != "VIEWER":
        return True
    return False


def statements_can_gl(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "statements.gl", nav="nav.statements")


def document_can_view(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    if "document_management.view" in keys or "document_management.edit" in keys:
        return True
    return "nav.document_management" in keys


def document_can_edit(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "document_management.edit", nav="nav.document_management")


def eod_can_advance(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "eod.advance_system_date", nav="nav.end_of_day")


def eod_can_fix_issues(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "eod.fix_issues", nav="nav.end_of_day")


def subscription_can_tenant_account(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        role, _ = _keys_from(user)
        return role in ("ADMIN", "LOAN_OFFICER", "SUPERADMIN")
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "subscription.tenant_account", nav="nav.subscription")


def subscription_can_vendor_console(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        role, _ = _keys_from(user)
        return role in ("VENDOR", "SUPERADMIN")
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    if "subscription.vendor_console" in keys:
        return True
    if role == "VENDOR" and "nav.subscription" in keys:
        return True
    return False


def subscription_can_platform_admin(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        role, _ = _keys_from(user)
        return role == "SUPERADMIN"
    role, keys = _keys_from(user)
    if role != "SUPERADMIN":
        return False
    return "subscription.platform_admin" in keys or "nav.subscription" in keys


def teller_can_single_receipt(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "teller.single_receipt", nav="nav.teller")


def teller_can_batch_and_reverse(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "teller.batch_and_reverse", nav="nav.teller")


def teller_can_scheduled_receipts(user: dict[str, Any] | None = None) -> bool:
    """Future value-dated receipts (data take-on); same nav.teller legacy rule as other Teller areas."""
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "teller.scheduled_receipts", nav="nav.teller")


def portfolio_can_view_reports(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "portfolio_reports.view_reports", nav="nav.portfolio_reports")


def portfolio_can_data_exports(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    if "portfolio_reports.data_exports" in keys:
        return True
    if "nav.portfolio_reports" in keys and role != "VIEWER":
        return True
    return False


def customers_can_approve(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "customers.approve", nav="nav.customers")


def customers_can_view_only(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "customers.view_only", nav="nav.customers")


def customers_can_workspace(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "customers.workspace", nav="nav.customers")


def loan_management_can_schedules_repayments(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "loan_management.schedules_repayments", nav="nav.loan_management")


def loan_management_can_batch_capture(user: dict[str, Any] | None = None) -> bool:
    """Migration batch loan CSV import; never implied by nav.loan_management alone."""
    if _rbac_off():
        role, _ = _keys_from(user)
        return role == "SUPERADMIN"
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return "loan_management.batch_capture" in keys


def accounting_can_chart_templates_mapping(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "accounting.chart_templates_mapping", nav="nav.accounting")


def accounting_can_financial_reports(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "accounting.financial_reports", nav="nav.accounting")


def accounting_can_bank_reconciliation(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "accounting.bank_reconciliation", nav="nav.accounting")


def notifications_can_send(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "notifications.send", nav="nav.notifications")


def notifications_can_history(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "notifications.history", nav="nav.notifications")


def notifications_can_templates(user: dict[str, Any] | None = None) -> bool:
    if _rbac_off():
        return True
    role, keys = _keys_from(user)
    if role == "SUPERADMIN":
        return True
    return _has(keys, "notifications.templates", nav="nav.notifications")
