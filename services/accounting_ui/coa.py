"""Thin COA / product-map surface for the Chart of Accounts tab (delegates to AccountingService)."""

from __future__ import annotations

from accounting.service import AccountingService


class CoaUi:
    __slots__ = ("_svc",)

    def __init__(self, svc: AccountingService) -> None:
        self._svc = svc

    def is_coa_initialized(self):
        return self._svc.is_coa_initialized()

    def initialize_default_coa(self):
        return self._svc.initialize_default_coa()

    def list_accounts(self):
        return self._svc.list_accounts()

    def peek_next_grandchild_codes_for_parent(self, parent_id, n: int):
        return self._svc.peek_next_grandchild_codes_for_parent(parent_id, n)

    def create_subaccounts_under_tagged_parent(self, *args, **kwargs):
        return self._svc.create_subaccounts_under_tagged_parent(*args, **kwargs)

    def refresh_source_cash_account_cache(self):
        return self._svc.refresh_source_cash_account_cache()

    def update_gl_account_name(self, *args, **kwargs):
        return self._svc.update_gl_account_name(*args, **kwargs)

    def set_gl_account_active(self, *args, **kwargs):
        return self._svc.set_gl_account_active(*args, **kwargs)

    def update_gl_account_code(self, *args, **kwargs):
        return self._svc.update_gl_account_code(*args, **kwargs)

    def list_all_transaction_templates(self):
        return self._svc.list_all_transaction_templates()

    def list_product_gl_subaccount_map(self):
        return self._svc.list_product_gl_subaccount_map()

    def list_leaf_accounts_for_system_tag(self, tag: str):
        return self._svc.list_leaf_accounts_for_system_tag(tag)

    def upsert_product_gl_subaccount_map(self, *args, **kwargs):
        return self._svc.upsert_product_gl_subaccount_map(*args, **kwargs)

    def suggest_next_grandchild_code_for_parent_id(self, parent_id: str):
        return self._svc.suggest_next_grandchild_code_for_parent_id(parent_id)

    def create_account(self, *args, **kwargs):
        return self._svc.create_account(*args, **kwargs)

    def get_account_subtree_ids(self, account_id):
        return self._svc.get_account_subtree_ids(account_id)

    def update_account_parent(self, *args, **kwargs):
        return self._svc.update_account_parent(*args, **kwargs)
