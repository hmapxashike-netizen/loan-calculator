"""Transaction template / journal-link surface for the Templates tab."""

from __future__ import annotations

from accounting_service import AccountingService


class TransactionTemplatesUi:
    __slots__ = ("_svc",)

    def __init__(self, svc: AccountingService) -> None:
        self._svc = svc

    def list_all_transaction_templates(self):
        return self._svc.list_all_transaction_templates()

    def list_accounts(self):
        return self._svc.list_accounts()

    def delete_transaction_template(self, template_id):
        return self._svc.delete_transaction_template(template_id)

    def update_transaction_template(self, *args, **kwargs):
        return self._svc.update_transaction_template(*args, **kwargs)

    def link_journal(self, *args, **kwargs):
        return self._svc.link_journal(*args, **kwargs)

    def initialize_default_transaction_templates(self):
        return self._svc.initialize_default_transaction_templates()
