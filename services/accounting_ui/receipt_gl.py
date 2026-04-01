"""Receipt allocation → GL mapping surface for the mapping tab."""

from __future__ import annotations

from accounting_service import AccountingService


class ReceiptGlMappingUi:
    __slots__ = ("_svc",)

    def __init__(self, svc: AccountingService) -> None:
        self._svc = svc

    def list_receipt_gl_mappings(self):
        return self._svc.list_receipt_gl_mappings()

    def list_all_transaction_templates(self):
        return self._svc.list_all_transaction_templates()

    def upsert_receipt_gl_mapping(self, *args, **kwargs):
        return self._svc.upsert_receipt_gl_mapping(*args, **kwargs)

    def delete_receipt_gl_mapping(self, mapping_id: int):
        return self._svc.delete_receipt_gl_mapping(mapping_id)

    def initialize_default_receipt_gl_mappings(self):
        return self._svc.initialize_default_receipt_gl_mappings()

    def reset_receipt_gl_mappings_to_defaults(self):
        return self._svc.reset_receipt_gl_mappings_to_defaults()
