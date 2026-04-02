"""Single AccountingService + narrow UI facades (one shared DB service instance)."""

from __future__ import annotations

from dataclasses import dataclass

from accounting.service import AccountingService

from .coa import CoaUi
from .receipt_gl import ReceiptGlMappingUi
from .reports import FinancialReportsUi
from .templates import TransactionTemplatesUi


@dataclass(frozen=True)
class AccountingUiBundle:
    """Shared `AccountingService` behind tab-scoped facades (keeps files small, tests can inject `svc`)."""

    svc: AccountingService
    coa: CoaUi
    templates: TransactionTemplatesUi
    receipt_gl: ReceiptGlMappingUi
    reports: FinancialReportsUi


def build_accounting_ui_bundle(svc: AccountingService | None = None) -> AccountingUiBundle:
    s = svc or AccountingService()
    return AccountingUiBundle(
        svc=s,
        coa=CoaUi(s),
        templates=TransactionTemplatesUi(s),
        receipt_gl=ReceiptGlMappingUi(s),
        reports=FinancialReportsUi(s),
    )
