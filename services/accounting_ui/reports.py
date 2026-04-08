"""Financial statement / snapshot surface for the Reports tab."""

from __future__ import annotations

from accounting.service import AccountingService


class FinancialReportsUi:
    __slots__ = ("_svc",)

    def __init__(self, svc: AccountingService) -> None:
        self._svc = svc

    def get_trial_balance(self, as_of):
        return self._svc.get_trial_balance(as_of)

    def get_profit_and_loss(self, start, end):
        return self._svc.get_profit_and_loss(start, end)

    def get_balance_sheet(self, as_of):
        return self._svc.get_balance_sheet(as_of)

    def get_balance_sheet_with_pnl_adjustment(self, as_of, pl_period_start, *, system_config=None):
        return self._svc.get_balance_sheet_with_pnl_adjustment(
            as_of, pl_period_start, system_config=system_config
        )

    def get_net_profit_loss(self, start, end):
        return self._svc.get_net_profit_loss(start, end)

    def get_statement_of_changes_in_equity(self, start, end):
        return self._svc.get_statement_of_changes_in_equity(start, end)

    def get_cash_flow_statement(self, start, end):
        return self._svc.get_cash_flow_statement(start, end)

    def list_statement_snapshots(self, *args, **kwargs):
        return self._svc.list_statement_snapshots(*args, **kwargs)

    def get_statement_snapshot_with_lines(self, snapshot_id):
        return self._svc.get_statement_snapshot_with_lines(snapshot_id)
