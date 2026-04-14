from dataclasses import dataclass
from typing import Any

from datetime import date
from decimal import Decimal
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from config import get_database_url
from decimal_utils import as_10dp, as_2dp
from loan_management import load_system_config_from_db, _merge_cash_gl_into_payload

from .core import suggest_next_grandchild_account_code
from .dal import (
    AccountingRepository,
    assert_journal_lines_balanced,
    get_conn,
    journal_lines_balance_totals,
    journal_totals_balanced_for_posting,
)
from .defaults_loader import (
    get_default_receipt_gl_mapping_tuples,
    get_default_transaction_template_tuples,
)
from .equity_close import build_month_end_pnl_close_lines
from .equity_config import net_profit_loss_from_balance_rows, resolve_accounting_equity_config
from .periods import (
    get_month_period_bounds,
    get_year_period_bounds,
    is_eom,
    is_eoy,
    normalize_accounting_period_config,
)
from .posting_policy import get_gl_posting_policy

# Journals with these tags zero nominal I&E into equity at month-end; exclude from P&L *reports*
# so period totals show operating activity. GL close logic must NOT use this filter.
# DAL also excludes journal_entries.event_id starting with PNL_CLOSE: when this tuple includes
# MONTH_END_PNL (matches post_month_end_pnl_close_to_cye idempotency keys; covers untagged legacy).
PNL_REPORT_EXCLUDED_EVENT_TAGS: tuple[str, ...] = ("MONTH_END_PNL",)


def _get_system_business_date_strict() -> date:
    """
    Read system business date without fallback to wall-clock date.

    GL posting policy must anchor to configured business date only.
    """
    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT current_system_date FROM system_business_config WHERE id = %s",
                (1,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row or not row.get("current_system_date"):
        raise RuntimeError(
            "System business date is not configured. Set system_business_config.current_system_date before posting GL."
        )
    d = row["current_system_date"]
    return d.date() if hasattr(d, "date") else d


@dataclass(frozen=True)
class JournalSimulationResult:
    """
    Result of simulate_event: lines plus balance diagnostics.
    Posting uses 2dp material balance (avoid); simulation **flags** only.
    """

    lines: list[dict]
    balanced: bool
    total_debit: Decimal
    total_credit: Decimal
    warning: str | None

    @staticmethod
    def empty() -> "JournalSimulationResult":
        return JournalSimulationResult(
            lines=[],
            balanced=True,
            total_debit=Decimal("0"),
            total_credit=Decimal("0"),
            warning=None,
        )


class AccountingService:
    def __init__(self):
        pass

    def is_transaction_templates_initialized(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.is_transaction_templates_initialized()
        finally:
            conn.close()

    def initialize_default_transaction_templates(self):
        DEFAULT_TEMPLATES = get_default_transaction_template_tuples()
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.initialize_default_transaction_templates(DEFAULT_TEMPLATES)
            return True
        finally:
            conn.close()

    def is_coa_initialized(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.is_coa_initialized()
        finally:
            conn.close()

    def initialize_default_coa(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            if not repo.is_coa_initialized():
                repo.initialize_default_coa()
                return True
            return False
        finally:
            conn.close()

    def list_accounts(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_accounts()
        finally:
            conn.close()

    def peek_next_grandchild_codes_for_parent(self, parent_account_id: str, count: int) -> list[str]:
        """Preview next N grandchild codes (BASE-NN) under parent; does not persist."""
        if count < 1:
            return []
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT code FROM accounts WHERE id = %s", (parent_account_id,))
                row = cur.fetchone()
            if not row:
                raise ValueError("Parent account not found.")
            parent_code = (row["code"] or "").strip().upper()
            # Important: collisions must be checked globally, not only under the stored parent_id,
            # because legacy/bad COA data can mis-parent a code (e.g. BASE-02 under a different parent).
            existing_list = repo.list_codes_for_base_and_grandchildren(parent_code)
            sim = set(str(x).strip().upper() for x in existing_list)
            out: list[str] = []
            for _ in range(count):
                nxt = suggest_next_grandchild_account_code(parent_code, sim)
                out.append(nxt)
                sim.add(nxt)
            return out
        finally:
            conn.close()

    def create_subaccounts_under_tagged_parent(
        self,
        parent_id: str,
        children: list[tuple[str, str]],
        *,
        resolution_mode: str,
        product_assignments: list[tuple[str, int]] | None = None,
        parent_system_tag: str | None = None,
    ) -> list[str]:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.create_child_accounts_batch(
                parent_id,
                children,
                parent_subaccount_resolution=(resolution_mode or "").strip().upper(),
                product_assignments=product_assignments,
                parent_system_tag=parent_system_tag,
            )
        finally:
            conn.close()

    def update_gl_account_name(self, account_id: str, name: str) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.update_account_name(account_id, name)
        finally:
            conn.close()

    def update_gl_account_code(self, account_id: str, new_code: str) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.update_account_code(account_id, new_code)
        finally:
            conn.close()

    def set_gl_account_active(self, account_id: str, is_active: bool) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.set_account_is_active(account_id, is_active)
        finally:
            conn.close()

    def list_posting_leaf_accounts(self) -> list[dict]:
        """Active accounts with no active children (posting leaves), with display_label path."""
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_posting_leaf_accounts()
        finally:
            conn.close()

    def compute_source_cash_leaf_accounts(self, *, root_code: str = "A100000") -> list[dict]:
        """Recompute allowed source-cash accounts from the live chart (see repository rules)."""
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.compute_source_cash_leaf_accounts(root_code=root_code)
        finally:
            conn.close()

    def refresh_source_cash_account_cache(self, *, root_code: str = "A100000") -> dict:
        """
        Persist the source-cash account list into ``system_config.source_cash_account_cache``.
        Call from admin maintenance UI; loan capture and Teller read the cache only (no per-request tree walk).
        """
        from datetime import datetime, timezone

        from loan_management import load_system_config_from_db, save_system_config_to_db

        entries = self.compute_source_cash_leaf_accounts(root_code=root_code)
        cfg = load_system_config_from_db() or {}
        cfg["source_cash_account_cache"] = {
            "version": 1,
            "root_code": root_code,
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
            "entries": entries,
        }
        if not save_system_config_to_db(cfg):
            raise RuntimeError("Failed to save system configuration after rebuilding source cash cache.")
        return dict(cfg["source_cash_account_cache"])

    def is_parent_account(self, account_code: str) -> bool:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.is_parent_account(account_code)
        finally:
            conn.close()

    def get_child_account_summaries(self, parent_code: str, start_date: date, end_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_child_account_summaries(parent_code, start_date, end_date)
        finally:
            conn.close()

    def list_all_transaction_templates(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_all_transaction_templates()
        finally:
            conn.close()

    def get_transaction_templates(self, event_type: str):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            rows = repo.get_transaction_templates(event_type)
            return list(rows or [])
        finally:
            conn.close()

    def fetch_account_row_for_system_tag(self, system_tag: str):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.fetch_account_row_for_system_tag(system_tag)
        finally:
            conn.close()

    def list_active_direct_children_accounts(self, parent_id):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_active_direct_children_accounts(parent_id)
        finally:
            conn.close()

    def try_resolve_posting_account_for_tag(
        self,
        system_tag: str,
        *,
        loan_id: int | None = None,
        account_overrides: dict | None = None,
    ):
        """
        Try resolve_posting_account_for_tag without raising.
        Returns (account_row_or_None, error_message_or_None).
        """
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            try:
                acc = repo.resolve_posting_account_for_tag(
                    system_tag,
                    loan_id=loan_id,
                    account_overrides=account_overrides or {},
                )
                return (acc, None)
            except ValueError as e:
                return (None, str(e))
        finally:
            conn.close()

    def update_transaction_template(self, template_id: str, **fields) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.update_transaction_template(template_id, **fields)
        finally:
            conn.close()

    def delete_transaction_template(self, template_id: str) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.delete_transaction_template(template_id)
        finally:
            conn.close()

    def link_journal(self, event_type, system_tag, direction, description, trigger_type="EVENT"):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.link_journal(event_type, system_tag, direction, description, trigger_type)
            return True
        finally:
            conn.close()

    def create_account(
        self,
        code,
        name,
        category,
        system_tag=None,
        parent_id=None,
        subaccount_resolution=None,
    ):
        code = (code or "").strip() if code is not None else ""
        name = (name or "").strip() if name is not None else ""
        if not code or not name:
            raise ValueError("Account code and name are required.")
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.create_account(
                code,
                name,
                category,
                system_tag,
                parent_id,
                subaccount_resolution=subaccount_resolution,
            )
            return True
        finally:
            conn.close()

    def suggest_next_grandchild_code_for_parent_id(self, parent_account_id: str) -> str:
        """Suggest next ``BASE-NN`` code for children of the given parent account UUID."""
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT code FROM accounts WHERE id = %s", (parent_account_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError("Parent account not found.")
                parent_code = (row["code"] or "").strip().upper()
            existing = [str(x).strip().upper() for x in repo.list_codes_for_base_and_grandchildren(parent_code)]
            return suggest_next_grandchild_account_code(parent_code, existing)
        finally:
            conn.close()

    def update_account_subaccount_resolution(self, account_id, subaccount_resolution) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.update_account_subaccount_resolution(account_id, subaccount_resolution)
        finally:
            conn.close()

    def list_disbursement_bank_options(self, active_only: bool = True):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_disbursement_bank_options(active_only=active_only)
        finally:
            conn.close()

    def add_disbursement_bank_option(self, label: str, gl_account_id: str, sort_order: int = 0) -> int:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.insert_disbursement_bank_option(label, gl_account_id, sort_order=sort_order)
        finally:
            conn.close()

    def set_disbursement_bank_option_active(self, option_id: int, is_active: bool) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.set_disbursement_bank_option_active(option_id, is_active)
        finally:
            conn.close()

    def list_product_gl_subaccount_map(self, product_code: str | None = None):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_product_gl_subaccount_map(product_code=product_code)
        finally:
            conn.close()

    def list_leaf_accounts_for_system_tag(self, system_tag: str) -> list[dict]:
        """Posting leaves under the COA row that carries this system_tag (for product map UI and validation)."""
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_posting_leaves_under_system_tag(system_tag)
        finally:
            conn.close()

    def upsert_product_gl_subaccount_map(self, product_code: str, system_tag: str, gl_account_id: str) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.upsert_product_gl_subaccount_map(product_code, system_tag, gl_account_id)
        finally:
            conn.close()

    def delete_product_gl_subaccount_map(self, map_id: int) -> None:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.delete_product_gl_subaccount_map(map_id)
        finally:
            conn.close()

    def get_account_subtree_ids(self, account_id):
        """IDs of this account and all descendants (for valid parent options when editing hierarchy)."""
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_account_subtree_ids(account_id)
        finally:
            conn.close()

    def update_account_parent(self, account_id, parent_id=None) -> None:
        """Change an existing account's parent (or clear to top-level)."""
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.update_account_parent(account_id, parent_id)
        finally:
            conn.close()

    def get_trial_balance(self, as_of_date: date = None):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_trial_balance(as_of_date)
        finally:
            conn.close()

    def get_journal_entries(self, start_date: date = None, end_date: date = None, account_code: str = None):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            rows = repo.get_journal_entries(start_date, end_date, account_code)
            return self._annotate_journal_entries_balance(rows)
        finally:
            conn.close()

    @staticmethod
    def _annotate_journal_entries_balance(rows):
        """Add double_entry_balanced and line totals per header (flag legacy bad rows)."""
        if not rows:
            return rows
        out = []
        for row in rows:
            e = dict(row)
            lines = e.get("lines")
            if lines is None:
                lines = []
            if isinstance(lines, str):
                lines = json.loads(lines)
            td, tc = journal_lines_balance_totals(lines)
            e["double_entry_balanced"] = journal_totals_balanced_for_posting(td, tc)
            e["lines_total_debit"] = td
            e["lines_total_credit"] = tc
            out.append(e)
        return out

    def list_unbalanced_journal_entries(self):
        """
        Journal headers where sum(debits) != sum(credits). For integrity checks and
        after fixing LOAN_APPROVAL (or any) posting logic.
        """
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_unbalanced_journal_entries()
        finally:
            conn.close()

    def repost_loan_approval_journal(self, loan_id: int, *, created_by: str = "repair") -> None:
        """
        Re-post LOAN_APPROVAL for a loan from the current loans row (same payload as save_loan).
        Replaces the existing journal for (event_id, event_tag) when the schema supports it.
        """
        from loan_management import (
            _date_conv,
            build_loan_approval_journal_payload,
            get_loan,
        )

        loan = get_loan(loan_id)
        if not loan:
            raise ValueError(f"Loan {loan_id} not found")
        payload = build_loan_approval_journal_payload(loan)
        disb_date_str = loan.get("disbursement_date") or loan.get("start_date")
        e_date = _date_conv(disb_date_str) if disb_date_str else None
        self.post_event(
            event_type="LOAN_APPROVAL",
            reference=f"LOAN-{loan_id}",
            description=f"Loan Approval and Disbursement for {loan_id}",
            event_id=str(loan_id),
            created_by=created_by,
            entry_date=e_date,
            payload=payload,
            loan_id=int(loan_id),
        )

    def get_account_ledger(
        self,
        account_code: str,
        start_date: date = None,
        end_date: date = None,
        *,
        include_descendants: bool = False,
    ):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_account_ledger(
                account_code, start_date, end_date, include_descendants=include_descendants
            )
        finally:
            conn.close()

    def get_profit_and_loss(self, start_date: date, end_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            # P&L is Income and Expense (exclude month-end close journals; GL clearing unchanged)
            balances = repo.get_balances_by_category(
                ["INCOME", "EXPENSE"],
                start_date,
                end_date,
                exclude_event_tags=PNL_REPORT_EXCLUDED_EVENT_TAGS,
            )
            return balances
        finally:
            conn.close()

    def get_balance_sheet(self, as_of_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            # Balance sheet is Asset, Liability, Equity
            balances = repo.get_balances_by_category(['ASSET', 'LIABILITY', 'EQUITY'], end_date=as_of_date)
            return balances
        finally:
            conn.close()

    def get_net_profit_loss(self, start_date: date, end_date: date) -> Decimal:
        """Net P&L for [start_date, end_date] (same convention as P&L report)."""
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            rows = repo.get_balances_by_category(
                ["INCOME", "EXPENSE"],
                start_date=start_date,
                end_date=end_date,
                exclude_event_tags=PNL_REPORT_EXCLUDED_EVENT_TAGS,
            )
            return net_profit_loss_from_balance_rows(rows)
        finally:
            conn.close()

    def get_balance_sheet_with_pnl_adjustment(
        self,
        as_of_date: date,
        pl_period_start: date,
        *,
        system_config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Balance sheet accounts as of ``as_of_date`` plus supplemental net P&L for
        ``[pl_period_start, as_of_date]`` (aligns with P&L for the same range).
        """
        _ = system_config  # reserved for future display prefs
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            rows = repo.get_balances_by_category(
                ["ASSET", "LIABILITY", "EQUITY"], end_date=as_of_date
            )
            pl_rows = repo.get_balances_by_category(
                ["INCOME", "EXPENSE"],
                start_date=pl_period_start,
                end_date=as_of_date,
                exclude_event_tags=PNL_REPORT_EXCLUDED_EVENT_TAGS,
            )
            net = net_profit_loss_from_balance_rows(pl_rows)
            return {
                "rows": rows,
                "supplemental": {
                    "label": "Net profit/(loss) for period (P&L basis)",
                    "net_amount": net,
                    "period_start": pl_period_start,
                    "period_end": as_of_date,
                },
            }
        finally:
            conn.close()

    def post_month_end_pnl_close_to_cye(
        self,
        as_of_date: date,
        *,
        created_by: str = "system",
        system_config: dict[str, Any] | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """
        On accounting month-end, zero cumulative INCOME/EXPENSE balances through that
        month-end into **current year earnings** (idempotent per period).
        """
        cfg = system_config if system_config is not None else (load_system_config_from_db() or {})
        period_cfg = normalize_accounting_period_config(cfg)
        if not force and not is_eom(as_of_date, period_cfg):
            return {"status": "skipped", "reason": "not_month_end"}

        month_bounds = get_month_period_bounds(as_of_date, period_cfg)
        period_end = month_bounds.end_date
        period_key = period_end.strftime("%Y-%m")
        event_id = f"PNL_CLOSE:{period_key}"
        event_tag = "MONTH_END_PNL"

        eq = resolve_accounting_equity_config(cfg)
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            if repo.get_active_journal_header(event_id, event_tag):
                return {"status": "skipped", "reason": "already_posted", "event_id": event_id}

            cye = repo.get_account_id_by_code(eq.current_year_earnings_account_code)
            if not cye:
                raise ValueError(
                    f"Current year earnings account {eq.current_year_earnings_account_code!r} "
                    "not found or inactive — create it or set accounting_equity.current_year_earnings_account_code."
                )

            ie = repo.get_balances_by_category(["INCOME", "EXPENSE"], end_date=period_end)
            lines = build_month_end_pnl_close_lines(
                ie_balances=ie,
                cye_account_id=str(cye["id"]),
            )
            if not lines:
                return {"status": "skipped", "reason": "no_balances"}

            assert_journal_lines_balanced(
                lines,
                context=f"post_month_end_pnl_close_to_cye({period_key!r})",
            )
            self._validate_not_posting_to_parent_after_transition(conn, period_end, lines)

            repo.save_journal_entry(
                period_end,
                f"PNL-CLOSE-{period_key}",
                f"Month-end P&L close to current year earnings ({period_key})",
                event_id,
                event_tag,
                created_by,
                lines,
                posting_policy="standard",
                gl_anchor_date=_get_system_business_date_strict(),
            )
            return {"status": "posted", "event_id": event_id, "period_key": period_key}
        finally:
            conn.close()

    def post_year_end_cye_to_re(
        self,
        as_of_date: date,
        *,
        created_by: str = "system",
        system_config: dict[str, Any] | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """
        On fiscal year-end, transfer **current year earnings** balance to **retained earnings**
        (idempotent per fiscal year-end date).
        """
        cfg = system_config if system_config is not None else (load_system_config_from_db() or {})
        period_cfg = normalize_accounting_period_config(cfg)
        if not force and not is_eoy(as_of_date, period_cfg):
            return {"status": "skipped", "reason": "not_year_end"}

        year_bounds = get_year_period_bounds(as_of_date, period_cfg)
        fy_end = year_bounds.end_date
        event_id = f"CYE_TO_RE:{fy_end.isoformat()}"
        event_tag = "YEAR_END_EQUITY"

        eq = resolve_accounting_equity_config(cfg)
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            if repo.get_active_journal_header(event_id, event_tag):
                return {"status": "skipped", "reason": "already_posted", "event_id": event_id}

            cye_acc = repo.get_account_id_by_code(eq.current_year_earnings_account_code)
            re_acc = repo.get_account_id_by_code(eq.retained_earnings_account_code)
            if not cye_acc or not re_acc:
                raise ValueError(
                    "Missing equity accounts for year-end close — check "
                    "accounting_equity.current_year_earnings_account_code and "
                    "retained_earnings_account_code exist and are active."
                )

            eq_rows = repo.get_balances_by_category(["EQUITY"], end_date=fy_end)
            cye_code = eq.current_year_earnings_account_code.upper()
            cye_row = next(
                (r for r in (eq_rows or []) if str(r.get("code") or "").strip().upper() == cye_code),
                None,
            )
            if not cye_row:
                raise ValueError(f"Could not load balance for account {cye_code!r}.")

            d = as_10dp(Decimal(str(cye_row.get("debit") or 0)))
            c = as_10dp(Decimal(str(cye_row.get("credit") or 0)))
            net = as_10dp(c - d)
            if net == 0:
                return {"status": "skipped", "reason": "cye_zero", "event_id": event_id}

            if net > 0:
                lines = [
                    {
                        "account_id": str(cye_acc["id"]),
                        "debit": net,
                        "credit": Decimal("0"),
                        "memo": "Year-end: transfer current year earnings to retained earnings",
                    },
                    {
                        "account_id": str(re_acc["id"]),
                        "debit": Decimal("0"),
                        "credit": net,
                        "memo": "Year-end: transfer from current year earnings",
                    },
                ]
            else:
                amt = as_10dp(-net)
                lines = [
                    {
                        "account_id": str(cye_acc["id"]),
                        "debit": Decimal("0"),
                        "credit": amt,
                        "memo": "Year-end: transfer current year earnings deficit to retained earnings",
                    },
                    {
                        "account_id": str(re_acc["id"]),
                        "debit": amt,
                        "credit": Decimal("0"),
                        "memo": "Year-end: transfer from current year earnings",
                    },
                ]

            assert_journal_lines_balanced(
                lines,
                context=f"post_year_end_cye_to_re({fy_end.isoformat()!r})",
            )
            self._validate_not_posting_to_parent_after_transition(conn, fy_end, lines)

            repo.save_journal_entry(
                fy_end,
                f"CYE-TO-RE-{fy_end.isoformat()}",
                f"Fiscal year-end: current year earnings to retained earnings ({fy_end.isoformat()})",
                event_id,
                event_tag,
                created_by,
                lines,
                posting_policy="standard",
                gl_anchor_date=_get_system_business_date_strict(),
            )
            return {"status": "posted", "event_id": event_id, "fiscal_year_end": str(fy_end)}
        finally:
            conn.close()

    def get_statement_of_changes_in_equity(self, start_date: date, end_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            # Equity categories
            balances = repo.get_balances_by_category(['EQUITY'], start_date, end_date)
            return balances
        finally:
            conn.close()

    def get_cash_flow_statement(self, start_date: date, end_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            balances = repo.get_balances_by_category(['ASSET', 'LIABILITY'], start_date, end_date)
            pnl = repo.get_balances_by_category(
                ["INCOME", "EXPENSE"],
                start_date,
                end_date,
                exclude_event_tags=PNL_REPORT_EXCLUDED_EVENT_TAGS,
            )
            return {"balances": balances, "pnl": pnl}
        finally:
            conn.close()

    def list_statement_snapshots(
        self,
        *,
        statement_type: str | None = None,
        period_type: str | None = None,
        period_end_date_from=None,
        period_end_date_to=None,
        limit: int = 200,
    ):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_statement_snapshots(
                statement_type=statement_type,
                period_type=period_type,
                period_end_date_from=period_end_date_from,
                period_end_date_to=period_end_date_to,
                limit=limit,
            )
        finally:
            conn.close()

    def get_statement_snapshot_with_lines(self, snapshot_id: str):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM financial_statement_snapshots WHERE id = %s",
                    (snapshot_id,),
                )
                header = cur.fetchone()
            if not header:
                return None
            lines = repo.get_statement_snapshot_lines(snapshot_id)
            return {"header": header, "lines": lines}
        finally:
            conn.close()

    def _rows_to_snapshot_lines(self, rows, *, mode: str):
        lines = []
        for row in rows or []:
            debit = Decimal(str(row.get("debit") or 0))
            credit = Decimal(str(row.get("credit") or 0))
            if mode == "trial_balance":
                amount = debit - credit
            elif mode == "income_expense":
                amount = (credit - debit) if row.get("category") == "INCOME" else (debit - credit)
            elif mode == "balance_sheet":
                amount = (debit - credit) if row.get("category") == "ASSET" else (credit - debit)
            elif mode == "equity":
                amount = credit - debit
            else:
                amount = debit - credit
            lines.append(
                {
                    "line_code": row.get("code"),
                    "line_name": row.get("name") or "",
                    "line_category": row.get("category"),
                    "debit": debit,
                    "credit": credit,
                    "amount": amount,
                    "payload": {},
                }
            )
        return lines

    def save_period_close_snapshots(self, *, as_of_date: date, generated_by: str = "system"):
        # v2: P&L and cash-flow P&L legs exclude MONTH_END_PNL (economic activity); TB/BS unchanged.
        snapshot_calc_pl_cf = "v2"
        system_cfg = load_system_config_from_db() or {}
        period_cfg = normalize_accounting_period_config(system_cfg)
        month_bounds = get_month_period_bounds(as_of_date, period_cfg)
        year_bounds = get_year_period_bounds(as_of_date, period_cfg)
        is_month_close = as_of_date == month_bounds.end_date
        is_year_close = as_of_date == year_bounds.end_date

        if not is_month_close and not is_year_close:
            return {"saved": [], "skipped": True}

        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            saved = []

            def _save_set(period_type: str, start_date: date, end_date: date):
                period_label = {"period_type": period_type, "period_start": start_date, "period_end": end_date}

                tb = repo.get_trial_balance(end_date)
                repo.create_statement_snapshot(
                    statement_type="TRIAL_BALANCE",
                    period_type=period_type,
                    period_start_date=start_date,
                    period_end_date=end_date,
                    source_ledger_cutoff_date=end_date,
                    generated_by=generated_by,
                    lines=self._rows_to_snapshot_lines(tb, mode="trial_balance"),
                )
                saved.append({"statement_type": "TRIAL_BALANCE", **period_label})

                pl = repo.get_balances_by_category(
                    ["INCOME", "EXPENSE"],
                    start_date,
                    end_date,
                    exclude_event_tags=PNL_REPORT_EXCLUDED_EVENT_TAGS,
                )
                repo.create_statement_snapshot(
                    statement_type="PROFIT_AND_LOSS",
                    period_type=period_type,
                    period_start_date=start_date,
                    period_end_date=end_date,
                    source_ledger_cutoff_date=end_date,
                    generated_by=generated_by,
                    calculation_version=snapshot_calc_pl_cf,
                    lines=self._rows_to_snapshot_lines(pl, mode="income_expense"),
                )
                saved.append({"statement_type": "PROFIT_AND_LOSS", **period_label})

                bs = repo.get_balances_by_category(["ASSET", "LIABILITY", "EQUITY"], end_date=end_date)
                repo.create_statement_snapshot(
                    statement_type="BALANCE_SHEET",
                    period_type=period_type,
                    period_start_date=start_date,
                    period_end_date=end_date,
                    source_ledger_cutoff_date=end_date,
                    generated_by=generated_by,
                    lines=self._rows_to_snapshot_lines(bs, mode="balance_sheet"),
                )
                saved.append({"statement_type": "BALANCE_SHEET", **period_label})

                equity = repo.get_balances_by_category(["EQUITY"], start_date, end_date)
                repo.create_statement_snapshot(
                    statement_type="CHANGES_IN_EQUITY",
                    period_type=period_type,
                    period_start_date=start_date,
                    period_end_date=end_date,
                    source_ledger_cutoff_date=end_date,
                    generated_by=generated_by,
                    lines=self._rows_to_snapshot_lines(equity, mode="equity"),
                )
                saved.append({"statement_type": "CHANGES_IN_EQUITY", **period_label})

                cf_bal = repo.get_balances_by_category(["ASSET", "LIABILITY"], start_date, end_date)
                cf_pnl = repo.get_balances_by_category(
                    ["INCOME", "EXPENSE"],
                    start_date,
                    end_date,
                    exclude_event_tags=PNL_REPORT_EXCLUDED_EVENT_TAGS,
                )
                cf_lines = self._rows_to_snapshot_lines(cf_bal, mode="trial_balance")
                cf_lines.extend(self._rows_to_snapshot_lines(cf_pnl, mode="income_expense"))
                repo.create_statement_snapshot(
                    statement_type="CASH_FLOW",
                    period_type=period_type,
                    period_start_date=start_date,
                    period_end_date=end_date,
                    source_ledger_cutoff_date=end_date,
                    generated_by=generated_by,
                    calculation_version=snapshot_calc_pl_cf,
                    lines=cf_lines,
                )
                saved.append({"statement_type": "CASH_FLOW", **period_label})

            if is_month_close:
                _save_set("MONTH", month_bounds.start_date, month_bounds.end_date)
            if is_year_close:
                _save_set("YEAR", year_bounds.start_date, year_bounds.end_date)

            return {"saved": saved, "skipped": False}
        finally:
            conn.close()

    # Receipt GL mapping helpers

    def is_receipt_gl_mapping_initialized(self) -> bool:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.is_receipt_gl_mapping_initialized()
        finally:
            conn.close()

    def _build_default_receipt_gl_mappings(self):
        """Bundled defaults: `accounting_defaults/receipt_gl_mapping.json` or built-in."""
        return get_default_receipt_gl_mapping_tuples()

    def initialize_default_receipt_gl_mappings(self) -> bool:
        """
        Load default allocation→event mappings. Only runs if table is empty.
        Returns True if defaults were loaded, False if already initialized.
        """
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            if repo.is_receipt_gl_mapping_initialized():
                return False
            repo.initialize_default_receipt_gl_mappings(self._build_default_receipt_gl_mappings())
            return True
        finally:
            conn.close()

    def reset_receipt_gl_mappings_to_defaults(self) -> None:
        """
        Clear all mappings and reload defaults. Use to see updated default definitions.
        """
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.reset_receipt_gl_mappings(self._build_default_receipt_gl_mappings())
        finally:
            conn.close()

    def list_receipt_gl_mappings(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_receipt_gl_mappings()
        finally:
            conn.close()

    def upsert_receipt_gl_mapping(
        self,
        *,
        mapping_id=None,
        trigger_source,
        allocation_key,
        event_type,
        amount_source,
        amount_sign=1,
        is_active=True,
        priority=100,
    ):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.upsert_receipt_gl_mapping(
                mapping_id=mapping_id,
                trigger_source=trigger_source,
                allocation_key=allocation_key,
                event_type=event_type,
                amount_source=amount_source,
                amount_sign=amount_sign,
                is_active=is_active,
                priority=priority,
            )
        finally:
            conn.close()

    def delete_receipt_gl_mapping(self, mapping_id: int):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.delete_receipt_gl_mapping(mapping_id)
        finally:
            conn.close()

    def get_account_hybrid_balance(self, account_code: str, start_date: date, end_date: date):
        """
        Service wrapper for the hybrid balance / hierarchy report for a given account code.
        """
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_account_hybrid_balance(account_code, start_date, end_date)
        finally:
            conn.close()

    def simulate_event(
        self,
        event_type: str,
        amount: Decimal = None,
        payload: dict = None,
        is_reversal: bool = False,
        loan_id: int | None = None,
        repayment_id: int | None = None,
    ):
        """
        Dry-run journal lines for an event. Does **not** persist.

        If totals differ at **2dp** (after per-line 10dp), returns ``balanced=False`` and a **warning**.
        ``post_event`` uses the same rule and **raises** to avoid posting.
        """
        if payload is None:
            payload = {}
        payload = dict(payload)
        if loan_id is not None:
            payload = _merge_cash_gl_into_payload(int(loan_id), repayment_id, payload)

        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            templates = repo.get_transaction_templates(event_type)
            if not templates:
                return JournalSimulationResult.empty()

            overrides = payload.get("account_overrides")
            if not isinstance(overrides, dict):
                overrides = {}

            lines: list[dict] = []
            for tmpl in templates:
                account = repo.resolve_posting_account_for_tag(
                    tmpl["system_tag"],
                    loan_id=loan_id,
                    account_overrides=overrides,
                )
                account_name = account["name"] if account else f"Missing Account ({tmpl['system_tag']})"
                account_code = account["code"] if account else "???"

                line_amount = payload.get(tmpl["system_tag"], amount)
                if line_amount is None:
                    line_amount = Decimal("0.0")
                else:
                    line_amount = as_10dp(Decimal(str(line_amount)))

                direction = tmpl["direction"]
                if is_reversal:
                    direction = "CREDIT" if direction == "DEBIT" else "DEBIT"

                debit = line_amount if direction == "DEBIT" else Decimal("0.0")
                credit = line_amount if direction == "CREDIT" else Decimal("0.0")

                if debit > 0 or credit > 0:
                    lines.append({
                        "account_name": account_name,
                        "account_code": account_code,
                        "debit": debit,
                        "credit": credit,
                        "memo": tmpl["description"]
                    })
            if not lines:
                return JournalSimulationResult.empty()

            td, tc = journal_lines_balance_totals(lines)
            balanced = journal_totals_balanced_for_posting(td, tc)
            warning = None
            if not balanced:
                warning = (
                    f"Double-entry check failed at 2dp: total debits {td} ≠ total credits {tc} "
                    f"(as 2dp: {as_2dp(td)} vs {as_2dp(tc)}). "
                    "Posting would be blocked — fix amounts or templates before approval."
                )
            return JournalSimulationResult(
                lines=lines,
                balanced=balanced,
                total_debit=td,
                total_credit=tc,
                warning=warning,
            )
        finally:
            conn.close()

    def post_event(
        self,
        event_type: str,
        reference: str,
        description: str,
        event_id: str,
        created_by: str,
        entry_date: date = None,
        amount: Decimal = None,
        payload: dict = None,
        is_reversal: bool = False,
        loan_id: int | None = None,
        repayment_id: int | None = None,
        posting_policy: str | None = None,
    ):
        policy = posting_policy or get_gl_posting_policy()
        anchor_date = _get_system_business_date_strict() if policy == "standard" else None
        if entry_date is None:
            entry_date = anchor_date if anchor_date is not None else _get_system_business_date_strict()
        
        if payload is None:
            payload = {}
        payload = dict(payload)
        if loan_id is not None:
            payload = _merge_cash_gl_into_payload(int(loan_id), repayment_id, payload)
        overrides = payload.get("account_overrides")
        if not isinstance(overrides, dict):
            overrides = {}
        
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            templates = repo.get_transaction_templates(event_type)
            if not templates:
                return
            
            lines = []
            for tmpl in templates:
                account = repo.resolve_posting_account_for_tag(
                    tmpl["system_tag"],
                    loan_id=loan_id,
                    account_overrides=overrides,
                )
                if not account:
                    raise ValueError(f"Account not found for system tag: {tmpl['system_tag']}")
                
                line_amount = payload.get(tmpl["system_tag"], amount)
                if line_amount is None:
                    line_amount = Decimal("0.0")
                else:
                    line_amount = as_10dp(Decimal(str(line_amount)))

                direction = tmpl["direction"]
                if is_reversal:
                    direction = "CREDIT" if direction == "DEBIT" else "DEBIT"

                debit = line_amount if direction == "DEBIT" else Decimal("0.0")
                credit = line_amount if direction == "CREDIT" else Decimal("0.0")

                if debit > 0 or credit > 0:
                    lines.append({
                        "account_id": account["id"],
                        "debit": debit,
                        "credit": credit,
                        "memo": tmpl["description"] or description
                    })

            # Prevent duplicate journal_items within a single journal header.
            # This can happen if `transaction_templates` contains duplicate rows
            # for the same event_type/system_tag/direction.
            if lines:
                seen: set[tuple[object, object, object, object]] = set()
                deduped_lines = []
                for line in lines:
                    key = (
                        line["account_id"],
                        line.get("debit", Decimal("0.0")),
                        line.get("credit", Decimal("0.0")),
                        line.get("memo"),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped_lines.append(line)
                lines = deduped_lines

            if lines:
                assert_journal_lines_balanced(
                    lines,
                    context=f"AccountingService.post_event({event_type!r})",
                )

            # Defensive check: ensure we are not posting to parent accounts
            # after they have transitioned to parent mode. The database trigger
            # will enforce this, but we fail fast here for clearer error
            # messages at the service layer.
            if lines:
                self._validate_not_posting_to_parent_after_transition(conn, entry_date, lines)

            if lines:
                repo.save_journal_entry(
                    entry_date,
                    reference,
                    description,
                    event_id,
                    event_type,
                    created_by,
                    lines,
                    posting_policy=policy,
                    gl_anchor_date=anchor_date,
                    do_commit=False,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def bulk_post_events(
        self,
        items: list[dict[str, Any]],
        *,
        posting_policy: str | None = None,
    ) -> None:
        """
        Post many GL events in a single DB transaction (one commit at end).

        Each item dict supports the same keys as post_event: event_type, reference,
        description, event_id, created_by, entry_date (optional), amount (optional),
        payload (optional), is_reversal (optional), loan_id, repayment_id (optional).
        Skips items whose event_type has no transaction templates (same as post_event).
        """
        if not items:
            return
        policy = posting_policy or get_gl_posting_policy()
        anchor_date = _get_system_business_date_strict() if policy == "standard" else None
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            templates_cache: dict[str, list[dict[str, Any]]] = {}
            needs_cash_merge_by_event: dict[str, bool] = {}
            # Only cache account resolution when there are no overrides; override maps can vary per item.
            account_cache: dict[tuple[str, int | None], dict[str, Any] | None] = {}
            journal_entries: list[dict[str, Any]] = []
            for item in items:
                event_type = item["event_type"]
                reference = item["reference"]
                description = item["description"]
                event_id = item["event_id"]
                created_by = item.get("created_by") or "system"
                entry_date = item.get("entry_date")
                if entry_date is None:
                    entry_date = anchor_date if anchor_date is not None else _get_system_business_date_strict()
                amount = item.get("amount")
                if amount is not None and not isinstance(amount, Decimal):
                    amount = Decimal(str(amount))
                payload = item.get("payload")
                if payload is None:
                    payload = {}
                payload = dict(payload)
                loan_id = item.get("loan_id")
                repayment_id = item.get("repayment_id")
                is_reversal = bool(item.get("is_reversal", False))
                overrides = payload.get("account_overrides")
                if not isinstance(overrides, dict):
                    overrides = {}

                templates = templates_cache.get(event_type)
                if templates is None:
                    templates = repo.get_transaction_templates(event_type)
                    templates_cache[event_type] = templates
                if not templates:
                    continue

                need_cash = needs_cash_merge_by_event.get(event_type)
                if need_cash is None:
                    need_cash = any((t.get("system_tag") == "cash_operating") for t in templates)
                    needs_cash_merge_by_event[event_type] = need_cash
                if need_cash and loan_id is not None:
                    # Only do the extra DB lookup when templates actually require cash_operating.
                    payload = _merge_cash_gl_into_payload(int(loan_id), repayment_id, payload)
                    overrides = payload.get("account_overrides")
                    if not isinstance(overrides, dict):
                        overrides = {}

                lines: list[dict[str, Any]] = []
                for tmpl in templates:
                    sys_tag = tmpl["system_tag"]
                    # Cache only when overrides empty (stable resolution path).
                    if overrides:
                        account = repo.resolve_posting_account_for_tag(
                            sys_tag,
                            loan_id=loan_id,
                            account_overrides=overrides,
                        )
                    else:
                        ck = (str(sys_tag), int(loan_id) if loan_id is not None else None)
                        if ck in account_cache:
                            account = account_cache[ck]
                        else:
                            account = repo.resolve_posting_account_for_tag(
                                sys_tag,
                                loan_id=loan_id,
                                account_overrides={},
                            )
                            account_cache[ck] = account
                    if not account:
                        raise ValueError(f"Account not found for system tag: {tmpl['system_tag']}")

                    line_amount = payload.get(sys_tag, amount)
                    if line_amount is None:
                        line_amount = Decimal("0.0")
                    else:
                        line_amount = as_10dp(Decimal(str(line_amount)))

                    direction = tmpl["direction"]
                    if is_reversal:
                        direction = "CREDIT" if direction == "DEBIT" else "DEBIT"

                    debit = line_amount if direction == "DEBIT" else Decimal("0.0")
                    credit = line_amount if direction == "CREDIT" else Decimal("0.0")

                    if debit > 0 or credit > 0:
                        lines.append(
                            {
                                "account_id": account["id"],
                                "debit": debit,
                                "credit": credit,
                                "memo": tmpl["description"] or description,
                            }
                        )

                if lines:
                    seen: set[tuple[object, object, object, object]] = set()
                    deduped_lines = []
                    for line in lines:
                        key = (
                            line["account_id"],
                            line.get("debit", Decimal("0.0")),
                            line.get("credit", Decimal("0.0")),
                            line.get("memo"),
                        )
                        if key in seen:
                            continue
                        seen.add(key)
                        deduped_lines.append(line)
                    lines = deduped_lines

                if lines:
                    assert_journal_lines_balanced(
                        lines,
                        context=f"AccountingService.bulk_post_events({event_type!r})",
                    )
                    self._validate_not_posting_to_parent_after_transition(conn, entry_date, lines)
                    journal_entries.append(
                        {
                            "entry_date": entry_date,
                            "reference": reference,
                            "description": description,
                            "event_id": event_id,
                            "event_tag": event_type,
                            "created_by": created_by,
                            "lines": lines,
                        }
                    )
            if journal_entries:
                repo.bulk_save_journal_entries(
                    journal_entries,
                    posting_policy=policy,
                    gl_anchor_date=anchor_date,
                    do_commit=False,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _validate_not_posting_to_parent_after_transition(self, conn, entry_date: date, lines):
        """
        Service‑level guard that mirrors the database trigger preventing
        postings to parent accounts after their transition timestamp.
        """
        if not lines:
            return

        account_ids = [line["account_id"] for line in lines]

        try:
            with conn.cursor() as cur:
                # Support both integer and UUID primary keys on accounts.
                # Cast parameter to uuid[] to satisfy operators when id is uuid.
                cur.execute(
                    """
                    SELECT id, code, is_parent, transitioned_to_parent_at
                    FROM accounts
                    WHERE id = ANY(%s::uuid[])
                    """,
                    (account_ids,),
                )
                rows = cur.fetchall()
        except psycopg2.errors.UndefinedColumn:
            # Backwards‑compatibility: older schemas may not yet have the
            # is_parent / transitioned_to_parent_at columns. In that case,
            # skip this guard rather than failing the entire posting.
            return
        except psycopg2.errors.UndefinedFunction:
            # Backwards‑compatibility for non‑UUID schemas (e.g. integer ids);
            # fall back to a generic ANY() without casting.
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, code, is_parent, transitioned_to_parent_at
                    FROM accounts
                    WHERE id = ANY(%s)
                    """,
                    (account_ids,),
                )
                rows = cur.fetchall()

        accounts = {row["id"]: row for row in rows}

        # Compare using a date boundary; transitioned_to_parent_at is a timestamp.
        for line in lines:
            acc = accounts.get(line["account_id"])
            if not acc:
                continue
            if acc["is_parent"] and acc["transitioned_to_parent_at"] is not None:
                # If the journal entry date is after the transition timestamp's date,
                # treat it as a forbidden posting.
                if entry_date > acc["transitioned_to_parent_at"].date():
                    raise ValueError(
                        f"Account {acc['code']} is a parent and cannot accept postings "
                        f"after {acc['transitioned_to_parent_at'].date()}."
                    )

    def convert_to_parent(self, account_id: str) -> bool:
        """
        Service wrapper for the convert_to_parent(account_id) helper, which
        transitions a standalone account into a parent account.
        """
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.convert_to_parent(account_id)
            return True
        finally:
            conn.close()
