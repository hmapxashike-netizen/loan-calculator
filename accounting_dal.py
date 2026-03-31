import json
from datetime import date
from decimal import Decimal

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2.extras import RealDictCursor

from config import get_database_url
from decimal_utils import amounts_equal_at_2dp, as_10dp, as_2dp
from accounting_core import (
    assert_coa_grandchild_matches_parent,
    build_coa_path_label,
    coa_grandchild_prefix_matches_immediate_parent,
    split_account_code,
)


def journal_lines_balance_totals(lines: list[dict]) -> tuple[Decimal, Decimal]:
    """
    Sum debits and credits for a set of journal line dicts (10dp per line).
    Lines use 'debit' / 'credit' keys as in posting or GL views.
    """
    total_d = Decimal("0")
    total_c = Decimal("0")
    for line in lines:
        total_d += as_10dp(line.get("debit") or 0)
        total_c += as_10dp(line.get("credit") or 0)
    return total_d, total_c


def journal_totals_balanced_for_posting(td: Decimal, tc: Decimal) -> bool:
    """
    True if debit/credit totals are treated as balanced (per-line 10dp sums, then 2dp material check).
    Differences that vanish at 2dp are ignored.
    """
    return amounts_equal_at_2dp(td, tc)


def is_journal_double_entry_balanced(lines: list[dict]) -> bool:
    """True when sum(debits) and sum(credits) match at 2dp after per-line 10dp amounts."""
    if not lines:
        return True
    td, tc = journal_lines_balance_totals(lines)
    return journal_totals_balanced_for_posting(td, tc)


def assert_journal_lines_balanced(lines: list[dict], *, context: str) -> None:
    """
    Enforce double-entry for posting: per-line 10dp, then totals must match at **2dp**.
    Raises ValueError when imbalance is material at 2dp.
    """
    if not lines:
        return
    td, tc = journal_lines_balance_totals(lines)
    if not journal_totals_balanced_for_posting(td, tc):
        raise ValueError(
            f"{context}: journal not balanced at 2dp — total debits {td} != total credits {tc} "
            f"(as 2dp: {as_2dp(td)} vs {as_2dp(tc)})"
        )


def get_conn():
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)

class AccountingRepository:
    def __init__(self, conn):
        self.conn = conn

    def is_transaction_templates_initialized(self) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM transaction_templates")
            return cur.fetchone()["count"] > 0

    def initialize_default_transaction_templates(self, templates) -> None:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM transaction_templates")
            for evt, tag, side, desc, trigger_type in templates:
                cur.execute("""
                    INSERT INTO transaction_templates (event_type, system_tag, direction, description, trigger_type)
                    VALUES (%s, %s, %s, %s, %s)
                """, (evt, tag, side, desc, trigger_type))
        self.conn.commit()

    def is_coa_initialized(self) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM accounts")
            return cur.fetchone()["count"] > 0

    def replace_account_template_rows(self, rows: list) -> None:
        """
        Replace account_template content (code, name, category, system_tag, parent_code).
        Used so Initialize COA matches bundled / exported chart defaults.
        """
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM account_template")
            for code, name, cat, tag, parent in rows:
                cur.execute(
                    """
                    INSERT INTO account_template (code, name, category, system_tag, parent_code)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (code, name, cat, tag, parent),
                )
        self.conn.commit()

    def initialize_default_coa(self) -> None:
        from accounting_defaults_loader import get_chart_account_template_tuples

        rows = get_chart_account_template_tuples()
        self.replace_account_template_rows(rows)
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO accounts (code, name, category, system_tag, is_active)
                SELECT code, name, category, system_tag, is_active
                FROM account_template
            """)
            cur.execute("""
                UPDATE accounts a
                SET parent_id = p.id
                FROM account_template t
                JOIN accounts p ON p.code = t.parent_code
                WHERE a.code = t.code AND t.parent_code IS NOT NULL
            """)
        self.conn.commit()

    def create_account(
        self,
        code,
        name,
        category,
        system_tag=None,
        parent_id=None,
        subaccount_resolution=None,
    ):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM accounts WHERE code = %s", (code,))
            if cur.fetchone():
                raise ValueError(
                    f"Account code {code!r} already exists. Choose another code or use the existing account."
                )
            try:
                cur.execute(
                    """
                    INSERT INTO accounts (code, name, category, system_tag, parent_id, is_active, subaccount_resolution)
                    VALUES (%s, %s, %s, %s, %s, TRUE, %s)
                    """,
                    (code, name, category, system_tag, parent_id, subaccount_resolution),
                )
            except pg_errors.UniqueViolation as e:
                self.conn.rollback()
                raise ValueError(
                    f"Account code {code!r} already exists. Choose another code or use the existing account."
                ) from e
        self.conn.commit()

    def get_account_subtree_ids(self, root_id) -> list:
        """
        Return account id for root_id and every descendant (recursive children).
        Used to prevent assigning a parent that would create a cycle in the hierarchy.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH RECURSIVE sub AS (
                    SELECT id FROM accounts WHERE id = %s
                    UNION ALL
                    SELECT c.id FROM accounts c
                    INNER JOIN sub ON c.parent_id = sub.id
                )
                SELECT id FROM sub
                """,
                (root_id,),
            )
            rows = cur.fetchall()
        return [r["id"] for r in rows]

    def update_account_parent(self, account_id, parent_id=None) -> None:
        """Set parent_id for an existing account. parent_id=None clears the parent (top-level)."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM accounts WHERE id = %s", (account_id,))
            if not cur.fetchone():
                raise ValueError("Account not found.")
            if parent_id is not None:
                cur.execute("SELECT id FROM accounts WHERE id = %s", (parent_id,))
                if not cur.fetchone():
                    raise ValueError("Parent account not found.")
            subtree = self.get_account_subtree_ids(account_id)
            forbidden = {str(x) for x in subtree}
            if parent_id is not None and str(parent_id) in forbidden:
                raise ValueError(
                    "Invalid parent: cannot set parent to this account or any of its descendants "
                    "(that would create a cycle in the chart)."
                )
            cur.execute(
                "UPDATE accounts SET parent_id = %s WHERE id = %s",
                (parent_id, account_id),
            )
        self.conn.commit()

    def get_account_by_tag(self, system_tag: str):
        """
        Resolve a system_tag to a posting (leaf) account.
        Raises ValueError if the tag points to a parent/non‑posting account.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM accounts WHERE system_tag = %s AND is_active = TRUE",
                (system_tag,),
            )
            account = cur.fetchone()
            if not account:
                return None

            # A parent account has one or more active children; treat it as non‑posting.
            cur.execute(
                "SELECT 1 FROM accounts WHERE parent_id = %s AND is_active = TRUE LIMIT 1",
                (account["id"],),
            )
            if cur.fetchone():
                raise ValueError(
                    f"System tag '{system_tag}' is mapped to parent account {account['code']} which cannot accept postings. "
                    "Map this tag to a posting (child) account instead."
                )

            return account

    def _account_has_active_children(self, account_id) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM accounts WHERE parent_id = %s AND is_active = TRUE LIMIT 1",
                (account_id,),
            )
            return cur.fetchone() is not None

    def get_account_by_id(self, account_id):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM accounts WHERE id = %s AND is_active = TRUE",
                (account_id,),
            )
            return cur.fetchone()

    def fetch_account_row_for_system_tag(self, system_tag: str):
        """Return the active account row tagged with system_tag (may be a parent with children)."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM accounts
                WHERE system_tag = %s AND is_active = TRUE
                ORDER BY code
                LIMIT 1
                """,
                (system_tag,),
            )
            return cur.fetchone()

    def list_active_direct_children_accounts(self, parent_id):
        """Posting candidates: active direct children of a parent account, ordered by code."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, code, name
                FROM accounts
                WHERE parent_id = %s AND is_active = TRUE
                ORDER BY code
                """,
                (parent_id,),
            )
            return list(cur.fetchall() or [])

    def assert_account_is_posting_leaf(self, account: dict) -> None:
        """Raise if account has active children (cannot post to roll-up parents)."""
        if self._account_has_active_children(account["id"]):
            raise ValueError(
                f"Account {account.get('code')} cannot accept postings: it has active child accounts."
            )

    def resolve_posting_account_for_tag(
        self,
        system_tag: str,
        *,
        loan_id: int | None = None,
        account_overrides: dict | None = None,
    ):
        """
        Resolve template system_tag to a single posting (leaf) account row.

        Backward compatible:
        - No active children on tagged account → same as get_account_by_tag.
        - Active children + subaccount_resolution NULL → same error as get_account_by_tag.
        - Active children + PRODUCT / LOAN_CAPTURE → use maps + loan_id.
        - account_overrides: { system_tag: account_uuid_str } wins first.
        """
        overrides = account_overrides or {}
        oid = overrides.get(system_tag)
        if oid:
            acc = self.get_account_by_id(oid)
            if not acc:
                raise ValueError(
                    f"account_overrides[{system_tag!r}] points to missing or inactive account id {oid!r}."
                )
            self.assert_account_is_posting_leaf(acc)
            return acc

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM accounts WHERE system_tag = %s AND is_active = TRUE",
                (system_tag,),
            )
            account = cur.fetchone()
        if not account:
            return None

        if not self._account_has_active_children(account["id"]):
            return account

        mode = (account.get("subaccount_resolution") or "").strip().upper()
        if not mode:
            raise ValueError(
                f"System tag '{system_tag}' maps to {account['code']} which has child accounts. "
                "Set **Subaccount resolution** (PRODUCT / LOAN_CAPTURE / JOURNAL) on that COA row, "
                "add maps, or pass account_overrides in the posting payload."
            )

        if mode == "JOURNAL":
            raise ValueError(
                f"Tag '{system_tag}' is configured for **JOURNAL** resolution: provide "
                f"payload['account_overrides'][{system_tag!r}] = <leaf account uuid> for automated posting."
            )

        if loan_id is None:
            raise ValueError(
                f"loan_id is required to resolve tag '{system_tag}' ({account['code']}, mode={mode})."
            )

        if mode == "LOAN_CAPTURE":
            if system_tag != "cash_operating":
                raise ValueError(
                    f"LOAN_CAPTURE resolution for {account['code']} applies to system_tag 'cash_operating' only; "
                    f"got {system_tag!r}."
                )
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cash_gl_account_id, disbursement_bank_option_id
                    FROM loans WHERE id = %s
                    """,
                    (loan_id,),
                )
                lr = cur.fetchone()
            if lr and lr.get("cash_gl_account_id"):
                leaf = self.get_account_by_id(str(lr["cash_gl_account_id"]))
                if not leaf:
                    raise ValueError(
                        f"Loan {loan_id} cash_gl_account_id points to a missing or inactive GL account."
                    )
                self.assert_account_is_posting_leaf(leaf)
                return leaf
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d.gl_account_id
                    FROM loans l
                    JOIN disbursement_bank_options d ON d.id = l.disbursement_bank_option_id
                    WHERE l.id = %s AND d.is_active = TRUE
                    LIMIT 1
                    """,
                    (loan_id,),
                )
                row = cur.fetchone()
            if not row or not row.get("gl_account_id"):
                raise ValueError(
                    f"Loan {loan_id} has no cash_gl_account_id and no active disbursement bank option; "
                    "set **loans.cash_gl_account_id** at loan capture (from **Maintenance — source cash account cache** list), "
                    "or link a legacy disbursement_bank_option_id if used."
                )
            leaf = self.get_account_by_id(row["gl_account_id"])
            if not leaf:
                raise ValueError("Disbursement bank option points to a missing or inactive GL account.")
            self.assert_account_is_posting_leaf(leaf)
            return leaf

        if mode == "PRODUCT":
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT product_code FROM loans WHERE id = %s",
                    (loan_id,),
                )
                lr = cur.fetchone()
            pc = (lr.get("product_code") or "").strip() if lr else ""
            if not pc:
                raise ValueError(
                    f"Loan {loan_id} has no product_code; cannot resolve PRODUCT subaccount for tag '{system_tag}'."
                )
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT gl_account_id FROM product_gl_subaccount_map
                    WHERE product_code = %s AND system_tag = %s
                    LIMIT 1
                    """,
                    (pc, system_tag),
                )
                m = cur.fetchone()
            if not m:
                raise ValueError(
                    f"No product_gl_subaccount_map row for product_code={pc!r} and system_tag={system_tag!r}. "
                    "Add a mapping under Accounting → Product GL subaccounts."
                )
            leaf = self.get_account_by_id(m["gl_account_id"])
            if not leaf:
                raise ValueError("Product GL map points to a missing or inactive account.")
            self.assert_account_is_posting_leaf(leaf)
            try:
                self._require_gl_row_code_matches_immediate_parent(leaf["id"])
            except ValueError as _vc:
                raise ValueError(
                    f"Product GL map for product_code={pc!r}, system_tag={system_tag!r} uses account "
                    f"{leaf.get('code')!r} whose code does not match its **immediate parent** in the COA "
                    f"(for codes like BASE-NN, the parent row must have code BASE — e.g. A100001-02 belongs "
                    f"under A100001, not under an interest leaf). Remap under Accounting → Chart of Accounts "
                    f"→ Advanced product → leaf map, or fix parent_id / code in the database. Detail: {_vc}"
                ) from _vc
            try:
                self._require_gl_row_not_ambiguous_duplicate_under_parent(leaf["id"])
            except ValueError as _vd:
                raise ValueError(
                    f"Product GL map for product_code={pc!r}, system_tag={system_tag!r} points to an ambiguous leaf "
                    f"{leaf.get('code')!r}: {_vd}. Fix duplicate sibling codes/suffixes under the same parent."
                ) from _vd
            return leaf

        raise ValueError(
            f"Unknown subaccount_resolution {account.get('subaccount_resolution')!r} on account {account['code']}."
        )

    def list_child_codes_for_parent(self, parent_id):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT code FROM accounts WHERE parent_id = %s ORDER BY code",
                (parent_id,),
            )
            return [r["code"] for r in cur.fetchall()]

    def list_codes_for_base_and_grandchildren(self, base_code: str) -> list[str]:
        """
        Return all codes that would collide with grandchild allocation for ``base_code``:
        - the base itself (BASE)
        - any grandchild codes (BASE-NN) anywhere in the chart (even if mis-parented).

        This is used by code suggestion so we never propose a code that already exists
        elsewhere due to legacy / incorrect parent_id links.
        """
        b = (base_code or "").strip().upper()
        if not b:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT code
                FROM accounts
                WHERE UPPER(TRIM(code)) = %s
                   OR UPPER(TRIM(code)) LIKE %s
                """,
                (b, f"{b}-%"),
            )
            return [r["code"] for r in (cur.fetchall() or [])]

    def _account_ids_passing_grandchild_parent_code_rule(self, account_ids: set[str]) -> set[str]:
        """Keep only accounts whose stored code matches the COA grandchild↔parent rule."""
        if not account_ids:
            return set()
        rows = self.list_accounts()
        by_id = {str(a["id"]): dict(a) for a in rows}
        good: set[str] = set()
        for aid in account_ids:
            a = by_id.get(str(aid))
            if not a:
                continue
            pid = a.get("parent_id")
            par = by_id.get(str(pid)) if pid is not None else None
            pcode = par.get("code") if par else None
            ok, _ = coa_grandchild_prefix_matches_immediate_parent(
                child_code=str(a.get("code") or ""),
                parent_code=str(pcode) if pcode else None,
            )
            if ok:
                good.add(str(aid))
        return good

    def _require_gl_row_code_matches_immediate_parent(self, gl_account_id) -> None:
        rows = self.list_accounts()
        by_id = {str(a["id"]): dict(a) for a in rows}
        acc = by_id.get(str(gl_account_id).strip())
        if not acc:
            raise ValueError("GL account not found.")
        pid = acc.get("parent_id")
        par = by_id.get(str(pid)) if pid is not None else None
        pcode = par.get("code") if par else None
        assert_coa_grandchild_matches_parent(
            child_code=str(acc.get("code") or ""),
            parent_code=str(pcode) if pcode else None,
        )

    def _require_gl_row_not_ambiguous_duplicate_under_parent(self, gl_account_id) -> None:
        """
        Guard against legacy / bad COA data where two active siblings share either:
        - the same normalized code (case/whitespace-insensitive), or
        - the same grandchild suffix under the same parent (e.g. two siblings both ending in -02).

        If duplicates exist, UI pickers must not offer them and mapping/posting must fail fast.
        """
        rows = self.list_accounts()
        by_id = {str(a["id"]): dict(a) for a in rows}
        acc = by_id.get(str(gl_account_id).strip())
        if not acc:
            raise ValueError("GL account not found.")

        pid = acc.get("parent_id")
        if pid is None:
            return

        # Only consider active siblings under the same immediate parent.
        siblings = [
            a
            for a in by_id.values()
            if a.get("is_active") is not False and str(a.get("parent_id")) == str(pid)
        ]
        if len(siblings) <= 1:
            return

        def _norm(code: str) -> str:
            return (code or "").strip().upper()

        # 1) Duplicate code under same parent (case/whitespace-insensitive).
        wanted = _norm(str(acc.get("code") or ""))
        if wanted:
            same_code = [a for a in siblings if _norm(str(a.get("code") or "")) == wanted]
            if len(same_code) > 1:
                raise ValueError(
                    f"Duplicate sibling account code under the same parent_id={pid}: {wanted!r} appears {len(same_code)} times."
                )

        # 2) Duplicate grandchild suffix under same parent (ambiguous -NN).
        try:
            _base, wanted_suffix = split_account_code(wanted)
        except Exception:
            wanted_suffix = None
        if wanted_suffix is None:
            return

        same_suffix: list[str] = []
        for a in siblings:
            c = _norm(str(a.get("code") or ""))
            try:
                _b, s = split_account_code(c)
            except Exception:
                continue
            if s is not None and s == wanted_suffix:
                same_suffix.append(c)
        if len(same_suffix) > 1:
            same_suffix.sort()
            raise ValueError(
                f"Duplicate sibling grandchild suffix -{wanted_suffix:02d} under the same parent_id={pid}. "
                f"Found: {', '.join(same_suffix[:6])}{' …' if len(same_suffix) > 6 else ''}. "
                "Fix the COA so each parent has unique -NN suffixes."
            )

    def update_account_subaccount_resolution(self, account_id, subaccount_resolution) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM accounts WHERE id = %s", (account_id,))
            if not cur.fetchone():
                raise ValueError("Account not found.")
            cur.execute(
                "UPDATE accounts SET subaccount_resolution = %s WHERE id = %s",
                (subaccount_resolution, account_id),
            )
        self.conn.commit()

    def _upsert_product_gl_subaccount_map_with_cursor(self, cur, product_code: str, system_tag: str, gl_account_id) -> None:
        pc = (product_code or "").strip()
        st = (system_tag or "").strip()
        if not pc or not st:
            raise ValueError("product_code and system_tag are required.")
        cur.execute(
            """
            INSERT INTO product_gl_subaccount_map (product_code, system_tag, gl_account_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (product_code, system_tag)
            DO UPDATE SET gl_account_id = EXCLUDED.gl_account_id
            """,
            (pc, st, gl_account_id),
        )

    def create_child_accounts_batch(
        self,
        parent_id: str,
        children: list[tuple[str, str]],
        *,
        parent_subaccount_resolution: str | None,
        product_assignments: list[tuple[str, int]] | None = None,
        parent_system_tag: str | None = None,
    ) -> list[str]:
        """
        Insert N child GL rows (code, name), inherit parent category, system_tag NULL on children.
        Set parent's subaccount_resolution. Optionally upsert product_gl_subaccount_map per (product, child index).
        Single transaction.
        """
        if not children:
            raise ValueError("At least one subaccount row is required.")
        mode = (parent_subaccount_resolution or "").strip().upper() or None
        if not mode:
            raise ValueError("Posting rule (subaccount resolution) is required when creating subaccounts.")
        new_ids: list[str] = []
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT id, code, category FROM accounts WHERE id = %s FOR UPDATE",
                    (parent_id,),
                )
                par = cur.fetchone()
                if not par:
                    raise ValueError("Parent account not found.")
                category = par["category"]
                parent_code_for_rule = str(par["code"] or "").strip()
                for code, name in children:
                    code = (code or "").strip()
                    name = (name or "").strip()
                    if not code or not name:
                        raise ValueError("Each subaccount requires a non-empty code and name.")
                    assert_coa_grandchild_matches_parent(
                        child_code=code,
                        parent_code=parent_code_for_rule,
                    )
                    cur.execute("SELECT 1 FROM accounts WHERE code = %s", (code,))
                    if cur.fetchone():
                        raise ValueError(f"Account code {code!r} already exists.")
                    cur.execute(
                        """
                        INSERT INTO accounts (code, name, category, system_tag, parent_id, is_active, subaccount_resolution)
                        VALUES (%s, %s, %s, NULL, %s, TRUE, NULL)
                        RETURNING id
                        """,
                        (code, name, category, parent_id),
                    )
                    row = cur.fetchone()
                    if not row:
                        raise ValueError("Insert failed.")
                    new_ids.append(str(row["id"]))
                cur.execute(
                    "UPDATE accounts SET subaccount_resolution = %s WHERE id = %s",
                    (mode, parent_id),
                )
                if product_assignments:
                    pst = (parent_system_tag or "").strip()
                    if not pst:
                        raise ValueError("Parent system tag is required for product mappings.")
                    for pc, idx in product_assignments:
                        if idx < 0 or idx >= len(new_ids):
                            raise ValueError("Invalid product map index.")
                        self._upsert_product_gl_subaccount_map_with_cursor(cur, pc, pst, new_ids[idx])
        except Exception:
            self.conn.rollback()
            raise
        self.conn.commit()
        return new_ids

    def update_account_name(self, account_id: str, name: str) -> None:
        name = (name or "").strip()
        if not name:
            raise ValueError("Account name is required.")
        with self.conn.cursor() as cur:
            cur.execute("UPDATE accounts SET name = %s WHERE id = %s", (name, account_id))
            if cur.rowcount == 0:
                raise ValueError("Account not found.")
        self.conn.commit()

    def update_account_code(self, account_id: str, new_code: str) -> None:
        """
        Update an account's code (admin-only operation).

        Safe because postings reference account_id, not code — but we must enforce:
        - account exists
        - account has no active children (can't rename internal nodes)
        - new code is unique (case/whitespace-insensitive)
        - grandchild codes BASE-NN match immediate parent code BASE
        """
        nc = (new_code or "").strip().upper()
        if not nc:
            raise ValueError("New code is required.")
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, code, parent_id FROM accounts WHERE id = %s FOR UPDATE",
                (account_id,),
            )
            acc = cur.fetchone()
            if not acc:
                raise ValueError("Account not found.")

            cur.execute(
                "SELECT 1 FROM accounts WHERE parent_id = %s AND is_active = TRUE LIMIT 1",
                (account_id,),
            )
            if cur.fetchone():
                raise ValueError("Cannot change code of an account that still has active children.")

            # Uniqueness (ignore case/whitespace).
            cur.execute(
                """
                SELECT 1 FROM accounts
                WHERE UPPER(TRIM(code)) = %s
                  AND id <> %s
                LIMIT 1
                """,
                (nc, account_id),
            )
            if cur.fetchone():
                raise ValueError(f"Account code {nc!r} already exists.")

            # Enforce grandchild↔parent rule (if applicable).
            pid = acc.get("parent_id")
            parent_code = None
            if pid is not None:
                cur.execute("SELECT code FROM accounts WHERE id = %s", (pid,))
                pr = cur.fetchone()
                parent_code = (pr.get("code") or "").strip().upper() if pr else None
            assert_coa_grandchild_matches_parent(child_code=nc, parent_code=parent_code)

            cur.execute("UPDATE accounts SET code = %s WHERE id = %s", (nc, account_id))
            if cur.rowcount == 0:
                raise ValueError("Update failed.")
        self.conn.commit()

    def set_account_is_active(self, account_id: str, is_active: bool) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM accounts WHERE id = %s", (account_id,))
            if not cur.fetchone():
                raise ValueError("Account not found.")
            if not is_active:
                cur.execute(
                    "SELECT 1 FROM accounts WHERE parent_id = %s AND is_active = TRUE LIMIT 1",
                    (account_id,),
                )
                if cur.fetchone():
                    raise ValueError(
                        "Cannot deactivate an account that still has active subaccounts. "
                        "Deactivate or reassign children first."
                    )
                cur.execute(
                    "DELETE FROM product_gl_subaccount_map WHERE gl_account_id = %s",
                    (account_id,),
                )
            cur.execute(
                "UPDATE accounts SET is_active = %s WHERE id = %s",
                (is_active, account_id),
            )
        self.conn.commit()

    def list_disbursement_bank_options(self, active_only: bool = True):
        with self.conn.cursor() as cur:
            if active_only:
                cur.execute(
                    """
                    SELECT d.*, a.code AS gl_account_code, a.name AS gl_account_name
                    FROM disbursement_bank_options d
                    JOIN accounts a ON a.id = d.gl_account_id
                    WHERE d.is_active = TRUE
                    ORDER BY d.sort_order, d.id
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT d.*, a.code AS gl_account_code, a.name AS gl_account_name
                    FROM disbursement_bank_options d
                    JOIN accounts a ON a.id = d.gl_account_id
                    ORDER BY d.sort_order, d.id
                    """
                )
            return cur.fetchall()

    def insert_disbursement_bank_option(self, label: str, gl_account_id, sort_order: int = 0) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO disbursement_bank_options (label, gl_account_id, sort_order)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (label, gl_account_id, sort_order),
            )
            new_id = cur.fetchone()["id"]
        self.conn.commit()
        return int(new_id)

    def set_disbursement_bank_option_active(self, option_id: int, is_active: bool) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE disbursement_bank_options SET is_active = %s, updated_at = NOW() WHERE id = %s",
                (is_active, option_id),
            )
        self.conn.commit()

    def list_product_gl_subaccount_map(self, product_code: str | None = None):
        with self.conn.cursor() as cur:
            if product_code:
                cur.execute(
                    """
                    SELECT m.*, a.code AS gl_account_code, a.name AS gl_account_name
                    FROM product_gl_subaccount_map m
                    JOIN accounts a ON a.id = m.gl_account_id
                    WHERE m.product_code = %s
                    ORDER BY m.system_tag
                    """,
                    (product_code,),
                )
            else:
                cur.execute(
                    """
                    SELECT m.*, a.code AS gl_account_code, a.name AS gl_account_name
                    FROM product_gl_subaccount_map m
                    JOIN accounts a ON a.id = m.gl_account_id
                    ORDER BY m.product_code, m.system_tag
                    """
                )
            return cur.fetchall()

    def upsert_product_gl_subaccount_map(self, product_code: str, system_tag: str, gl_account_id) -> None:
        allowed = {str(x["id"]) for x in self.list_posting_leaves_under_system_tag(system_tag)}
        gid = str(gl_account_id).strip()
        if not allowed:
            raise ValueError(
                f"No posting leaves under COA system_tag={system_tag!r}. "
                "Ensure one active tagged parent exists and (if needed) create subaccounts under it."
            )
        if gid not in allowed:
            raise ValueError(
                f"GL account id {gid!r} is not a posting leaf under the COA branch for system_tag={system_tag!r}. "
                "Pick a leaf under the same tagged parent (e.g. regular_interest_accrued under the interest "
                "accrued asset tree, not cash_operating / bank operating)."
            )
        self._require_gl_row_code_matches_immediate_parent(gl_account_id)
        self._require_gl_row_not_ambiguous_duplicate_under_parent(gl_account_id)
        with self.conn.cursor() as cur:
            self._upsert_product_gl_subaccount_map_with_cursor(cur, product_code, system_tag, gl_account_id)
        self.conn.commit()

    def delete_product_gl_subaccount_map(self, map_id: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM product_gl_subaccount_map WHERE id = %s", (map_id,))
        self.conn.commit()

    def list_accounts(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT a.*, p.code as parent_code
                FROM accounts a
                LEFT JOIN accounts p ON a.parent_id = p.id
                ORDER BY a.code
            """)
            return cur.fetchall()

    def list_posting_leaf_accounts(self) -> list[dict]:
        """
        Accounts that accept postings: active rows with **no** active children (deepest nodes only).

        If A700000 has no children, it appears alone. If it has children A71/A72/A73, only those
        branches are expanded; any branch with further children continues until leaves.

        Returns dicts: id (str), code, name, display_label (ancestor codes › … › code — name).
        Single ``list_accounts`` round-trip; O(n) in Python.
        """
        rows = self.list_accounts()
        active: list[dict] = [dict(r) for r in rows if r.get("is_active") is not False]
        by_id: dict[str, dict] = {}
        for a in active:
            aid = a.get("id")
            if aid is not None:
                by_id[str(aid)] = a

        ids_that_are_parent: set[str] = set()
        for a in active:
            p = a.get("parent_id")
            if p is not None:
                ids_that_are_parent.add(str(p))

        leaves = [a for a in active if str(a.get("id")) not in ids_that_are_parent]

        out: list[dict] = []
        for a in sorted(leaves, key=lambda x: str(x.get("code") or "")):
            aid = a.get("id")
            if aid is None:
                continue
            code = str(a.get("code") or "").strip()
            name = str(a.get("name") or "").strip()
            label, _ok = build_coa_path_label(leaf_account_id=str(aid), by_id=by_id)
            out.append({"id": str(aid), "code": code, "name": name, "display_label": label})
        return out

    def list_posting_leaves_under_system_tag(self, system_tag: str) -> list[dict]:
        """
        Posting leaves that sit under the **single** active COA row carrying ``system_tag``.

        Used for ``product_gl_subaccount_map`` and admin pickers so a tag like
        ``regular_interest_accrued`` cannot be mapped to a leaf from another branch
        (e.g. ``cash_operating`` / bank operating grandchild codes).
        """
        st = (system_tag or "").strip()
        if not st:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, code FROM accounts
                WHERE system_tag = %s AND is_active = TRUE
                ORDER BY code
                """,
                (st,),
            )
            roots = cur.fetchall() or []
        if len(roots) > 1:
            codes = ", ".join(str(r["code"]) for r in roots[:6])
            more = f" (+{len(roots) - 6} more)" if len(roots) > 6 else ""
            raise ValueError(
                f"COA must have exactly one active account with system_tag={st!r}; "
                f"found {len(roots)} ({codes}{more}). Fix duplicates before mapping products."
            )
        if not roots:
            return []
        root_id = roots[0]["id"]
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH RECURSIVE sub AS (
                    SELECT id FROM accounts WHERE id = %s
                    UNION ALL
                    SELECT c.id FROM accounts c
                    INNER JOIN sub ON c.parent_id = sub.id
                )
                SELECT a.id, a.code, a.name
                FROM accounts a
                INNER JOIN sub s ON a.id = s.id
                WHERE a.is_active = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM accounts c
                      WHERE c.parent_id = a.id AND c.is_active = TRUE
                  )
                ORDER BY a.code
                """,
                (root_id,),
            )
            rows = cur.fetchall() or []
        if not rows:
            return []
        full_leaves = self.list_posting_leaf_accounts()
        allowed_ids = {str(r["id"]) for r in rows}
        allowed_ids = self._account_ids_passing_grandchild_parent_code_rule(allowed_ids)
        out = [x for x in full_leaves if x["id"] in allowed_ids]

        # Guard: do not offer ambiguous duplicate siblings (legacy/bad COA data).
        # If a parent has duplicate normalized codes or duplicate -NN suffixes, hide those leaves from pickers.
        all_rows = self.list_accounts()
        by_id = {str(a["id"]): dict(a) for a in all_rows}

        def _norm(code: str) -> str:
            return (code or "").strip().upper()

        # Build duplicates under same parent among active children.
        active_children_by_parent: dict[str, list[dict]] = {}
        for a in by_id.values():
            if a.get("is_active") is False:
                continue
            pid = a.get("parent_id")
            if pid is None:
                continue
            active_children_by_parent.setdefault(str(pid), []).append(a)

        dup_ids: set[str] = set()
        for pid, kids in active_children_by_parent.items():
            if len(kids) <= 1:
                continue
            # (a) duplicate normalized code
            code_to_ids: dict[str, list[str]] = {}
            for k in kids:
                cid = str(k.get("id"))
                code_to_ids.setdefault(_norm(str(k.get("code") or "")), []).append(cid)
            for c, ids in code_to_ids.items():
                if c and len(ids) > 1:
                    dup_ids.update(ids)

            # (b) duplicate grandchild suffix -NN under same parent (regardless of base)
            suffix_to_ids: dict[int, list[str]] = {}
            for k in kids:
                cid = str(k.get("id"))
                c = _norm(str(k.get("code") or ""))
                try:
                    _b, s = split_account_code(c)
                except Exception:
                    continue
                if s is not None:
                    suffix_to_ids.setdefault(int(s), []).append(cid)
            for sfx, ids in suffix_to_ids.items():
                if len(ids) > 1:
                    dup_ids.update(ids)

        if dup_ids:
            out = [x for x in out if x["id"] not in dup_ids]
        out.sort(key=lambda x: str(x.get("code") or ""))
        return out

    def compute_source_cash_leaf_accounts(self, *, root_code: str = "A100000") -> list[dict]:
        """
        Build the allowed "source cash / bank" account list for loan capture and receipts.

        Rules (under ``root_code``, default A100000 — CASH AND CASH EQUIVALENTS tree):
        - If the root has **no** active child accounts, the root itself is listed (when it is a leaf).
        - Otherwise, for each **direct child** of the root (first-level branch), collect every **posting
          leaf** in that branch's subtree — i.e. active accounts with no active children (any depth).

        Returns rows: ``{"id": str(uuid), "code": str, "name": str}`` sorted by code.
        """
        root_code = (root_code or "").strip()
        if not root_code:
            return []

        rows = self.list_accounts()
        all_accts: list[dict] = [dict(r) for r in rows]

        def is_active_row(a: dict) -> bool:
            return a.get("is_active") is not False

        active = [a for a in all_accts if is_active_row(a)]
        by_id: dict[str, dict] = {str(a["id"]): a for a in active if a.get("id") is not None}

        root = next((a for a in active if (a.get("code") or "").strip() == root_code), None)
        if not root or root.get("id") is None:
            return []

        root_id = str(root["id"])

        def has_active_child(aid: str) -> bool:
            aid = str(aid)
            for a in active:
                p = a.get("parent_id")
                if p is None:
                    continue
                if str(p) == aid:
                    return True
            return False

        direct_children = [a for a in active if a.get("parent_id") is not None and str(a["parent_id"]) == root_id]

        def subtree_ids(start_id: str) -> set[str]:
            start_id = str(start_id)
            stack = [start_id]
            seen: set[str] = set()
            while stack:
                nid = stack.pop()
                if nid in seen:
                    continue
                seen.add(nid)
                for a in active:
                    if a.get("parent_id") is not None and str(a["parent_id"]) == nid:
                        stack.append(str(a["id"]))
            return seen

        out: list[dict] = []

        if not direct_children:
            if not has_active_child(root_id):
                out.append(
                    {
                        "id": root_id,
                        "code": str(root.get("code") or ""),
                        "name": str(root.get("name") or ""),
                    }
                )
        else:
            seen_ids: set[str] = set()
            for branch in sorted(direct_children, key=lambda x: str(x.get("code") or "")):
                bid = str(branch["id"])
                for nid in subtree_ids(bid):
                    if not has_active_child(nid):
                        a = by_id.get(nid)
                        if a and nid not in seen_ids:
                            seen_ids.add(nid)
                            out.append(
                                {
                                    "id": nid,
                                    "code": str(a.get("code") or ""),
                                    "name": str(a.get("name") or ""),
                                }
                            )

        out.sort(key=lambda r: r["code"])
        return out

    def is_parent_account(self, account_code: str) -> bool:
        """Return True if the given code has one or more active child accounts."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM accounts p
                JOIN accounts c ON c.parent_id = p.id AND c.is_active = TRUE
                WHERE p.code = %s
                LIMIT 1
                """,
                (account_code,),
            )
            return cur.fetchone() is not None

    def get_child_account_summaries(self, parent_code: str, start_date, end_date):
        """
        For a parent account code, return net movement per child in the date range.
        Each row: child code/name + total debit/credit for that child.
        Rolls up all postings from descendant leaves up to the immediate child level.
        Also includes a row for journals posted **directly** to the parent account ID
        (labelled “— direct to parent”) so the sum of rows matches the subtree ledger.
        Returns: code, name, ob_debit, ob_credit, period_debit, period_credit
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH RECURSIVE tree AS (
                    -- Base: immediate active children of the specified parent code
                    SELECT 
                        c.id AS top_child_id, 
                        c.code AS top_child_code, 
                        c.name AS top_child_name, 
                        c.id AS desc_id
                    FROM accounts p
                    JOIN accounts c ON c.parent_id = p.id
                    WHERE p.code = %s AND c.is_active = TRUE

                    UNION ALL

                    -- Recursive: any active child of an already-found descendant
                    SELECT 
                        t.top_child_id, 
                        t.top_child_code, 
                        t.top_child_name, 
                        a.id AS desc_id
                    FROM tree t
                    JOIN accounts a ON a.parent_id = t.desc_id
                    WHERE a.is_active = TRUE
                )
                SELECT 
                    t.top_child_code AS code, 
                    t.top_child_name AS name,
                    COALESCE(SUM(ji.debit) FILTER (WHERE je.entry_date < %s), 0) AS ob_debit,
                    COALESCE(SUM(ji.credit) FILTER (WHERE je.entry_date < %s), 0) AS ob_credit,
                    COALESCE(SUM(ji.debit) FILTER (WHERE je.entry_date >= %s AND je.entry_date <= %s), 0) AS period_debit,
                    COALESCE(SUM(ji.credit) FILTER (WHERE je.entry_date >= %s AND je.entry_date <= %s), 0) AS period_credit
                FROM tree t
                -- We join all descendants to journal items.
                LEFT JOIN journal_items ji ON ji.account_id = t.desc_id
                LEFT JOIN journal_entries je
                    ON ji.entry_id = je.id
                   AND je.status = 'POSTED'
                   AND COALESCE(je.is_active, TRUE) = TRUE
                GROUP BY t.top_child_code, t.top_child_name
                ORDER BY t.top_child_code
                """,
                (parent_code, start_date, start_date, start_date, end_date, start_date, end_date),
            )
            rows = list(cur.fetchall())

            # Postings on the parent account itself are excluded from the recursive tree above.
            cur.execute(
                """
                SELECT
                    p.code AS code,
                    p.name AS name,
                    COALESCE(SUM(ji.debit) FILTER (WHERE je.entry_date < %s), 0) AS ob_debit,
                    COALESCE(SUM(ji.credit) FILTER (WHERE je.entry_date < %s), 0) AS ob_credit,
                    COALESCE(SUM(ji.debit) FILTER (WHERE je.entry_date >= %s AND je.entry_date <= %s), 0) AS period_debit,
                    COALESCE(SUM(ji.credit) FILTER (WHERE je.entry_date >= %s AND je.entry_date <= %s), 0) AS period_credit
                FROM accounts p
                LEFT JOIN journal_items ji ON ji.account_id = p.id
                LEFT JOIN journal_entries je
                    ON ji.entry_id = je.id
                   AND je.status = 'POSTED'
                   AND COALESCE(je.is_active, TRUE) = TRUE
                WHERE p.code = %s
                GROUP BY p.code, p.name
                """,
                (start_date, start_date, start_date, end_date, start_date, end_date, parent_code),
            )
            prow = cur.fetchone()
            if prow:
                pod = float(prow.get("ob_debit") or 0)
                poc = float(prow.get("ob_credit") or 0)
                ppd = float(prow.get("period_debit") or 0)
                ppc = float(prow.get("period_credit") or 0)
                if pod or poc or ppd or ppc:
                    rows.append(
                        {
                            "code": prow["code"],
                            "name": f"{prow['name']} — direct to parent",
                            "ob_debit": prow["ob_debit"],
                            "ob_credit": prow["ob_credit"],
                            "period_debit": prow["period_debit"],
                            "period_credit": prow["period_credit"],
                        }
                    )

            rows.sort(key=lambda r: str(r.get("code") or ""))
            return rows

    def get_transaction_templates(self, event_type: str):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM transaction_templates WHERE event_type = %s", (event_type,))
            return cur.fetchall()

    def list_all_transaction_templates(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM transaction_templates ORDER BY event_type")
            return cur.fetchall()

    def link_journal(self, event_type, system_tag, direction, description, trigger_type="EVENT"):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transaction_templates (event_type, system_tag, direction, description, trigger_type)
                VALUES (%s, %s, %s, %s, %s)
            """, (event_type, system_tag, direction, description, trigger_type))
        self.conn.commit()

    def update_transaction_template(self, template_id, *, event_type=None, system_tag=None,
                                    direction=None, description=None, trigger_type=None):
        """
        Editable transaction templates: allow admin users to change mappings
        without touching code.
        """
        fields = []
        params = []
        if event_type is not None:
            fields.append("event_type = %s")
            params.append(event_type)
        if system_tag is not None:
            fields.append("system_tag = %s")
            params.append(system_tag)
        if direction is not None:
            fields.append("direction = %s")
            params.append(direction)
        if description is not None:
            fields.append("description = %s")
            params.append(description)
        if trigger_type is not None:
            fields.append("trigger_type = %s")
            params.append(trigger_type)

        if not fields:
            return

        params.append(template_id)

        with self.conn.cursor() as cur:
            cur.execute(
                f"UPDATE transaction_templates SET {', '.join(fields)} WHERE id = %s",
                tuple(params),
            )
        self.conn.commit()

    def delete_transaction_template(self, template_id):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM transaction_templates WHERE id = %s", (template_id,))
        self.conn.commit()

    # ------------------------------------------------------------
    # Receipt GL mapping (allocation_key -> accounting event)
    # ------------------------------------------------------------

    def is_receipt_gl_mapping_initialized(self) -> bool:
        with self.conn.cursor() as cur:
            try:
                cur.execute("SELECT COUNT(*) as count FROM receipt_gl_mapping")
                return cur.fetchone()["count"] > 0
            except Exception:
                return False

    def reset_receipt_gl_mappings(self, rows) -> None:
        """
        Clear all mappings and insert the given rows.
        rows: list of (trigger_source, allocation_key, event_type, amount_source, amount_sign, priority)
        """
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM receipt_gl_mapping")
            for trigger_source, allocation_key, event_type, amount_source, amount_sign, priority in rows:
                cur.execute(
                    """
                    INSERT INTO receipt_gl_mapping (
                        trigger_source, allocation_key, event_type,
                        amount_source, amount_sign, is_active, priority
                    )
                    VALUES (%s, %s, %s, %s, %s, TRUE, %s)
                    """,
                    (trigger_source, allocation_key, event_type, amount_source, amount_sign, priority),
                )
        self.conn.commit()

    def initialize_default_receipt_gl_mappings(self, rows) -> None:
        """
        Insert default allocation→event mappings. Only inserts if table is empty.
        rows: list of (trigger_source, allocation_key, event_type, amount_source, amount_sign, priority)
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM receipt_gl_mapping")
            if cur.fetchone()["count"] > 0:
                return
            for trigger_source, allocation_key, event_type, amount_source, amount_sign, priority in rows:
                cur.execute(
                    """
                    INSERT INTO receipt_gl_mapping (
                        trigger_source, allocation_key, event_type,
                        amount_source, amount_sign, is_active, priority
                    )
                    VALUES (%s, %s, %s, %s, %s, TRUE, %s)
                    """,
                    (trigger_source, allocation_key, event_type, amount_source, amount_sign, priority),
                )
        self.conn.commit()

    def list_receipt_gl_mappings(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM receipt_gl_mapping
                ORDER BY trigger_source, priority, allocation_key, event_type
                """
            )
            return cur.fetchall()

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
        """
        Create or update a mapping row that tells the posting engine how to
        translate allocations into accounting events.
        """
        with self.conn.cursor() as cur:
            if mapping_id:
                cur.execute(
                    """
                    UPDATE receipt_gl_mapping
                    SET trigger_source = %s,
                        allocation_key = %s,
                        event_type = %s,
                        amount_source = %s,
                        amount_sign = %s,
                        is_active = %s,
                        priority = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        trigger_source,
                        allocation_key,
                        event_type,
                        amount_source,
                        amount_sign,
                        is_active,
                        priority,
                        mapping_id,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO receipt_gl_mapping (
                        trigger_source, allocation_key, event_type,
                        amount_source, amount_sign, is_active, priority
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        trigger_source,
                        allocation_key,
                        event_type,
                        amount_source,
                        amount_sign,
                        is_active,
                        priority,
                    ),
                )
        self.conn.commit()

    def delete_receipt_gl_mapping(self, mapping_id):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM receipt_gl_mapping WHERE id = %s", (mapping_id,))
        self.conn.commit()

    def save_journal_entry(self, entry_date, reference, description, event_id, event_tag, created_by, lines):
        if lines:
            assert_journal_lines_balanced(
                lines,
                context=f"journal save (reference={reference!r}, event_tag={event_tag!r})",
            )
        with self.conn.cursor() as cur:
            def _insert_header_and_lines(
                *,
                hdr_entry_date,
                hdr_reference,
                hdr_description,
                hdr_event_id,
                hdr_event_tag,
                hdr_entry_type,
                hdr_created_by,
                journal_lines,
            ):
                cur.execute(
                    """
                    INSERT INTO journal_entries (
                        entry_date, reference, description, event_id, event_tag, entry_type, created_by, is_active
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                    RETURNING id
                    """,
                    (
                        hdr_entry_date,
                        hdr_reference,
                        hdr_description,
                        hdr_event_id,
                        hdr_event_tag,
                        hdr_entry_type,
                        hdr_created_by,
                    ),
                )
                new_id = cur.fetchone()["id"]
                for line in journal_lines:
                    cur.execute(
                        """
                        INSERT INTO journal_items (entry_id, account_id, debit, credit, memo)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            new_id,
                            line["account_id"],
                            line.get("debit", 0.0),
                            line.get("credit", 0.0),
                            line.get("memo"),
                        ),
                    )
                return new_id

            def _replace_lines_for_entry(entry_id, journal_lines):
                cur.execute(
                    "DELETE FROM journal_items WHERE entry_id = %s",
                    (entry_id,),
                )
                for line in journal_lines:
                    cur.execute(
                        """
                        INSERT INTO journal_items (entry_id, account_id, debit, credit, memo)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            entry_id,
                            line["account_id"],
                            line.get("debit", 0.0),
                            line.get("credit", 0.0),
                            line.get("memo"),
                        ),
                    )

            def _active_entry_for_event(eid, etag):
                if eid is None or etag is None:
                    return None
                cur.execute(
                    """
                    SELECT id, entry_date
                    FROM journal_entries
                    WHERE event_id = %s
                      AND event_tag = %s
                      AND COALESCE(is_active, TRUE) = TRUE
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (eid, etag),
                )
                return cur.fetchone()

            def _period_is_closed(period_key: str) -> bool:
                try:
                    cur.execute(
                        "SELECT is_closed FROM financial_periods WHERE period_key = %s",
                        (period_key,),
                    )
                    r = cur.fetchone()
                    return bool(r and r.get("is_closed"))
                except psycopg2.errors.UndefinedTable:
                    return False

            def _earliest_open_period_posting_date():
                # Post adjustments in earliest open period, first day of that month.
                try:
                    cur.execute(
                        """
                        SELECT period_key
                        FROM financial_periods
                        WHERE is_closed = FALSE
                        ORDER BY period_key
                        LIMIT 1
                        """
                    )
                    r = cur.fetchone()
                    if r and r.get("period_key"):
                        ym = str(r["period_key"])
                        y, m = ym.split("-")
                        return date(int(y), int(m), 1)
                except psycopg2.errors.UndefinedTable:
                    pass
                # Fallback to posting on the provided entry date if periods table unavailable.
                return entry_date

            def _load_lines_by_entry(entry_id):
                cur.execute(
                    """
                    SELECT account_id, COALESCE(debit, 0) AS debit, COALESCE(credit, 0) AS credit, memo
                    FROM journal_items
                    WHERE entry_id = %s
                    """,
                    (entry_id,),
                )
                return cur.fetchall()

            def _build_delta_lines(old_lines, new_lines, memo_prefix: str):
                from collections import defaultdict

                old_by_acc = defaultdict(lambda: {"debit": Decimal("0"), "credit": Decimal("0")})
                new_by_acc = defaultdict(lambda: {"debit": Decimal("0"), "credit": Decimal("0")})
                for l in old_lines:
                    acc = l["account_id"]
                    old_by_acc[acc]["debit"] += as_10dp(l.get("debit") or 0)
                    old_by_acc[acc]["credit"] += as_10dp(l.get("credit") or 0)
                for l in new_lines:
                    acc = l["account_id"]
                    new_by_acc[acc]["debit"] += as_10dp(l.get("debit") or 0)
                    new_by_acc[acc]["credit"] += as_10dp(l.get("credit") or 0)

                all_acc = set(old_by_acc.keys()) | set(new_by_acc.keys())
                out = []
                for acc in all_acc:
                    d = as_10dp(new_by_acc[acc]["debit"] - old_by_acc[acc]["debit"])
                    c = as_10dp(new_by_acc[acc]["credit"] - old_by_acc[acc]["credit"])
                    if abs(d) <= Decimal("0.0000000001") and abs(c) <= Decimal("0.0000000001"):
                        continue
                    out.append(
                        {
                            "account_id": acc,
                            "debit": d,
                            "credit": c,
                            "memo": memo_prefix,
                        }
                    )
                return out

            existing = _active_entry_for_event(event_id, event_tag)
            original_entry_date = existing["entry_date"] if existing else entry_date
            period_key = original_entry_date.strftime("%Y-%m")
            period_closed = _period_is_closed(period_key)

            if not period_closed:
                # OPEN period: correction by replacement (soft-supersede old active row).
                existing_entry_id = existing["id"] if existing else None
                if existing_entry_id is not None:
                    try:
                        cur.execute(
                            """
                            UPDATE journal_entries
                            SET is_active = FALSE,
                                superseded_at = NOW(),
                                superseded_by_id = NULL
                            WHERE id = %s
                            """,
                            (existing_entry_id,),
                        )
                    except psycopg2.errors.UndefinedColumn:
                        pass
                # Use a savepoint so we can gracefully handle legacy unique-index setups.
                cur.execute("SAVEPOINT je_open_replace_sp")
                try:
                    entry_id = _insert_header_and_lines(
                        hdr_entry_date=entry_date,
                        hdr_reference=reference,
                        hdr_description=description,
                        hdr_event_id=event_id,
                        hdr_event_tag=event_tag,
                        hdr_entry_type="EVENT",
                        hdr_created_by=created_by,
                        journal_lines=lines,
                    )
                    cur.execute("RELEASE SAVEPOINT je_open_replace_sp")
                    if existing_entry_id is not None:
                        try:
                            cur.execute(
                                """
                                UPDATE journal_entries
                                SET superseded_by_id = %s
                                WHERE id = %s
                                """,
                                (entry_id, existing_entry_id),
                            )
                        except psycopg2.errors.UndefinedColumn:
                            pass
                except psycopg2.errors.UniqueViolation:
                    # Legacy DBs may still enforce uniqueness without is_active filter.
                    # Fallback: update existing row in place to keep operation idempotent.
                    cur.execute("ROLLBACK TO SAVEPOINT je_open_replace_sp")
                    if existing_entry_id is None:
                        raise
                    cur.execute(
                        """
                        UPDATE journal_entries
                        SET entry_date = %s,
                            reference = %s,
                            description = %s,
                            entry_type = %s,
                            created_by = %s,
                            is_active = TRUE,
                            superseded_at = NULL,
                            superseded_by_id = NULL
                        WHERE id = %s
                        """,
                        (
                            entry_date,
                            reference,
                            description,
                            "EVENT",
                            created_by,
                            existing_entry_id,
                        ),
                    )
                    _replace_lines_for_entry(existing_entry_id, lines)
                    cur.execute("RELEASE SAVEPOINT je_open_replace_sp")
            else:
                # CLOSED period: keep original active and post delta adjustment in earliest open period.
                old_lines = _load_lines_by_entry(existing["id"]) if existing else []
                delta_lines = _build_delta_lines(
                    old_lines,
                    lines,
                    memo_prefix=f"Adjustment for {event_id or 'event'}",
                )
                if delta_lines:
                    assert_journal_lines_balanced(
                        delta_lines,
                        context=f"journal adjustment save (event_id={event_id!r}, event_tag={event_tag!r})",
                    )
                    posting_date = _earliest_open_period_posting_date()
                    adj_event_id = f"ADJ::{event_tag or 'EVENT'}::{event_id or reference or 'UNKNOWN'}"
                    adj_event_tag = "PERIOD_ADJUSTMENT"
                    existing_adj = _active_entry_for_event(adj_event_id, adj_event_tag)
                    adj_entry_id = _insert_header_and_lines(
                        hdr_entry_date=posting_date,
                        hdr_reference=reference,
                        hdr_description=(
                            f"Adjustment for {event_id} originally dated {original_entry_date.isoformat()}"
                            if event_id is not None
                            else f"Adjustment originally dated {original_entry_date.isoformat()}"
                        ),
                        hdr_event_id=adj_event_id,
                        hdr_event_tag=adj_event_tag,
                        hdr_entry_type="PERIOD_ADJUSTMENT",
                        hdr_created_by=created_by,
                        journal_lines=delta_lines,
                    )
                    if existing_adj is not None:
                        cur.execute(
                            """
                            UPDATE journal_entries
                            SET is_active = FALSE,
                                superseded_at = NOW(),
                                superseded_by_id = %s
                            WHERE id = %s
                            """,
                            (adj_entry_id, existing_adj["id"]),
                        )
        self.conn.commit()

    def list_unbalanced_journal_entries(self):
        """
        Rows where debit/credit totals still disagree after per-line 10dp then **2dp** rounding.

        Ignores sub–2dp drift (same rule as assert_journal_lines_balanced / posting).
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT je.id,
                       je.entry_date,
                       je.reference,
                       je.event_id,
                       je.event_tag,
                       SUM(ROUND(COALESCE(ji.debit, 0), 10)) AS total_debit,
                       SUM(ROUND(COALESCE(ji.credit, 0), 10)) AS total_credit,
                       ROUND(SUM(ROUND(COALESCE(ji.debit, 0), 10)), 2)
                         - ROUND(SUM(ROUND(COALESCE(ji.credit, 0), 10)), 2) AS imbalance_2dp
                FROM journal_entries je
                JOIN journal_items ji ON ji.entry_id = je.id
                WHERE COALESCE(je.is_active, TRUE) = TRUE
                GROUP BY je.id, je.entry_date, je.reference, je.event_id, je.event_tag
                HAVING ROUND(SUM(ROUND(COALESCE(ji.debit, 0), 10)), 2)
                    <> ROUND(SUM(ROUND(COALESCE(ji.credit, 0), 10)), 2)
                ORDER BY je.entry_date, je.reference
                """
            )
            return cur.fetchall()

    def get_journal_entries(self, start_date=None, end_date=None, account_code=None):
        with self.conn.cursor() as cur:
            where_clauses = ["COALESCE(je.is_active, TRUE) = TRUE"]
            params = []
            
            if start_date:
                where_clauses.append("je.entry_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("je.entry_date <= %s")
                params.append(end_date)
            if account_code:
                where_clauses.append("je.id IN (SELECT entry_id FROM journal_items ji2 JOIN accounts a2 ON ji2.account_id = a2.id WHERE a2.code = %s)")
                params.append(account_code)
                
            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            cur.execute(f"""
                SELECT je.*, 
                    COALESCE(
                        json_agg(json_build_object(
                            'account_id', ji.account_id,
                            'account_code', a.code,
                            'account_name', a.name,
                            'debit', ji.debit,
                            'credit', ji.credit,
                            'memo', ji.memo
                        )) FILTER (WHERE ji.id IS NOT NULL),
                        '[]'
                    ) as lines
                FROM journal_entries je
                LEFT JOIN journal_items ji ON je.id = ji.entry_id
                LEFT JOIN accounts a ON ji.account_id = a.id
                {where_sql}
                GROUP BY je.id
                ORDER BY je.entry_date DESC, je.created_at DESC
            """, tuple(params))
            return cur.fetchall()

    def get_account_ledger(self, account_code, start_date=None, end_date=None, include_descendants=False):
        """
        Ledger for one account. If ``include_descendants`` is True, opening balance and
        transaction lines include every **active** descendant account (and the account
        itself), using the same date rules as ``get_child_account_summaries``.
        """
        with self.conn.cursor() as cur:
            # Get account details
            cur.execute("SELECT id, code, name, category FROM accounts WHERE code = %s", (account_code,))
            account = cur.fetchone()
            if not account:
                return None

            if include_descendants:
                cur.execute(
                    """
                    WITH RECURSIVE sub AS (
                        SELECT id FROM accounts WHERE code = %s
                        UNION ALL
                        SELECT a.id
                        FROM accounts a
                        INNER JOIN sub s ON a.parent_id = s.id
                        WHERE COALESCE(a.is_active, TRUE) = TRUE
                    )
                    SELECT id FROM sub
                    """,
                    (account_code,),
                )
                id_rows = cur.fetchall()
                account_ids = [r["id"] for r in id_rows]
            else:
                account_ids = [account["id"]]

            if not account_ids:
                return None

            # Calculate opening balance
            ob_params: list = [account_ids]
            ob_date_filter = ""
            if start_date:
                ob_date_filter = "AND je.entry_date < %s"
                ob_params.append(start_date)

            cur.execute(
                f"""
                SELECT COALESCE(SUM(ji.debit), 0) as ob_debit, COALESCE(SUM(ji.credit), 0) as ob_credit
                FROM journal_items ji
                JOIN journal_entries je
                  ON ji.entry_id = je.id
                 AND je.status = 'POSTED'
                 AND COALESCE(je.is_active, TRUE) = TRUE
                WHERE ji.account_id = ANY(%s::uuid[]) {ob_date_filter}
                """,
                tuple(ob_params),
            )
            ob = cur.fetchone()

            # Fetch transactions
            tx_params: list = [account_ids]
            tx_where_clauses = [
                "ji.account_id = ANY(%s::uuid[])",
                "je.status = 'POSTED'",
                "COALESCE(je.is_active, TRUE) = TRUE",
            ]

            if start_date:
                tx_where_clauses.append("je.entry_date >= %s")
                tx_params.append(start_date)
            if end_date:
                tx_where_clauses.append("je.entry_date <= %s")
                tx_params.append(end_date)

            tx_where = " AND ".join(tx_where_clauses)

            cur.execute(
                f"""
                SELECT je.entry_date, je.reference, je.description, je.event_id,
                       ji.debit, ji.credit, ji.memo
                FROM journal_items ji
                JOIN journal_entries je ON ji.entry_id = je.id
                WHERE {tx_where}
                ORDER BY
                    je.entry_date ASC,
                    /* Same-day ordering rule:
                       Reversal journals (event_id like 'REV-*') must appear after originals. */
                    CASE
                        WHEN je.event_id IS NOT NULL AND je.event_id LIKE 'REV-%%' THEN 1
                        ELSE 0
                    END ASC,
                    je.created_at ASC
                """,
                tuple(tx_params),
            )
            transactions = cur.fetchall()

            return {
                "account": account,
                "opening_balance": ob,
                "transactions": transactions,
                "include_descendants": include_descendants,
            }

    def get_trial_balance(self, as_of_date=None):
        with self.conn.cursor() as cur:
            date_filter = "AND je.entry_date <= %s" if as_of_date else ""
            params = (as_of_date,) if as_of_date else ()
            
            cur.execute(f"""
                SELECT a.code, a.name, a.category, 
                       COALESCE(SUM(ji.debit), 0) as debit, 
                       COALESCE(SUM(ji.credit), 0) as credit
                FROM accounts a
                LEFT JOIN (
                    SELECT ji.account_id, ji.debit, ji.credit
                    FROM journal_items ji
                    JOIN journal_entries je ON ji.entry_id = je.id
                    WHERE je.status = 'POSTED'
                      AND COALESCE(je.is_active, TRUE) = TRUE
                      {date_filter}
                ) ji ON a.id = ji.account_id
                GROUP BY a.code, a.name, a.category
                ORDER BY a.code
            """, params)
            return cur.fetchall()

    def get_balances_by_category(self, categories, start_date=None, end_date=None):
        if not categories:
            return []
        with self.conn.cursor() as cur:
            date_filter = ""
            date_params = ()
            if start_date and end_date:
                date_filter = "AND je.entry_date >= %s AND je.entry_date <= %s"
                date_params = (start_date, end_date)
            elif end_date:
                date_filter = "AND je.entry_date <= %s"
                date_params = (end_date,)
                
            placeholders = ', '.join(['%s'] * len(categories))
            params = date_params + tuple(categories)
            
            cur.execute(f"""
                SELECT a.code, a.name, a.category, 
                       COALESCE(SUM(ji.debit), 0) as debit, 
                       COALESCE(SUM(ji.credit), 0) as credit
                FROM accounts a
                LEFT JOIN (
                    SELECT ji.account_id, ji.debit, ji.credit
                    FROM journal_items ji
                    JOIN journal_entries je ON ji.entry_id = je.id
                    WHERE je.status = 'POSTED'
                      AND COALESCE(je.is_active, TRUE) = TRUE
                      {date_filter}
                ) ji ON a.id = ji.account_id
                WHERE a.category IN ({placeholders})
                GROUP BY a.code, a.name, a.category
                ORDER BY a.code
            """, params)
            return cur.fetchall()

    def get_account_hybrid_balance(self, account_code, start_date, end_date):
        """
        Hybrid balance calculation for a (potentially parent) account.
        - Parent frozen balance: all postings to the parent account up to its
          transitioned_to_parent_at (if any).
        - Children net movement: net (debit - credit) for all descendants in the
          requested period [start_date, end_date].
        Returns a dict with a "header" row and a list of "children" rows.
        """
        with self.conn.cursor() as cur:
            # Header: parent hybrid balance and metadata.
            cur.execute(
                """
                WITH RECURSIVE root_account AS (
                    SELECT id, code, name, is_parent, transitioned_to_parent_at
                    FROM accounts
                    WHERE code = %s
                ),
                descendants AS (
                    SELECT a.id, a.code, a.name, a.parent_id
                    FROM accounts a
                    JOIN root_account r ON a.id = r.id
                    UNION ALL
                    SELECT c.id, c.code, c.name, c.parent_id
                    FROM accounts c
                    JOIN descendants d ON c.parent_id = d.id
                ),
                subtree_postings AS (
                    SELECT
                        a.id          AS account_id,
                        a.code        AS account_code,
                        a.name        AS account_name,
                        SUM(ji.debit)  AS total_debit,
                        SUM(ji.credit) AS total_credit
                    FROM descendants a
                    JOIN journal_items ji ON ji.account_id = a.id
                    JOIN journal_entries je
                      ON ji.entry_id = je.id
                     AND je.status = 'POSTED'
                     AND COALESCE(je.is_active, TRUE) = TRUE
                     AND je.entry_date >= %s
                     AND je.entry_date <= %s
                    GROUP BY a.id, a.code, a.name
                ),
                parent_frozen_balance AS (
                    SELECT
                        r.id AS account_id,
                        COALESCE(SUM(ji.debit), 0)  AS debit,
                        COALESCE(SUM(ji.credit), 0) AS credit
                    FROM root_account r
                    LEFT JOIN journal_items ji ON ji.account_id = r.id
                    LEFT JOIN journal_entries je
                      ON ji.entry_id = je.id
                     AND je.status = 'POSTED'
                     AND COALESCE(je.is_active, TRUE) = TRUE
                     AND je.entry_date::timestamptz <= COALESCE(r.transitioned_to_parent_at, '9999-12-31'::timestamptz)
                    GROUP BY r.id
                ),
                children_movement AS (
                    SELECT
                        sp.account_id,
                        sp.account_code,
                        sp.account_name,
                        COALESCE(sp.total_debit, 0)  AS debit,
                        COALESCE(sp.total_credit, 0) AS credit,
                        COALESCE(sp.total_debit, 0) - COALESCE(sp.total_credit, 0) AS net_movement
                    FROM subtree_postings sp
                    JOIN descendants d ON sp.account_id = d.id
                    JOIN root_account r ON d.id <> r.id
                )
                SELECT
                    r.code                         AS parent_code,
                    r.name                         AS parent_name,
                    r.is_parent,
                    r.transitioned_to_parent_at,
                    pfb.debit  AS parent_debit_frozen,
                    pfb.credit AS parent_credit_frozen,
                    (pfb.debit - pfb.credit)       AS parent_balance_frozen,
                    COALESCE(SUM(cm.net_movement), 0) AS children_net_movement,
                    (pfb.debit - pfb.credit) + COALESCE(SUM(cm.net_movement), 0) AS parent_hybrid_balance
                FROM root_account r
                LEFT JOIN parent_frozen_balance pfb ON pfb.account_id = r.id
                LEFT JOIN children_movement cm ON TRUE
                GROUP BY
                    r.code, r.name, r.is_parent, r.transitioned_to_parent_at,
                    pfb.debit, pfb.credit
                """,
                (account_code, start_date, end_date),
            )
            header = cur.fetchone()

            # Children: one row per child account with net movement for the period.
            cur.execute(
                """
                WITH RECURSIVE root_account AS (
                    SELECT id FROM accounts WHERE code = %s
                ),
                descendants AS (
                    SELECT a.id, a.code, a.name, a.parent_id
                    FROM accounts a
                    JOIN root_account r ON a.id = r.id
                    UNION ALL
                    SELECT c.id, c.code, c.name, c.parent_id
                    FROM accounts c
                    JOIN descendants d ON c.parent_id = d.id
                ),
                subtree_postings AS (
                    SELECT
                        a.id          AS account_id,
                        a.code        AS account_code,
                        a.name        AS account_name,
                        SUM(ji.debit)  AS total_debit,
                        SUM(ji.credit) AS total_credit
                    FROM descendants a
                    JOIN journal_items ji ON ji.account_id = a.id
                    JOIN journal_entries je
                      ON ji.entry_id = je.id
                     AND je.status = 'POSTED'
                     AND COALESCE(je.is_active, TRUE) = TRUE
                     AND je.entry_date >= %s
                     AND je.entry_date <= %s
                    GROUP BY a.id, a.code, a.name
                )
                SELECT
                    sp.account_code,
                    sp.account_name,
                    COALESCE(sp.total_debit, 0) - COALESCE(sp.total_credit, 0) AS net_movement
                FROM subtree_postings sp
                JOIN descendants d ON sp.account_id = d.id
                JOIN root_account r ON d.id <> r.id
                ORDER BY sp.account_code
                """,
                (account_code, start_date, end_date),
            )
            children = cur.fetchall()

            return {
                "header": header,
                "children": children,
            }

    def convert_to_parent(self, account_id):
        """
        Convenience wrapper around the database convert_to_parent(p_account_id) helper.
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT convert_to_parent(%s)", (account_id,))
        self.conn.commit()

    def create_statement_snapshot(
        self,
        *,
        statement_type,
        period_type,
        period_start_date,
        period_end_date,
        source_ledger_cutoff_date,
        generated_by="system",
        calculation_version="v1",
        lines=None,
    ):
        lines = lines or []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO financial_statement_snapshots (
                    statement_type, period_type, period_start_date, period_end_date,
                    source_ledger_cutoff_date, generated_by, calculation_version
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    statement_type,
                    period_type,
                    period_start_date,
                    period_end_date,
                    source_ledger_cutoff_date,
                    generated_by,
                    calculation_version,
                ),
            )
            snapshot_id = cur.fetchone()["id"]

            for idx, line in enumerate(lines, start=1):
                payload = line.get("payload", {})
                cur.execute(
                    """
                    INSERT INTO financial_statement_snapshot_lines (
                        snapshot_id, line_order, line_code, line_name, line_category,
                        debit, credit, amount, currency_code, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        snapshot_id,
                        idx,
                        line.get("line_code"),
                        line.get("line_name") or "Line",
                        line.get("line_category"),
                        line.get("debit", 0),
                        line.get("credit", 0),
                        line.get("amount", 0),
                        line.get("currency_code"),
                        json.dumps(payload),
                    ),
                )
        self.conn.commit()
        return snapshot_id

    def get_latest_statement_snapshot(self, *, statement_type, period_type, period_end_date):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM financial_statement_snapshots
                WHERE statement_type = %s
                  AND period_type = %s
                  AND period_end_date = %s
                  AND status = 'FINAL'
                ORDER BY generated_at DESC
                LIMIT 1
                """,
                (statement_type, period_type, period_end_date),
            )
            return cur.fetchone()

    def list_statement_snapshots(
        self,
        *,
        statement_type: str | None = None,
        period_type: str | None = None,
        period_end_date_from=None,
        period_end_date_to=None,
        limit: int = 200,
    ):
        where = ["status = 'FINAL'"]
        params = []
        if statement_type:
            where.append("statement_type = %s")
            params.append(statement_type)
        if period_type:
            where.append("period_type = %s")
            params.append(period_type)
        if period_end_date_from is not None:
            where.append("period_end_date >= %s")
            params.append(period_end_date_from)
        if period_end_date_to is not None:
            where.append("period_end_date <= %s")
            params.append(period_end_date_to)
        where_sql = " AND ".join(where) if where else "TRUE"
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM financial_statement_snapshots
                WHERE {where_sql}
                ORDER BY period_end_date DESC, statement_type, generated_at DESC
                LIMIT %s
                """,
                tuple(params + [int(limit)]),
            )
            return cur.fetchall()

    def get_statement_snapshot_lines(self, snapshot_id):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM financial_statement_snapshot_lines
                WHERE snapshot_id = %s
                ORDER BY line_order
                """,
                (snapshot_id,),
            )
            return cur.fetchall()
