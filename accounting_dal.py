import json
from decimal import Decimal

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2.extras import RealDictCursor

from config import get_database_url
from decimal_utils import amounts_equal_at_2dp, as_10dp, as_2dp


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

    def initialize_default_coa(self) -> None:
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

    def create_account(self, code, name, category, system_tag=None, parent_id=None):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM accounts WHERE code = %s", (code,))
            if cur.fetchone():
                raise ValueError(
                    f"Account code {code!r} already exists. Choose another code or use the existing account."
                )
            try:
                cur.execute(
                    """
                    INSERT INTO accounts (code, name, category, system_tag, parent_id, is_active)
                    VALUES (%s, %s, %s, %s, %s, TRUE)
                    """,
                    (code, name, category, system_tag, parent_id),
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

    def list_accounts(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT a.*, p.code as parent_code
                FROM accounts a
                LEFT JOIN accounts p ON a.parent_id = p.id
                ORDER BY a.code
            """)
            return cur.fetchall()

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
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.code, c.name,
                       COALESCE(SUM(ji.debit), 0) AS debit,
                       COALESCE(SUM(ji.credit), 0) AS credit
                FROM accounts p
                JOIN accounts c ON c.parent_id = p.id AND c.is_active = TRUE
                LEFT JOIN journal_items ji ON ji.account_id = c.id
                LEFT JOIN journal_entries je
                    ON ji.entry_id = je.id
                   AND je.status = 'POSTED'
                   AND je.entry_date >= %s
                   AND je.entry_date <= %s
                WHERE p.code = %s
                GROUP BY c.code, c.name
                ORDER BY c.code
                """,
                (start_date, end_date, parent_code),
            )
            return cur.fetchall()

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
            # EOD (and other flows) pass deterministic identifiers via (event_id, event_tag).
            # If the same event is posted twice (user re-runs EOD, auto-EOD overlaps, or a
            # previous run partially succeeded), journal_entries can be duplicated because
            # the schema currently has no uniqueness constraint on event_id.
            #
            # DAL guard: if journal exists for this (event_id, event_tag), replace it
            # so the persisted GL matches the *latest* deterministic computation.
            if event_id is not None and event_tag is not None:
                try:
                    cur.execute(
                        """
                        SELECT id
                        FROM journal_entries
                        WHERE event_id = %s AND event_tag = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (event_id, event_tag),
                    )
                    row = cur.fetchone()
                    if row is not None:
                        existing_entry_id = row["id"]
                        # Delete header (ON DELETE CASCADE should remove items if FK exists).
                        cur.execute("DELETE FROM journal_entries WHERE id = %s", (existing_entry_id,))
                        # In case cascade is missing in the current DB, also delete items explicitly.
                        try:
                            cur.execute("DELETE FROM journal_items WHERE entry_id = %s", (existing_entry_id,))
                        except psycopg2.errors.UndefinedColumn:
                            pass
                except psycopg2.errors.UndefinedColumn:
                    # Older schemas may not yet have event_id/event_tag; fall back to insert.
                    pass
            cur.execute("""
                INSERT INTO journal_entries (entry_date, reference, description, event_id, event_tag, created_by)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """, (entry_date, reference, description, event_id, event_tag, created_by))
            entry_id = cur.fetchone()["id"]
            
            for line in lines:
                cur.execute("""
                    INSERT INTO journal_items (entry_id, account_id, debit, credit, memo)
                    VALUES (%s, %s, %s, %s, %s)
                """, (entry_id, line["account_id"], line.get("debit", 0.0), line.get("credit", 0.0), line.get("memo")))
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
                GROUP BY je.id, je.entry_date, je.reference, je.event_id, je.event_tag
                HAVING ROUND(SUM(ROUND(COALESCE(ji.debit, 0), 10)), 2)
                    <> ROUND(SUM(ROUND(COALESCE(ji.credit, 0), 10)), 2)
                ORDER BY je.entry_date, je.reference
                """
            )
            return cur.fetchall()

    def get_journal_entries(self, start_date=None, end_date=None, account_code=None):
        with self.conn.cursor() as cur:
            where_clauses = []
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

    def get_account_ledger(self, account_code, start_date=None, end_date=None):
        with self.conn.cursor() as cur:
            # Get account details
            cur.execute("SELECT id, code, name, category FROM accounts WHERE code = %s", (account_code,))
            account = cur.fetchone()
            if not account:
                return None
                
            # Calculate opening balance
            ob_params = [account['id']]
            ob_date_filter = ""
            if start_date:
                ob_date_filter = "AND je.entry_date < %s"
                ob_params.append(start_date)
                
            cur.execute(f"""
                SELECT COALESCE(SUM(ji.debit), 0) as ob_debit, COALESCE(SUM(ji.credit), 0) as ob_credit
                FROM journal_items ji
                JOIN journal_entries je ON ji.entry_id = je.id AND je.status = 'POSTED'
                WHERE ji.account_id = %s {ob_date_filter}
            """, tuple(ob_params))
            ob = cur.fetchone()
            
            # Fetch transactions
            tx_params = [account['id']]
            tx_where_clauses = ["ji.account_id = %s", "je.status = 'POSTED'"]
            
            if start_date:
                tx_where_clauses.append("je.entry_date >= %s")
                tx_params.append(start_date)
            if end_date:
                tx_where_clauses.append("je.entry_date <= %s")
                tx_params.append(end_date)
                
            tx_where = " AND ".join(tx_where_clauses)
            
            cur.execute(f"""
                SELECT je.entry_date, je.reference, je.description, je.event_id,
                       ji.debit, ji.credit, ji.memo
                FROM journal_items ji
                JOIN journal_entries je ON ji.entry_id = je.id
                WHERE {tx_where}
                ORDER BY
                    je.entry_date ASC,
                    /* Same-day ordering rule:
                       Reversal journals (event_id like 'REV-%') must appear after originals. */
                    CASE
                        WHEN je.event_id IS NOT NULL AND je.event_id LIKE 'REV-%' THEN 1
                        ELSE 0
                    END ASC,
                    je.created_at ASC
            """, tuple(tx_params))
            transactions = cur.fetchall()
            
            return {
                "account": account,
                "opening_balance": ob,
                "transactions": transactions
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
                    WHERE je.status = 'POSTED' {date_filter}
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
                    WHERE je.status = 'POSTED' {date_filter}
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
