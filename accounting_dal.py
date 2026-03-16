import psycopg2
from psycopg2.extras import RealDictCursor
from config import get_database_url

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
            cur.execute("""
                INSERT INTO accounts (code, name, category, system_tag, parent_id, is_active)
                VALUES (%s, %s, %s, %s, %s, TRUE)
            """, (code, name, category, system_tag, parent_id))
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

    def link_journal(self, event_type, system_tag, direction, description):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transaction_templates (event_type, system_tag, direction, description)
                VALUES (%s, %s, %s, %s)
            """, (event_type, system_tag, direction, description))
        self.conn.commit()

    def save_journal_entry(self, entry_date, reference, description, event_id, event_tag, created_by, lines):
        with self.conn.cursor() as cur:
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
                ORDER BY je.entry_date ASC, je.created_at ASC
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
