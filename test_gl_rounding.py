import os
os.environ['FARNDACRED_DB_PASSWORD'] = '' # I'll just use the empty password or default
from accounting_dal import get_conn
from decimal import Decimal

try:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT je.event_id, ji.debit, ji.credit 
        FROM journal_entries je
        JOIN journal_items ji ON ji.entry_id = je.id
        WHERE je.event_tag = 'ACCRUAL_REGULAR_INTEREST'
        ORDER BY je.created_at DESC LIMIT 5
    """)
    for r in cur.fetchall():
        print(f"Event: {r['event_id']}, Debit: {r['debit']}, Credit: {r['credit']}")
except Exception as e:
    print(f"Error connecting: {e}")
