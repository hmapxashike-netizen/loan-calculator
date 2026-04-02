import json
from decimal import Decimal
from accounting.dal import get_conn

def check_gl():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT je.event_tag, je.entry_date, ji.debit, ji.credit 
            FROM journal_entries je
            JOIN journal_items ji ON ji.entry_id = je.id
            WHERE je.event_id LIKE 'EOD-%-1-%' AND je.event_tag = 'ACCRUAL_REGULAR_INTEREST'
            ORDER BY je.entry_date
        """)
        rows = cur.fetchall()
        for r in rows:
            print(f"Date: {r['entry_date']}, Debit: {r['debit']}, Credit: {r['credit']}")
            
        print("Daily state:")
        cur.execute("""
            SELECT as_of_date, regular_interest_daily 
            FROM loan_daily_state 
            WHERE loan_id = 1 
            ORDER BY as_of_date
        """)
        for r in cur.fetchall():
            print(f"Date: {r['as_of_date']}, Daily: {r['regular_interest_daily']}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    check_gl()
