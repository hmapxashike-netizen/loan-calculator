import os
import psycopg2

os.environ['FARNDACRED_DB_USER'] = 'postgres'
os.environ['FARNDACRED_DB_PASSWORD'] = 'M1k@y1@2017'
os.environ['FARNDACRED_DB_NAME'] = 'farndacred_db'
os.environ['FARNDACRED_DB_HOST'] = 'localhost'
os.environ['FARNDACRED_DB_PORT'] = '5432'

from config import get_database_url

conn = psycopg2.connect(get_database_url())
cur = conn.cursor()

try:
    # 1. Delete unapplied_funds related to repayment 9
    cur.execute("DELETE FROM unapplied_funds WHERE allocation_repayment_id = 9 OR source_repayment_id = 9 OR repayment_id = 9;")
    print(f"Deleted {cur.rowcount} from unapplied_funds")

    # 2. Delete loan_repayment_allocation
    cur.execute("DELETE FROM loan_repayment_allocation WHERE repayment_id = 9;")
    print(f"Deleted {cur.rowcount} from loan_repayment_allocation")

    # 3. Delete journal items then entries
    cur.execute("DELETE FROM journal_items WHERE entry_id IN (SELECT id FROM journal_entries WHERE event_id = 'repayment-9' OR reference = 'Unapplied funds allocation' OR event_id LIKE '%%REPAY-9-%%');")
    print(f"Deleted {cur.rowcount} from journal_items")

    cur.execute("DELETE FROM journal_entries WHERE event_id = 'repayment-9' OR reference = 'Unapplied funds allocation' OR event_id LIKE '%%REPAY-9-%%';")
    print(f"Deleted {cur.rowcount} from journal_entries")

    # 4. Delete loan_repayments
    cur.execute("DELETE FROM loan_repayments WHERE id = 9;")
    print(f"Deleted {cur.rowcount} from loan_repayments")

    conn.commit()
    print("Committed successfully.")
except Exception as e:
    conn.rollback()
    print("Error:", e)
finally:
    conn.close()
