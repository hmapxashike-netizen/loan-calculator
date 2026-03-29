import os
import psycopg2
import json
from decimal import Decimal
from psycopg2.extras import RealDictCursor
from datetime import date

os.environ['FARNDACRED_DB_USER'] = 'postgres'
os.environ['FARNDACRED_DB_PASSWORD'] = 'M1k@y1@2017'
os.environ['FARNDACRED_DB_NAME'] = 'farndacred_db'
os.environ['FARNDACRED_DB_HOST'] = 'localhost'
os.environ['FARNDACRED_DB_PORT'] = '5432'

from statements import generate_customer_facing_statement
import pandas as pd

def custom_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    return str(obj)

rows, meta = generate_customer_facing_statement(1, date(2025, 7, 10), date(2025, 10, 1), as_of_date=date(2025, 10, 1))

print("--- Statement Rows ---")
for r in rows:
    print(json.dumps(r, default=custom_default))

from config import get_database_url
conn = psycopg2.connect(get_database_url())
cur = conn.cursor(cursor_factory=RealDictCursor)

print("\n--- loan_daily_state (2025-08-31 to 2025-09-01) ---")
cur.execute("SELECT as_of_date, total_exposure, principal_not_due, principal_arrears, interest_accrued_balance, interest_arrears_balance, default_interest_balance, penalty_interest_balance FROM loan_daily_state WHERE loan_id = 1 AND as_of_date >= '2025-08-31' ORDER BY as_of_date;")
for row in cur.fetchall():
    print(json.dumps(row, default=custom_default))

print("\n--- loan_repayments & allocation (2025-09-01) ---")
cur.execute("""
    SELECT r.id as rep_id, r.amount, r.value_date, a.alloc_principal_not_due, a.alloc_principal_arrears, a.alloc_interest_arrears, a.alloc_default_interest, a.alloc_penalty_interest, a.alloc_total 
    FROM loan_repayments r 
    LEFT JOIN loan_repayment_allocation a ON r.id = a.repayment_id 
    WHERE r.loan_id = 1 AND r.value_date = '2025-09-01' ORDER BY r.id;
""")
for row in cur.fetchall():
    print(json.dumps(row, default=custom_default))

print("\n--- unapplied_funds_ledger (2025-09-01) ---")
cur.execute("SELECT * FROM unapplied_funds_ledger WHERE loan_id = 1 AND value_date = '2025-09-01';")
for row in cur.fetchall():
    print(json.dumps(row, default=custom_default))

conn.close()
