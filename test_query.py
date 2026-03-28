import os
import psycopg2
import json
from decimal import Decimal
from psycopg2.extras import RealDictCursor

os.environ['FARNDACRED_DB_PASSWORD'] = 'M1k@y1@2017'
os.environ['FARNDACRED_DB_NAME'] = 'farndacred_db'
from config import get_database_url

def custom_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    return str(obj)

conn = psycopg2.connect(get_database_url())
cur = conn.cursor(cursor_factory=RealDictCursor)

print("\n--- journal_entries for repayment 9 ---")
cur.execute("SELECT * FROM journal_entries WHERE event_id = 'repayment-9' OR reference = 'Unapplied funds allocation';")
for row in cur.fetchall():
    print(json.dumps(row, default=custom_default))

conn.close()
