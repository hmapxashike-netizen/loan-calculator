import os
import psycopg2
import json
from decimal import Decimal
from psycopg2.extras import RealDictCursor

os.environ['FARNDACRED_DB_USER'] = 'postgres'
os.environ['FARNDACRED_DB_PASSWORD'] = 'M1k@y1@2017'
os.environ['FARNDACRED_DB_NAME'] = 'farndacred_db'
os.environ['FARNDACRED_DB_HOST'] = 'localhost'
os.environ['FARNDACRED_DB_PORT'] = '5432'
from config import get_database_url

def custom_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    return str(obj)

conn = psycopg2.connect(get_database_url())
cur = conn.cursor(cursor_factory=RealDictCursor)

print("--- unapplied_funds_ledger (negative balances) ---")
cur.execute("SELECT * FROM unapplied_funds_ledger WHERE unapplied_running_balance < 0;")
for row in cur.fetchall():
    print(json.dumps(row, default=custom_default))

conn.close()
