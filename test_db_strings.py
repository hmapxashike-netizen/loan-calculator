import psycopg2
from config import get_database_url
import os

os.environ['FARNDACRED_DB_USER'] = 'postgres'
os.environ['FARNDACRED_DB_PASSWORD'] = 'M1k@y1@2017'
os.environ['FARNDACRED_DB_NAME'] = 'farndacred_db'
os.environ['FARNDACRED_DB_HOST'] = 'localhost'
os.environ['FARNDACRED_DB_PORT'] = '5432'

conn = psycopg2.connect(get_database_url())
cur = conn.cursor()
cur.execute("SELECT id, reference FROM loan_repayments WHERE reference ILIKE '%Unapplied%'")
print(cur.fetchall())
