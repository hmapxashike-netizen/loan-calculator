import os
import psycopg2
from datetime import date
from psycopg2.extras import RealDictCursor

os.environ['FARNDACRED_DB_USER'] = 'postgres'
os.environ['FARNDACRED_DB_PASSWORD'] = 'M1k@y1@2017'
os.environ['FARNDACRED_DB_NAME'] = 'farndacred_db'
os.environ['FARNDACRED_DB_HOST'] = 'localhost'
os.environ['FARNDACRED_DB_PORT'] = '5432'

from loan_management import reallocate_repayment, load_system_config_from_db
from eod import run_single_loan_eod

cfg = load_system_config_from_db()
# Receipts 10, 11, 12, 13, 14
for rep_id in [10, 11, 12, 13, 14]:
    print(f"Reallocating {rep_id}...")
    reallocate_repayment(rep_id, system_config=cfg)

# Re-run EOD for 01.09
run_single_loan_eod(1, date(2025, 9, 1), sys_cfg=cfg)
print("Done.")
