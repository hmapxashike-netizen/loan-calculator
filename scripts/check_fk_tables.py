import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config, psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(config.get_database_url())
cur = conn.cursor(cursor_factory=RealDictCursor)

cur.execute("""
  SELECT tc.table_name, kcu.column_name
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
  JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name
  JOIN information_schema.table_constraints tc2 ON rc.unique_constraint_name = tc2.constraint_name
  WHERE tc2.table_name = 'loans' AND tc.constraint_type = 'FOREIGN KEY'
  ORDER BY tc.table_name
""")
print("Tables with FK -> loans:")
for r in cur.fetchall():
    print(f"  {r['table_name']}.{r['column_name']}")

# Also count rows per table for loan_id=10
tables = ['loan_repayments', 'loan_daily_state', 'loan_repayment_allocation',
          'unapplied_funds', 'loan_schedules']
print("\nRow counts for loan_id=10:")
for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) AS n FROM {t} WHERE loan_id=10")
        print(f"  {t}: {cur.fetchone()['n']}")
    except Exception as e:
        print(f"  {t}: ERROR {e}")
        conn.rollback()

conn.close()
