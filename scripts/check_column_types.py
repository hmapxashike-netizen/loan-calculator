import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config, psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(config.get_database_url())
cur  = conn.cursor(cursor_factory=RealDictCursor)
cur.execute("""
    SELECT column_name, data_type, numeric_precision, numeric_scale
    FROM information_schema.columns
    WHERE table_name = 'loan_daily_state'
    ORDER BY ordinal_position
""")
for r in cur.fetchall():
    print(f"{r['column_name']:<45} {r['data_type']:<20} p={r['numeric_precision']}  s={r['numeric_scale']}")
conn.close()
