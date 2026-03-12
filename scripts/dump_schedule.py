import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config, psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(config.get_database_url())
cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute("""SELECT sl.* FROM schedule_lines sl
               JOIN loan_schedules ls ON sl.loan_schedule_id = ls.id
               WHERE ls.loan_id=10 ORDER BY sl."Period" """)
rows = cur.fetchall()
for r in rows:
    print(dict(r))
conn.close()
