import psycopg2
import psycopg2.extras
import json
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

conn = psycopg2.connect('dbname=farndacred_db user=postgres host=localhost port=5432 password=M1k@y1@2017')
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute('SELECT value_date, repayment_id, entry_kind, unapplied_delta, unapplied_running_balance FROM unapplied_funds_ledger WHERE loan_id=1 ORDER BY value_date, repayment_id;')
rows = cur.fetchall()
for row in rows:
    print(json.dumps(row, default=str, cls=DecimalEncoder))
