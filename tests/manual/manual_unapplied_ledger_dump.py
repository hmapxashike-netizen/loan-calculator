"""Dump unapplied_funds_ledger rows for a loan (adjust loan_id in script). Requires DB credentials via config."""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import get_database_url


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


def main() -> None:
    conn = psycopg2.connect(get_database_url())
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT value_date, repayment_id, entry_kind, unapplied_delta, unapplied_running_balance
        FROM unapplied_funds_ledger
        WHERE loan_id = 1
        ORDER BY value_date, repayment_id
        """
    )
    rows = cur.fetchall()
    for row in rows:
        print(json.dumps(row, default=str, cls=DecimalEncoder))
    conn.close()


if __name__ == "__main__":
    main()
