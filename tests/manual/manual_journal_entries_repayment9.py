"""Print journal_entries for repayment-9 / unapplied allocation. Requires DB via config."""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import get_database_url


def custom_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    return str(obj)


def main() -> None:
    conn = psycopg2.connect(get_database_url())
    cur = conn.cursor(cursor_factory=RealDictCursor)

    print("\n--- journal_entries for repayment 9 ---")
    cur.execute(
        """
        SELECT * FROM journal_entries
        WHERE event_id = 'repayment-9' OR reference = 'Unapplied funds allocation'
        """
    )
    for row in cur.fetchall():
        print(json.dumps(row, default=custom_default))

    conn.close()


if __name__ == "__main__":
    main()
