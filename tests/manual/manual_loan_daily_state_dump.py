"""Dump loan_daily_state for loan 1 over a date window. Requires DB via config."""

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

    print("--- loan_daily_state ---")
    cur.execute(
        """
        SELECT * FROM loan_daily_state
        WHERE loan_id = 1 AND as_of_date BETWEEN '2025-08-30' AND '2025-09-01'
        ORDER BY as_of_date
        """
    )
    for row in cur.fetchall():
        print(json.dumps(row, default=custom_default))

    conn.close()


if __name__ == "__main__":
    main()
