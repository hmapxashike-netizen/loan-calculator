"""List unapplied_funds_ledger rows with negative running balance. Requires DB via config."""

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

    print("--- unapplied_funds_ledger (negative balances) ---")
    cur.execute("SELECT * FROM unapplied_funds_ledger WHERE unapplied_running_balance < 0")
    for row in cur.fetchall():
        print(json.dumps(row, default=custom_default))

    conn.close()


if __name__ == "__main__":
    main()
