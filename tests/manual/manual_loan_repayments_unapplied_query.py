"""List loan_repayments with Unapplied in reference. Requires DB via config.get_database_url()."""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg2

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import get_database_url


def main() -> None:
    conn = psycopg2.connect(get_database_url())
    cur = conn.cursor()
    cur.execute("SELECT id, reference FROM loan_repayments WHERE reference ILIKE '%Unapplied%'")
    print(cur.fetchall())
    conn.close()


if __name__ == "__main__":
    main()
