"""Sample recent accrual journal lines. Run from repo root; needs DB config (env / config.py)."""

from __future__ import annotations

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from accounting.dal import get_conn


def main() -> None:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT je.event_id, ji.debit, ji.credit
            FROM journal_entries je
            JOIN journal_items ji ON ji.entry_id = je.id
            WHERE je.event_tag = 'ACCRUAL_REGULAR_INTEREST'
            ORDER BY je.created_at DESC LIMIT 5
            """
        )
        for r in cur.fetchall():
            print(f"Event: {r['event_id']}, Debit: {r['debit']}, Credit: {r['credit']}")
        conn.close()
    except Exception as e:
        print(f"Error connecting: {e}")


if __name__ == "__main__":
    main()
