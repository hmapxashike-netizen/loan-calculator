"""
Migration 79: accounting equity config + CURRENT YEAR EARNINGS account (C300005).

- Merges ``system_config.accounting_equity`` with defaults for RE/CYE account codes.
- Inserts account C300005 when missing (parent C300000), matching builtin COA.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accounting.equity_config import merge_default_accounting_equity  # noqa: E402
from loan_management import load_system_config_from_db, save_system_config_to_db  # noqa: E402
from accounting.dal import get_conn  # noqa: E402


def _ensure_cye_account(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM accounts WHERE code = %s", ("C300005",))
        if cur.fetchone():
            return False
        cur.execute("SELECT id FROM accounts WHERE code = %s", ("C300000",))
        parent = cur.fetchone()
        if not parent:
            print(
                "Migration 79: parent C300000 not found — skip C300005 insert "
                "(initialize COA first)."
            )
            return False
        cur.execute(
            """
            INSERT INTO accounts (code, name, category, system_tag, parent_id, is_active)
            VALUES (%s, %s, %s, %s, %s, TRUE)
            """,
            (
                "C300005",
                "CURRENT YEAR EARNINGS",
                "EQUITY",
                "current_year_earnings",
                parent["id"],
            ),
        )
    return True


def main() -> None:
    cfg = load_system_config_from_db()
    if cfg:
        merged = merge_default_accounting_equity(cfg)
        if merged.get("accounting_equity") != cfg.get("accounting_equity"):
            if save_system_config_to_db(merged):
                print("Migration 79: system_config.accounting_equity merged with defaults.")
            else:
                raise SystemExit("Migration 79 failed: could not save system_config.")
        else:
            print("Migration 79: accounting_equity already present.")
    else:
        print(
            "Migration 79: no system_config row — skipped JSON merge. "
            "Defaults apply when config is first saved from the app."
        )

    conn = get_conn()
    try:
        inserted = _ensure_cye_account(conn)
        if inserted:
            conn.commit()
            print("Migration 79: inserted account C300005 (CURRENT YEAR EARNINGS).")
        else:
            conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
