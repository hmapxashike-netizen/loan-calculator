#!/usr/bin/env python3
"""
Apply schema/66_public_tenants_registry.sql, ensure tenant_default schema exists, seed public.tenants.

Uses config.get_database_url() (FARNDACRED_* / LMS_* env vars). Run from repo root:

    python scripts/apply_tenant_registry_migration.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2

from config import get_database_url


def main() -> int:
    sql_path = ROOT / "schema" / "66_public_tenants_registry.sql"
    if not sql_path.is_file():
        print(f"Missing migration file: {sql_path}", file=sys.stderr)
        return 1

    ddl = sql_path.read_text(encoding="utf-8")

    try:
        conn = psycopg2.connect(get_database_url())
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        return 1

    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(ddl)
        cur.execute("CREATE SCHEMA IF NOT EXISTS tenant_default")
        cur.execute(
            """
            INSERT INTO public.tenants (company_name, schema_name, is_active)
            VALUES ('Farnda Demo', 'tenant_default', TRUE)
            ON CONFLICT (schema_name) DO NOTHING
            """
        )
        cur.close()
    finally:
        conn.close()

    print("Applied 66_public_tenants_registry.sql, ensured schema tenant_default, seeded Farnda Demo (if missing).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
