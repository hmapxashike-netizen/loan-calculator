#!/usr/bin/env python3
"""
Apply schema/70_totp_superadmin_backup_codes.sql using psql and config.get_database_url().

Do not run the .sql file with Python — use this script or psql -f directly.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import get_database_url


def _redact_database_url(url: str) -> str:
    """Safe one-line summary for logging (never print passwords)."""
    try:
        u = urlparse(url)
        host = u.hostname or "?"
        port = f":{u.port}" if u.port else ""
        user = u.username or ""
        auth = f"{user}:***" if user else "***"
        path = u.path or ""
        return f"{u.scheme}://{auth}@{host}{port}{path}"
    except Exception:
        return "postgresql://***"


def main() -> int:
    sql_path = ROOT / "schema" / "70_totp_superadmin_backup_codes.sql"
    if not sql_path.is_file():
        print(f"Missing: {sql_path}", file=sys.stderr)
        return 1

    psql = shutil.which("psql")
    if not psql:
        print(
            "psql is not on PATH. Install PostgreSQL client tools, then run:\n"
            f'  psql "<your-database-url>" -v ON_ERROR_STOP=1 -f "{sql_path}"\n'
            "Or rely on the app: it creates user_totp_backup_codes on first TOTP enrolment.",
            file=sys.stderr,
        )
        return 2

    url = get_database_url()
    cmd = [psql, url, "-v", "ON_ERROR_STOP=1", "-f", str(sql_path)]
    print(
        "Running psql with",
        _redact_database_url(url),
        "-v ON_ERROR_STOP=1 -f",
        sql_path.name,
    )
    r = subprocess.run(cmd, cwd=str(ROOT))
    if r.returncode != 0:
        return r.returncode
    print("Applied:", sql_path.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
