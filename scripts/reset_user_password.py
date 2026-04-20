"""Break-glass: set a user's password when DB credentials are available locally.

Does not disable two-step verification — after reset you still sign in with password plus
authenticator or an unused backup code (same as normal super-admin login).

Usage:
    python scripts/reset_user_password.py user@example.com

Uses get_database_url() from config (same DB as the app).
"""
from __future__ import annotations

import getpass
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_MIN_LEN = 10


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/reset_user_password.py <email>", file=sys.stderr)
        return 1

    email = sys.argv[1].strip()

    pw = getpass.getpass("New password (hidden): ")
    pw2 = getpass.getpass("Confirm new password: ")
    if pw != pw2:
        print("Passwords do not match.", file=sys.stderr)
        return 2
    if len(pw) < _MIN_LEN:
        print(f"Password must be at least {_MIN_LEN} characters.", file=sys.stderr)
        return 2

    from auth.service import AuthService
    from dal import UserRepository, get_conn

    conn = get_conn()
    try:
        users = UserRepository(conn)
        auth = AuthService(conn)
        user = users.get_by_email(email)
        if not user:
            print(f"No user with email {email!r}.", file=sys.stderr)
            return 3
        if not user.is_active:
            print("User exists but is inactive; activate the account first.", file=sys.stderr)
            return 4

        h = auth.hash_password(pw)
        users.update_password(user.id, h)
        print(f"Password updated for {email!r} ({user.role}). Sign in with the app; 2FA still applies if enabled.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
