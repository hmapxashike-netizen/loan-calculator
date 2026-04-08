"""Read-side RBAC helpers for menu construction and guards."""

from __future__ import annotations

from dal import get_conn
from rbac.repository import RbacRepository


def get_permission_keys_for_role_key(role_key: str) -> frozenset[str]:
    with get_conn() as conn:
        repo = RbacRepository(conn)
        return frozenset(repo.get_permission_keys_for_role_key(role_key))


def list_assignable_role_keys() -> list[str]:
    with get_conn() as conn:
        repo = RbacRepository(conn)
        return [str(r["role_key"]) for r in repo.list_roles()]


def rbac_tables_ready() -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'rbac_roles'
                    """
                )
                return cur.fetchone() is not None
    except Exception:
        return False
