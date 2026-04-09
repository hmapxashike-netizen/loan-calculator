from __future__ import annotations

from typing import Any

from psycopg2.extras import RealDictCursor

from auth.permission_catalog import PermissionRecord, all_permission_records, permission_by_key


class RbacRepository:
    """CRUD for roles, permission definitions, and role_permission grants."""

    def __init__(self, conn):
        self.conn = conn

    def list_roles(self) -> list[dict[str, Any]]:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, role_key, display_name, is_system, created_at
                FROM rbac_roles
                ORDER BY is_system DESC, role_key
                """
            )
            return [dict(r) for r in cur.fetchall()]

    def get_role_by_key(self, role_key: str) -> dict[str, Any] | None:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, role_key, display_name, is_system FROM rbac_roles WHERE role_key = %s",
                (role_key,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def get_role_by_id(self, role_id: int) -> dict[str, Any] | None:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, role_key, display_name, is_system FROM rbac_roles WHERE id = %s",
                (role_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def create_role(self, role_key: str, display_name: str, *, is_system: bool = False) -> int:
        rk = role_key.strip().upper().replace(" ", "_")
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO rbac_roles (role_key, display_name, is_system)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (rk, display_name.strip(), is_system),
            )
            row = cur.fetchone()
            rid = int(row["id"])
        self.conn.commit()
        return rid

    def delete_role_if_custom(self, role_id: int) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM rbac_roles WHERE id = %s AND is_system = FALSE RETURNING id",
                (role_id,),
            )
            deleted = cur.fetchone() is not None
        self.conn.commit()
        return deleted

    def list_permissions(self) -> list[dict[str, Any]]:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT permission_key, label, category, summary, grants_md, risk_tag,
                       grant_restricted_to_superadmin, nav_section
                FROM rbac_permissions
                ORDER BY category, label
                """
            )
            return [dict(r) for r in cur.fetchall()]

    def sync_permissions_from_catalog(self, records: tuple[PermissionRecord, ...] | None = None) -> None:
        recs = records if records is not None else all_permission_records()
        with self.conn.cursor() as cur:
            for p in recs:
                cur.execute(
                    """
                    INSERT INTO rbac_permissions (
                        permission_key, label, category, summary, grants_md, risk_tag,
                        grant_restricted_to_superadmin, nav_section, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (permission_key) DO UPDATE SET
                        label = EXCLUDED.label,
                        category = EXCLUDED.category,
                        summary = EXCLUDED.summary,
                        grants_md = EXCLUDED.grants_md,
                        risk_tag = EXCLUDED.risk_tag,
                        grant_restricted_to_superadmin = EXCLUDED.grant_restricted_to_superadmin,
                        nav_section = EXCLUDED.nav_section,
                        updated_at = NOW()
                    """,
                    (
                        p.permission_key,
                        p.label,
                        p.category,
                        p.summary,
                        p.grants_md,
                        p.risk_tag,
                        p.grant_restricted_to_superadmin,
                        p.nav_section,
                    ),
                )
        self.conn.commit()

    def get_permission_keys_for_role_key(self, role_key: str) -> set[str]:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT rp.permission_key
                FROM rbac_role_permissions rp
                INNER JOIN rbac_roles r ON r.id = rp.role_id
                WHERE r.role_key = %s
                """,
                (role_key,),
            )
            return {str(r["permission_key"]) for r in cur.fetchall()}

    def replace_role_permissions(
        self,
        role_id: int,
        permission_keys: set[str],
        *,
        actor_is_superadmin: bool,
    ) -> None:
        meta = permission_by_key()
        for key in permission_keys:
            if key not in meta:
                raise ValueError(f"Unknown permission_key: {key}")
            rec = meta[key]
            if rec.grant_restricted_to_superadmin and not actor_is_superadmin:
                raise PermissionError(
                    f"Only a super administrator may grant permission: {rec.label} ({key})"
                )

        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM rbac_role_permissions WHERE role_id = %s", (role_id,))
            for key in sorted(permission_keys):
                cur.execute(
                    """
                    INSERT INTO rbac_role_permissions (role_id, permission_key)
                    VALUES (%s, %s)
                    ON CONFLICT (role_id, permission_key) DO NOTHING
                    """,
                    (role_id, key),
                )
        self.conn.commit()

    def clone_permissions_from_role(self, target_role_id: int, source_role_key: str, *, actor_is_superadmin: bool) -> None:
        keys = self.get_permission_keys_for_role_key(source_role_key)
        self.replace_role_permissions(target_role_id, keys, actor_is_superadmin=actor_is_superadmin)
