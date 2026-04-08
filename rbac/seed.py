"""
Seed rbac_roles, rbac_permissions, and rbac_role_permissions to match pre-RBAC menu behaviour.
"""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from auth.permission_catalog import (
    PERMISSION_DASHBOARD_ADMIN,
    PERMISSION_DASHBOARD_OFFICER,
    RESERVED_SUPERADMIN_MARKER,
    all_permission_records,
    nav_permission_key_for_section,
)


def _nav_keys_all() -> set[str]:
    return {
        p.permission_key
        for p in all_permission_records()
        if p.nav_section is not None
    }


def seed_rbac_defaults(conn) -> None:
    from rbac.repository import RbacRepository

    repo = RbacRepository(conn)
    repo.sync_permissions_from_catalog()

    system_roles = (
        ("SUPERADMIN", "Super administrator", True),
        ("ADMIN", "Administrator", True),
        ("LOAN_OFFICER", "Loan officer", True),
        ("BORROWER", "Borrower", True),
        ("VENDOR", "Vendor", True),
    )

    role_ids: dict[str, int] = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for role_key, display_name, is_system in system_roles:
            cur.execute(
                """
                INSERT INTO rbac_roles (role_key, display_name, is_system)
                VALUES (%s, %s, %s)
                ON CONFLICT (role_key) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    is_system = EXCLUDED.is_system
                RETURNING id
                """,
                (role_key, display_name, is_system),
            )
            row = cur.fetchone()
            role_ids[role_key] = int(row["id"])

        all_nav = _nav_keys_all()
        sys_cfg_key = nav_permission_key_for_section("System configurations")
        officer_nav = all_nav - {sys_cfg_key} if sys_cfg_key else all_nav

        grants_spec = (
            ("BORROWER", set()),
            (
                "VENDOR",
                {nav_permission_key_for_section("Subscription") or "nav.subscription"},
            ),
            ("LOAN_OFFICER", {PERMISSION_DASHBOARD_OFFICER} | officer_nav),
            ("ADMIN", {PERMISSION_DASHBOARD_ADMIN} | all_nav),
            (
                "SUPERADMIN",
                {PERMISSION_DASHBOARD_ADMIN} | all_nav | {RESERVED_SUPERADMIN_MARKER},
            ),
        )

        for role_key, keys in grants_spec:
            rid = role_ids[role_key]
            cur.execute("DELETE FROM rbac_role_permissions WHERE role_id = %s", (rid,))
            for pk in sorted(keys):
                cur.execute(
                    "INSERT INTO rbac_role_permissions (role_id, permission_key) VALUES (%s, %s)",
                    (rid, pk),
                )

    conn.commit()
