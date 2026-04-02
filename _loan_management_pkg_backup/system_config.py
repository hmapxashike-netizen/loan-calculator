"""System-wide JSON config row in ``config`` table (key ``system_config``)."""

from __future__ import annotations

import json

from .db import _connection

_CONFIG_KEY_SYSTEM = "system_config"


def load_system_config_from_db() -> dict | None:
    """
    Load system configurations from the config table.
    Returns None if not found or on error.
    """
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM config WHERE key = %s",
                    (_CONFIG_KEY_SYSTEM,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
    except Exception:
        pass
    return None


def save_system_config_to_db(config: dict) -> bool:
    """
    Save system configurations to the config table.
    Uses INSERT ... ON CONFLICT (key) DO UPDATE for upsert.
    Returns True on success, False on error.
    """
    try:
        value_json = json.dumps(config)
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO config (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """,
                    (_CONFIG_KEY_SYSTEM, value_json),
                )
        return True
    except Exception:
        return False
