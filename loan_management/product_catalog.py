"""Loan products, per-product JSON config, and system config in the ``config`` table."""

from __future__ import annotations

import json
from typing import Iterable

from .db import RealDictCursor, _connection

CONFIG_KEY_PRODUCT_PREFIX = "product_config:"
CONFIG_KEY_SYSTEM = "system_config"


def list_products(active_only: bool = True) -> list[dict]:
    """List products. Each dict: id, code, name, loan_type, is_active, created_at, updated_at."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                where = " WHERE is_active = TRUE" if active_only else ""
                cur.execute(
                    f"SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products{where} ORDER BY code"
                )
                return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []


def get_product(product_id: int) -> dict | None:
    """Get product by id."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products WHERE id = %s",
                    (product_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
    except Exception:
        return None


def get_product_by_code(code: str) -> dict | None:
    """Get product by code."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, code, name, loan_type, is_active, created_at, updated_at FROM products WHERE code = %s",
                    (code.strip(),),
                )
                row = cur.fetchone()
                return dict(row) if row else None
    except Exception:
        return None


def create_product(code: str, name: str, loan_type: str) -> int:
    """Create a product. Returns product id."""
    code = code.strip().upper()
    name = name.strip()
    lt = {"Consumer Loan": "consumer_loan", "Term Loan": "term_loan", "Bullet Loan": "bullet_loan", "Customised Repayments": "customised_repayments"}.get(
        loan_type, loan_type.replace(" ", "_").lower()
    )
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products (code, name, loan_type, is_active) VALUES (%s, %s, %s, TRUE) RETURNING id",
                (code, name, lt),
            )
            return cur.fetchone()[0]


def update_product(product_id: int, *, name: str | None = None, loan_type: str | None = None, is_active: bool | None = None) -> None:
    """Update product name, loan_type, and/or is_active."""
    updates = []
    args = []
    if name is not None:
        updates.append("name = %s")
        args.append(name.strip())
    if loan_type is not None:
        lt = {"Consumer Loan": "consumer_loan", "Term Loan": "term_loan", "Bullet Loan": "bullet_loan", "Customised Repayments": "customised_repayments"}.get(
            loan_type, loan_type.replace(" ", "_").lower()
        )
        updates.append("loan_type = %s")
        args.append(lt)
    if is_active is not None:
        updates.append("is_active = %s")
        args.append(is_active)
    if not updates:
        return
    args.append(product_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE products SET updated_at = NOW(), {', '.join(updates)} WHERE id = %s",
                args,
            )


def delete_product(product_id: int) -> None:
    """Delete a product and its config. Raises ValueError if any loans reference this product."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, code FROM products WHERE id = %s", (product_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("Product not found.")
            code = row["code"]
            cur.execute("SELECT COUNT(*) AS n FROM loans WHERE product_code = %s", (code,))
            n = cur.fetchone()["n"] or 0
            if n > 0:
                raise ValueError(f"Cannot delete: {n} loan(s) use this product. Deactivate it instead.")
            cur.execute("DELETE FROM config WHERE key = %s", (CONFIG_KEY_PRODUCT_PREFIX + code,))
            cur.execute("DELETE FROM products WHERE id = %s", (product_id,))


def get_product_config_from_db(code: str) -> dict | None:
    """Load product config JSON from config table."""
    try:
        key = CONFIG_KEY_PRODUCT_PREFIX + code.strip()
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM config WHERE key = %s", (key,))
                row = cur.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
    except Exception:
        pass
    return None


def batch_get_product_configs_from_db(codes: Iterable[str]) -> dict[str, dict | None]:
    """
    Load product config JSON for many product codes in one round-trip.

    Used by EOD to avoid one DB connection per active loan when merging product
    settings into system config. Missing or invalid JSON values map to None.
    """
    seen_codes: set[str] = set()
    key_to_code: dict[str, str] = {}
    for raw in codes:
        c = str(raw or "").strip()
        if not c or c in seen_codes:
            continue
        seen_codes.add(c)
        key = CONFIG_KEY_PRODUCT_PREFIX + c
        key_to_code[key] = c
    if not key_to_code:
        return {}
    keys = list(key_to_code.keys())
    out: dict[str, dict | None] = {code: None for code in seen_codes}
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT key, value FROM config WHERE key = ANY(%s)",
                    (keys,),
                )
                for row in cur.fetchall():
                    k, v = row[0], row[1]
                    code = key_to_code.get(str(k))
                    if code is None:
                        continue
                    if not v:
                        out[code] = None
                        continue
                    try:
                        out[code] = json.loads(v)
                    except Exception:
                        out[code] = None
    except Exception:
        pass
    return out


def save_product_config_to_db(code: str, config: dict) -> bool:
    """Save product config JSON."""
    try:
        key = CONFIG_KEY_PRODUCT_PREFIX + code.strip()
        value_json = json.dumps(config)
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO config (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """,
                    (key, value_json),
                )
        return True
    except Exception:
        return False


def load_system_config_from_db() -> dict | None:
    """
    Load system configurations from the config table.
    Returns None if not found or on error.
    """
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM config WHERE key = %s", (CONFIG_KEY_SYSTEM,))
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
                    (CONFIG_KEY_SYSTEM, value_json),
                )
        return True
    except Exception:
        return False
