"""
Customer module: capture and manage Individuals and Corporate entities.
Customers are not deleted; use set_active(customer_id, False) to deactivate.
"""

from __future__ import annotations

import contextlib
from typing import Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None

from config import get_database_url


def _get_conn():
    if psycopg2 is None:
        raise RuntimeError("Install psycopg2-binary to use the customer module.")
    return psycopg2.connect(get_database_url())


@contextlib.contextmanager
def _connection():
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _row(dict_cursor) -> dict | None:
    if dict_cursor is None:
        return None
    row = dict_cursor.fetchone()
    return dict(row) if row else None


def _rows(dict_cursor) -> list[dict]:
    if dict_cursor is None:
        return []
    return [dict(r) for r in dict_cursor.fetchall()]


# ---------- Individuals ----------

def create_individual(
    name: str,
    national_id: str | None = None,
    employer_details: str | None = None,
    phone1: str | None = None,
    phone2: str | None = None,
    email1: str | None = None,
    email2: str | None = None,
    addresses: list[dict] | None = None,
    sector_id: int | None = None,
    subsector_id: int | None = None,
) -> int:
    """Create an individual customer. Returns customer_id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO customers (type, status, sector_id, subsector_id)
                   VALUES ('individual', 'active', %s, %s) RETURNING id""",
                (sector_id, subsector_id),
            )
            customer_id = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO individuals (customer_id, name, national_id, employer_details, phone1, phone2, email1, email2)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (customer_id, name, national_id, employer_details, phone1, phone2, email1, email2),
            )
            if addresses:
                for addr in addresses:
                    cur.execute(
                        """INSERT INTO customer_addresses (customer_id, address_type, line1, line2, city, region, postal_code, country)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            customer_id,
                            addr.get("address_type"),
                            addr.get("line1"),
                            addr.get("line2"),
                            addr.get("city"),
                            addr.get("region"),
                            addr.get("postal_code"),
                            addr.get("country"),
                        ),
                    )
    return customer_id


def update_individual(
    customer_id: int,
    name: str | None = None,
    national_id: str | None = None,
    employer_details: str | None = None,
    phone1: str | None = None,
    phone2: str | None = None,
    email1: str | None = None,
    email2: str | None = None,
    sector_id: int | None = None,
    subsector_id: int | None = None,
) -> None:
    """Update individual details. Pass only fields to change."""
    with _connection() as conn:
        with conn.cursor() as cur:
            updates = []
            vals = []
            for key, val in [
                ("name", name), ("national_id", national_id), ("employer_details", employer_details),
                ("phone1", phone1), ("phone2", phone2), ("email1", email1), ("email2", email2),
            ]:
                if val is not None:
                    updates.append(f"{key} = %s")
                    vals.append(val)
            if updates:
                vals.append(customer_id)
                cur.execute(
                    f"UPDATE individuals SET {', '.join(updates)}, updated_at = NOW() WHERE customer_id = %s",
                    vals,
                )
            
            cust_updates = []
            cust_vals = []
            if sector_id is not None:
                cust_updates.append("sector_id = %s")
                cust_vals.append(sector_id)
            if subsector_id is not None:
                cust_updates.append("subsector_id = %s")
                cust_vals.append(subsector_id)
            if cust_updates:
                cust_vals.append(customer_id)
                cur.execute(
                    f"UPDATE customers SET {', '.join(cust_updates)}, updated_at = NOW() WHERE id = %s",
                    cust_vals,
                )


# ---------- Corporates ----------

def _contact_row(d: dict) -> tuple:
    return (
        d.get("full_name", ""),
        d.get("national_id"),
        d.get("designation"),
        d.get("phone1"),
        d.get("phone2"),
        d.get("email"),
        d.get("address_line1"),
        d.get("address_line2"),
        d.get("city"),
        d.get("country"),
    )


def create_corporate(
    legal_name: str,
    trading_name: str | None = None,
    reg_number: str | None = None,
    tin: str | None = None,
    addresses: list[dict] | None = None,
    contact_person: dict | None = None,
    directors: list[dict] | None = None,
    shareholders: list[dict] | None = None,
    sector_id: int | None = None,
    subsector_id: int | None = None,
) -> int:
    """Create a corporate customer. Returns customer_id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO customers (type, status, sector_id, subsector_id)
                   VALUES ('corporate', 'active', %s, %s) RETURNING id""",
                (sector_id, subsector_id),
            )
            customer_id = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO corporates (customer_id, legal_name, trading_name, reg_number, tin)
                   VALUES (%s, %s, %s, %s, %s)""",
                (customer_id, legal_name, trading_name, reg_number, tin),
            )
            if addresses:
                for addr in addresses:
                    cur.execute(
                        """INSERT INTO customer_addresses (customer_id, address_type, line1, line2, city, region, postal_code, country)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            customer_id,
                            addr.get("address_type"),
                            addr.get("line1"),
                            addr.get("line2"),
                            addr.get("city"),
                            addr.get("region"),
                            addr.get("postal_code"),
                            addr.get("country"),
                        ),
                    )
            if contact_person:
                t = _contact_row(contact_person)
                cur.execute(
                    """INSERT INTO corporate_contact_persons (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (customer_id,) + t,
                )
            if directors:
                for d in directors:
                    t = _contact_row(d)
                    cur.execute(
                        """INSERT INTO corporate_directors (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (customer_id,) + t,
                    )
            if shareholders:
                for s in shareholders:
                    t = _contact_row(s)
                    cur.execute(
                        """INSERT INTO corporate_shareholders (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country, shareholding_pct)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (customer_id,) + t + (s.get("shareholding_pct"),),
                    )
    return customer_id


def create_corporate_with_entities(
    legal_name: str,
    trading_name: str | None = None,
    reg_number: str | None = None,
    tin: str | None = None,
    addresses: list[dict] | None = None,
    contact_person: dict | None = None,
    directors: list[dict] | None = None,
    shareholders: list[dict] | None = None,
    sector_id: int | None = None,
    subsector_id: int | None = None,
) -> dict:
    """
    Create a corporate customer and return created entity IDs.
    Returns:
      {
        "customer_id": int,
        "contact_person_ids": list[int],
        "director_ids": list[int],
        "shareholder_ids": list[int],
      }
    """
    out = {
        "customer_id": None,
        "contact_person_ids": [],
        "director_ids": [],
        "shareholder_ids": [],
    }
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO customers (type, status, sector_id, subsector_id)
                   VALUES ('corporate', 'active', %s, %s) RETURNING id""",
                (sector_id, subsector_id),
            )
            customer_id = cur.fetchone()[0]
            out["customer_id"] = customer_id
            cur.execute(
                """INSERT INTO corporates (customer_id, legal_name, trading_name, reg_number, tin)
                   VALUES (%s, %s, %s, %s, %s)""",
                (customer_id, legal_name, trading_name, reg_number, tin),
            )
            if addresses:
                for addr in addresses:
                    cur.execute(
                        """INSERT INTO customer_addresses (customer_id, address_type, line1, line2, city, region, postal_code, country)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            customer_id,
                            addr.get("address_type"),
                            addr.get("line1"),
                            addr.get("line2"),
                            addr.get("city"),
                            addr.get("region"),
                            addr.get("postal_code"),
                            addr.get("country"),
                        ),
                    )
            if contact_person:
                t = _contact_row(contact_person)
                cur.execute(
                    """INSERT INTO corporate_contact_persons (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (customer_id,) + t,
                )
                out["contact_person_ids"].append(cur.fetchone()[0])
            if directors:
                for d in directors:
                    t = _contact_row(d)
                    cur.execute(
                        """INSERT INTO corporate_directors (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           RETURNING id""",
                        (customer_id,) + t,
                    )
                    out["director_ids"].append(cur.fetchone()[0])
            if shareholders:
                for s in shareholders:
                    t = _contact_row(s)
                    cur.execute(
                        """INSERT INTO corporate_shareholders (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country, shareholding_pct)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           RETURNING id""",
                        (customer_id,) + t + (s.get("shareholding_pct"),),
                    )
                    out["shareholder_ids"].append(cur.fetchone()[0])
    return out


def update_corporate(
    customer_id: int,
    legal_name: str | None = None,
    trading_name: str | None = None,
    reg_number: str | None = None,
    tin: str | None = None,
    sector_id: int | None = None,
    subsector_id: int | None = None,
) -> None:
    """Update corporate details."""
    with _connection() as conn:
        with conn.cursor() as cur:
            updates = []
            vals = []
            for key, val in [
                ("legal_name", legal_name), ("trading_name", trading_name),
                ("reg_number", reg_number), ("tin", tin),
            ]:
                if val is not None:
                    updates.append(f"{key} = %s")
                    vals.append(val)
            if updates:
                vals.append(customer_id)
                cur.execute(
                    f"UPDATE corporates SET {', '.join(updates)}, updated_at = NOW() WHERE customer_id = %s",
                    vals,
                )
            
            cust_updates = []
            cust_vals = []
            if sector_id is not None:
                cust_updates.append("sector_id = %s")
                cust_vals.append(sector_id)
            if subsector_id is not None:
                cust_updates.append("subsector_id = %s")
                cust_vals.append(subsector_id)
            if cust_updates:
                cust_vals.append(customer_id)
                cur.execute(
                    f"UPDATE customers SET {', '.join(cust_updates)}, updated_at = NOW() WHERE id = %s",
                    cust_vals,
                )


# ---------- Sector / Subsector (configurable; on customers) ----------

def list_sectors() -> list[dict]:
    """List all sectors for dropdowns/config. Returns [] if sectors table does not exist (run schema/11_sectors_subsectors_agents.sql)."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, name, sort_order FROM sectors ORDER BY sort_order, name")
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        if psycopg2 and hasattr(psycopg2, "ProgrammingError") and isinstance(e, psycopg2.ProgrammingError):
            return []
        raise


def list_subsectors(sector_id: int | None = None) -> list[dict]:
    """List subsectors; optionally filter by sector_id. Returns [] if subsectors table does not exist."""
    try:
        with _connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if sector_id is not None:
                    cur.execute(
                        "SELECT id, sector_id, name, sort_order FROM subsectors WHERE sector_id = %s ORDER BY sort_order, name",
                        (sector_id,),
                    )
                else:
                    cur.execute("SELECT id, sector_id, name, sort_order FROM subsectors ORDER BY sector_id, sort_order, name")
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        if psycopg2 and hasattr(psycopg2, "ProgrammingError") and isinstance(e, psycopg2.ProgrammingError):
            return []
        raise


def create_sector(name: str, sort_order: int = 0) -> int:
    """Create a sector. Returns sector id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sectors (name, sort_order) VALUES (%s, %s) RETURNING id",
                (name.strip(), sort_order),
            )
            return cur.fetchone()[0]


def create_subsector(sector_id: int, name: str, sort_order: int = 0) -> int:
    """Create a subsector. Returns subsector id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO subsectors (sector_id, name, sort_order) VALUES (%s, %s, %s) RETURNING id",
                (sector_id, name.strip(), sort_order),
            )
            return cur.fetchone()[0]


def update_customer_sector(
    customer_id: int,
    sector_id: int | None = None,
    subsector_id: int | None = None,
) -> None:
    """Update customer's sector and/or subsector."""
    updates = []
    vals = []
    if sector_id is not None:
        updates.append("sector_id = %s")
        vals.append(sector_id)
    if subsector_id is not None:
        updates.append("subsector_id = %s")
        vals.append(subsector_id)
    if not updates:
        return
    vals.append(customer_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE customers SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s",
                vals,
            )


# ---------- Addresses ----------

def add_address(
    customer_id: int,
    address_type: str | None = None,
    line1: str | None = None,
    line2: str | None = None,
    city: str | None = None,
    region: str | None = None,
    postal_code: str | None = None,
    country: str | None = None,
) -> int:
    """Add an address for a customer. Returns address id."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO customer_addresses (customer_id, address_type, line1, line2, city, region, postal_code, country)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (customer_id, address_type, line1, line2, city, region, postal_code, country),
            )
            return cur.fetchone()[0]


def list_addresses(customer_id: int) -> list[dict]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, address_type, line1, line2, city, region, postal_code, country FROM customer_addresses WHERE customer_id = %s ORDER BY id",
                (customer_id,),
            )
            return _rows(cur)


def update_address(address_id: int, **kwargs: Any) -> None:
    allowed = {"address_type", "line1", "line2", "city", "region", "postal_code", "country"}
    updates = [f"{k} = %s" for k in kwargs if k in allowed]
    if not updates:
        return
    vals = [kwargs[k] for k in kwargs if k in allowed]
    vals.append(address_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE customer_addresses SET {', '.join(updates)} WHERE id = %s",
                vals,
            )


def delete_address(address_id: int) -> None:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM customer_addresses WHERE id = %s", (address_id,))


# ---------- Corporate: Contact Person ----------

def add_contact_person(
    customer_id: int,
    full_name: str,
    national_id: str | None = None,
    designation: str | None = None,
    phone1: str | None = None,
    phone2: str | None = None,
    email: str | None = None,
    address_line1: str | None = None,
    address_line2: str | None = None,
    city: str | None = None,
    country: str | None = None,
) -> int:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO corporate_contact_persons (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country),
            )
            return cur.fetchone()[0]


def list_contact_persons(customer_id: int) -> list[dict]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country FROM corporate_contact_persons WHERE customer_id = %s ORDER BY id",
                (customer_id,),
            )
            return _rows(cur)


def update_contact_person(contact_id: int, **kwargs: Any) -> None:
    allowed = {"full_name", "national_id", "designation", "phone1", "phone2", "email", "address_line1", "address_line2", "city", "country"}
    updates = [f"{k} = %s" for k in kwargs if k in allowed]
    if not updates:
        return
    vals = [kwargs[k] for k in kwargs if k in allowed]
    vals.append(contact_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE corporate_contact_persons SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s",
                vals,
            )


def delete_contact_person(contact_id: int) -> None:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM corporate_contact_persons WHERE id = %s", (contact_id,))


# ---------- Corporate: Directors ----------

def add_director(
    customer_id: int,
    full_name: str,
    national_id: str | None = None,
    designation: str | None = None,
    phone1: str | None = None,
    phone2: str | None = None,
    email: str | None = None,
    address_line1: str | None = None,
    address_line2: str | None = None,
    city: str | None = None,
    country: str | None = None,
) -> int:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO corporate_directors (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country),
            )
            return cur.fetchone()[0]


def list_directors(customer_id: int) -> list[dict]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country FROM corporate_directors WHERE customer_id = %s ORDER BY id",
                (customer_id,),
            )
            return _rows(cur)


def update_director(director_id: int, **kwargs: Any) -> None:
    allowed = {"full_name", "national_id", "designation", "phone1", "phone2", "email", "address_line1", "address_line2", "city", "country"}
    updates = [f"{k} = %s" for k in kwargs if k in allowed]
    if not updates:
        return
    vals = [kwargs[k] for k in kwargs if k in allowed]
    vals.append(director_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE corporate_directors SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s",
                vals,
            )


def delete_director(director_id: int) -> None:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM corporate_directors WHERE id = %s", (director_id,))


# ---------- Corporate: Shareholders ----------

def add_shareholder(
    customer_id: int,
    full_name: str,
    national_id: str | None = None,
    designation: str | None = None,
    phone1: str | None = None,
    phone2: str | None = None,
    email: str | None = None,
    address_line1: str | None = None,
    address_line2: str | None = None,
    city: str | None = None,
    country: str | None = None,
    shareholding_pct: float | None = None,
) -> int:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO corporate_shareholders (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country, shareholding_pct)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (customer_id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country, shareholding_pct),
            )
            return cur.fetchone()[0]


def list_shareholders(customer_id: int) -> list[dict]:
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, full_name, national_id, designation, phone1, phone2, email, address_line1, address_line2, city, country, shareholding_pct FROM corporate_shareholders WHERE customer_id = %s ORDER BY id",
                (customer_id,),
            )
            return _rows(cur)


def update_shareholder(shareholder_id: int, **kwargs: Any) -> None:
    allowed = {"full_name", "national_id", "designation", "phone1", "phone2", "email", "address_line1", "address_line2", "city", "country", "shareholding_pct"}
    updates = [f"{k} = %s" for k in kwargs if k in allowed]
    if not updates:
        return
    vals = [kwargs[k] for k in kwargs if k in allowed]
    vals.append(shareholder_id)
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE corporate_shareholders SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s",
                vals,
            )


def delete_shareholder(shareholder_id: int) -> None:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM corporate_shareholders WHERE id = %s", (shareholder_id,))


# ---------- List / Get / Status ----------

def list_customers(
    status: str | None = None,
    customer_type: str | None = None,
) -> list[dict]:
    """List customers; filter by status ('active'/'inactive') and/or type ('individual'/'corporate')."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = "SELECT id, type, status, sector_id, subsector_id, created_at, updated_at FROM customers WHERE 1=1"
            params = []
            if status:
                q += " AND status = %s"
                params.append(status)
            if customer_type:
                q += " AND type = %s"
                params.append(customer_type)
            q += " ORDER BY id"
            cur.execute(q, params or None)
            return _rows(cur)


def get_customer(customer_id: int) -> dict | None:
    """Get full customer record (header + individual or corporate + addresses + contact person/directors/shareholders)."""
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, type, status, sector_id, subsector_id, created_at, updated_at FROM customers WHERE id = %s",
                (customer_id,),
            )
            row = _row(cur)
            if not row:
                return None
            out = dict(row)
            if out["type"] == "individual":
                cur.execute("SELECT name, national_id, employer_details, phone1, phone2, email1, email2 FROM individuals WHERE customer_id = %s", (customer_id,))
                ind = _row(cur)
                out["individual"] = dict(ind) if ind else None
            else:
                cur.execute("SELECT legal_name, trading_name, reg_number, tin FROM corporates WHERE customer_id = %s", (customer_id,))
                corp = _row(cur)
                out["corporate"] = dict(corp) if corp else None
                out["contact_persons"] = list_contact_persons(customer_id)
                out["directors"] = list_directors(customer_id)
                out["shareholders"] = list_shareholders(customer_id)
            out["addresses"] = list_addresses(customer_id)
            return out


def set_active(customer_id: int, active: bool) -> None:
    """Set customer status to active or inactive. Customers are not deleted."""
    status = "active" if active else "inactive"
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE customers SET status = %s, updated_at = NOW() WHERE id = %s", (status, customer_id))


def get_display_name(customer_id: int) -> str:
    """Short label for a customer (e.g. for dropdowns)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT type FROM customers WHERE id = %s", (customer_id,))
            r = cur.fetchone()
            if not r:
                return ""
            if r[0] == "individual":
                cur.execute("SELECT name FROM individuals WHERE customer_id = %s", (customer_id,))
                row = cur.fetchone()
                return row[0] if row else ""
            cur.execute("SELECT COALESCE(trading_name, legal_name) FROM corporates WHERE customer_id = %s", (customer_id,))
            row = cur.fetchone()
            return row[0] if row else ""
