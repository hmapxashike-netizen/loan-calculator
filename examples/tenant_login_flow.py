"""
Mock tenant selection → resolve ``schema_name`` → store in ``st.session_state`` → run queries.

Prerequisites
-------------
1. Apply ``schema/66_public_tenants_registry.sql`` to your database.
2. Seed ``public.tenants`` (see SQL file comment) and ensure the target schema exists.
3. Configure ``.streamlit/secrets.toml`` with ``[postgres]`` (see ``secrets.toml.example``).

Run::

    python -m streamlit run examples/tenant_login_flow.py
"""

from __future__ import annotations

import streamlit as st

from style import apply_custom_styles, render_main_header, render_sub_header, render_sub_sub_header

from sqlalchemy import text

from db.tenant_registry import (
    TenantCompanyNotFoundError,
    clear_tenant_context,
    get_stored_tenant_company,
    get_stored_tenant_schema,
    list_active_tenants,
    remember_tenant_context,
    tenant_session_scope_current,
)
from db.tenant_session import TenantDatabaseConfigError

st.set_page_config(page_title="Tenant login (demo)", layout="centered")
apply_custom_styles()

render_main_header("Tenant context (demo)")
st.caption("Resolves company → schema via ``public.tenants``; stores schema in ``session_state`` only.")

# --------------------------------------------------------------------------- gate
if get_stored_tenant_schema() is None:
    render_sub_sub_header("Select or enter company")
    try:
        tenants = list_active_tenants()
    except TenantDatabaseConfigError as e:
        st.error(str(e))
        st.stop()
    except Exception as e:
        st.exception(e)
        st.stop()

    choice: str | None = None
    if tenants:
        labels = [t["company_name"] for t in tenants]
        choice = st.selectbox("Company", options=labels, index=0, key="tenant_demo_pick")
    else:
        st.info("No rows in ``public.tenants`` yet — enter a company name that exists after you seed the table.")
    manual = st.text_input("Or type company name (overrides dropdown if non-empty)", value="")

    company_to_resolve = manual.strip() if manual.strip() else choice
    if st.button("Continue", type="primary"):
        if not company_to_resolve:
            st.warning("Choose a company or enter a name.")
        else:
            try:
                schema = remember_tenant_context(company_to_resolve)
                st.success(f"Connected context: **{company_to_resolve}** → schema `{schema}`")
                st.rerun()
            except TenantCompanyNotFoundError as e:
                st.error(str(e))
            except Exception as e:
                st.exception(e)
    st.stop()

# --------------------------------------------------------------------------- main (tenant already in session)
company = get_stored_tenant_company() or "—"
schema = get_stored_tenant_schema() or "—"
st.success(f"Active tenant: **{company}** · schema **`{schema}`**")

c1, c2 = st.columns(2)
with c1:
    if st.button("Run sample query in tenant schema", type="primary"):
        try:
            with tenant_session_scope_current() as session:
                row = session.execute(text("SELECT current_schema(), current_setting('search_path')")).one()
            st.json({"current_schema": row[0], "search_path": row[1]})
        except Exception as e:
            st.exception(e)
with c2:
    if st.button("Switch tenant / logout"):
        clear_tenant_context()
        st.rerun()

st.divider()
st.markdown(
    "**Integration tip:** On each Streamlit page, call ``require_tenant_schema_in_session()`` "
    "or use ``tenant_session_scope_current()`` for DB work. Never store a SQLAlchemy ``Session`` in "
    "``st.session_state``."
)
