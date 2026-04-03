"""
Example Streamlit page: query using schema-per-tenant SQLAlchemy session.

Run (from repo root, with .streamlit/secrets.toml configured):

    python -m streamlit run examples/tenant_session_streamlit_demo.py
"""

from __future__ import annotations

import streamlit as st

from style import apply_custom_styles, render_main_header, render_sub_header, render_sub_sub_header

from sqlalchemy import text

from db.tenant_session import TenantSchemaValidationError, tenant_session_scope

st.set_page_config(page_title="Tenant DB demo", layout="wide")
apply_custom_styles()
render_main_header("Tenant session demo (schema-per-tenant)")

default_schema = st.text_input("Tenant schema", value="tenant_default", help="Must exist in PostgreSQL; identifier only.")

if st.button("Run dummy query", type="primary"):
    try:
        with tenant_session_scope(default_schema.strip()) as session:
            # Example: current schema and path (no app tables required)
            row = session.execute(text("SELECT current_schema(), current_setting('search_path')")).one()
            st.success("Query OK.")
            st.json({"current_schema": row[0], "search_path_setting": row[1]})
    except TenantSchemaValidationError as e:
        st.error(f"Invalid schema name: {e}")
    except Exception as e:
        st.exception(e)

st.caption("Uses db.tenant_session + st.secrets['postgres']; see .streamlit/secrets.toml.example.")
