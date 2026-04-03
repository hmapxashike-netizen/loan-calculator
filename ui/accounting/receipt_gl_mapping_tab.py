"""Accounting: receipt allocation to GL events mapping tab."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st



from style import render_main_header, render_sub_header, render_sub_sub_header

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def render_receipt_gl_mapping_tab(*, receipt_gl) -> None:
    render_sub_sub_header("Receipt Allocation → Accounting Events")
    if st.session_state.pop("acco_rgl_restored_ok", None):
        st.success("Receipt → GL mappings were updated from bundled defaults.")

    st.caption(
        "This table tells the system how to translate repayment allocations "
        "into accounting events (and therefore GL postings)."
    )

    _table_exists = True
    try:
        mappings = receipt_gl.list_receipt_gl_mappings()
    except Exception as e:
        if "receipt_gl_mapping" in str(e) and "does not exist" in str(e).lower():
            _table_exists = False
            st.warning(
                "The `receipt_gl_mapping` table has not been created yet. "
                "Click the button below to create it (uses the same database connection as the app)."
            )
            if st.button("Create receipt_gl_mapping table"):
                try:
                    import psycopg2

                    sql_path = _PROJECT_ROOT / "schema" / "38_receipt_gl_mapping.sql"
                    if not sql_path.exists():
                        st.error(f"Migration file not found: {sql_path}")
                    else:
                        sql = sql_path.read_text(encoding="utf-8")
                        from config import get_database_url

                        conn = psycopg2.connect(get_database_url())
                        try:
                            with conn.cursor() as cur:
                                cur.execute(sql)
                            conn.commit()
                            st.success("Table created. Refreshing...")
                            st.rerun()
                        finally:
                            conn.close()
                except Exception as ex:
                    st.error(f"Could not create table: {ex}")
                    st.exception(ex)
            mappings = []
        else:
            raise

    st.caption(
        "Bundled defaults live in `accounting_defaults/receipt_gl_mapping.json` (active rows only), "
        "else built-in. Export from live DB: `python scripts/export_accounting_defaults.py`. "
        "Loading or replacing defaults is only available at the bottom of this tab, inside **Danger zone**."
    )

    if mappings:
        df_map = pd.DataFrame(mappings)
        st.dataframe(
            df_map[
                [
                    "id",
                    "trigger_source",
                    "allocation_key",
                    "event_type",
                    "amount_source",
                    "amount_sign",
                    "is_active",
                    "priority",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No receipt GL mappings defined yet.")

    st.divider()
    _show_rgl_form = st.checkbox(
        "Show **Add / Edit Mapping**",
        value=False,
        key="acco_rgl_show_form",
        help="Create or update receipt → GL mapping rows. Hidden by default to reduce clutter.",
    )
    if _show_rgl_form:
        render_sub_sub_header("Add / Edit Mapping")

        # Dropdown options from DB for Receipt GL Mapping form
        _templates_for_events = receipt_gl.list_all_transaction_templates()
        _event_types_for_mapping = sorted(set(t["event_type"] for t in (_templates_for_events or [])))
        _predefined_allocation_keys = [
            "alloc_principal_arrears", "alloc_principal_not_due",
            "alloc_interest_arrears", "alloc_interest_accrued",
            "alloc_penalty_interest", "alloc_default_interest",
            "alloc_regular_interest", "alloc_fees_charges", "amount",
        ]
        _allocation_keys_from_db = sorted(set(m["allocation_key"] for m in (mappings or [])))
        _amount_sources_from_db = sorted(set(m["amount_source"] for m in (mappings or [])))
        _allocation_key_options = sorted(set(_predefined_allocation_keys + _allocation_keys_from_db))
        _amount_source_options = sorted(set(_predefined_allocation_keys + _amount_sources_from_db))

        with st.form("receipt_gl_mapping_form"):
            col1, col2, col3 = st.columns(3)
            with col1:
                mapping_options = ["New mapping"]
                mapping_options += [f"Edit id={m['id']} ({m['trigger_source']} / {m['allocation_key']} → {m['event_type']})" for m in (mappings or [])]
                edit_sel = st.selectbox(
                    "Mapping (choose existing to update or New mapping to create)",
                    mapping_options,
                    key="rgl_edit_sel",
                )
                edit_id = ""
                if edit_sel != "New mapping" and "id=" in edit_sel:
                    edit_id = edit_sel.replace("Edit id=", "").split(" (")[0].strip()
                trigger_source = st.selectbox(
                    "Trigger Source",
                    ["SAVE_RECEIPT", "SAVE_REVERSAL", "APPLY_UNAPPLIED"],
                    index=0,
                    key="rgl_trigger",
                )
            with col2:
                allocation_key = st.selectbox(
                    "Allocation Key",
                    _allocation_key_options,
                    key="rgl_alloc_key",
                    help="Allocation bucket from repayment engine.",
                )
                event_type = st.selectbox(
                    "Accounting Event Type",
                    _event_types_for_mapping if _event_types_for_mapping else ["PAYMENT_PRINCIPAL", "PAYMENT_REGULAR_INTEREST", "WRITEOFF_RECOVERY"],
                    key="rgl_event_type",
                )
            with col3:
                amount_source = st.selectbox(
                    "Amount Source",
                    _amount_source_options,
                    key="rgl_amount_source",
                    help="Usually same as allocation key.",
                )
                amount_sign = st.selectbox(
                    "Sign",
                    [1, -1],
                    index=0,
                    format_func=lambda x: "Normal (+1)" if x == 1 else "Reversal (-1)",
                    key="rgl_sign",
                )

            col4, col5 = st.columns(2)
            with col4:
                is_active = st.checkbox("Active", value=True, key="rgl_active")
            with col5:
                priority = st.number_input(
                    "Priority (lower runs first)",
                    min_value=0,
                    max_value=1000,
                    value=100,
                    step=10,
                    key="rgl_priority",
                )

            col_save, col_del = st.columns(2)
            with col_save:
                submit_map = st.form_submit_button("Save Mapping")
            with col_del:
                delete_map = st.form_submit_button("Delete Mapping")

            if submit_map:
                if not allocation_key or not event_type or not amount_source:
                    st.error("Allocation Key, Event Type, and Amount Source are required.")
                else:
                    mapping_id = int(edit_id) if edit_id.strip() else None
                    receipt_gl.upsert_receipt_gl_mapping(
                        mapping_id=mapping_id,
                        trigger_source=trigger_source,
                        allocation_key=allocation_key.strip(),
                        event_type=event_type.strip(),
                        amount_source=amount_source.strip(),
                        amount_sign=int(amount_sign),
                        is_active=is_active,
                        priority=int(priority),
                    )
                    st.success("Mapping saved.")
                    st.rerun()
            if delete_map and edit_id.strip():
                receipt_gl.delete_receipt_gl_mapping(int(edit_id))
                st.success("Mapping deleted.")
                st.rerun()

    if _table_exists:
        st.divider()
        with st.expander(
            "Danger zone — load or replace ALL receipt → GL mappings from bundled defaults",
            expanded=False,
        ):
            if not mappings:
                st.warning(
                    "The table is **empty**. This action inserts the bundled default mapping set "
                    "(from JSON or built-in). It does not delete existing rows."
                )
                _rgl_ack = st.checkbox(
                    "I want to insert the bundled default receipt → GL mappings.",
                    key="rgl_danger_ack_init",
                )
                _rgl_phrase = st.text_input(
                    "Type **LOAD DEFAULT MAPPINGS** exactly (case-sensitive).",
                    key="rgl_danger_phrase_init",
                )
                _rgl_ready = _rgl_ack and (_rgl_phrase.strip() == "LOAD DEFAULT MAPPINGS")
                if st.button(
                    "Load bundled default mappings (empty table only)",
                    key="rgl_danger_init_btn",
                    type="primary",
                    disabled=not _rgl_ready,
                ):
                    try:
                        if receipt_gl.initialize_default_receipt_gl_mappings():
                            st.session_state["acco_rgl_restored_ok"] = True
                            st.rerun()
                        else:
                            st.info("Mappings were already present; nothing to load.")
                    except Exception as ex:
                        st.error(f"Could not load defaults: {ex}")
                        st.exception(ex)
            else:
                st.error(
                    "This **permanently deletes every row** in `receipt_gl_mapping` and reloads bundled defaults. "
                    "Export first if you need to keep the current configuration."
                )
                _rgl_ack = st.checkbox(
                    "I understand all existing receipt → GL mapping rows will be deleted and replaced.",
                    key="rgl_danger_ack_reset",
                )
                _rgl_phrase = st.text_input(
                    "Type **REPLACE RECEIPT MAPPINGS** exactly (case-sensitive).",
                    key="rgl_danger_phrase_reset",
                )
                _rgl_ready = _rgl_ack and (_rgl_phrase.strip() == "REPLACE RECEIPT MAPPINGS")
                if st.button(
                    "Replace all receipt → GL mappings from bundled defaults",
                    key="rgl_danger_reset_btn",
                    type="primary",
                    disabled=not _rgl_ready,
                ):
                    try:
                        receipt_gl.reset_receipt_gl_mappings_to_defaults()
                        st.session_state["acco_rgl_restored_ok"] = True
                        st.rerun()
                    except Exception as ex:
                        st.error(f"Could not replace mappings: {ex}")
                        st.exception(ex)
