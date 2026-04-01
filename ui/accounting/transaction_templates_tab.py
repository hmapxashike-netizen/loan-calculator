"""Accounting: transaction templates (journal links) tab."""

from __future__ import annotations

import streamlit as st


def render_transaction_templates_tab(*, templates_ui) -> None:
    st.subheader("Transaction Templates (Journal Links)")

    if st.session_state.pop("acco_tt_restored_ok", None):
        st.success("Transaction templates were replaced from bundled defaults.")

    # Show current template counts (helps confirm reset)
    _templates_now = templates_ui.list_all_transaction_templates()
    _event_count = len(set([t["event_type"] for t in _templates_now])) if _templates_now else 0
    st.caption(f"Currently loaded: {_event_count} event types / {len(_templates_now)} journal legs.")

    templates = templates_ui.list_all_transaction_templates()
    if templates:
        evt_options = sorted(list({t["event_type"] for t in templates}))
        evt_sel = st.selectbox("Filter by Event Type", ["(All)"] + evt_options, key="tt_edit_evt")
        rows = [
            t
            for t in templates
            if evt_sel == "(All)" or t["event_type"] == evt_sel
        ]

        # Build dropdown options from DB for edit form
        accounts = templates_ui.list_accounts() or []
        system_tags_from_accounts = sorted({a["system_tag"] for a in accounts if a.get("system_tag")})
        system_tags_from_templates = sorted({t["system_tag"] for t in templates})
        all_system_tags = sorted(set(system_tags_from_accounts) | set(system_tags_from_templates))

        # Journal number lookup for display only.
        journal_numbers = {
            "LOAN_APPROVAL": "1",
            "FEE_AMORTISATION_DRAWDOWN": "2",
            "FEE_AMORTISATION_ARRANGEMENT": "2a",
            "FEE_AMORTISATION_ADMIN": "2b",
        }

        # Table header
        h0, h1, h2, h3, h4, h5, h6, h7 = st.columns([1, 2, 2, 1, 2, 1, 1, 1])
        with h0:
            st.markdown("**Journal #**")
        with h1:
            st.markdown("**Event Type**")
        with h2:
            st.markdown("**System Tag**")
        with h3:
            st.markdown("**Dr/Cr**")
        with h4:
            st.markdown("**Description**")
        with h5:
            st.markdown("**Trigger**")
        with h6:
            st.markdown("**Edit**")
        with h7:
            st.markdown("**Delete**")

        editing_id = st.session_state.get("tt_editing_id")

        for t in rows:
            col0, col1, col2, col3, col4, col5, col6, col7 = st.columns([1, 2, 2, 1, 2, 1, 1, 1])
            with col0:
                st.text(journal_numbers.get(t["event_type"], ""))
            with col1:
                st.text(t["event_type"])
            with col2:
                st.text(t["system_tag"])
            with col3:
                st.text(t["direction"][:1] if t.get("direction") else "-")
            with col4:
                st.text((t.get("description") or "")[:40] + ("..." if len(t.get("description") or "") > 40 else ""))
            with col5:
                st.text(t.get("trigger_type") or "EVENT")
            with col6:
                if st.button("Edit", key=f"tt_edit_{t['id']}"):
                    st.session_state["tt_editing_id"] = str(t["id"])
                    st.rerun()
            with col7:
                if st.button("Delete", key=f"tt_del_{t['id']}"):
                    templates_ui.delete_transaction_template(t["id"])
                    st.session_state.pop("tt_editing_id", None)
                    st.success("Template deleted.")
                    st.rerun()

        # Edit form (shown when editing a template)
        if editing_id:
            t_edit = next((x for x in templates if str(x["id"]) == editing_id), None)
            if t_edit:
                st.divider()
                st.markdown("**Edit template**")
                with st.form("tt_edit_form"):
                    new_desc = st.text_input(
                        "Description",
                        value=t_edit.get("description") or "",
                        key="tt_edit_desc",
                    )
                    col_a, col_b = st.columns(2)
                    with col_a:
                        new_trigger = st.selectbox(
                            "Trigger Type",
                            ["EVENT", "EOD", "EOM"],
                            index=["EVENT", "EOD", "EOM"].index(t_edit.get("trigger_type", "EVENT")),
                            key="tt_edit_trig",
                        )
                        current_tag = t_edit["system_tag"]
                        tag_options = [current_tag] if current_tag and current_tag not in all_system_tags else []
                        tag_options.extend(all_system_tags)
                        tag_idx = tag_options.index(current_tag) if current_tag in tag_options else 0
                        new_system_tag = st.selectbox(
                            "System Tag (GL account)",
                            tag_options,
                            index=tag_idx,
                            key="tt_edit_tag",
                        )
                    with col_b:
                        new_direction = st.selectbox(
                            "Direction",
                            ["DEBIT", "CREDIT"],
                            index=0 if t_edit["direction"] == "DEBIT" else 1,
                            key="tt_edit_dir",
                        )
                    col_save, col_cancel = st.columns(2)
                    with col_save:
                        save_btn = st.form_submit_button("Save")
                    with col_cancel:
                        cancel_btn = st.form_submit_button("Cancel")
                    if save_btn:
                        templates_ui.update_transaction_template(
                            t_edit["id"],
                            system_tag=new_system_tag.strip(),
                            direction=new_direction,
                            description=new_desc.strip() or None,
                            trigger_type=new_trigger,
                        )
                        st.session_state.pop("tt_editing_id", None)
                        st.success("Template updated.")
                        st.rerun()
                    elif cancel_btn:
                        st.session_state.pop("tt_editing_id", None)
                        st.rerun()
    else:
        st.info("No transaction templates defined.")

    st.divider()
    _show_link_journal = st.checkbox(
        "Show **Link New Journal (Double Entry)**",
        value=False,
        key="acco_tt_show_link_journal",
        help="Adds a debit and credit template row for one event type. Hidden by default to reduce clutter.",
    )
    if _show_link_journal:
        st.subheader("Link New Journal (Double Entry)")
        st.caption(
            "Adds one **debit** and one **credit** template row for an **event_type**. "
            "Posting code (e.g. `AccountingService.post_event`) must use the **exact same** event type string. "
            "Choose **Use existing** or **Define new**; the new name field appears as soon as you pick Define new."
        )
        _accounts = templates_ui.list_accounts() or []
        _all_system_tags = sorted(set(a["system_tag"] for a in _accounts if a.get("system_tag")))
        _all_system_tags = _all_system_tags or ["cash_operating", "loan_principal", "deferred_fee_liability"]
        _templates_for_evt = templates_ui.list_all_transaction_templates() or []
        _event_types = sorted(set(t["event_type"] for t in _templates_for_evt))

        _evt_mode = st.radio(
            "Event type",
            ["Use existing", "Define new"],
            horizontal=True,
            key="link_evt_mode",
            help="Define new: enter the identifier your posting code will use (same spelling/casing after normalization).",
        )
        evt_resolved = ""
        if _evt_mode == "Use existing":
            if _event_types:
                evt_resolved = st.selectbox(
                    "Existing event type",
                    _event_types,
                    key="link_evt_existing",
                )
            else:
                st.info("No event types in the database yet. Switch to **Define new** above.")
        else:
            _new_evt = st.text_input(
                "New event type name",
                placeholder="e.g. LOAN_DISBURSEMENT",
                key="link_evt_new_name",
                help="Use the same identifier your code will pass to post_event(event_type=...). "
                "Stored uppercase with spaces → underscores.",
            )
            evt_resolved = (_new_evt or "").strip().upper().replace(" ", "_")

        trigger_type = st.selectbox("Trigger Type", ["EVENT", "EOD", "EOM"], index=0, key="link_trig")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Debit leg**")
            debit_tag = st.selectbox("System tag to debit", _all_system_tags, key="link_debit_tag")
        with col2:
            st.markdown("**Credit leg**")
            credit_tag = st.selectbox("System tag to credit", _all_system_tags, key="link_credit_tag")

        desc = st.text_input("Description / memo", key="link_desc")

        if st.button("Add journal link", key="link_journal_btn", type="primary"):
            if not evt_resolved or not debit_tag or not credit_tag:
                st.error("Event type (existing or new name), debit tag, and credit tag are required.")
            elif _evt_mode == "Define new" and len(evt_resolved) < 2:
                st.error("Enter a valid new event type name (at least 2 characters after cleanup).")
            else:
                try:
                    templates_ui.link_journal(evt_resolved, debit_tag, "DEBIT", desc, trigger_type)
                    templates_ui.link_journal(evt_resolved, credit_tag, "CREDIT", desc, trigger_type)
                    st.success(f"Double-entry template for **{evt_resolved}** added.")
                    st.rerun()
                except Exception as ex:
                    st.error(str(ex))

    st.divider()
    with st.expander(
        "Danger zone — replace ALL transaction templates from bundled defaults",
        expanded=False,
    ):
        st.error(
            "This **permanently deletes every row** in transaction templates and reloads from "
            "`accounting_defaults/transaction_templates.json` when present, otherwise built-in definitions. "
            "Export the live database first: `python scripts/export_accounting_defaults.py`."
        )
        _tt_danger_ack = st.checkbox(
            "I understand all existing journal template rows will be deleted and replaced.",
            key="tt_danger_ack",
        )
        _tt_danger_phrase = st.text_input(
            "Type the phrase **REPLACE TEMPLATES** exactly (case-sensitive) to enable the action.",
            key="tt_danger_phrase",
        )
        _tt_danger_ready = _tt_danger_ack and (_tt_danger_phrase.strip() == "REPLACE TEMPLATES")
        if st.button(
            "Replace all templates from bundled defaults",
            key="tt_danger_restore_btn",
            type="primary",
            disabled=not _tt_danger_ready,
            help="Requires the checkbox and exact phrase above.",
        ):
            try:
                templates_ui.initialize_default_transaction_templates()
                st.session_state["acco_tt_restored_ok"] = True
                st.rerun()
            except Exception as e:
                st.error(f"Restore failed: {e}")
