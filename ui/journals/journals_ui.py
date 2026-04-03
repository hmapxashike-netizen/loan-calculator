"""Standalone Journals UI (manual posting + balance adjustments)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st


from style import render_main_header, render_sub_header, render_sub_sub_header

from accounting.service import AccountingService
from config import get_database_url
from ui.journals.helpers import (
    MANUAL_SUBACCOUNT_PLACEHOLDER,
    ordered_system_tags_for_direction,
    widget_key_part,
)
from ui.journals.posting_leaves import (
    clear_posting_leaf_accounts_cache,
    get_posting_leaf_accounts_for_balance_adjust,
)


def render_journals_ui(*, get_system_date) -> None:
    svc = AccountingService()

    try:
        bad_journals = svc.list_unbalanced_journal_entries()
        if bad_journals:
            st.error(
                f"**Data integrity:** {len(bad_journals)} journal header(s) are **materially** unbalanced "
                "(per-line 10dp, then totals compared at **2dp**; sub–2dp drift is ignored). "
                "**Avoid:** unbalanced journals cannot be saved. "
                "**Flag:** listed here + **Balanced** in Statements. "
                "Use **Repair LOAN_APPROVAL** when applicable."
            )
            with st.expander("Unbalanced journal headers (detail) & repair"):
                st.dataframe(pd.DataFrame([dict(r) for r in bad_journals]), width="stretch")
                st.caption(
                    "**imbalance_2dp** = rounded total debits minus rounded total credits (each total = sum of 10dp line amounts). "
                    "Classic LOAN_APPROVAL bug: principal debit too small vs cash + deferred fees — re-post fixes when supported."
                )
                repair_id = st.number_input(
                    "Loan ID (LOAN_APPROVAL repair)",
                    min_value=1,
                    step=1,
                    key="journals_repair_loan_id",
                )
                if st.button("Re-post LOAN_APPROVAL from loan record", key="journals_repair_loan_btn"):
                    try:
                        svc.repost_loan_approval_journal(int(repair_id), created_by="ui_user")
                        st.success(f"Re-posted LOAN_APPROVAL for loan {int(repair_id)}.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
    except Exception as ex:
        st.caption(f"Could not check journal double-entry integrity: {ex}")
    
    tab_manual, tab_adjust = st.tabs(["Manual Journals", "Balance Adjustments"])
    
    with tab_manual:
        render_sub_sub_header("Post Manual Journal")
        with st.form("journals_manual_journal_form"):
            templates_all = svc.list_all_transaction_templates()
            event_types = sorted({t["event_type"] for t in templates_all})
    
            col_l, col_r = st.columns(2)
            with col_l:
                loan_id = st.text_input(
                    "Loan ID (optional)",
                    help="Required for templates that resolve accounts from the loan (e.g. PRODUCT / LOAN_CAPTURE).",
                )
                event_type = st.selectbox(
                    "Journal template (event type)",
                    event_types if event_types else ["(no templates configured)"],
                )
                amount = st.number_input("Amount", min_value=0.0, step=0.01, format="%.2f")
            with col_r:
                st.caption(
                    "Enter a **loan ID** when the template needs it to auto-pick accounts. "
                    "Otherwise use subaccount dropdowns where shown."
                )
                description = st.text_input("Narration (description)")
                is_reversal = st.checkbox("Reverse entry (swap debits and credits)", value=False)
    
            _et_ok = event_type and event_type != "(no templates configured)"
            loan_id_int = int(str(loan_id).strip()) if loan_id and str(loan_id).strip().isdigit() else None
            tmpl_rows = svc.get_transaction_templates(event_type) if _et_ok else []
            dr_tags = ordered_system_tags_for_direction(tmpl_rows, "DEBIT")
            cr_tags = ordered_system_tags_for_direction(tmpl_rows, "CREDIT")
    
            st.markdown("**Accounts from template**")
            st.caption(
                "Debit lines (left) and credit lines (right). When the tagged account has **subaccounts**, "
                "choose a leaf; **"
                + MANUAL_SUBACCOUNT_PLACEHOLDER
                + "** is not valid for posting."
            )
            c_dr, c_cr = st.columns(2)
    
            def _mj_render_one_side(side_label: str, side_k: str, tags: list[str]) -> None:
                st.markdown(f"**{side_label}**")
                if not tags:
                    st.caption("No lines on this side for this template.")
                    return
                for tag in tags:
                    st.caption(f"Tag `{tag}`")
                    row = svc.fetch_account_row_for_system_tag(tag)
                    if not row:
                        st.warning(f"No chart account carries system tag `{tag}`.")
                        continue
                    resolved, err = svc.try_resolve_posting_account_for_tag(tag, loan_id=loan_id_int)
                    kids = svc.list_active_direct_children_accounts(row["id"])
                    if resolved:
                        st.caption(
                            f"Posting account: **{resolved.get('code')}** — {resolved.get('name')} "
                            "*(auto-resolved)*"
                        )
                        continue
                    if kids:
                        labels = [MANUAL_SUBACCOUNT_PLACEHOLDER] + [
                            f"{c['code']} — {c['name']}" for c in kids
                        ]
                        sk = f"mj_pick_{side_k}_{widget_key_part(event_type)}_{widget_key_part(tag)}"
                        st.selectbox(
                            "Posting account",
                            range(len(labels)),
                            format_func=lambda i, lab=labels: lab[i],
                            index=0,
                            key=sk,
                            help="Pick the leaf account; the first option cannot be posted.",
                        )
                        continue
                    st.error(err or f"Cannot resolve `{tag}` and there are no subaccounts to choose.")
    
            with c_dr:
                _mj_render_one_side("Debit accounts", "dr", dr_tags)
            with c_cr:
                _mj_render_one_side("Credit accounts", "cr", cr_tags)
    
            journal_to_reverse = None
            if is_reversal:
                all_entries = svc.get_journal_entries()
                candidates = [
                    e
                    for e in all_entries
                    if e["event_tag"] == event_type
                    and (not loan_id or (e.get("event_id") == loan_id))
                ]
                if candidates:
                    labels = [
                        f"{e['entry_date']} | {e.get('reference') or ''} | {e['event_tag']} (ID: {e['id']})"
                        for e in candidates
                    ]
                    sel = st.selectbox(
                        "Journal to reverse",
                        labels,
                        help="Same template and accounts as the original; debits/credits are swapped on post.",
                    )
                    journal_to_reverse = candidates[labels.index(sel)]
                else:
                    st.info(
                        "No matching journals found to reverse for this template "
                        "and (if provided) loan ID."
                    )
    
            with st.expander("Extra account_overrides (JSON, optional)", expanded=False):
                overrides_json = st.text_area(
                    "Additional overrides",
                    height=56,
                    placeholder='{"some_system_tag": "uuid-of-leaf-account"}',
                    help="Merged with account picks above; explicit picks for a tag win over JSON for that tag.",
                    key="manual_journal_overrides_json",
                    label_visibility="collapsed",
                )
                st.caption("Optional. Per-tag dropdown selections above are applied automatically.")
    
            submitted3 = st.form_submit_button("Post journal")
            if submitted3:
                if not _et_ok or amount <= 0:
                    st.error("Choose a valid template and enter an amount greater than zero.")
                elif is_reversal and journal_to_reverse is None:
                    st.error("Select the original journal entry to reverse.")
                else:
                    ref = f"MANUAL-{int(datetime.now().timestamp())}"
                    if is_reversal and journal_to_reverse:
                        ref = f"REV-{journal_to_reverse.get('reference') or journal_to_reverse['id']}"
                        if not description:
                            description = f"Reversal of entry {journal_to_reverse['id']}"
                    _manual_loan_id = None
                    if loan_id and str(loan_id).strip().isdigit():
                        _manual_loan_id = int(str(loan_id).strip())
    
                    import json as _json
    
                    _merge_ov: dict = {}
                    _pick_errs: list[str] = []
                    _payload_ok = True
                    if overrides_json and str(overrides_json).strip():
                        try:
                            _pour = _json.loads(overrides_json)
                            if not isinstance(_pour, dict):
                                st.error("account_overrides JSON must be an object.")
                                _payload_ok = False
                            else:
                                _merge_ov.update(_pour)
                        except _json.JSONDecodeError as je:
                            st.error(f"Invalid JSON: {je}")
                            _payload_ok = False
    
                    tmpl_submit = svc.get_transaction_templates(event_type) if _payload_ok else []
                    if _payload_ok and not tmpl_submit:
                        st.error("This event type has no transaction template lines.")
                        _payload_ok = False
    
                    if _payload_ok:
                        dr_s = ordered_system_tags_for_direction(tmpl_submit, "DEBIT")
                        cr_s = ordered_system_tags_for_direction(tmpl_submit, "CREDIT")
                        for side_k, tags in (("dr", dr_s), ("cr", cr_s)):
                            for tag in tags:
                                row = svc.fetch_account_row_for_system_tag(tag)
                                if not row:
                                    _pick_errs.append(f"No chart account for system tag `{tag}`.")
                                    continue
                                kids = svc.list_active_direct_children_accounts(row["id"])
                                resolved, err = svc.try_resolve_posting_account_for_tag(
                                    tag, loan_id=_manual_loan_id
                                )
                                if resolved:
                                    continue
                                if not kids:
                                    _pick_errs.append(
                                        f"`{tag}`: {err or 'Cannot resolve posting account.'}"
                                    )
                                    continue
                                sk = f"mj_pick_{side_k}_{widget_key_part(event_type)}_{widget_key_part(tag)}"
                                idx = int(st.session_state.get(sk, 0))
                                uuid_list = [None] + [str(c["id"]) for c in kids]
                                if idx <= 0 or idx >= len(uuid_list) or not uuid_list[idx]:
                                    _pick_errs.append(
                                        f"Choose a sub account for `{tag}` on the "
                                        f"{'debit' if side_k == 'dr' else 'credit'} side "
                                        f"({MANUAL_SUBACCOUNT_PLACEHOLDER!r} is not valid for posting)."
                                    )
                                else:
                                    _merge_ov[tag] = uuid_list[idx]
    
                    if _pick_errs:
                        for msg in _pick_errs:
                            st.error(msg)
                        _payload_ok = False
    
                    _manual_payload = {"account_overrides": _merge_ov} if (_payload_ok and _merge_ov) else None
    
                    if _payload_ok:
                        try:
                            svc.post_event(
                                event_type=event_type,
                                reference=ref,
                                description=description,
                                event_id=loan_id
                                or (journal_to_reverse.get("event_id") if journal_to_reverse else "MANUAL"),
                                created_by="ui_user",
                                entry_date=datetime.today().date(),
                                amount=Decimal(str(amount)),
                                payload=_manual_payload,
                                is_reversal=is_reversal,
                                loan_id=_manual_loan_id,
                            )
                            st.success("Manual journal posted successfully.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error posting journal: {e}")
    
    with tab_adjust:
        render_sub_sub_header("Balance Adjustment Journal")
        st.info(
            "One-off GL corrections. Only **posting accounts** are listed: active accounts with **no** "
            "active subaccounts (the youngest nodes in each branch — e.g. A700000 alone if it has no children; "
            "otherwise A710000 / A720000 / … down to leaves). Labels show the code path from the root."
        )
        st.caption(
            "The leaf list is cached briefly (about two minutes) to avoid repeating the same chart walk on "
            "every Streamlit rerun; one `list_accounts` query plus an in-memory pass is already light."
        )
    
        posting_leaves = get_posting_leaf_accounts_for_balance_adjust()
    
        dr_i = 0
        cr_i = 0
        with st.form("balance_adjust_form"):
            col_dt, col_amt = st.columns([1, 1])
            with col_dt:
                value_date = st.date_input("Value Date", value=get_system_date())
            with col_amt:
                amount = st.number_input("Amount", min_value=0.0, step=0.01)
    
            col_dr, col_cr = st.columns(2)
            if posting_leaves:
                with col_dr:
                    dr_i = st.selectbox(
                        "Debit account",
                        range(len(posting_leaves)),
                        format_func=lambda i, pl=posting_leaves: pl[i]["display_label"],
                        key="bal_adj_dr_leaf_idx",
                    )
                with col_cr:
                    cr_i = st.selectbox(
                        "Credit account",
                        range(len(posting_leaves)),
                        format_func=lambda i, pl=posting_leaves: pl[i]["display_label"],
                        key="bal_adj_cr_leaf_idx",
                    )
            else:
                with col_dr:
                    st.selectbox(
                        "Debit account",
                        ["(no posting accounts in chart)"],
                        disabled=True,
                        key="bal_adj_dr_leaf_idx_empty",
                    )
                with col_cr:
                    st.selectbox(
                        "Credit account",
                        ["(no posting accounts in chart)"],
                        disabled=True,
                        key="bal_adj_cr_leaf_idx_empty",
                    )
    
            narration = st.text_input("Narration / Description", key="bal_adj_narr")
    
            submitted_adj = st.form_submit_button(
                "Post Balance Adjustment",
                disabled=not posting_leaves,
            )
    
        if submitted_adj and posting_leaves:
            dr_row = posting_leaves[int(dr_i)] if 0 <= int(dr_i) < len(posting_leaves) else None
            cr_row = posting_leaves[int(cr_i)] if 0 <= int(cr_i) < len(posting_leaves) else None
            dr_code = dr_row["code"] if dr_row else None
            cr_code = cr_row["code"] if cr_row else None
            dr_id = dr_row["id"] if dr_row else None
            cr_id = cr_row["id"] if cr_row else None
    
            if not dr_code or not cr_code or not dr_id or not cr_id:
                st.error("Please select both debit and credit posting accounts.")
            elif dr_code == cr_code:
                st.error("Debit and credit accounts must be different.")
            elif amount <= 0:
                st.error("Amount must be greater than zero.")
            else:
                try:
                    conn = psycopg2.connect(
                        get_database_url(), cursor_factory=psycopg2.extras.RealDictCursor
                    )
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO journal_entries (entry_date, reference, description, event_id, event_tag, created_by)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                RETURNING id
                                """,
                                (
                                    value_date,
                                    "BAL_ADJ",
                                    narration or "Balance adjustment journal",
                                    None,
                                    "BALANCE_ADJUSTMENT",
                                    "ui_user",
                                ),
                            )
                            entry_id = cur.fetchone()["id"]
    
                            cur.execute(
                                """
                                INSERT INTO journal_items (entry_id, account_id, debit, credit, memo)
                                VALUES (%s, %s, %s, %s, %s)
                                """,
                                (entry_id, dr_id, Decimal(str(amount)), Decimal("0.0"), narration),
                            )
                            cur.execute(
                                """
                                INSERT INTO journal_items (entry_id, account_id, debit, credit, memo)
                                VALUES (%s, %s, %s, %s, %s)
                                """,
                                (entry_id, cr_id, Decimal("0.0"), Decimal(str(amount)), narration),
                            )
                        conn.commit()
                        st.success("Balance adjustment journal posted successfully.")
                        clear_posting_leaf_accounts_cache()
                        st.rerun()
                    finally:
                        conn.close()
                except Exception as e:
                    st.error(f"Error posting balance adjustment journal: {e}")
