"""Accounting: Chart of Accounts tab (slice 1)."""

from __future__ import annotations

import pandas as pd
import streamlit as st


from style import render_main_header, render_sub_header, render_sub_sub_header

from ui.components import inject_tertiary_hyperlink_css_once
from ui.journals.posting_leaves import clear_posting_leaf_accounts_cache


def _coa_parent_display_label(acct_list: list, parent_id) -> str:
    """Human-readable parent for COA edit confirmations."""
    if parent_id is None:
        return "(None — top level)"
    for a in acct_list:
        if str(a.get("id")) == str(parent_id):
            c = (a.get("code") or "").strip()
            n = (a.get("name") or "").strip()
            return f"{c} — {n}" if (c or n) else str(parent_id)
    return f"(unknown id {parent_id})"


def render_accounting_coa_tab(
    *,
    coa,
    loan_management_available: bool,
    list_products,
) -> None:
        _coa_banner = st.session_state.pop("coa_parent_edit_banner", None)
        if _coa_banner:
            _bkind, _btext = _coa_banner
            if _bkind == "success":
                st.success(_btext)
            else:
                st.info(_btext)
        render_sub_sub_header("Chart of Accounts")
        if not coa.is_coa_initialized():
            st.warning("Chart of Accounts is not initialized.")
            if st.button("Initialize Default Chart of Accounts"):
                coa.initialize_default_coa()
                st.success(
                    "Chart of Accounts initialized from bundled defaults "
                    "(accounting_defaults/chart_of_accounts.json when present; else built-in)."
                )
                st.rerun()
    
        accounts = coa.list_accounts()
        if accounts:
            df_accounts = pd.DataFrame([{
                "Code": a["code"],
                "Name": a["name"],
                "Category": a["category"],
                "System Tag": a["system_tag"] or "",
                "Parent Code": a["parent_code"] or "",
                "Resolve subaccounts": (a.get("subaccount_resolution") or ""),
            } for a in accounts])
            st.dataframe(df_accounts, use_container_width=True, hide_index=True)
    
        _coa_rows = list(accounts or [])

        st.session_state.setdefault("coa_subwiz_show", False)
        st.session_state.setdefault("acco_coa_show_add_account", False)
        st.session_state.setdefault("acco_coa_show_edit_parent", False)
        inject_tertiary_hyperlink_css_once()
        _coa_tool_c1, _coa_tool_c2, _coa_tool_c3 = st.columns(3, gap="small")
        with _coa_tool_c1:
            if st.button(
                "Add or edit subaccounts",
                type="tertiary",
                key="coa_toggle_subwiz",
                use_container_width=True,
                help="Create posting subaccounts under a tagged parent, choose how the system resolves the leaf, "
                "or edit names and soft-deactivate existing subaccounts. Codes cannot be changed after creation.",
            ):
                st.session_state["coa_subwiz_show"] = not bool(st.session_state.get("coa_subwiz_show"))
                st.rerun()
        with _coa_tool_c2:
            if st.button(
                "Show Add Custom Account",
                type="tertiary",
                key="coa_toggle_add_account",
                use_container_width=True,
                help="Create a new GL account. Hidden by default to reduce clutter.",
            ):
                st.session_state["acco_coa_show_add_account"] = not bool(
                    st.session_state.get("acco_coa_show_add_account")
                )
                st.rerun()
        with _coa_tool_c3:
            if st.button(
                "Show Edit account parent",
                type="tertiary",
                key="coa_toggle_edit_parent",
                use_container_width=True,
                help="Change the parent of an existing account. Hidden by default to reduce clutter.",
            ):
                st.session_state["acco_coa_show_edit_parent"] = not bool(
                    st.session_state.get("acco_coa_show_edit_parent")
                )
                st.rerun()

        _show_sub_wiz = bool(st.session_state.get("coa_subwiz_show"))
        if _show_sub_wiz:
            render_sub_sub_header("Subaccount setup")
            _wiz_create, _wiz_edit = st.tabs(["Create subaccounts", "Edit or deactivate"])
            with _wiz_create:
                _sw_banner = st.session_state.pop("coa_subwiz_save_banner", None)
                if _sw_banner:
                    _sw_kind, _sw_text = _sw_banner
                    if _sw_kind == "success":
                        st.success(_sw_text)
                    elif _sw_kind == "warning":
                        st.warning(_sw_text)
                    else:
                        st.error(_sw_text)
                _tagged_for_sub = [a for a in _coa_rows if (a.get("system_tag") or "").strip()]
                if not _tagged_for_sub:
                    st.info("No GL rows have a **system tag** yet. Subaccounts are created under the tagged parent that templates reference.")
                else:
                    _tw_labels = [f"{a.get('code','')} — {a.get('name','')}  (tag: {a.get('system_tag')})" for a in _tagged_for_sub]
                    _tw_ids = [str(a["id"]) for a in _tagged_for_sub]
                    _tw_i = st.selectbox(
                        "Parent account (must carry the system tag)",
                        range(len(_tw_labels)),
                        format_func=lambda i: _tw_labels[i],
                        key="coa_subwiz_parent_i",
                        help="Pick the GL row that already has the **system tag** used by transaction templates. "
                        "New subaccounts sit under this parent and inherit its category; they do not get their own tag.",
                    )
                    _par = _tagged_for_sub[_tw_i]
                    _par_id = str(_par["id"])
                    _par_tag = (_par.get("system_tag") or "").strip()
                    _par_name = (_par.get("name") or "").strip()
                    _n_existing = sum(1 for a in _coa_rows if a.get("parent_id") is not None and str(a.get("parent_id")) == _par_id)
                    _min_subs = 2 if _n_existing == 0 else 1
    
                    try:
                        _wprods = list_products(active_only=False) if loan_management_available else []
                    except Exception:
                        _wprods = []
                    _wplab = [f"{p['code']} — {p['name']}" for p in _wprods] if _wprods else []
                    _wpcodes = [p["code"] for p in _wprods] if _wprods else []
    
                    st.markdown(
                        "<div style='margin:0.2rem 0 0.45rem 0;font-size:1.15rem;'>"
                        "<strong>Parent system tag:</strong> "
                        f"<code style='background:#f1f5f9;border-radius:4px;padding:0.12rem 0.35rem;'>{_par_tag or '—'}</code>"
                        "<span style='color:#64748b;font-size:1.025rem;margin-left:0.5rem;'>"
                        "(updates with the parent you select above)</span></div>",
                        unsafe_allow_html=True,
                    )
    
                    _mode_labels = {
                        "loan_capture": "Loan capture — user picks operating cash subaccount (cash_operating tag only)",
                        "journal": "Manual journals — user picks subaccount in journals / overrides",
                        "product": "By product — one subaccount per product (name, code, product in each row)",
                    }
                    _mode = st.radio(
                        "How will the system pick the subaccount for postings?",
                        options=list(_mode_labels.keys()),
                        format_func=lambda k: _mode_labels[k],
                        key="coa_subwiz_mode",
                        help="This sets the parent row **posting rule**. It applies wherever the parent tag would have received an entry; "
                        "the chosen leaf subaccount receives it instead.",
                    )
                    st.caption(
                        "**Loan capture:** loan capture / Teller use the **source cash account cache** (A100000 tree). "
                        "After save, the cache is refreshed so new leaves appear when eligible. "
                        "Only valid when the parent tag is **cash_operating**."
                    )
                    st.caption(
                        "**Manual journals:** posting leaf lists already include every leaf account; pick the subaccount in the "
                        "Journals UI or pass **account_overrides** for automated events. "
                        "**By product:** each new subaccount must be mapped to a **different** product code."
                    )
    
                    cct1, cct2, cct3 = st.columns(3)
                    with cct1:
                        if _mode == "product":
                            if _wpcodes:
                                if len(_wpcodes) > 99:
                                    st.warning(
                                        "More than **99** products: only the first **99** are used here "
                                        "(grandchild code suffix limit `NN`)."
                                    )
                                _n_cap = min(len(_wpcodes), 99)
                                st.caption(
                                    f"**By product:** **{_n_cap}** subaccount row(s) — one per product (catalog size)."
                                )
                                _n_new = _n_cap
                            else:
                                st.warning("No products in the catalog. Add products first, or choose another resolution mode.")
                                _n_new = _min_subs
                        else:
                            _n_new = int(
                                st.number_input(
                                    "How many sibling subaccounts to create",
                                    min_value=_min_subs,
                                    max_value=30,
                                    value=_min_subs,
                                    step=1,
                                    key="coa_subwiz_n",
                                    help="First time this parent has subaccounts: create **at least two** siblings. "
                                    "If subaccounts already exist, you may add **one or more** additional siblings.",
                                )
                            )
                    with cct2:
                        st.caption(f"Existing subaccounts under this parent: **{_n_existing}**")
                    with cct3:
                        st.caption("Category is copied from the parent automatically.")
    
                    if st.session_state.get("coa_subwiz_autoname_parent") != str(_par_id):
                        st.session_state["coa_subwiz_autoname_parent"] = str(_par_id)
                        st.session_state["coa_subwiz_autoname_track"] = {}
                        st.session_state["coa_subwiz_autocode_track"] = {}
                    if st.session_state.get("_coa_subwiz_track_mode") != _mode:
                        _prev_mode_sw = st.session_state.get("_coa_subwiz_track_mode")
                        st.session_state["_coa_subwiz_track_mode"] = _mode
                        st.session_state["coa_subwiz_autoname_track"] = {}
                        st.session_state["coa_subwiz_autocode_track"] = {}
                        if _prev_mode_sw == "product" and _mode != "product":
                            for _kj in range(35):
                                st.session_state.pop(f"coa_subwiz_name_{_kj}", None)
                                st.session_state.pop(f"coa_subwiz_prod_{_kj}", None)
                                st.session_state.pop(f"coa_subwiz_prodtxt_{_kj}", None)
    
                    if _mode == "product" and _wplab:
                        _track_nm = st.session_state.setdefault("coa_subwiz_autoname_track", {})
                        for _jn in range(int(_n_new)):
                            _pi0 = st.session_state.get(f"coa_subwiz_prod_{_jn}")
                            if _pi0 is None:
                                _pi0 = min(_jn, len(_wplab) - 1)
                            _pi0 = int(_pi0)
                            if not (0 <= _pi0 < len(_wpcodes)):
                                _pi0 = min(_jn, len(_wplab) - 1)
                            _pcd = _wpcodes[_pi0]
                            _sug_nm = f"{_par_name} - {_pcd}" if _par_name else _pcd
                            _sig_nm = (str(_par_id), _mode, _jn, _pi0)
                            if _track_nm.get(_jn) != _sig_nm:
                                _track_nm[_jn] = _sig_nm
                                st.session_state[f"coa_subwiz_name_{_jn}"] = _sug_nm
    
                    _suggested: list[str] = []
                    try:
                        _suggested = coa.peek_next_grandchild_codes_for_parent(_par_id, int(_n_new))
                    except Exception as _ex:
                        st.warning(f"Could not suggest grandchild codes: {_ex}. Enter codes manually (must be unique).")
                        _suggested = [""] * int(_n_new)
    
                    if _mode == "product":
                        st.markdown(
                            "**Subaccounts** — each row: **name**, **code** (defaults to next free `PARENT-NN` when possible), "
                            "**product** (one product per row; no duplicates across rows)."
                        )
                    else:
                        st.markdown(
                            "**Subaccounts** — **name** and **code** per row "
                            "(codes default to the next free `PARENT-NN` pattern when possible)."
                        )
    
                    _names: list[str] = []
                    _codes: list[str] = []
                    _prod_assign: list[tuple[str, int]] | None = [] if _mode == "product" else None
    
                    for _j in range(int(_n_new)):
                        _def_c = _suggested[_j] if _j < len(_suggested) else ""
                        _code_key = f"coa_subwiz_code_{_j}"
                        _track_cd = st.session_state.setdefault("coa_subwiz_autocode_track", {})
                        _sig_cd = (str(_par_id), _mode, _j, str(_def_c or "").strip().upper())
                        _prev_sig_cd = _track_cd.get(_j)
                        _cur_cd = st.session_state.get(_code_key)
                        _should_set_cd = _cur_cd is None or not str(_cur_cd).strip()
                        if (not _should_set_cd) and _prev_sig_cd and len(_prev_sig_cd) >= 4:
                            _prev_sug = str(_prev_sig_cd[3] or "").strip().upper()
                            _should_set_cd = str(_cur_cd).strip().upper() == _prev_sug
                        if _prev_sig_cd != _sig_cd and _should_set_cd:
                            st.session_state[_code_key] = _def_c
                        _track_cd[_j] = _sig_cd
                        if _mode == "product":
                            _rj1, _rj2, _rj3 = st.columns(3)
                            with _rj1:
                                _nm = st.text_input(
                                    f"Row {_j + 1} — name",
                                    key=f"coa_subwiz_name_{_j}",
                                    help="Displayed name in the chart and dropdowns.",
                                )
                            with _rj2:
                                _cd = st.text_input(
                                    f"Row {_j + 1} — code",
                                    key=_code_key,
                                    help="Must be unique. Grandchild pattern under a 7-character parent: `BASE-01`, `BASE-02`, …",
                                )
                            with _rj3:
                                if _wplab:
                                    _pi2 = st.selectbox(
                                        f"Row {_j + 1} — product",
                                        range(len(_wplab)),
                                        index=min(_j, len(_wplab) - 1),
                                        format_func=lambda i, _labels=_wplab: _labels[i],
                                        key=f"coa_subwiz_prod_{_j}",
                                        help=(
                                            f"Maps (`product_code`, `{_par_tag}`) to this subaccount. "
                                            "Each row must use a different product. "
                                            "Name defaults to **parent name - product code**."
                                        ),
                                    )
                                    _prod_assign.append((_wpcodes[_pi2], _j))
                                else:
                                    _pc_txt = st.text_input(
                                        f"Row {_j + 1} — product code",
                                        key=f"coa_subwiz_prodtxt_{_j}",
                                        help="Must match `loans.product_code` / products.code.",
                                    )
                                    _prod_assign.append((_pc_txt.strip(), _j))
                            _names.append(_nm)
                            _codes.append(_cd)
                        else:
                            _rj1, _rj2 = st.columns(2)
                            with _rj1:
                                _nm = st.text_input(
                                    f"Row {_j + 1} — name",
                                    key=f"coa_subwiz_name_{_j}",
                                    help="Displayed name in the chart and dropdowns.",
                                )
                            with _rj2:
                                _cd = st.text_input(
                                    f"Row {_j + 1} — code",
                                    key=_code_key,
                                    help="Must be unique. Grandchild pattern under a 7-character parent: `BASE-01`, `BASE-02`, …",
                                )
                            _names.append(_nm)
                            _codes.append(_cd)
    
                    if st.button("Create subaccounts and save rule", type="primary", key="coa_subwiz_save"):
                        _children_tuples: list[tuple[str, str]] = []
                        for _j in range(int(_n_new)):
                            _c = (_codes[_j] or "").strip()
                            _n = (_names[_j] or "").strip()
                            if not _c or not _n:
                                st.error(f"Row {_j + 1}: code and name are required.")
                                break
                            _children_tuples.append((_c, _n))
                        else:
                            _par_code = (_par.get("code") or "").strip()
                            _par_nm = (_par.get("name") or "").strip()
                            _par_lbl = f"{_par_code}" + (f" — {_par_nm}" if _par_nm else "")
                            _n_ch = len(_children_tuples)
                            if _mode == "loan_capture" and _par_tag != "cash_operating":
                                st.error(
                                    "This rule applies only when the parent system tag is **cash_operating**. "
                                    "Pick a different rule or use the tagged cash parent."
                                )
                            elif _mode == "product":
                                _pcs = [x[0] for x in (_prod_assign or [])]
                                if len(_pcs) != len(set(_pcs)):
                                    st.error("Each subaccount must map to a **different** product code.")
                                elif any(not p for p in _pcs):
                                    st.error("Every row needs a product code.")
                                else:
                                    try:
                                        coa.create_subaccounts_under_tagged_parent(
                                            _par_id,
                                            _children_tuples,
                                            resolution_mode="PRODUCT",
                                            product_assignments=_prod_assign,
                                            parent_system_tag=_par_tag,
                                        )
                                        try:
                                            clear_posting_leaf_accounts_cache()
                                        except Exception:
                                            pass
                                        _plist = ", ".join(_pcs[:8])
                                        if len(_pcs) > 8:
                                            _plist += f", … (+{len(_pcs) - 8} more)"
                                        st.session_state["coa_subwiz_save_banner"] = (
                                            "success",
                                            f"**Save complete.** Created **{_n_ch}** subaccount(s) under **{_par_lbl}**. "
                                            f"Resolution **PRODUCT** — `product_gl_subaccount_map` updated for system tag "
                                            f"`{_par_tag}` (products: {_plist}).",
                                        )
                                        st.rerun()
                                    except Exception as _e:
                                        st.error(f"**Save failed.** {_e}")
                            else:
                                try:
                                    coa.create_subaccounts_under_tagged_parent(
                                        _par_id,
                                        _children_tuples,
                                        resolution_mode="LOAN_CAPTURE" if _mode == "loan_capture" else "JOURNAL",
                                        product_assignments=None,
                                        parent_system_tag=None,
                                    )
                                    _cache_warn = None
                                    if _mode == "loan_capture":
                                        try:
                                            coa.refresh_source_cash_account_cache()
                                        except Exception as _re:
                                            _cache_warn = str(_re)
                                    try:
                                        clear_posting_leaf_accounts_cache()
                                    except Exception:
                                        pass
                                    _rule = "LOAN_CAPTURE" if _mode == "loan_capture" else "JOURNAL"
                                    if _cache_warn:
                                        st.session_state["coa_subwiz_save_banner"] = (
                                            "warning",
                                            f"**Subaccounts saved** ({_n_ch} under **{_par_lbl}**); parent rule **{_rule}** applied. "
                                            f"**Source cash cache** did not refresh: {_cache_warn}",
                                        )
                                    else:
                                        st.session_state["coa_subwiz_save_banner"] = (
                                            "success",
                                            f"**Save complete.** Created **{_n_ch}** subaccount(s) under **{_par_lbl}**. "
                                            f"Parent posting rule **{_rule}** saved."
                                            + (
                                                " Source cash account cache refreshed."
                                                if _mode == "loan_capture"
                                                else ""
                                            ),
                                        )
                                    st.rerun()
                                except Exception as _e:
                                    st.error(f"**Save failed.** {_e}")
    
            with _wiz_edit:
                _edit_banner = st.session_state.pop("coa_subwiz_edit_banner", None)
                if _edit_banner:
                    _bk, _bt = _edit_banner
                    if _bk == "success":
                        st.success(_bt)
                    elif _bk == "warning":
                        st.warning(_bt)
                    else:
                        st.error(_bt)
                _cpp = {str(a.get("parent_id")) for a in _coa_rows if a.get("parent_id")}
                _parents_with_children = [a for a in _coa_rows if str(a.get("id")) in _cpp]
                if not _parents_with_children:
                    st.info("No accounts with subaccounts yet.")
                else:
                    _ep_labels = [f"{a.get('code','')} — {a.get('name','')}" for a in _parents_with_children]
                    _ep_ids = [str(a["id"]) for a in _parents_with_children]
                    _ep_i = st.selectbox(
                        "Parent account",
                        range(len(_ep_labels)),
                        format_func=lambda i: _ep_labels[i],
                        key="coa_subwiz_edit_parent",
                    )
                    _ep_id = _ep_ids[_ep_i]
                    _ep = _parents_with_children[_ep_i]
                    _ep_code = str(_ep.get("code") or "").strip().upper()
                    _kids = [a for a in _coa_rows if a.get("parent_id") is not None and str(a.get("parent_id")) == _ep_id]
                    _kids.sort(key=lambda a: (str(a.get("code") or ""),))
                    if not _kids:
                        st.warning("No subaccounts under this parent.")
                    else:
                        _k_labels = [
                            f"{a.get('code','')} — {a.get('name','')}  [{'active' if a.get('is_active') is not False else 'inactive'}]"
                            for a in _kids
                        ]
                        _k_ids = [str(a["id"]) for a in _kids]
                        _ki = st.selectbox(
                            "Subaccount",
                            range(len(_k_labels)),
                            format_func=lambda i: _k_labels[i],
                            key="coa_subwiz_edit_child",
                        )
                        _ch = _kids[_ki]
                        # Display current code as text (not a widget) so we can refresh it immediately
                        # without Streamlit's "cannot modify widget state after instantiation" constraint.
                        _cur_code_state_key = f"coa_subwiz_edit_code_display_{_k_ids[_ki]}"
                        if _cur_code_state_key not in st.session_state:
                            st.session_state[_cur_code_state_key] = str(_ch.get("code") or "")
                        st.markdown(
                            f"**Current code:** `{st.session_state.get(_cur_code_state_key) or str(_ch.get('code') or '')}`"
                        )
                        st.markdown("**Re-code subaccount (admin)**")
                        _new_code_raw = st.text_input(
                            "New code (or suffix NN)",
                            value="",
                            key="coa_subwiz_edit_code_new",
                            help="Enter full code like `A120001-01` or just suffix like `01` / `1` to build from the selected parent.",
                        )
                        _confirm_recode = st.checkbox(
                            "I confirm re-coding this subaccount",
                            key="coa_subwiz_recode_ack",
                            help="This changes `accounts.code`. Postings are keyed by account_id, so existing journals remain linked.",
                        )
                        _new_nm = st.text_input(
                            "Account name",
                            value=str(_ch.get("name") or ""),
                            key="coa_subwiz_edit_name",
                            help="Safe to change; does not affect posting keys.",
                        )
                        ec1, ec2 = st.columns(2)
                        with ec1:
                            if st.button("Save name", key="coa_subwiz_save_name"):
                                try:
                                    coa.update_gl_account_name(_k_ids[_ki], _new_nm)
                                    st.success("Name updated.")
                                    st.rerun()
                                except Exception as _e:
                                    st.error(str(_e))
                        with ec2:
                            _confirm_deact = st.checkbox(
                                "I confirm soft-deactivate this subaccount",
                                key="coa_subwiz_deact_ack",
                                help="Sets the row inactive, removes product→GL map rows pointing at it, and hides it from new posting pickers. "
                                "Cannot deactivate if this row still has active children.",
                            )
                            if st.button("Soft-deactivate", key="coa_subwiz_deact"):
                                if not _confirm_deact:
                                    st.error("Check the confirmation box first.")
                                else:
                                    try:
                                        coa.set_gl_account_active(_k_ids[_ki], False)
                                        try:
                                            clear_posting_leaf_accounts_cache()
                                        except Exception:
                                            pass
                                        st.success("Subaccount deactivated.")
                                        st.rerun()
                                    except Exception as _e:
                                        st.error(str(_e))
                        if st.button("Change code", key="coa_subwiz_recode_btn"):
                            if not _confirm_recode:
                                st.error("Check the re-code confirmation box first.")
                            else:
                                try:
                                    _in = str(_new_code_raw or "").strip()
                                    if re.fullmatch(r"\d{1,2}", _in) and _ep_code:
                                        _nn = int(_in)
                                        _in = f"{_ep_code}-{_nn:02d}"
                                    coa.update_gl_account_code(_k_ids[_ki], _in)
                                    # Update the disabled field immediately (no rerun needed).
                                    st.session_state[_cur_code_state_key] = _in
                                    st.session_state["coa_subwiz_edit_banner"] = (
                                        "success",
                                        f"**Code updated.** {_ch.get('code')} → {_in}",
                                    )
                                    st.success(f"Code updated: {_ch.get('code')} → {_in}")
                                except Exception as _e:
                                    st.session_state["coa_subwiz_edit_banner"] = ("error", f"**Code change failed.** {_e}")
                                    st.error(f"Code change failed: {_e}")
    
            with st.expander("Advanced: edit product → leaf map only", expanded=False):
                st.caption(
                    "Add or change a row in **product_gl_subaccount_map** without creating new GL accounts. "
                    "Use the same **system tag** as the tagged parent (often matches template tags)."
                )
                try:
                    _aprods = list_products(active_only=False) if loan_management_available else []
                except Exception:
                    _aprods = []
                _aplab = [f"{p['code']} — {p['name']}" for p in _aprods] if _aprods else []
                _apcodes = [p["code"] for p in _aprods] if _aprods else []
                _atmpl = coa.list_all_transaction_templates() or []
                _atags = sorted({t["system_tag"] for t in _atmpl if t.get("system_tag")})
                try:
                    _amrows = coa.list_product_gl_subaccount_map() or []
                except Exception as _aex:
                    _amrows = []
                    st.warning(f"Could not load map: {_aex}")
                if _amrows:
                    _leaf_allow: dict[str, set[str] | str] = {}
                    for m in _amrows:
                        tg = (m.get("system_tag") or "").strip()
                        if not tg or tg in _leaf_allow:
                            continue
                        try:
                            _leaf_allow[tg] = {str(x["id"]) for x in coa.list_leaf_accounts_for_system_tag(tg)}
                        except ValueError as _vex:
                            _leaf_allow[tg] = f"COA: {_vex}"
                    _coa_by_id = {str(a["id"]): a for a in _coa_rows}
                    _map_rows_disp = []
                    for m in _amrows:
                        tg = (m.get("system_tag") or "").strip()
                        aids = _leaf_allow.get(tg)
                        gid = str(m.get("gl_account_id") or "")
                        if isinstance(aids, str):
                            _ok = False
                            _why = aids
                        elif isinstance(aids, set):
                            _ok = gid in aids
                            _why = "" if _ok else "Leaf is not under this tag's COA branch"
                        else:
                            _ok, _why = False, "Unknown tag"
                        _acc = _coa_by_id.get(gid)
                        if not _acc:
                            _stem_ok, _stem_msg = False, "Mapped GL id not found in chart"
                        else:
                            _pid = _acc.get("parent_id")
                            _par = _coa_by_id.get(str(_pid)) if _pid is not None else None
                            _stem_ok, _stem_msg = coa_grandchild_prefix_matches_immediate_parent(
                                child_code=str(_acc.get("code") or ""),
                                parent_code=str(_par.get("code") or "") if _par else None,
                            )
                        _all_ok = _ok and _stem_ok
                        _check_parts: list[str] = []
                        if not _ok and _why:
                            _check_parts.append(_why)
                        if not _stem_ok and _stem_msg:
                            _check_parts.append(_stem_msg)
                        _check = "; ".join(_check_parts)
                        _map_rows_disp.append(
                            {
                                "id": m["id"],
                                "Product": m["product_code"],
                                "Template tag": m["system_tag"],
                                "Leaf GL": f"{m.get('gl_account_code')} — {m.get('gl_account_name')}",
                                "Code↔parent": "✓" if _stem_ok else "✗",
                                "OK": "✓" if _all_ok else "✗",
                                "Check": _check,
                            }
                        )
                    st.dataframe(
                        pd.DataFrame(_map_rows_disp),
                        hide_index=True,
                        use_container_width=True,
                    )
                    if any(r["OK"] == "✗" for r in _map_rows_disp):
                        st.warning(
                            "Rows marked **✗** are invalid: either the leaf is **not** under the tagged COA branch, "
                            "or **Code↔parent** is wrong (e.g. **A100001-02** must have **immediate** parent **A100001**, "
                            "not **A120001** — the code stem must match the parent row). "
                            "Fix parent linkage in the database or remap using **Leaf GL** for that tag."
                        )
                ac1, ac2, ac3 = st.columns(3)
                with ac1:
                    if _aplab:
                        _api = st.selectbox(
                            "Product",
                            range(len(_aplab)),
                            format_func=lambda i: _aplab[i],
                            key="coa_pmap_adv_prod",
                        )
                        _apc = _apcodes[_api]
                    else:
                        _apc = st.text_input("Product code", key="coa_pmap_adv_pc")
                with ac2:
                    _atag = st.selectbox("Template system tag", _atags, key="coa_pmap_adv_tag") if _atags else st.text_input(
                        "System tag", key="coa_pmap_adv_tagtxt"
                    )
                _tu_leaf = str(_atag or "").strip()
                _adv_leaf_rows: list = []
                _adv_leaf_err: str | None = None
                if _tu_leaf:
                    try:
                        _adv_leaf_rows = coa.list_leaf_accounts_for_system_tag(_tu_leaf)
                    except ValueError as _lfx:
                        _adv_leaf_err = str(_lfx)
                _adv_alab = [x.get("display_label") or f"{x.get('code')} — {x.get('name')}" for x in _adv_leaf_rows]
                _adv_leaf_by_code = {
                    str(x.get("code") or "").strip().upper(): x
                    for x in _adv_leaf_rows
                    if str(x.get("code") or "").strip()
                }
                _adv_leaf_by_label = {
                    str((x.get("display_label") or f"{x.get('code')} — {x.get('name')}")).strip().upper(): x
                    for x in _adv_leaf_rows
                }
                with ac3:
                    if _adv_leaf_err:
                        st.error(_adv_leaf_err)
                    else:
                        _leaf_entry_key = "coa_pmap_adv_leaf_entry"
                        _leaf_entry_tag_key = "coa_pmap_adv_leaf_entry_tag"
                        if st.session_state.get(_leaf_entry_tag_key) != _tu_leaf:
                            st.session_state[_leaf_entry_tag_key] = _tu_leaf
                            st.session_state[_leaf_entry_key] = _adv_alab[0] if _adv_alab else ""
                        st.text_input(
                            "Leaf GL (editable; same COA branch as tag)",
                            key=_leaf_entry_key,
                            help=(
                                "Type full code, full label/path, or only suffix (e.g. `03`) when the tag branch "
                                "has one grandchild base. This field is fully editable."
                            ),
                        )
                if st.button("Save product → leaf map", key="coa_pmap_adv_save"):
                    _tu = str(_atag or "").strip()
                    _leaf_input = str(st.session_state.get("coa_pmap_adv_leaf_entry") or "").strip()
                    _picked_leaf = None
                    if _leaf_input and not _adv_leaf_err and _adv_leaf_rows:
                        _u = _leaf_input.upper()
                        _picked_leaf = _adv_leaf_by_code.get(_u) or _adv_leaf_by_label.get(_u)
                        if not _picked_leaf:
                            _m = re.findall(r"[A-Za-z]\d{6}(?:-\d{2})?", _leaf_input)
                            if _m:
                                _picked_leaf = _adv_leaf_by_code.get(str(_m[-1]).strip().upper())
                        if not _picked_leaf and re.fullmatch(r"-?\d{1,2}", _leaf_input):
                            _suffix = int(_leaf_input.replace("-", ""))
                            if 1 <= _suffix <= 99:
                                _bases = sorted(
                                    {
                                        str(x.get("code") or "").split("-")[0].strip().upper()
                                        for x in _adv_leaf_rows
                                        if "-" in str(x.get("code") or "")
                                    }
                                )
                                if len(_bases) == 1:
                                    _probe = f"{_bases[0]}-{_suffix:02d}"
                                    _picked_leaf = _adv_leaf_by_code.get(_probe)
                    if str(_apc or "").strip() and _tu and _picked_leaf:
                        try:
                            coa.upsert_product_gl_subaccount_map(
                                str(_apc).strip(), _tu, str(_picked_leaf.get("id"))
                            )
                            st.success("Saved map row.")
                            st.rerun()
                        except Exception as _e:
                            st.error(str(_e))
                    else:
                        st.error(
                            "Product code, tag, and a valid Leaf GL are required. "
                            "Enter a code/label from the selected tag branch, or a suffix like 03 when unambiguous."
                        )
    
        _show_add_acct = bool(st.session_state.get("acco_coa_show_add_account"))
        if _show_add_acct:
            render_sub_sub_header("Add Custom Account")
            _coa_accounts = coa.list_accounts() or []
            _parent_labels = ["(None — top level)"]
            _parent_ids: list = [None]
            for _a in sorted(_coa_accounts, key=lambda x: (x.get("code") or "")):
                _pid = _a.get("id")
                if _pid is not None:
                    _parent_labels.append(f"{_a.get('code', '')} — {_a.get('name', '')}")
                    _parent_ids.append(_pid)
            with st.form("add_account_form"):
                use_grand = st.checkbox(
                    "Use next **grandchild** code (`BASE-NN`) under selected parent",
                    value=False,
                    key="add_acct_grandchild",
                    help="Requires a parent account. Ignores the manual code field and allocates the next free -NN suffix.",
                )
                code = st.text_input("Account Code (7-char or grandchild e.g. A100001-02)", key="add_acct_code_in")
                name = st.text_input("Account Name")
                category = st.selectbox("Category", ["ASSET", "LIABILITY", "EQUITY", "INCOME", "EXPENSE"])
                _pi = st.selectbox(
                    "Parent account (optional)",
                    options=list(range(len(_parent_labels))),
                    format_func=lambda i: _parent_labels[i],
                    help="Choose a parent for roll-up reporting. The parent must already exist in the chart. "
                    "Typically the category matches the parent’s category.",
                    key="add_acct_parent_sel",
                )
                parent_id = _parent_ids[_pi]
                system_tag = st.text_input("System Tag (Optional)")
                res_opts = ["(none)", "PRODUCT", "LOAN_CAPTURE", "JOURNAL"]
                res_i = st.selectbox(
                    "Subaccount resolution (optional; for tagged parents with children)",
                    range(len(res_opts)),
                    format_func=lambda i: res_opts[i],
                    key="add_acct_subres",
                )
                submitted = st.form_submit_button("Create Account")
                if submitted:
                    eff_code = (code or "").strip()
                    if use_grand and parent_id:
                        try:
                            eff_code = coa.suggest_next_grandchild_code_for_parent_id(str(parent_id))
                        except Exception as e:
                            st.error(str(e))
                            eff_code = ""
                    subres = None if res_opts[res_i] == "(none)" else res_opts[res_i]
                    if eff_code and name:
                        try:
                            coa.create_account(
                                eff_code,
                                name,
                                category,
                                system_tag=system_tag.strip() if system_tag else None,
                                parent_id=parent_id,
                                subaccount_resolution=subres,
                            )
                            st.success("Account created!")
                            st.rerun()
                        except ValueError as e:
                            st.error(str(e))
                        except Exception as e:
                            st.error(f"Could not create account: {e}")
                    else:
                        st.error("Code and Name are required.")
    
        _show_edit_parent = bool(st.session_state.get("acco_coa_show_edit_parent"))
        if _show_edit_parent:
            render_sub_sub_header("Edit account parent")
            st.caption(
                "Set or change the parent for an **existing** account. "
                "This account and its descendants cannot be chosen as parent (prevents cycles)."
            )
            _edit_list = coa.list_accounts() or []
            if _edit_list:
                _sorted_edit = sorted(_edit_list, key=lambda x: (x.get("code") or ""))
                _elabels = [f"{a.get('code', '')} — {a.get('name', '')}" for a in _sorted_edit]
                _eids = [a["id"] for a in _sorted_edit]
                st.caption(
                    "Pick the account, choose the new parent, then click **Update parent**. "
                    "(Not inside a form so your parent choice is saved to session state before the button runs.)"
                )
                _ei = st.selectbox(
                    "Account to edit",
                    options=list(range(len(_elabels))),
                    format_func=lambda i: _elabels[i],
                    key="edit_acct_pick",
                )
                selected_id = _eids[_ei]
                acct_row = _sorted_edit[_ei]
                current_parent_id = acct_row.get("parent_id")
                # String IDs: UUID vs str from DB/cursors can break `x in set` and hide valid parents.
                _subtree_raw = coa.get_account_subtree_ids(selected_id)
                subtree = {str(x) for x in _subtree_raw}
                _plabels: list[str] = ["(None — top level)"]
                _pids: list = [None]
                for _a in _sorted_edit:
                    if str(_a["id"]) not in subtree:
                        _plabels.append(f"{_a.get('code', '')} — {_a.get('name', '')}")
                        _pids.append(_a["id"])
                default_pi = 0
                for _i, _pid in enumerate(_pids):
                    if current_parent_id is not None and str(_pid) == str(current_parent_id):
                        default_pi = _i
                        break
                default_pi = min(default_pi, max(0, len(_plabels) - 1))
                # Integer index into _pids (avoids label→id dict collisions if two rows share same label text).
                _sk_idx = f"coa_edit_parent_idx_{str(selected_id)}"
                if _sk_idx not in st.session_state:
                    st.session_state[_sk_idx] = default_pi
                else:
                    _cur = st.session_state[_sk_idx]
                    if not isinstance(_cur, int) or _cur < 0 or _cur >= len(_plabels):
                        st.session_state[_sk_idx] = default_pi
                st.selectbox(
                    "New parent",
                    options=list(range(len(_plabels))),
                    format_func=lambda i: _plabels[i],
                    key=_sk_idx,
                    help="Choose (None — top level) to clear the parent. "
                    "If an account is missing here, it may be under this account in the tree (cannot be parent).",
                )
                if st.button("Update parent", key="coa_btn_update_parent", type="primary"):
                    try:
                        _idx = st.session_state.get(_sk_idx, default_pi)
                        try:
                            _idx = int(_idx)
                        except (TypeError, ValueError):
                            _idx = default_pi
                        _idx = max(0, min(_idx, len(_pids) - 1))
                        new_parent_id = _pids[_idx]
                        old_lbl = _coa_parent_display_label(_sorted_edit, current_parent_id)
                        new_lbl = _coa_parent_display_label(_sorted_edit, new_parent_id)
                        acct_code = (acct_row.get("code") or "").strip() or "?"
                        acct_name = (acct_row.get("name") or "").strip()
                        acct_title = f"{acct_code} — {acct_name}" if acct_name else acct_code
                        if str(current_parent_id or "") == str(new_parent_id or ""):
                            st.session_state["coa_parent_edit_banner"] = (
                                "info",
                                f"**No change saved.** **{acct_title}** already has parent **{old_lbl}**.",
                            )
                        else:
                            coa.update_account_parent(selected_id, new_parent_id)
                            st.session_state.pop(_sk_idx, None)
                            st.session_state["coa_parent_edit_banner"] = (
                                "success",
                                f"**Saved.** **{acct_title}**: parent **{old_lbl}** → **{new_lbl}**.",
                            )
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))
                    except Exception as e:
                        st.error(f"Could not update parent: {e}")
            else:
                st.caption("Initialize the chart of accounts above before editing parents.")
    
