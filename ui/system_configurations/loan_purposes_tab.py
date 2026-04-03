"""Loan purposes tab."""

from __future__ import annotations

import pandas as pd
import streamlit as st



from style import render_main_header, render_sub_header, render_sub_sub_header

def render_loan_purposes_tab(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    list_loan_purposes,
    get_loan_purpose_by_id,
    create_loan_purpose,
    set_loan_purpose_active,
    update_loan_purpose,
    count_loan_purposes_rows,
    clear_all_loan_purposes,
) -> None:
    render_sub_sub_header("Loan purposes")
    st.caption(
        "Single source of truth: table **`loan_purposes`** in the database (not JSON system config). "
        "**Loan capture** reads the same table. Do not add a **duplicate name** (case-insensitive); use **Activate** or **Edit** instead."
    )
    if not loan_management_available:
        st.error("Loan management module is required to manage loan purposes.")
    else:
        lp_load_error: str | None = None
        lp_rows: list = []
        try:
            lp_rows = list_loan_purposes(active_only=False)
        except Exception as _lp_ex:
            lp_load_error = str(_lp_ex)
            st.error(f"**Could not list loan purposes** (read failed): {_lp_ex}")
            st.caption(
                "Until this is fixed, the blue “empty table” message is **not reliable**. "
                "**Add purpose** still talks to the database, so a duplicate-name error can appear even when the list is blank."
            )

        lp_count: int | None = None
        if lp_load_error is None:
            try:
                lp_count = count_loan_purposes_rows()
            except Exception:
                lp_count = None

        if lp_load_error is None and lp_count is not None:
            st.caption(
                f"**SQL `COUNT(*)` on `loan_purposes`:** {lp_count} row(s). Listed below: **{len(lp_rows)}** row(s)."
            )

        if (
            lp_load_error is None
            and lp_count is not None
            and lp_count > 0
            and len(lp_rows) == 0
        ):
            st.error(
                "**Mismatch:** `COUNT(*)` is greater than zero but the SELECT returned no rows. "
                "Check PostgreSQL **row-level security** on `loan_purposes`, **search_path**, and server logs."
            )

        _lp_n_active = sum(1 for r in lp_rows if r.get("is_active", True))
        _lp_n_inactive = len(lp_rows) - _lp_n_active
        st.markdown(
            f"**Listed in UI:** {len(lp_rows)} purpose(s) — **{_lp_n_active}** active (loan capture), "
            f"**{_lp_n_inactive}** inactive."
        )
        if lp_rows:
            _lp_disp = pd.DataFrame(lp_rows)
            _cols = [
                c
                for c in ["id", "name", "sort_order", "is_active"]
                if c in _lp_disp.columns
            ]
            st.dataframe(
                _lp_disp[_cols],
                hide_index=True,
                width="stretch",
                height=min(320, 56 + len(lp_rows) * 36),
            )
        elif lp_load_error is None:
            st.info(
                "**List is empty:** there are no rows to show (and the list query succeeded). "
                "If **Add purpose** still says the name exists, compare with **SQL COUNT** above or clear the table (expandable below)."
            )
        lp_n1, lp_n2, lp_n3 = st.columns(3)
        with lp_n1:
            lp_new_name = st.text_input(
                "New purpose name",
                key="syscfg_lp_new_name",
                placeholder="e.g. Working capital",
            )
        with lp_n2:
            lp_new_sort = st.number_input(
                "Sort order", min_value=0, value=0, step=1, key="syscfg_lp_new_sort"
            )
        with lp_n3:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Add purpose", key="syscfg_lp_add_btn"):
                if lp_new_name and str(lp_new_name).strip():
                    try:
                        create_loan_purpose(
                            str(lp_new_name).strip(), int(lp_new_sort)
                        )
                        st.success("Loan purpose added.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
                        if "already exists" in str(ex).lower():
                            if len(lp_rows) == 0:
                                st.warning(
                                    "**Red vs blue:** INSERT sees a duplicate name, but the list above is empty. "
                                    "The row is still in `loan_purposes` (or a list/query issue). Use **Clear all purposes** "
                                    "below or run `python scripts/clear_loan_purposes.py` from the project root."
                                )
                            try:
                                _lp_refresh = list_loan_purposes(active_only=False)
                            except Exception as _lpe:
                                _lp_refresh = []
                                st.caption(f"Refresh list failed: {_lpe}")
                            if _lp_refresh:
                                st.info(
                                    "Existing names returned by the list query:"
                                )
                                _df_ref = pd.DataFrame(_lp_refresh)
                                _cref = [
                                    c
                                    for c in ["id", "name", "sort_order", "is_active"]
                                    if c in _df_ref.columns
                                ]
                                st.dataframe(
                                    _df_ref[_cref],
                                    hide_index=True,
                                    width="stretch",
                                    height=min(
                                        260, 56 + len(_lp_refresh) * 36
                                    ),
                                )
                else:
                    st.error("Enter a purpose name.")
        if lp_rows:
            st.markdown("**Edit & activate / deactivate**")
            h1, h2, h3, h4 = st.columns([2.2, 1, 1, 1.2])
            with h1:
                st.caption("Name")
            with h2:
                st.caption("Sort")
            with h3:
                st.caption("Status")
            with h4:
                st.caption("Actions")
            for pr in lp_rows:
                pid = int(pr.get("id"))
                r1, r2, r3, r4 = st.columns([2.2, 1, 1, 1.2])
                with r1:
                    st.text(str(pr.get("name") or ""))
                with r2:
                    st.text(str(pr.get("sort_order", 0)))
                with r3:
                    st.text(
                        "Active"
                        if pr.get("is_active", True)
                        else "Inactive"
                    )
                with r4:
                    act = bool(pr.get("is_active", True))
                    btn_lbl = "Deactivate" if act else "Activate"
                    if st.button(btn_lbl, key=f"syscfg_lp_act_{pid}"):
                        try:
                            set_loan_purpose_active(pid, not act)
                            st.success("Updated.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
                with st.expander(f"Edit purpose #{pid}", expanded=False):
                    e1, e2 = st.columns(2)
                    with e1:
                        ren = st.text_input(
                            "Name",
                            value=str(pr.get("name") or ""),
                            key=f"syscfg_lp_ren_{pid}",
                        )
                    with e2:
                        so = st.number_input(
                            "Sort order",
                            min_value=0,
                            value=int(pr.get("sort_order") or 0),
                            step=1,
                            key=f"syscfg_lp_so_{pid}",
                        )
                    if st.button("Save changes", key=f"syscfg_lp_save_{pid}"):
                        try:
                            update_loan_purpose(
                                pid, name=ren.strip(), sort_order=int(so)
                            )
                            st.success("Saved.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
        else:
            st.caption(
                "Use **Add purpose** when the list is empty, or **Clear all purposes** if the table should be reset."
            )

        with st.expander(
            "Clear all loan purposes (reset `loan_purposes`)", expanded=False
        ):
            st.warning(
                "Deletes **every** row in `loan_purposes` and sets **`loans.loan_purpose_id`** to NULL where it was set. "
                "CLI equivalent: **`python scripts/clear_loan_purposes.py`**."
            )
            _lp_clr = st.text_input(
                "Type **DELETE ALL PURPOSES** to enable the button",
                key="syscfg_lp_clear_confirm",
            )
            if st.button(
                "Clear loan_purposes table",
                key="syscfg_lp_clear_btn",
                type="primary",
            ):
                if (_lp_clr or "").strip() != "DELETE ALL PURPOSES":
                    st.error("Confirmation text must match exactly.")
                else:
                    try:
                        _lu, _pd = clear_all_loan_purposes()
                        st.success(
                            f"Cleared: {int(_pd)} purpose row(s) deleted; {int(_lu)} loan(s) had purpose cleared."
                        )
                        st.rerun()
                    except Exception as _cl_ex:
                        st.error(str(_cl_ex))
