"""Loan grade scales tab."""

from __future__ import annotations

import streamlit as st



from style import render_main_header, render_sub_header, render_sub_sub_header

def render_grade_scales_tab() -> None:
    render_sub_sub_header("Loan grade scales")
    st.caption(
        "Maps **days past due** to **grade** and **performing / non-performing**. "
        "**Standard** DPD columns drive IFRS-facing labels (ECL report and single-loan IFRS tool). "
        "**Regulatory** DPD columns drive **Portfolio reports → Loan classification (regulatory)**. "
        "Set **supervisory** and **standard** provision **% of total exposure** per grade in the expander below."
    )
    try:
        from grade_scale_config import (
            delete_loan_grade_scale_rule_hard,
            format_dpd_range,
            grade_scale_schema_ready,
            insert_loan_grade_scale_rule,
            list_loan_grade_scale_rules,
            provision_pct_from_value,
            update_loan_grade_scale_rule,
        )
    except Exception as ex:
        st.error(f"Grade scale module unavailable: {ex}")
    else:
        _gs_ok, _gs_msg = grade_scale_schema_ready()
        if not _gs_ok:
            st.warning(_gs_msg)
            st.caption("Run **scripts/run_migration_63.py** if the table has not been created.")
        gr_rows = list_loan_grade_scale_rules(active_only=False)
        if "syscfg_gr_add_form_open" not in st.session_state:
            st.session_state["syscfg_gr_add_form_open"] = False
        st.session_state.setdefault("syscfg_gr_edit_id", None)

        if _gs_ok and gr_rows:
            with st.expander("Provision % by grade (regulatory vs standard)", expanded=False):
                st.caption(
                    "**Regulatory %** — used on **Portfolio reports → Loan classification (regulatory)** "
                    "(provision = total exposure × % ÷ 100). "
                    "**Standard %** — used as **PD%** in **IFRS provision** (unsecured × PD%) for that IFRS grade; "
                    "if no grade matches, the **PD band** table by DPD is used instead."
                )
                _ph0, _ph1, _ph2 = st.columns([2.4, 1, 1])
                with _ph0:
                    st.caption("Grade")
                with _ph1:
                    st.caption("Regulatory %")
                with _ph2:
                    st.caption("Standard %")
                _pct_rows: list[tuple[int, float, float]] = []
                for _gr in gr_rows:
                    _gid = int(_gr["id"])
                    _pc0, _pc1, _pc2 = st.columns([2.4, 1, 1])
                    with _pc0:
                        st.markdown(
                            f"**{_gr.get('grade_name') or ''}** · {_gr.get('performance_status') or ''}"
                        )
                    with _pc1:
                        _vr = st.number_input(
                            f"Regulatory % rule {_gid}",
                            min_value=0.0,
                            max_value=100.0,
                            value=float(
                                provision_pct_from_value(
                                    _gr.get("regulatory_provision_pct")
                                )
                            ),
                            step=0.01,
                            format="%.2f",
                            key=f"syscfg_gr_pct_r_{_gid}",
                            label_visibility="collapsed",
                        )
                    with _pc2:
                        _vs = st.number_input(
                            f"Standard % rule {_gid}",
                            min_value=0.0,
                            max_value=100.0,
                            value=float(
                                provision_pct_from_value(
                                    _gr.get("standard_provision_pct")
                                )
                            ),
                            step=0.01,
                            format="%.2f",
                            key=f"syscfg_gr_pct_s_{_gid}",
                            label_visibility="collapsed",
                        )
                    _pct_rows.append((_gid, _vr, _vs))
                if st.button("Save all provision %", key="syscfg_gr_pct_save_all"):
                    try:
                        for _gid, _vr, _vs in _pct_rows:
                            update_loan_grade_scale_rule(
                                _gid,
                                regulatory_provision_pct=_vr,
                                standard_provision_pct=_vs,
                            )
                        st.success("Provision percentages saved.")
                        st.rerun()
                    except Exception as _pex:
                        st.error(str(_pex))

        if not st.session_state["syscfg_gr_add_form_open"]:
            if st.button("Add rule", key="syscfg_gr_open_add_form"):
                st.session_state["syscfg_gr_add_form_open"] = True
                st.rerun()
        else:
            _hc1, _hc2 = st.columns([1, 5])
            with _hc1:
                if st.button("Cancel", key="syscfg_gr_cancel_add_form"):
                    st.session_state["syscfg_gr_add_form_open"] = False
                    st.rerun()
            with _hc2:
                st.caption("Fill in the fields below, then click **Add rule** to save.")
            ga1, ga2, ga3 = st.columns(3)
            with ga1:
                g_new_name = st.text_input(
                    "New grade name",
                    key="syscfg_gr_new_name",
                    placeholder="e.g. Pass",
                )
            with ga2:
                g_new_perf = st.selectbox(
                    "Performance status",
                    ["Performing", "NonPerforming"],
                    key="syscfg_gr_new_perf",
                )
            with ga3:
                g_new_sort = st.number_input(
                    "Sort order",
                    min_value=0,
                    value=100,
                    step=1,
                    key="syscfg_gr_new_sort",
                )
            st.caption(
                "DPD bands are inclusive. Leave **no upper limit** unchecked to cap the band; check it for open-ended (e.g. 91+)."
            )
            _gp1, _gp2 = st.columns(2)
            with _gp1:
                g_reg_pct = st.number_input(
                    "Regulatory provision %",
                    min_value=0.0,
                    max_value=100.0,
                    value=0.0,
                    step=0.01,
                    format="%.2f",
                    key="syscfg_gr_new_reg_pct",
                    help="Of total exposure, for regulatory classification reporting.",
                )
            with _gp2:
                g_std_pct = st.number_input(
                    "Standard provision %",
                    min_value=0.0,
                    max_value=100.0,
                    value=0.0,
                    step=0.01,
                    format="%.2f",
                    key="syscfg_gr_new_std_pct",
                    help="Used as IFRS **PD%** (unsecured × this % ÷ 100) when this grade applies.",
                )
            gr1, gr2, gr3, gr4 = st.columns(4)
            with gr1:
                g_rm = st.number_input(
                    "Regulatory DPD min",
                    min_value=0,
                    value=0,
                    step=1,
                    key="syscfg_gr_rm",
                )
                g_r_open = st.checkbox(
                    "Regulatory: no upper limit", key="syscfg_gr_r_open"
                )
                g_rx = st.number_input(
                    "Regulatory DPD max",
                    min_value=0,
                    value=30,
                    step=1,
                    key="syscfg_gr_rx",
                    disabled=g_r_open,
                )
            with gr2:
                g_sm = st.number_input(
                    "Standard (IFRS) DPD min",
                    min_value=0,
                    value=0,
                    step=1,
                    key="syscfg_gr_sm",
                )
                g_s_open = st.checkbox(
                    "Standard: no upper limit", key="syscfg_gr_s_open"
                )
                g_sx = st.number_input(
                    "Standard DPD max",
                    min_value=0,
                    value=90,
                    step=1,
                    key="syscfg_gr_sx",
                    disabled=g_s_open,
                )
            with gr3:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("Add rule", key="syscfg_gr_add"):
                    if g_new_name and str(g_new_name).strip():
                        try:
                            insert_loan_grade_scale_rule(
                                grade_name=str(g_new_name).strip(),
                                performance_status=str(g_new_perf),
                                regulatory_dpd_min=int(g_rm),
                                regulatory_dpd_max=None if g_r_open else int(g_rx),
                                standard_dpd_min=int(g_sm),
                                standard_dpd_max=None if g_s_open else int(g_sx),
                                sort_order=int(g_new_sort),
                                is_active=True,
                                regulatory_provision_pct=g_reg_pct,
                                standard_provision_pct=g_std_pct,
                            )
                            st.session_state["syscfg_gr_add_form_open"] = False
                            st.success("Rule added.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
                    else:
                        st.error("Grade name is required.")
            with gr4:
                st.caption(
                    "Order matters: first matching active rule wins. Keep Pass (0 dpd) before wider bands."
                )
        if gr_rows:
            st.markdown("**Configured rules**")
            st.caption(
                "Click a **grade** in the first column to open the edit panel below the table."
            )
            _gw = [1.35, 1.05, 1.15, 1.15, 0.65, 0.65, 0.75, 0.75]
            gh1, gh2, gh3, gh4, gh5, gh6, gh7, gh8 = st.columns(_gw)
            with gh1:
                st.caption("Grade")
            with gh2:
                st.caption("Performance")
            with gh3:
                st.caption("Regulatory DPD")
            with gh4:
                st.caption("Standard DPD")
            with gh5:
                st.caption("Reg %")
            with gh6:
                st.caption("Std %")
            with gh7:
                st.caption("Sort")
            with gh8:
                st.caption("Active")
            for gr in gr_rows:
                gid = int(gr["id"])
                u1, u2, u3, u4, u5, u6, u7, u8 = st.columns(_gw)
                with u1:
                    _gbtn = str(gr.get("grade_name") or "").strip() or f"Rule #{gid}"
                    _sel = int(st.session_state.get("syscfg_gr_edit_id") or -1) == gid
                    if st.button(
                        _gbtn,
                        key=f"syscfg_gr_pick_{gid}",
                        help="Edit this grade",
                        type="primary" if _sel else "secondary",
                    ):
                        st.session_state["syscfg_gr_edit_id"] = gid
                        st.rerun()
                with u2:
                    st.text(str(gr.get("performance_status") or ""))
                with u3:
                    st.text(
                        format_dpd_range(
                            int(gr.get("regulatory_dpd_min") or 0),
                            int(gr["regulatory_dpd_max"])
                            if gr.get("regulatory_dpd_max") is not None
                            else None,
                        )
                    )
                with u4:
                    st.text(
                        format_dpd_range(
                            int(gr.get("standard_dpd_min") or 0),
                            int(gr["standard_dpd_max"])
                            if gr.get("standard_dpd_max") is not None
                            else None,
                        )
                    )
                with u5:
                    st.text(
                        f"{float(provision_pct_from_value(gr.get('regulatory_provision_pct'))):.2f}"
                    )
                with u6:
                    st.text(
                        f"{float(provision_pct_from_value(gr.get('standard_provision_pct'))):.2f}"
                    )
                with u7:
                    st.text(str(gr.get("sort_order", 0)))
                with u8:
                    st.text("Yes" if gr.get("is_active", True) else "No")

            _edit_id_raw = st.session_state.get("syscfg_gr_edit_id")
            _known_ids = {int(r["id"]) for r in gr_rows}
            if _edit_id_raw is not None and int(_edit_id_raw) not in _known_ids:
                st.session_state["syscfg_gr_edit_id"] = None
                _edit_id_raw = None

            _ed_gr = (
                next((r for r in gr_rows if int(r["id"]) == int(_edit_id_raw)), None)
                if _edit_id_raw is not None
                else None
            )
            if _ed_gr is not None:
                gid = int(_ed_gr["id"])
                st.divider()
                _eh1, _eh2 = st.columns([4, 1])
                with _eh1:
                    st.markdown(
                        f"##### Edit · **{_ed_gr.get('grade_name') or ''}** "
                        f"· {_ed_gr.get('performance_status') or ''} `(id {gid})`"
                    )
                with _eh2:
                    if st.button("Close", key="syscfg_gr_edit_close", help="Hide the editor"):
                        st.session_state["syscfg_gr_edit_id"] = None
                        st.rerun()
                en = st.text_input(
                    "Grade name",
                    value=str(_ed_gr.get("grade_name") or ""),
                    key=f"syscfg_gr_en_{gid}",
                )
                ep = st.selectbox(
                    "Performance status",
                    ["Performing", "NonPerforming"],
                    index=0 if str(_ed_gr.get("performance_status")) == "Performing" else 1,
                    key=f"syscfg_gr_ep_{gid}",
                )
                e_so = st.number_input(
                    "Sort order",
                    min_value=0,
                    value=int(_ed_gr.get("sort_order") or 0),
                    step=1,
                    key=f"syscfg_gr_eso_{gid}",
                )
                e_act = st.checkbox(
                    "Active",
                    value=bool(_ed_gr.get("is_active", True)),
                    key=f"syscfg_gr_eact_{gid}",
                )
                e_rm = st.number_input(
                    "Regulatory DPD min",
                    min_value=0,
                    value=int(_ed_gr.get("regulatory_dpd_min") or 0),
                    step=1,
                    key=f"syscfg_gr_erm_{gid}",
                )
                e_r_open = st.checkbox(
                    "Regulatory: no upper limit",
                    value=_ed_gr.get("regulatory_dpd_max") is None,
                    key=f"syscfg_gr_eropen_{gid}",
                )
                _rmax_def = 30
                if _ed_gr.get("regulatory_dpd_max") is not None:
                    _rmax_def = int(_ed_gr["regulatory_dpd_max"])
                e_rx = st.number_input(
                    "Regulatory DPD max",
                    min_value=0,
                    value=_rmax_def,
                    step=1,
                    key=f"syscfg_gr_erx_{gid}",
                    disabled=e_r_open,
                )
                e_sm = st.number_input(
                    "Standard DPD min",
                    min_value=0,
                    value=int(_ed_gr.get("standard_dpd_min") or 0),
                    step=1,
                    key=f"syscfg_gr_esm_{gid}",
                )
                e_s_open = st.checkbox(
                    "Standard: no upper limit",
                    value=_ed_gr.get("standard_dpd_max") is None,
                    key=f"syscfg_gr_esopen_{gid}",
                )
                _smax_def = 90
                if _ed_gr.get("standard_dpd_max") is not None:
                    _smax_def = int(_ed_gr["standard_dpd_max"])
                e_sx = st.number_input(
                    "Standard DPD max",
                    min_value=0,
                    value=_smax_def,
                    step=1,
                    key=f"syscfg_gr_esx_{gid}",
                    disabled=e_s_open,
                )
                _epe1, _epe2 = st.columns(2)
                with _epe1:
                    e_reg_pct = st.number_input(
                        "Regulatory provision % (of exposure)",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(
                            provision_pct_from_value(_ed_gr.get("regulatory_provision_pct"))
                        ),
                        step=0.01,
                        format="%.2f",
                        key=f"syscfg_gr_e_rpct_{gid}",
                    )
                with _epe2:
                    e_std_pct = st.number_input(
                        "Standard provision % (IFRS PD%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(
                            provision_pct_from_value(_ed_gr.get("standard_provision_pct"))
                        ),
                        step=0.01,
                        format="%.2f",
                        key=f"syscfg_gr_e_spct_{gid}",
                    )
                c_save, c_del = st.columns(2)
                with c_save:
                    if st.button("Save rule", key=f"syscfg_gr_save_{gid}"):
                        try:
                            update_loan_grade_scale_rule(
                                gid,
                                grade_name=en.strip(),
                                performance_status=ep,
                                regulatory_dpd_min=int(e_rm),
                                regulatory_dpd_max=None if e_r_open else int(e_rx),
                                standard_dpd_min=int(e_sm),
                                standard_dpd_max=None if e_s_open else int(e_sx),
                                sort_order=int(e_so),
                                is_active=e_act,
                                regulatory_provision_pct=e_reg_pct,
                                standard_provision_pct=e_std_pct,
                            )
                            st.success("Saved.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
                with c_del:
                    if st.button("Delete rule", key=f"syscfg_gr_del_{gid}", type="primary"):
                        try:
                            delete_loan_grade_scale_rule_hard(gid)
                            st.session_state["syscfg_gr_edit_id"] = None
                            st.success("Deleted.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
            elif _edit_id_raw is None:
                st.caption("No grade selected — click a grade name above to edit.")
        else:
            st.info("No rules yet. Defaults are created on first load; refresh if empty.")
