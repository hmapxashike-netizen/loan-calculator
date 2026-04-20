"""Sectors & subsectors tab."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from style import render_main_header, render_sub_header, render_sub_sub_header


def render_sectors_tab(
    *,
    customers_available: bool,
    list_sectors,
    list_subsectors,
    create_sector,
    create_subsector,
) -> None:
    render_sub_sub_header("Sectors & subsectors")
    st.caption("Configure sectors and subsectors for customer classification.")
    if customers_available:
        sectors_list = list_sectors() or []
        rows: list[dict[str, object]] = []
        for s in sectors_list:
            sid = s.get("id")
            sname = str(s.get("name") or "").strip() or "—"
            subs = list_subsectors(sid) if sid is not None else []
            if subs:
                for sub in subs:
                    rows.append(
                        {
                            "Sector": sname,
                            "Subsector": str(sub.get("name") or "").strip() or "—",
                        }
                    )
            else:
                rows.append({"Sector": sname, "Subsector": "—"})
        if rows:
            st.dataframe(
                pd.DataFrame(rows),
                hide_index=True,
                width="stretch",
                column_config={
                    "Sector": st.column_config.TextColumn("Sector"),
                    "Subsector": st.column_config.TextColumn("Subsector"),
                },
            )
        else:
            st.info("No sectors yet. Add a sector below.")

        st.markdown("##### Add subsector")
        if sectors_list:
            sector_labels = [
                str(s.get("name") or f"Sector #{s.get('id')}").strip()
                for s in sectors_list
            ]
            a1, a2, a3 = st.columns([2, 2, 1], gap="small")
            with a1:
                pick = st.selectbox(
                    "Sector",
                    sector_labels,
                    label_visibility="collapsed",
                    key="syscfg_flat_pick_sector_for_sub",
                )
            with a2:
                new_sub_name = st.text_input(
                    "New subsector name",
                    key="syscfg_flat_new_subsector",
                    placeholder="Subsector name",
                    label_visibility="collapsed",
                )
            with a3:
                st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
                add_sub_clicked = st.button(
                    "Add subsector",
                    key="syscfg_flat_add_subsector",
                    type="primary",
                )
            if add_sub_clicked:
                sel = next(
                    (x for x in sectors_list if str(x.get("name") or "").strip() == pick),
                    None,
                )
                tid = sel.get("id") if sel else None
                if tid is None:
                    st.error("Pick a sector.")
                elif new_sub_name and str(new_sub_name).strip():
                    try:
                        create_subsector(int(tid), str(new_sub_name).strip())
                        st.success("Subsector added.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
                else:
                    st.error("Enter a subsector name.")

        st.markdown("##### Add sector")
        ns1, ns2 = st.columns([3, 1], gap="small")
        with ns1:
            new_sector_name = st.text_input(
                "New sector name",
                key="syscfg_new_sector",
                placeholder="e.g. Agriculture",
                label_visibility="collapsed",
            )
        with ns2:
            st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
            if st.button("Add sector", key="syscfg_add_sector", type="primary"):
                if new_sector_name and str(new_sector_name).strip():
                    try:
                        create_sector(str(new_sector_name).strip())
                        st.success("Sector added.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
                else:
                    st.error("Enter a sector name.")
    else:
        st.info("Customer module required to manage sectors.")
