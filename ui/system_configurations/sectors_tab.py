"""Sectors & subsectors tab."""

from __future__ import annotations

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
        sectors_list = list_sectors()
        if sectors_list:
            for s in sectors_list:
                with st.expander(
                    f"Sector: {s.get('name', '')}", expanded=False
                ):
                    subs = list_subsectors(s.get("id"))
                    for sub in subs:
                        st.caption(f"  • {sub.get('name', '')}")
                    new_sub = st.text_input(
                        "New subsector name",
                        key=f"new_sub_{s['id']}",
                        placeholder="Subsector name",
                    )
                    if (
                        st.button("Add subsector", key=f"add_sub_{s['id']}")
                        and new_sub
                        and new_sub.strip()
                    ):
                        try:
                            create_subsector(s["id"], new_sub.strip())
                            st.success("Subsector added.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
        new_sector_name = st.text_input(
            "New sector name",
            key="syscfg_new_sector",
            placeholder="e.g. Agriculture",
        )
        if (
            st.button("Add sector", key="syscfg_add_sector")
            and new_sector_name
            and new_sector_name.strip()
        ):
            try:
                create_sector(new_sector_name.strip())
                st.success("Sector added.")
                st.rerun()
            except Exception as ex:
                st.error(str(ex))
    else:
        st.info("Customer module required to manage sectors.")
