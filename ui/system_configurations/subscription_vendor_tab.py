"""Vendor subscription tier fees (public catalog). System configurations — ADMIN-only surface."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

import streamlit as st

from subscription import repository as sub_repo


def render_subscription_vendor_tab() -> None:
    st.caption("Platform-wide tier pricing (10 decimal places). Applies to all tenants.")
    try:
        tiers = sub_repo.list_vendor_tiers()
    except Exception as e:
        st.error(f"Could not load vendor tiers: {e}")
        return

    for row in tiers:
        name = row["tier_name"]
        st.markdown(f"**{name}**")
        c1, c2, c3, _ = st.columns([1.2, 1.2, 0.8, 2], gap="small")
        with c1:
            st.text_input(
                "Monthly fee",
                value=str(row["monthly_fee"]),
                key=f"sv_monthly_{name}",
            )
        with c2:
            st.text_input(
                "Quarterly fee",
                value=str(row["quarterly_fee"]),
                key=f"sv_quarterly_{name}",
            )
        with c3:
            st.checkbox(
                "Active",
                value=bool(row["is_active"]),
                key=f"sv_active_{name}",
            )

    if st.button("Save tier catalog", type="primary", key="sv_save_catalog"):
        for row in tiers:
            name = row["tier_name"]
            try:
                mf = Decimal(str(st.session_state.get(f"sv_monthly_{name}", "0")).strip() or "0")
                qf = Decimal(str(st.session_state.get(f"sv_quarterly_{name}", "0")).strip() or "0")
            except (InvalidOperation, ValueError):
                st.error(f"Invalid amount for {name}.")
                return
            try:
                sub_repo.upsert_vendor_tier(
                    tier_name=name,
                    monthly_fee=mf,
                    quarterly_fee=qf,
                    is_active=bool(st.session_state.get(f"sv_active_{name}", True)),
                )
            except Exception as e:
                st.error(f"Save failed for {name}: {e}")
                return
        st.success("Vendor tier catalog saved.")
        st.rerun()
