"""Vendor subscription tier fees and entitlement matrix (public catalog). System configurations — ADMIN-only surface."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

import pandas as pd
import streamlit as st

from subscription.nav_sections import (
    BANK_RECONCILIATION_ROW_LABEL,
    LOAN_APP_SIDEBAR_SECTIONS,
    tier_entitlement_matrix_row_labels,
)
from subscription import repository as sub_repo


def render_subscription_vendor_tab() -> None:
    st.caption(
        "Platform-wide tier pricing (10 decimal places). Only **one** tier may be marked **Active** at a time "
        "(the catalog SKU offered for new assignments). Columns in the matrix below are edited per tier independently."
    )
    try:
        tiers = sub_repo.list_vendor_tiers()
    except Exception as e:
        st.error(f"Could not load vendor tiers: {e}")
        return

    if not tiers:
        st.warning("No vendor tiers defined.")
        return

    row_labels = list(tier_entitlement_matrix_row_labels())
    tier_names = [str(t["tier_name"]) for t in tiers]

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
                "Active catalog tier",
                value=bool(row["is_active"]),
                key=f"sv_active_{name}",
                help="Only one tier may be active—see note under Save.",
            )

    st.markdown("##### Tier access matrix")
    st.caption(
        "Rows are main app sidebar sections (same labels as the loan app). Tick a cell to grant that section to that tier. "
        "**Loan management** covers the whole area including Loan Capture. "
        f"**{BANK_RECONCILIATION_ROW_LABEL}** is an extra accounting capability, not a sidebar item."
    )

    matrix_data: dict[str, list[bool]] = {}
    for trow in tiers:
        tname = str(trow["tier_name"])
        raw = trow.get("features")
        feat = sub_repo.merge_vendor_tier_features(raw if isinstance(raw, dict) else {})
        allowed = set(feat.get("allowed_sidebar_sections") or [])
        col: list[bool] = []
        for section in LOAN_APP_SIDEBAR_SECTIONS:
            col.append(section in allowed)
        col.append(bool(feat.get("bank_reconciliation", True)))
        matrix_data[tname] = col

    df = pd.DataFrame(matrix_data, index=row_labels)
    df = df.astype(bool)

    edited_df = st.data_editor(
        df,
        width="stretch",
        hide_index=False,
        num_rows="fixed",
        key="sv_tier_entitlement_matrix",
        column_config={
            tn: st.column_config.CheckboxColumn(tn, help=f"Grants for **{tn}** tier")
            for tn in tier_names
        },
    )

    if st.button("Save tier catalog", type="primary", key="sv_save_catalog"):
        active_vendors: list[str] = []
        for trow in tiers:
            tname = str(trow["tier_name"])
            _def_act = bool(trow.get("is_active", False))
            if bool(st.session_state.get(f"sv_active_{tname}", _def_act)):
                active_vendors.append(tname)
        if len(active_vendors) > 1:
            st.error(
                "Only **one** tier may be **Active catalog tier** at a time. "
                f"Uncheck all but one. Currently selected: **{', '.join(active_vendors)}**."
            )
            return

        for trow in tiers:
            tname = str(trow["tier_name"])
            try:
                mf = Decimal(str(st.session_state.get(f"sv_monthly_{tname}", "0")).strip() or "0")
                qf = Decimal(str(st.session_state.get(f"sv_quarterly_{tname}", "0")).strip() or "0")
            except (InvalidOperation, ValueError):
                st.error(f"Invalid amount for {tname}.")
                return

            allowed_sections: list[str] = []
            try:
                for section in LOAN_APP_SIDEBAR_SECTIONS:
                    if bool(edited_df.loc[section, tname]):
                        allowed_sections.append(section)
                bank_recon = bool(edited_df.loc[BANK_RECONCILIATION_ROW_LABEL, tname])
            except Exception as ex:
                st.error(f"Could not read matrix for {tname}: {ex}")
                return

            features = {
                "allowed_sidebar_sections": allowed_sections,
                "bank_reconciliation": bank_recon,
            }
            try:
                _def_act = bool(trow.get("is_active", False))
                sub_repo.upsert_vendor_tier(
                    tier_name=tname,
                    monthly_fee=mf,
                    quarterly_fee=qf,
                    is_active=bool(st.session_state.get(f"sv_active_{tname}", _def_act)),
                    features=features,
                )
            except Exception as e:
                st.error(f"Save failed for {tname}: {e}")
                return
        st.success("Vendor tier catalog saved.")
        st.rerun()
