"""Loan application pipeline statuses and business facility subtypes (merged into system_config)."""

from __future__ import annotations

from typing import Any, NamedTuple

import pandas as pd
import streamlit as st

from style import render_sub_sub_header

from loan_management.loan_pipeline_config import (
    default_business_facility_subtypes,
    default_loan_application_statuses,
)


class LoanPipelineConfigSnapshot(NamedTuple):
    loan_application_statuses: list[dict[str, Any]]
    business_facility_subtypes: list[str]


def render_loan_pipeline_tab(*, cfg: dict[str, Any]) -> LoanPipelineConfigSnapshot:
    render_sub_sub_header("Loan applications & business facilities")
    st.caption(
        "**Code** is stored uppercase on `loan_applications.status` (past-tense style, e.g. CREDIT_APPROVAL_GRANTED). "
        "**Button label** — imperative verb phrase (e.g. Grant Credit Approval). "
        "**Status display** — optional past-tense wording for banners (e.g. Credit Approval Granted); if blank, button label is used."
    )

    raw_statuses = cfg.get("loan_application_statuses")
    if not isinstance(raw_statuses, list) or not raw_statuses:
        rows: list[dict[str, Any]] = [dict(r) for r in default_loan_application_statuses()]
    else:
        rows = [dict(r) for r in raw_statuses if isinstance(r, dict)]

    df = pd.DataFrame(
        [
            {
                "code": str(r.get("code", "") or ""),
                "label": str(r.get("label", "") or ""),
                "display_label": str(r.get("display_label") or ""),
                "terminal": bool(r.get("terminal", False)),
                "action": str(r.get("action") or "").strip(),
            }
            for r in rows
        ]
    )
    edited = st.data_editor(
        df,
        num_rows="dynamic",
        column_config={
            "code": st.column_config.TextColumn("Code", help="Stored uppercase in loan_applications.status"),
            "label": st.column_config.TextColumn("Button label", help="Imperative verb phrase"),
            "display_label": st.column_config.TextColumn(
                "Status display",
                help="Past-tense label for Current status banners; optional",
            ),
            "terminal": st.column_config.CheckboxColumn("Terminal", help="No further status changes"),
            "action": st.column_config.TextColumn(
                "Action",
                help="Leave empty to set status, or soft_delete / supersede",
            ),
        },
        hide_index=True,
        width="stretch",
        key="syscfg_loan_pipeline_status_editor",
    )

    out_statuses: list[dict[str, Any]] = []
    for _, r in edited.iterrows():
        code = str(r.get("code", "")).strip()
        if not code:
            continue
        item: dict[str, Any] = {
            "code": code.upper(),
            "label": str(r.get("label", "")).strip() or code.upper(),
            "terminal": bool(r.get("terminal", False)),
        }
        dl = str(r.get("display_label") or "").strip()
        if dl:
            item["display_label"] = dl
        act = str(r.get("action", "")).strip().lower()
        if act:
            item["action"] = act
        out_statuses.append(item)

    subs_raw = cfg.get("business_facility_subtypes")
    if isinstance(subs_raw, list) and subs_raw:
        lines = "\n".join(str(x).strip() for x in subs_raw if str(x).strip())
    else:
        lines = "\n".join(default_business_facility_subtypes())

    bf_text = st.text_area(
        "Business facility subtypes (one per line)",
        value=lines,
        height=160,
        key="syscfg_business_facility_lines",
        help="Order is preserved in the Loan Application → Business Loan facility dropdown.",
    )
    subtypes = [ln.strip() for ln in (bf_text or "").splitlines() if ln.strip()]

    return LoanPipelineConfigSnapshot(
        loan_application_statuses=out_statuses or default_loan_application_statuses(),
        business_facility_subtypes=subtypes or default_business_facility_subtypes(),
    )
