"""Accounting periods & source-cash cache maintenance."""

from __future__ import annotations

from typing import Any, NamedTuple

import streamlit as st



from style import render_main_header, render_sub_header, render_sub_sub_header

class AccountingPeriodsSnapshot(NamedTuple):
    month_mode: str
    month_day: int
    fiscal_year_end_month: int
    snapshot_max_rows: int


def render_accounting_config_tab(*, cfg: dict[str, Any]) -> AccountingPeriodsSnapshot:
    render_sub_sub_header("Accounting periods")
    st.caption(
        "Define accounting month-end and fiscal year-end. The system uses this for EOM/EOY decisions in EOD and financial reporting."
    )
    acc_cfg = cfg.get("accounting_periods", {}) or {}
    month_mode = str(acc_cfg.get("month_end_mode") or "calendar")
    month_day_default = int(acc_cfg.get("month_end_day") or 31)
    fiscal_year_end_month_default = int(acc_cfg.get("fiscal_year_end_month") or 12)
    snapshot_max_rows_default = int(acc_cfg.get("snapshot_max_rows") or 100)

    mirror_calendar = st.checkbox(
        "Accounting month mirrors calendar month",
        value=(month_mode == "calendar"),
        key="syscfg_acc_mirror_calendar",
    )
    if mirror_calendar:
        month_mode = "calendar"
        month_day = 31
        st.caption("Month-end is the last calendar day of each month.")
    else:
        month_mode = "fixed_day"
        month_day = st.number_input(
            "Accounting month ends on day",
            min_value=1,
            max_value=31,
            value=month_day_default if 1 <= month_day_default <= 31 else 5,
            step=1,
            key="syscfg_acc_month_end_day",
            help="If a month has fewer days than this value, the month end is treated as that month's last day.",
        )

    month_labels = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    year_end_month_label = st.selectbox(
        "Fiscal year-end month",
        options=month_labels,
        index=max(0, min(11, fiscal_year_end_month_default - 1)),
        key="syscfg_acc_year_end_month",
        help="Year closes on the accounting month-end date of this month.",
    )
    fiscal_year_end_month = month_labels.index(year_end_month_label) + 1
    snapshot_max_rows = st.number_input(
        "Max snapshot rows in history view",
        min_value=10,
        max_value=1000,
        value=snapshot_max_rows_default
        if 10 <= snapshot_max_rows_default <= 1000
        else 100,
        step=10,
        key="syscfg_acc_snapshot_max_rows",
        help="Upper bound for rows returned when loading snapshot history. Controls performance only (does not affect what is stored).",
    )

    st.divider()
    with st.expander("Maintenance — source cash account cache", expanded=False):
        st.caption(
            "Loan capture and Teller read a **saved** list of posting accounts: leaves under **A100000**, "
            "one set per first-level branch (see accounting repository). Nothing is recomputed on each screen load."
        )
        _scc = cfg.get("source_cash_account_cache") or {}
        _scc_n = len(_scc.get("entries") or [])
        st.caption(
            f"Last rebuilt: **{_scc.get('refreshed_at') or 'never'}** · "
            f"Accounts in cache: **{_scc_n}** · Root: **{_scc.get('root_code') or 'A100000'}**"
        )
        with st.expander("Open only to rebuild the cache", expanded=False):
            st.caption(
                "Recomputes from the **live** chart. Misuse can confuse operators until they pick from the new list."
            )
            _scc_confirm = st.checkbox(
                "I am an administrator and I intend to rebuild the source-cash account list.",
                value=False,
                key="syscfg_scc_confirm_chk",
            )
            _scc_type = st.text_input(
                "Type REBUILD to enable the action",
                key="syscfg_scc_type_confirm",
                help="Prevents accidental one-click rebuilds.",
            )
            if st.button(
                "Rebuild source cash account cache",
                key="syscfg_scc_rebuild_btn",
                disabled=(not _scc_confirm)
                or (_scc_type.strip().upper() != "REBUILD"),
            ):
                try:
                    from accounting.service import AccountingService

                    _scc_block = AccountingService().refresh_source_cash_account_cache()
                    st.session_state.pop("system_config", None)
                    st.success(
                        f"Cache rebuilt at {_scc_block.get('refreshed_at')} — "
                        f"{len(_scc_block.get('entries') or [])} account(s)."
                    )
                    st.rerun()
                except Exception as _scc_ex:
                    st.error(str(_scc_ex))
    return AccountingPeriodsSnapshot(
        month_mode=month_mode,
        month_day=int(month_day),
        fiscal_year_end_month=int(fiscal_year_end_month),
        snapshot_max_rows=int(snapshot_max_rows),
    )
