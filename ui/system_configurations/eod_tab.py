"""EOD configurations tab (business date widget + JSON-backed EOD settings)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, NamedTuple

import streamlit as st


from style import render_main_header, render_sub_header, render_sub_sub_header

from accrual_convention import ACCRUAL_START_EFFECTIVE_DAY


class EodConfigSnapshot(NamedTuple):
    accrual_start_convention_selected: Any
    eod_mode: str
    eod_time: str
    eod_tasks: dict[str, bool]
    policy_mode: str
    blocking_stages: list
    advance_date_on_degraded: bool


def render_eod_config_tab(
    *,
    eod_mode: str,
    eod_time: str,
    eod_tasks: dict[str, bool],
    policy_mode: str,
    blocking_stages: list,
    advance_date_on_degraded: bool,
    accrual_start_convention_selected,
) -> EodConfigSnapshot:
    render_sub_sub_header("System business date")
    st.caption("Accruals and Amount Due use the system date, not the calendar.")
    try:
        from eod.system_business_date import (
            get_system_business_config,
            set_system_business_config,
        )

        sb_cfg = get_system_business_config()
        new_date = st.date_input(
            "Current system date",
            value=sb_cfg["current_system_date"],
            key="syscfg_system_date",
        )
        if new_date != sb_cfg["current_system_date"]:
            if st.button("Update system date", key="syscfg_update_date"):
                if set_system_business_config(current_system_date=new_date):
                    st.success("System date updated.")
                    st.rerun()
                else:
                    st.error("Failed to update.")
        rt = sb_cfg["eod_auto_run_time"]
        h = getattr(rt, "hour", 23)
        m = getattr(rt, "minute", 0)
        s = getattr(rt, "second", 0)
        default_time = (
            datetime.now()
            .replace(hour=h, minute=m, second=s, microsecond=0)
            .time()
        )
        new_time = st.time_input(
            "EOD auto-run time (when enabled)",
            value=default_time,
            key="syscfg_eod_auto_time",
        )
        new_auto = st.checkbox(
            "Enable auto EOD (trigger at configured time)",
            value=sb_cfg["is_auto_eod_enabled"],
            key="syscfg_auto_eod",
        )
        if st.button("Save auto EOD settings", key="syscfg_save_auto"):
            if set_system_business_config(
                eod_auto_run_time=new_time, is_auto_eod_enabled=new_auto
            ):
                st.success("Auto EOD settings saved.")
                st.rerun()
    except Exception as ex:
        st.warning(f"System business config not available (run migration 26): {ex}")

    st.divider()
    render_sub_sub_header("End of day (EOD) settings")
    st.caption(
        "Configure how and when EOD runs, and which high-level tasks should be included. "
        "The detailed orchestration is fixed in code for safety and auditability."
    )
    st.caption(
        "Scheduled regular interest: **disbursement** and **each instalment due date** start a "
        "period; interest accrues on every calendar day from that start through the day **before** "
        "the next due (the due date begins the next period). Stored config key "
        "`accrual_start_convention` is normalised to EFFECTIVE_DAY."
    )
    accrual_start_convention_selected = ACCRUAL_START_EFFECTIVE_DAY

    mode_label = st.radio(
        "EOD mode",
        ["Manual (run from End of day page)", "Automatic (external scheduler)"],
        index=0 if eod_mode == "manual" else 1,
        help=(
            "Automatic mode assumes an external scheduler (e.g. cron, Windows Task Scheduler) "
            "will invoke the EOD script at the configured time. The app itself does not run "
            "background jobs."
        ),
        key="syscfg_eod_mode",
    )
    if mode_label.startswith("Manual"):
        eod_mode = "manual"
    else:
        eod_mode = "automatic"

    if eod_mode == "automatic":
        hours, minutes = 23, 0
        try:
            parts = (eod_time or "23:00").split(":")
            hours, minutes = int(parts[0]), int(parts[1])
        except Exception:
            pass
        time_value = st.time_input(
            "Preferred EOD time (24h, server local time)",
            datetime.now()
            .replace(hour=hours, minute=minutes, second=0, microsecond=0)
            .time(),
            key="syscfg_eod_time",
        )
        eod_time = time_value.strftime("%H:%M")

    st.markdown("**EOD tasks**")
    st.caption(
        "Choose which high-level tasks should run as part of EOD. "
        "The detailed sequence is fixed in code for safety and auditability."
    )
    st.checkbox(
        "Run loan engine (update loan buckets & interest)",
        value=True,
        disabled=True,
        key="syscfg_eod_task_engine",
    )
    eod_tasks["post_accounting_events"] = st.checkbox(
        "Post accounting events after EOD",
        value=eod_tasks.get("post_accounting_events", False),
        key="syscfg_eod_task_acct",
    )
    eod_tasks["generate_statements"] = st.checkbox(
        "Generate statements batch after EOD",
        value=eod_tasks.get("generate_statements", False),
        key="syscfg_eod_task_stmt",
    )
    eod_tasks["snapshot_financial_statements"] = st.checkbox(
        "Save immutable month-end/year-end statement snapshots",
        value=eod_tasks.get("snapshot_financial_statements", True),
        key="syscfg_eod_task_stmt_snapshot",
        help="On accounting period close, persist Trial Balance, P&L, Balance Sheet, Cash Flow, and Statement of Changes in Equity.",
    )
    eod_tasks["send_notifications"] = st.checkbox(
        "Send notifications (e.g. SMS/email) based on EOD results",
        value=eod_tasks.get("send_notifications", False),
        key="syscfg_eod_task_notify",
    )

    st.markdown("**Stage failure policy**")
    st.caption(
        "Control which stage failures block EOD/date advance. "
        "Hybrid mode uses blocking stages below; non-blocking failures become DEGRADED."
    )
    policy_mode = st.selectbox(
        "Policy mode",
        ["strict", "hybrid", "best_effort"],
        index=["strict", "hybrid", "best_effort"].index(policy_mode)
        if policy_mode in {"strict", "hybrid", "best_effort"}
        else 1,
        key="syscfg_eod_policy_mode",
    )
    stage_options = [
        "loan_engine",
        "reallocate_after_reversals",
        "apply_unapplied_to_arrears",
        "accounting_events",
        "statements",
        "notifications",
    ]
    blocking_stages = st.multiselect(
        "Blocking stages",
        options=stage_options,
        default=[s for s in blocking_stages if s in stage_options],
        key="syscfg_eod_blocking_stages",
        help="If a blocking stage fails in hybrid mode, EOD run is FAILED and system date is not advanced.",
    )
    advance_date_on_degraded = st.checkbox(
        "Advance system date when run is DEGRADED",
        value=advance_date_on_degraded,
        key="syscfg_eod_advance_on_degraded",
        help="Use with care. Recommended OFF for conservative financial controls.",
    )
    return EodConfigSnapshot(
        accrual_start_convention_selected=accrual_start_convention_selected,
        eod_mode=eod_mode,
        eod_time=eod_time,
        eod_tasks=eod_tasks,
        policy_mode=policy_mode,
        blocking_stages=blocking_stages,
        advance_date_on_degraded=advance_date_on_degraded,
    )
