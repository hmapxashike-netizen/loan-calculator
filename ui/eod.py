"""End-of-day: run EOD, backfill, receipt reallocation, single-loan daily-state recompute."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import streamlit as st


from style import render_main_header, render_sub_header, render_sub_sub_header

from services import eod_service
from ui.streamlit_feedback import run_with_spinner


def _format_eod_duration_seconds(seconds: Any) -> str:
    """Human-readable wall-clock duration for EOD (from ``duration_seconds``)."""
    try:
        s = float(seconds)
    except (TypeError, ValueError):
        return "—"
    if s < 0:
        s = 0.0
    if s < 60:
        return f"{s:.1f} s"
    m = int(s // 60)
    rem = s - m * 60
    if m >= 60:
        h, m = divmod(m, 60)
        return f"{h}h {m}m {rem:.1f}s"
    return f"{m}m {rem:.1f}s"


def _render_eod_admin_system_date_tab() -> None:
    """Administrative override of system business date (database row id=1)."""
    from eod.system_business_date import get_effective_date, set_current_system_date

    render_sub_sub_header("Set system business date")
    st.warning(
        "This **overwrites** `system_business_config.current_system_date`. "
        "Accruals, amount due, and EOD all depend on this value. "
        "Moving the date **backward** without replaying EOD can leave `loan_daily_state` inconsistent "
        "with history; after migrations, prefer backfill EOD for the affected range. "
        "Moving **forward** skips calendar days unless you run EOD for them."
    )
    cur = get_effective_date()
    st.caption(f"Current system date (before change): **{cur.isoformat()}**")
    new_date = st.date_input("New system business date", value=cur, key="eod_admin_new_system_date")
    confirm = st.text_input(
        'Type **SET DATE** to enable “Apply”',
        key="eod_admin_set_date_confirm",
        placeholder="SET DATE",
    )
    if st.button(
        "Apply new system date",
        type="primary",
        key="eod_admin_apply_system_date",
        disabled=(confirm or "").strip() != "SET DATE",
    ):

        def _apply() -> tuple[bool, str]:
            ok = set_current_system_date(new_date)
            return ok, new_date.isoformat()

        ok, iso = run_with_spinner("Updating system business date…", _apply)
        if ok:
            st.success(f"System business date set to **{iso}**.")
            st.rerun()
        else:
            st.error("Update failed. Check server logs and database connectivity.")


def render_eod_ui(
    *,
    get_system_config: Callable[[], dict[str, Any]],
    loan_management_available: bool,
    loan_management_error: str,
    load_system_config_from_db: Callable[[], dict | None] | None,
    is_admin: bool = False,
) -> None:
    """EOD page: business context from ``eod_service``; config from injected ``get_system_config``."""
    ctx = eod_service.get_eod_business_context()
    current_system_date = ctx["current_system_date"]
    next_date = ctx["next_system_date"]

    d_col1, d_col2 = st.columns([1, 2])
    with d_col1:
        st.caption(f"Calendar date: {datetime.now().strftime('%Y-%m-%d')}")
    with d_col2:
        st.markdown(
            f"<div style='font-size: 2rem; font-weight: 700; text-align: right;'>System date: {current_system_date.isoformat()}</div>",
            unsafe_allow_html=True,
        )

    if is_admin:
        _tab_ops, _tab_admin = st.tabs(["EOD operations", "Admin — system date"])
    else:
        (_tab_ops,) = st.tabs(["EOD operations"])

    def _eod_operations_body() -> None:
        cfg = get_system_config()
        eod_cfg = cfg.get("eod_settings", {}) or {}
        mode = eod_cfg.get("mode", "manual")
        automatic_time = eod_cfg.get("automatic_time", "23:00")

        st.caption(
            f"EOD mode: **{mode.upper()}**"
            + (f" (scheduled around {automatic_time})" if mode == "automatic" else "")
            + ". Configure under **System configurations → EOD configurations**."
        )

        st.divider()
        fix_eod_issues = st.checkbox(
            "Fix EOD issues (no date advance)",
            value=False,
            key="eod_fix_issues",
            help="Shows maintenance tools: reallocate receipts, run EOD for a specific date (backfill only), and recompute loan daily state.",
        )
        if mode == "manual":
            eod_busy = eod_service.is_another_eod_session_active_safe()
            if eod_busy:
                st.info(
                    "**Probe:** another database session may be holding the EOD lock (run in progress elsewhere). "
                    "Buttons stay enabled — the server still allows only **one** EOD at a time; if a run is truly "
                    "active, **Run** will return immediately with “already in progress”. "
                    "If the UI wrongly thinks a run is active, you can still try **Run**; only a real conflict is blocked."
                )
                st.caption(
                    "If you get “already in progress” but nothing is running, **restart the Streamlit server** "
                    "so stale DB sessions release the advisory lock."
                )

            render_sub_sub_header("Run EOD (advance system date)")
            st.caption(
                "Runs EOD for the current system date. On success, system date advances by +1 day. "
                "Accruals and Amount Due logic use the system date, not the calendar."
            )

            loans_with_state, active_loans = (
                eod_service.count_loans_with_daily_state_vs_active(current_system_date)
            )

            if loans_with_state > 0:
                if active_loans > 0:
                    st.warning(
                        f"EOD already has daily-state rows for **{loans_with_state} / {active_loans}** active loan(s) "
                        f"on **{current_system_date.isoformat()}**. "
                        "Re-running is idempotent; it will not advance the system date again. "
                        "Confirm below to re-run."
                    )
                else:
                    st.warning(
                        f"EOD already has daily-state rows for **{loans_with_state}** loan(s) "
                        f"on **{current_system_date.isoformat()}**. "
                        "Re-running is idempotent; it will not advance the system date again. "
                        "Confirm below to re-run."
                    )
            # Show outcome of the last EOD run (if any) so the user
            # gets a clear confirmation message even after rerun.
            last_eod = st.session_state.get("eod_last_result")
            if last_eod and last_eod.get("success"):
                status_txt = last_eod.get("run_status") or "SUCCESS"
                n_loans = int(last_eod.get("loans_processed") or 0)
                dur = _format_eod_duration_seconds(last_eod.get("duration_seconds"))
                done_utc = last_eod.get("completed_at_utc")
                done_part = (
                    f" Completed (UTC): {done_utc}."
                    if done_utc
                    else ""
                )
                msg = (
                    f"EOD completed for {last_eod['as_of_date']} "
                    f"(status: {status_txt}). "
                    f"System date advanced to {last_eod['new_system_date']}. "
                    f"{n_loans} loan(s) processed in {dur}. "
                    f"Started (UTC): {last_eod['real_world_time']}.{done_part}"
                )
                st.success(msg)
                if last_eod.get("run_id"):
                    st.caption(f"Run ID: {last_eod['run_id']}")
            elif last_eod and not last_eod.get("success"):
                fail_stage = last_eod.get("failed_stage")
                raw_err = last_eod.get("error")
                err = (
                    "Unknown error"
                    if raw_err is None
                    else str(raw_err).strip() or "Unknown error"
                )
                is_concurrent = bool(last_eod.get("concurrent_eod")) or (
                    "already in progress" in err.lower()
                )
                if is_concurrent:
                    st.warning(f"**EOD did not start** (single-flight lock): {err}")
                    if st.button("Dismiss message", key="eod_dismiss_last"):
                        st.session_state.pop("eod_last_result", None)
                        st.rerun()
                elif fail_stage:
                    st.error(f"EOD failed at stage `{fail_stage}`: {err}")
                else:
                    st.error(f"EOD failed: {err}")
                if last_eod.get("run_id") and not is_concurrent:
                    st.caption(f"Run ID: {last_eod['run_id']} | status: {last_eod.get('run_status') or 'FAILED'}")

            # Auto-clear confirmation after a successful EOD run.
            # Streamlit forbids modifying a widget's session_state key after the widget
            # is instantiated in the current script run, so we clear it here (before
            # the widget is created) on the next rerun.
            if st.session_state.get("eod_confirm_clear_requested"):
                st.session_state["eod_confirm"] = False
                st.session_state["eod_confirm_clear_requested"] = False

            confirm = st.checkbox(
                f"I confirm: EOD will process accruals for **{current_system_date.isoformat()}**. "
                f"On success, system date will advance to **{next_date.isoformat()}**.",
                key="eod_confirm",
            )
            if st.button(
                "Run EOD now",
                type="primary",
                key="eod_run_now",
                disabled=not confirm,
            ):
                st.info(
                    f"**EOD in progress** — processing **{current_system_date.isoformat()}**. "
                    "Please wait; do not close or refresh this page until finished."
                )
                with st.spinner("Running EOD (loan engine, allocations, accounting)…"):
                    result = eod_service.run_full_eod_advance_system_date()
                if result["success"]:
                    # Persist result so confirmation survives the rerun and is
                    # visible together with the updated system date.
                    st.session_state["eod_last_result"] = result
                    # Prevent accidental re-run: auto-clear confirmation checkbox.
                    # Do it on the next rerun to avoid Streamlit API restrictions.
                    st.session_state["eod_confirm_clear_requested"] = True
                    st.rerun()
                else:
                    st.session_state["eod_last_result"] = result
                    st.rerun()

            if fix_eod_issues:
                render_sub_sub_header("Backfill EOD (specific date, no system date advance)")
                st.caption("Backfill only. Does not advance system date.")
                backfill_date = st.date_input("EOD as-of date", current_system_date, key="eod_backfill_date")
                if st.button(
                    "Run EOD for date only",
                    key="eod_backfill_btn",
                ):
                    st.info(
                        f"**EOD backfill in progress** for **{backfill_date.isoformat()}**. Please wait…"
                    )
                    try:
                        with st.spinner("Running EOD for selected date…"):
                            result = eod_service.run_backfill_eod_for_date(backfill_date)
                        duration = result.finished_at - result.started_at
                        st.success(
                            f"EOD completed for {result.as_of_date.isoformat()} – "
                            f"processed {result.loans_processed} loans. "
                            f"Status: {result.run_status}. System date unchanged."
                        )
                        st.caption(f"Run ID: {result.run_id} | Duration: {duration}")
                    except Exception as e:
                        st.error(f"EOD run failed: {e}")
        else:
            render_sub_sub_header("Manual EOD run")
            st.info(
                "EOD is configured for **automatic** mode. Manual runs are disabled here. "
                "Use your scheduling/ops tooling to trigger EOD."
            )

        if fix_eod_issues:
            # Available in both manual and automatic EOD modes — does not advance system date.
            render_sub_sub_header("Reallocate receipts")
            st.caption(
                "Re-runs waterfall allocation for selected **posted** receipts and **writes results to the database**: "
                "`loan_repayment_allocation` (updated in place) and `loan_daily_state` for each receipt’s **value date**, "
                "plus unapplied-funds adjustments where applicable. "
                "**Does not advance the system business date.**"
            )
            st.markdown(
                "**When to use what**\n"
                "- **Typical:** receipts with **value date = current system date** — fix same-day allocation without running full EOD.\n"
                "- **Other dates / whole book for a day:** use **Run EOD for specific date (backfill, no advance)** above — "
                "recomputes `loan_daily_state` for **all loans** for that as-of date (and runs other EOD stages per config).\n"
                "- **Per-receipt** tool here still works for **any** value date if you enter repayment IDs or pick loan + date; "
                "it only touches those receipts’ allocation rows and the related daily-state date(s)."
            )
            if not loan_management_available:
                st.warning(f"Loan management unavailable: {loan_management_error}")
            elif load_system_config_from_db is None:
                st.warning("Loan management config loader is not available; cannot reallocate.")
            else:
                rcol1, rcol2 = st.columns(2)
                with rcol1:
                    realloc_loan = st.number_input(
                        "Loan ID",
                        min_value=1,
                        step=1,
                        value=1,
                        key="eod_realloc_loan_id",
                        help="Posted receipts for this loan on the value date will be reallocated.",
                    )
                    realloc_vd = st.date_input(
                        "Value date",
                        value=current_system_date,
                        key="eod_realloc_value_date",
                    )
                with rcol2:
                    realloc_ids_text = st.text_area(
                        "Or repayment IDs (one per line or comma-separated)",
                        height=100,
                        placeholder="12\n15\n18",
                        key="eod_realloc_ids_text",
                        help="If provided with the button below, these IDs are used instead of loan+date.",
                    )

                b1, b2 = st.columns(2)
                with b1:
                    run_by_loan_date = st.button(
                        "Reallocate all on loan + value date",
                        key="eod_realloc_by_loan_date",
                        type="secondary",
                        disabled=not fix_eod_issues,
                    )
                with b2:
                    run_by_ids = st.button(
                        "Reallocate listed repayment IDs",
                        key="eod_realloc_by_ids",
                        type="secondary",
                        disabled=not fix_eod_issues,
                    )

                if run_by_loan_date:
                    cfg = load_system_config_from_db() or {}
                    try:
                        ids = eod_service.list_repayment_ids_for_loan_value_date(
                            int(realloc_loan), realloc_vd
                        )
                        if not ids:
                            st.warning(
                                f"No posted receipts for loan_id={int(realloc_loan)} on {realloc_vd.isoformat()}."
                            )
                        else:
                            with st.spinner(f"Reallocating {len(ids)} receipt(s)…"):
                                ok, err = eod_service.reallocate_repayments_for_ids(
                                    ids, system_config=cfg
                                )
                            if ok:
                                st.success(f"Reallocated repayment_id(s): {ok}")
                            if err:
                                for rid, msg in err:
                                    st.error(f"repayment_id={rid}: {msg}")
                    except Exception as e:
                        st.error(str(e))

                if run_by_ids:
                    parsed, bad_token = eod_service.parse_repayment_id_lines(
                        realloc_ids_text or ""
                    )
                    if bad_token is not None:
                        st.error(f"Not an integer: {bad_token!r}")
                    elif not parsed:
                        st.warning("Enter at least one repayment ID.")
                    else:
                        cfg = load_system_config_from_db() or {}
                        with st.spinner(f"Reallocating {len(parsed)} receipt(s)…"):
                            ok, err = eod_service.reallocate_repayments_for_ids(
                                parsed, system_config=cfg
                            )
                        if ok:
                            st.success(f"Reallocated repayment_id(s): {ok}")
                        if err:
                            for rid, msg in err:
                                st.error(f"repayment_id={rid}: {msg}")

        if fix_eod_issues:
            st.caption(
                "**Reallocate** only works when there is at least one receipt for that date. "
                "If all receipts were deleted or you need to refresh `loan_daily_state` from the "
                "engine and prior day, use this instead (single-loan EOD recompute)."
            )
            col_a, col_b = st.columns(2)
            with col_a:
                rl_loan = st.number_input(
                    "Loan ID",
                    min_value=1,
                    value=1,
                    step=1,
                    key="eod_recompute_loan_id",
                )
            with col_b:
                rl_date = st.date_input(
                    "As-of date (loan_daily_state row)",
                    value=current_system_date,
                    key="eod_recompute_as_of",
                )
            if st.button(
                "Recompute loan daily state for this loan + date",
                key="eod_run_single_loan_eod",
                disabled=not fix_eod_issues,
            ):
                if load_system_config_from_db is None:
                    st.error("System config loader is not available.")
                else:
                    cfg = load_system_config_from_db() or {}
                    try:
                        with st.spinner(f"Running engine for loan_id={int(rl_loan)} on {rl_date}…"):
                            eod_service.recompute_single_loan_daily_state(
                                int(rl_loan), rl_date, system_config=cfg
                            )
                        st.success(
                            f"Updated `loan_daily_state` for loan_id={int(rl_loan)} as of {rl_date}."
                        )
                    except Exception as ex:
                        st.error(str(ex))

    with _tab_ops:
        _eod_operations_body()
    if is_admin:
        with _tab_admin:
            _render_eod_admin_system_date_tab()
