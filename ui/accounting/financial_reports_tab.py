"""Accounting: financial reports tab (trial balance, P&L, BS, equity, cash flow, snapshots)."""

from __future__ import annotations

import pandas as pd
import streamlit as st


from style import render_main_header, render_sub_header, render_sub_sub_header

from display_formatting import format_display_amount


def render_financial_reports_tab(
    *,
    reports,
    get_system_config,
    get_system_date,
    money_df_column_config,
) -> None:
    render_sub_sub_header("Financial Reports")
    try:
        from accounting.periods import (
            normalize_accounting_period_config,
            get_month_period_bounds,
            is_eom,
            is_eoy,
        )
        period_cfg = normalize_accounting_period_config(get_system_config())
        month_bounds = get_month_period_bounds(get_system_date(), period_cfg)
        close_flags = []
        if is_eom(get_system_date(), period_cfg):
            close_flags.append("EOM")
        if is_eoy(get_system_date(), period_cfg):
            close_flags.append("EOY")
        st.caption(
            "Accounting month: "
            f"{month_bounds.start_date.isoformat()} to {month_bounds.end_date.isoformat()}"
            + (f" | Today is {' & '.join(close_flags)}." if close_flags else "")
        )
    except Exception:
        month_bounds = None
    rep_tb, rep_pl, rep_bs, rep_eq, rep_cf, rep_snap = st.tabs([
        "Trial Balance", "Profit & Loss", "Balance Sheet", "Statement of Equity", "Cash Flow", "Snapshots"
    ])

    with rep_tb:
        st.markdown("### Trial Balance")
        sys_date = get_system_date()
        tb_as_of = st.date_input("As of Date", value=sys_date, key="tb_as_of")

        if st.button("Generate Trial Balance"):
            tb = reports.get_trial_balance(tb_as_of)
            if tb:
                df_tb = pd.DataFrame([{
                    "Code": r["code"], "Name": r["name"], "Category": r["category"],
                    "Debit": float(r["debit"]), "Credit": float(r["credit"])
                } for r in tb])
                st.dataframe(
                    df_tb,
                    use_container_width=True,
                    hide_index=True,
                    column_config=money_df_column_config(df_tb),
                )
                st.write(
                    f"**Total Debits:** {format_display_amount(df_tb['Debit'].sum(), system_config=get_system_config())} | "
                    f"**Total Credits:** {format_display_amount(df_tb['Credit'].sum(), system_config=get_system_config())}"
                )
            else:
                st.info("No data.")

    with rep_pl:
        st.markdown("### Profit and Loss")
        sys_date = get_system_date()
        pl_dates = st.date_input(
            "Date Range", 
            value=((month_bounds.start_date, sys_date) if month_bounds else (sys_date.replace(day=1), sys_date)),
            key="pl_dates"
        )

        if st.button("Generate P&L"):
            if isinstance(pl_dates, (tuple, list)):
                pl_start = pl_dates[0] if len(pl_dates) > 0 else sys_date
                pl_as_of = pl_dates[1] if len(pl_dates) > 1 else pl_start
            else:
                pl_start = pl_as_of = pl_dates

            pl = reports.get_profit_and_loss(pl_start, pl_as_of)
            if pl:
                df_pl = pd.DataFrame([{
                    "Code": r["code"], "Name": r["name"], "Category": r["category"],
                    "Balance": float(r["credit"] - r["debit"]) if r["category"] == "INCOME" else float(r["debit"] - r["credit"])
                } for r in pl])
                st.dataframe(df_pl, use_container_width=True)
            else:
                st.info("No data.")

    with rep_bs:
        st.markdown("### Balance Sheet")
        sys_date = get_system_date()
        bs_as_of = st.date_input("As of Date", value=sys_date, key="bs_as_of")
        if st.button("Generate Balance Sheet"):
            bs = reports.get_balance_sheet(bs_as_of)
            if bs:
                df_bs = pd.DataFrame([{
                    "Code": r["code"], "Name": r["name"], "Category": r["category"],
                    "Balance": float(r["debit"] - r["credit"]) if r["category"] == "ASSET" else float(r["credit"] - r["debit"])
                } for r in bs])
                st.dataframe(df_bs, use_container_width=True)
            else:
                st.info("No data.")

    with rep_eq:
        st.markdown("### Statement of Changes in Equity")
        sys_date = get_system_date()
        eq_dates = st.date_input(
            "Date Range", 
            value=((month_bounds.start_date, sys_date) if month_bounds else (sys_date.replace(day=1), sys_date)),
            key="eq_dates"
        )

        if st.button("Generate Statement of Equity"):
            if isinstance(eq_dates, (tuple, list)):
                eq_start = eq_dates[0] if len(eq_dates) > 0 else sys_date
                eq_as_of = eq_dates[1] if len(eq_dates) > 1 else eq_start
            else:
                eq_start = eq_as_of = eq_dates

            eq = reports.get_statement_of_changes_in_equity(eq_start, eq_as_of)
            if eq:
                df_eq = pd.DataFrame([{
                    "Code": r["code"], "Name": r["name"], "Category": r["category"],
                    "Balance": float(r["credit"] - r["debit"])
                } for r in eq])
                st.dataframe(df_eq, use_container_width=True)
            else:
                st.info("No data.")

    with rep_cf:
        st.markdown("### Statement of Cash Flows (Indirect)")
        sys_date = get_system_date()
        cf_dates = st.date_input(
            "Date Range", 
            value=((month_bounds.start_date, sys_date) if month_bounds else (sys_date.replace(day=1), sys_date)),
            key="cf_dates"
        )

        if st.button("Generate Cash Flow"):
            if isinstance(cf_dates, (tuple, list)):
                cf_start = cf_dates[0] if len(cf_dates) > 0 else sys_date
                cf_as_of = cf_dates[1] if len(cf_dates) > 1 else cf_start
            else:
                cf_start = cf_as_of = cf_dates

            cf = reports.get_cash_flow_statement(cf_start, cf_as_of)
            st.json(cf)

    with rep_snap:
        st.markdown("### Statement Snapshot History")
        st.caption(
            "View immutable month-end and year-end financial statements captured at accounting period close."
        )

        stmt_type_display = {
            "TRIAL_BALANCE": "Trial Balance",
            "PROFIT_AND_LOSS": "Profit & Loss",
            "BALANCE_SHEET": "Balance Sheet",
            "CASH_FLOW": "Cash Flow",
            "CHANGES_IN_EQUITY": "Statement of Changes in Equity",
        }
        stmt_types = ["(All)"] + [stmt_type_display[k] for k in stmt_type_display]
        period_types = ["(All)", "MONTH", "YEAR"]

        acc_cfg = get_system_config().get("accounting_periods", {}) or {}
        snap_default_limit = int(acc_cfg.get("snapshot_max_rows") or 100)

        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            stmt_choice = st.selectbox("Statement type", stmt_types, index=0, key="snap_stmt_type")
        with col_f2:
            period_choice = st.selectbox("Period type", period_types, index=0, key="snap_period_type")
        with col_f3:
            limit = st.number_input(
                "Max rows",
                min_value=10,
                max_value=1000,
                value=snap_default_limit if 10 <= snap_default_limit <= 1000 else 100,
                step=10,
                key="snap_limit",
            )

        col_d1, col_d2 = st.columns(2)
        with col_d1:
            date_from = st.date_input("Period end date from", value=None, key="snap_from")
        with col_d2:
            date_to = st.date_input("Period end date to", value=None, key="snap_to")

        if st.button("Load snapshots", key="snap_load"):
            stmt_key = None
            if stmt_choice != "(All)":
                inv = {v: k for k, v in stmt_type_display.items()}
                stmt_key = inv.get(stmt_choice)
            period_key = None if period_choice == "(All)" else period_choice

            snaps = reports.list_statement_snapshots(
                statement_type=stmt_key,
                period_type=period_key,
                period_end_date_from=date_from if date_from else None,
                period_end_date_to=date_to if date_to else None,
                limit=int(limit),
            )
            if not snaps:
                st.info("No snapshots found for the selected filters.")
            else:
                df_snaps = pd.DataFrame(
                    [
                        {
                            "ID": str(r["id"]),
                            "Statement": stmt_type_display.get(r["statement_type"], r["statement_type"]),
                            "Period type": r["period_type"],
                            "Period start": r["period_start_date"],
                            "Period end": r["period_end_date"],
                            "Ledger cutoff": r["source_ledger_cutoff_date"],
                            "Status": r["status"],
                            "Generated at": r["generated_at"],
                            "Generated by": r["generated_by"],
                            "Calc version": r["calculation_version"],
                        }
                        for r in snaps
                    ]
                )
                st.dataframe(df_snaps, use_container_width=True, hide_index=True)

                snap_ids = [str(r["id"]) for r in snaps]
                sel_id = st.selectbox(
                    "Select snapshot to inspect",
                    options=snap_ids,
                    format_func=lambda x: next(
                        (
                            f"{stmt_type_display.get(r['statement_type'], r['statement_type'])} "
                            f"({r['period_type']}) – {r['period_end_date']}"
                            for r in snaps
                            if str(r["id"]) == x
                        ),
                        x,
                    ),
                    key="snap_sel_id",
                )
                if sel_id and st.button("View snapshot details", key="snap_view"):
                    snap = reports.get_statement_snapshot_with_lines(sel_id)
                    if not snap:
                        st.error("Snapshot not found.")
                    else:
                        header = snap["header"]
                        lines = snap["lines"] or []
                        st.markdown(
                            f"**{stmt_type_display.get(header['statement_type'], header['statement_type'])}** "
                            f"({header['period_type']}) for period "
                            f"{header['period_start_date']} → {header['period_end_date']} "
                            f"(ledger cutoff {header['source_ledger_cutoff_date']})"
                        )
                        st.caption(
                            f"Generated by `{header['generated_by']}` at {header['generated_at']} "
                            f"(calculation version: {header['calculation_version']})."
                        )

                        if lines:
                            df_lines = pd.DataFrame(
                                [
                                    {
                                        "Code": r.get("line_code"),
                                        "Name": r.get("line_name"),
                                        "Category": r.get("line_category"),
                                        "Debit": float(r.get("debit") or 0),
                                        "Credit": float(r.get("credit") or 0),
                                        "Amount": float(r.get("amount") or 0),
                                        "Currency": r.get("currency_code"),
                                    }
                                    for r in lines
                                ]
                            )
                            st.dataframe(df_lines, use_container_width=True, hide_index=True)
                        else:
                            st.info("No line items for this snapshot.")
