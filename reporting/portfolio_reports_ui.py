"""
Portfolio reporting: credit risk, listings, IIS movement, concentration, ECL (provisions).
Shell placeholders: roll rates, collection efficiency (others implemented).
Debtor maturity + bucketed arrears ageing: see portfolio_reporting.py (read-only).
"""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from io import StringIO

import pandas as pd
import streamlit as st

from style import render_main_header, render_sub_header, render_sub_sub_header

from decimal_utils import as_10dp
from loan_management import _connection
from reporting.portfolio_reporting import (
    ARREARS_BUCKET_KEYS,
    ARREARS_BUCKET_LABELS,
    MATURITY_BUCKET_KEYS,
    MATURITY_BUCKET_LABELS,
    REGULATORY_MATURITY_BUCKET_KEYS,
    REGULATORY_MATURITY_BUCKET_LABELS,
    RESTRUCTURE_SCOPE_REMODIFIED,
    RESTRUCTURE_SCOPE_SPLIT,
    RESTRUCTURE_SCOPE_TOPUP,
    build_arrears_aging_report,
    build_creditor_arrears_aging_report,
    build_creditor_maturity_profile_report,
    build_debtor_creditor_maturity_gap_summary,
    explain_creditor_maturity_profile_empty,
    build_maturity_profile_report,
    build_regulatory_maturity_profile_report,
    build_regulatory_maturity_summary_table,
    restructure_scope_sql,
)


def _portfolio_real_dict_cursor():
    """Defer psycopg2.extras import until a portfolio DB report actually runs."""
    from psycopg2.extras import RealDictCursor

    return RealDictCursor


# Legacy placeholder (sessions may still hold this until migration runs).
_PORTFOLIO_REPORT_PLACEHOLDER = "— Select report to view —"

# (Radio label, stable session id, [(selectbox label, report key rk), ...]). Group 6 has no sub-reports (bulk export only).
_PORTFOLIO_GROUPS: list[tuple[str, str, list[tuple[str, str]]]] = [
    (
        "1 · Portfolio Performance & Risk",
        "g_perf",
        [
            ("Debtor Arrears Ageing (current delinquency status)", "r21"),
            ("Portfolio at Risk (PAR) (total exposure of loans with arrears)", "r22"),
            ("Migration / Roll Rates (movement between delinquency buckets)", "shell_23"),
            ("Concentration Reports (exposure by sector, region, or product)", "r51"),
            ("Collection Efficiency (actual collections vs. targets)", "shell_41"),
        ],
    ),
    (
        "2 · Financial Reporting & Impairment",
        "g_ifrs",
        [
            ("IFRS Provisions (Single Loan) (individual impairment assessments)", "ifrs"),
            ("ECL / Provision Movement (changes in ECL over time)", "r52"),
            ("IIS Movement (interest in suspense tracking)", "r42"),
        ],
    ),
    (
        "3 · Treasury & Liquidity Management",
        "g_treas",
        [
            ("Debtor Maturity Profile (when your money is coming in)", "mat_11"),
            ("Creditor Maturity Profile (when your debts are due to be paid)", "shell_12"),
            ("Creditor Arrears Ageing (past-due borrowing obligations)", "treas_cred_arrears"),
            ("Gap Analysis (variance between debtor and creditor timing)", "shell_gap"),
        ],
    ),
    (
        "4 · Regulatory & Statutory Compliance",
        "g_reg",
        [
            ("Regulatory Maturity Profile (regulator-ready buckets)", "reg_mat"),
            ("Loan Classification (standard, sub-standard, doubtful, loss)", "r53"),
        ],
    ),
    (
        "5 · Sales & Operational Data",
        "g_sales",
        [
            ("Disbursed Loans (new business volume and BD tracking)", "r32"),
            ("Master Loan Listing (comprehensive active loan data)", "r31"),
        ],
    ),
    (
        "6 · Data export",
        "g_export",
        [],
    ),
]

_GROUP_ID_TO_REPORTS: dict[str, list[tuple[str, str]]] = {g[1]: g[2] for g in _PORTFOLIO_GROUPS}

# Sessions may still hold the old flat dropdown label until migration runs once.
_LEGACY_PORTFOLIO_LABEL_TO_RK: dict[str, str] = {
    "IFRS Provisions (single loan)": "ifrs",
    "Debtor maturity profile": "mat_11",
    "Regulatory maturity profile": "reg_mat",
    "Creditor maturity profile": "shell_12",
    "Creditor arrears ageing": "treas_cred_arrears",
    "Gap analysis": "shell_gap",
    "Debtor arrears (aging)": "r21",
    "Portfolio at risk (PAR)": "r22",
    "Migration / roll rates (shell)": "shell_23",
    "Master loan listing": "r31",
    "Disbursed loans (period)": "r32",
    "Collection efficiency (shell)": "shell_41",
    "IIS movement": "r42",
    "Concentration": "r51",
    "ECL / provisions (IFRS view)": "r52",
    "Loan classification (regulatory)": "r53",
}


def _portfolio_group_id_for_rk(rk: str) -> str:
    for _title, gid, items in _PORTFOLIO_GROUPS:
        for _lbl, rkk in items:
            if rkk == rk:
                return gid
    return "g_perf"


# Reports whose loan base queries support restructure tag filters (OR semantics).
_REPORT_KEYS_WITH_RESTRUCTURE_FILTER = frozenset(
    {"mat_11", "reg_mat", "r21", "r22", "r31", "r32", "r42", "r51", "r52", "r53", "shell_gap"}
)

def _project_root_for_export() -> str:
    """Project root (parent of ``reporting/``) so subprocess can run ``scripts/export_loan_tables.py``."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


_RESTRUCTURE_TAG_LABELS: tuple[tuple[str, str], ...] = (
    ("Remodified in place (same loan)", RESTRUCTURE_SCOPE_REMODIFIED),
    ("Originated from split", RESTRUCTURE_SCOPE_SPLIT),
    ("Modification top-up applied", RESTRUCTURE_SCOPE_TOPUP),
)
# Clear report selection on next run (cannot assign widget key after the widget is created).
_PORTFOLIO_CLOSE_NEXT_RUN_KEY = "_portfolio_close_report_next_run"
# After export success, bump nonce so Loan ID gets a new widget key (form + clear_on_submit=False
# otherwise keeps the submitted value; cannot assign the same key after the widget exists).
_PORTFOLIO_CLEAR_EXPORT_LOAN_ID_NEXT_RUN = "_portfolio_clear_export_loan_id_next_run"
_EXPORT_LOAN_ID_INPUT_NONCE_KEY = "_export_loan_id_input_nonce"
_EXPORT_OUTPUT_DIR_WIDGET_KEY = "portfolio_export_output_dir"

try:
    from eod.system_business_date import get_effective_date
except ImportError:

    def get_effective_date() -> date:
        return date.today()


def _default_as_of() -> date:
    try:
        return get_effective_date()
    except Exception:
        return date.today()


def _df_download(df: pd.DataFrame, filename: str, *, button_key: str) -> None:
    # No per-report CSV while viewing a report; bulk export lives under Data export only.
    if st.session_state.get("portfolio_sel_rk"):
        return
    if df.empty:
        return
    buf = StringIO()
    df.to_csv(buf, index=False)
    st.download_button(
        f"Download {filename}",
        data=buf.getvalue().encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        key=f"dl_{button_key}",
    )


def _report_creditor_maturity_profile(as_of: date, active_only: bool) -> None:
    """Creditor drawdowns: future scheduled amounts from ``creditor_schedule_lines`` placed in tenor buckets."""
    render_sub_sub_header("Creditor maturity profile")
    st.caption(
        "**Schedule-based:** each instalment with due date **after** this as-of contributes its scheduled "
        "**principal** (principal-only view) or **principal + interest** (cash-flow view) to the maturity bucket "
        "for that due date (days from as-of to due). This is **not** weighted to ``principal_not_due`` or other "
        "daily-state balances — compare to the **Debtor maturity profile** if you need balance-reconciled timing."
    )
    view_type_label = st.radio(
        "Maturity view",
        [
            "Principal-only (standard)",
            "Full cash flow (principal + interest)",
        ],
        horizontal=True,
        key="port_cred_mat_view",
    )
    view_type = "principal" if "Principal-only" in view_type_label else "cash_flow"

    try:
        df = build_creditor_maturity_profile_report(
            as_of,
            active_only=active_only,
            view_type=view_type,
        )
    except Exception as ex:
        st.error(f"Could not build creditor maturity profile: {ex}")
        return

    if df.empty:
        try:
            st.info(explain_creditor_maturity_profile_empty(as_of, active_only=active_only))
        except Exception:
            st.info(
                "No creditor maturity rows. Check **Active loans only**, that each drawdown has a **schedule** "
                "with **future** instalments after this as-of, and that line **principal** (or P+I in cash-flow view) "
                "is positive."
            )
        return

    rename = {k: lbl for k, lbl in zip(MATURITY_BUCKET_KEYS, MATURITY_BUCKET_LABELS, strict=True)}
    rename.update(
        {
            "creditor_drawdown_id": "Drawdown ID",
            "lender_name": "Lender",
            "creditor_facility_id": "Facility ID",
            "loan_type": "Loan type",
            "scheduled_future_total": "Scheduled future (total)",
            "bucket_sum": "Bucket sum",
        }
    )
    df_view = df.rename(columns=rename)

    st.dataframe(df_view, hide_index=True, width="stretch", height=360)
    _df_download(df, f"creditor_maturity_profile_{view_type}_{as_of.isoformat()}.csv", button_key="port_cl_mat_csv")


def _report_gap_analysis(
    as_of: date,
    active_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Gap analysis (debtor vs creditor timing)")
    st.caption(
        "Sums **Debtor maturity profile** (balance-weighted to ``principal_not_due``) and **Creditor maturity "
        "profile** (pure **scheduled** future flows) into the same tenor bucket labels. **Net** = inflows − "
        "outflows; **Cumulative net** runs shortest-to-longest bucket. Restructure tags apply to **debtor** loans only."
    )
    view_type_label = st.radio(
        "Maturity basis (both sides)",
        ["Principal-only", "Full cash flow (principal + interest)"],
        horizontal=True,
        key="port_gap_mat_view",
    )
    view_type = "principal" if view_type_label.startswith("Principal") else "cash_flow"
    try:
        df = build_debtor_creditor_maturity_gap_summary(
            as_of,
            active_only=active_only,
            view_type=view_type,
            restructure_scope=restructure_scope,
        )
    except Exception as ex:
        st.error(f"Could not build gap analysis: {ex}")
        return
    df_view = df.rename(
        columns={
            "bucket": "Tenor bucket",
            "debtor_cash_inflows": "Debtor cash inflows",
            "creditor_cash_outflows": "Creditor cash outflows",
            "net_position": "Net (in − out)",
            "cumulative_position": "Cumulative net",
        }
    )
    st.dataframe(df_view, hide_index=True, width="stretch", height=320)
    _df_download(
        df,
        f"liquidity_gap_maturity_{view_type}_{as_of.isoformat()}.csv",
        button_key="port_gap_csv",
    )


def _shell_report(title: str, planned: list[str]) -> None:
    render_sub_sub_header(title)
    st.info("**Planned — not implemented yet.** See scope notes below.")
    for line in planned:
        st.markdown(f"- {line}")


def _bucket_summary_grid(
    keys: tuple[str, ...],
    labels: tuple[str, ...],
    sums: pd.Series,
    *,
    max_cols: int = 4,
) -> None:
    """Show portfolio bucket totals in rows of up to `max_cols` metrics."""
    n = len(keys)
    i = 0
    while i < n:
        chunk_end = min(i + max_cols, n)
        cols = st.columns(chunk_end - i)
        for j, idx in enumerate(range(i, chunk_end)):
            k, lbl = keys[idx], labels[idx]
            with cols[j]:
                st.caption(lbl)
                st.markdown(f"**{float(sums[k]):,.2f}**")
        i = chunk_end


def _report_arrears_aging(
    as_of: date,
    active_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Debtor loans arrears (aging)")
    try:
        df = build_arrears_aging_report(
            as_of, active_only=active_only, restructure_scope=restructure_scope
        )
    except Exception as ex:
        st.error(f"Could not build arrears ageing report: {ex}")
        return
    if df.empty:
        st.info("No rows at this as-of date.")
        return
    rename = {k: lbl for k, lbl in zip(ARREARS_BUCKET_KEYS, ARREARS_BUCKET_LABELS, strict=True)}
    rename.update(
        {
            "loan_id": "Loan ID",
            "customer_name": "Customer",
            "product_code": "Product",
            "scheme": "Scheme",
            "state_as_of": "State as-of",
            "days_overdue": "Days overdue",
            "total_outstanding_balance": "Total outstanding balance",
            "total_delinquency_arrears": "Total delinquency arrears",
        }
    )
    df_view = df.rename(columns=rename)
    st.dataframe(df_view, hide_index=True, width="stretch", height=360)
    _df_download(df_view, "debtor_arrears_aging_buckets.csv", button_key="port_r21_csv")


def _report_creditor_arrears_aging(as_of: date, active_only: bool) -> None:
    render_sub_sub_header("Creditor arrears (ageing)")
    st.caption(
        "Same methodology as **Debtor loans arrears (ageing)**: principal and interest arrears are allocated to "
        "past-due instalments (newest due first); penalty, default interest, and fees use daily state series when "
        "available. **Active loans only** limits to drawdowns with status **active**."
    )
    try:
        df = build_creditor_arrears_aging_report(as_of, active_only=active_only)
    except Exception as ex:
        st.error(f"Could not build creditor arrears ageing report: {ex}")
        return
    if df.empty:
        st.info("No creditor drawdowns in arrears at this as-of date (or no daily state yet).")
        return
    rename = {k: lbl for k, lbl in zip(ARREARS_BUCKET_KEYS, ARREARS_BUCKET_LABELS, strict=True)}
    rename.update(
        {
            "creditor_drawdown_id": "Drawdown ID",
            "lender_name": "Lender",
            "creditor_facility_id": "Facility ID",
            "loan_type": "Loan type",
            "state_as_of": "State as-of",
            "days_overdue": "Days overdue",
            "total_outstanding_balance": "Total outstanding balance",
            "total_delinquency_arrears": "Total delinquency arrears",
        }
    )
    df_view = df.rename(columns=rename)
    st.dataframe(df_view, hide_index=True, width="stretch", height=360)
    _df_download(
        df_view,
        f"creditor_arrears_aging_{as_of.isoformat()}.csv",
        button_key="port_cl_arr_csv",
    )


def _report_debtor_maturity(
    as_of: date,
    active_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Debtor maturity profile")
    st.caption(
        "Principal-only: principal not due is spread across future instalments in proportion to scheduled principal. "
        "Full cash flow: scheduled principal and interest are spread across future instalments. "
        "Buckets use days from as-of to due date. Residual not mapped from the schedule is included in **360+ days**."
    )
    
    view_type_label = st.radio(
        "Maturity View",
        [
            "The Principal-Only View (The Standard)",
            "The Full Cash Flow View (Principal + Interest)"
        ],
        horizontal=True,
    )
    view_type = "principal" if "Principal-Only" in view_type_label else "cash_flow"

    try:
        df = build_maturity_profile_report(
            as_of,
            active_only=active_only,
            view_type=view_type,
            restructure_scope=restructure_scope,
        )
    except Exception as ex:
        st.error(f"Could not build maturity profile: {ex}")
        return
        
    if df.empty:
        st.info("No loans with future profile found at this as-of date.")
        return
        
    rename = {k: lbl for k, lbl in zip(MATURITY_BUCKET_KEYS, MATURITY_BUCKET_LABELS, strict=True)}
    df_view = df.rename(columns=rename)
    
    if view_type == "principal" and "recon_diff" in df.columns and df["recon_diff"].abs().max() > 0.02:
        st.warning(
            "Some rows have **recon_diff** ≠ 0 (`bucket_sum` vs `principal_not_due`). "
            "Inspect the export for rounding or schedule gaps."
        )
        
    st.dataframe(df_view, hide_index=True, width="stretch", height=360)
    _df_download(df, f"debtor_maturity_profile_{view_type}.csv", button_key="port_mat11_csv")


def _report_regulatory_maturity_profile(
    as_of: date,
    active_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Regulatory maturity profile")
    st.caption(
        "Debtor **cash inflows** are the same engine as **Debtor maturity profile**: `principal_not_due` from "
        "latest `loan_daily_state` on/before as-of, spread across **future** instalments by scheduled principal "
        "(or principal + interest in cash-flow view). Amounts are **re-bucketed** into regulatory bands "
        "(0–7, 8–14, 15–30, …, 360+). **Cash outflows** in the net summary below stay **0** (debtor-only table); "
        "use **Gap analysis** for combined timing in standard maturity buckets, or **Creditor maturity profile** "
        "for schedule-only liability cashflows."
    )
    view_type_label = st.radio(
        "Maturity basis",
        [
            "Principal-only (same as standard debtor maturity)",
            "Full cash flow (principal + interest)",
        ],
        horizontal=True,
        key="port_reg_mat_view",
    )
    view_type = "principal" if "Principal-only" in view_type_label else "cash_flow"

    try:
        summary_df = build_regulatory_maturity_summary_table(
            as_of,
            active_only=active_only,
            view_type=view_type,
            restructure_scope=restructure_scope,
        )
        detail_df = build_regulatory_maturity_profile_report(
            as_of,
            active_only=active_only,
            view_type=view_type,
            restructure_scope=restructure_scope,
        )
    except Exception as ex:
        st.error(f"Could not build regulatory maturity profile: {ex}")
        return

    try:
        from loan_management import load_system_config_from_db

        _cfg = load_system_config_from_db() or {}
    except Exception:
        _cfg = {}
    inst = str(_cfg.get("institution_code") or _cfg.get("regulatory_institution_id") or "").strip()
    fy_start = date(as_of.year, 1, 1)
    h1, h2, h3, h4 = st.columns(4)
    with h1:
        st.markdown(f"**Institution:** {inst or '—'}")
    with h2:
        st.markdown(f"**Financial year:** {as_of.year}")
    with h3:
        st.markdown(f"**Period start:** {fy_start.isoformat()}")
    with h4:
        st.markdown(f"**Period end / as-of:** {as_of.isoformat()}")
    st.caption(
        "Header uses calendar year for period start and **as-of** as period end. "
        "Set **institution_code** (or **regulatory_institution_id**) in system config JSON for the institution field."
    )

    if summary_df.empty:
        st.info("No loans with future principal-not-due at this as-of date.")
        return

    st.markdown("**Summary (regulatory buckets)**")
    disp = summary_df.copy()
    disp.columns = [
        "Bucket",
        "Cash inflows",
        "Cash outflows (maturing liabilities)",
        "Net position",
        "Cumulative position",
    ]
    st.dataframe(disp, hide_index=True, width="stretch", height=420)
    _df_download(
        summary_df,
        f"regulatory_maturity_summary_{as_of.isoformat()}_{view_type}.csv",
        button_key="port_reg_mat_sum_csv",
    )

    if not detail_df.empty and view_type == "principal" and "recon_diff" in detail_df.columns:
        if detail_df["recon_diff"].abs().max() > 0.02:
            st.warning(
                "Some loans have **recon_diff** ≠ 0 (`bucket_sum` vs `principal_not_due`). "
                "Inspect the loan-level export."
            )
    st.markdown("**Loan-level buckets** (export)")
    if not detail_df.empty:
        ren = {k: lbl for k, lbl in zip(REGULATORY_MATURITY_BUCKET_KEYS, REGULATORY_MATURITY_BUCKET_LABELS, strict=True)}
        st.dataframe(detail_df.rename(columns=ren), hide_index=True, width="stretch", height=280)
    _df_download(
        detail_df,
        f"regulatory_maturity_by_loan_{as_of.isoformat()}_{view_type}.csv",
        button_key="port_reg_mat_loan_csv",
    )


def _report_par(
    as_of: date,
    active_only: bool,
    par_threshold_days: int,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Portfolio at risk (PAR)")
    st.caption(
        f"**PAR ({par_threshold_days}+)** = sum of `total_exposure` where `days_overdue` > {par_threshold_days} "
        "÷ sum of `total_exposure` for the same portfolio base. Uses latest daily state per loan."
    )
    status_clause = "AND l.status = 'active'" if active_only else ""
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        SELECT
            l.id AS loan_id,
            lds.days_overdue,
            lds.total_exposure
        FROM loans l
        INNER JOIN LATERAL (
            SELECT days_overdue, total_exposure
            FROM loan_daily_state x
            WHERE x.loan_id = l.id AND x.as_of_date <= %s
            ORDER BY x.as_of_date DESC
            LIMIT 1
        ) lds ON TRUE
        WHERE 1=1 {status_clause}
        {rs_clause}
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=_portfolio_real_dict_cursor()) as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall() or []
    if not rows:
        st.warning("No loans with loan_daily_state on or before this date.")
        return
    df = pd.DataFrame(rows)
    te = pd.to_numeric(df["total_exposure"], errors="coerce").fillna(0.0)
    dpd = pd.to_numeric(df["days_overdue"], errors="coerce").fillna(0).astype(int)
    denom = float(te.sum())
    numer = float(te[dpd > int(par_threshold_days)].sum())
    par_pct = (numer / denom * 100.0) if denom > 0 else 0.0
    c1, c2, c3 = st.columns(3)
    c1.metric("Portfolio outstanding (denominator)", f"{denom:,.2f}")
    c2.metric(f"Exposure past {par_threshold_days} DPD (numerator)", f"{numer:,.2f}")
    c3.metric(f"PAR {par_threshold_days}+ %", f"{par_pct:.4f} %")
    st.dataframe(
        df.assign(in_par_bucket=dpd > int(par_threshold_days)),
        hide_index=True,
        width="stretch",
        height=280,
    )
    _df_download(df, f"par_detail_asof_{as_of.isoformat()}.csv", button_key="port_r22_csv")


def _report_master_listing(
    as_of: date,
    active_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Master loan listing")
    st.caption("Flat snapshot: loan + customer dimensions + latest daily state on or before as-of.")
    status_clause = "AND l.status = 'active'" if active_only else ""
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        SELECT
            l.id AS loan_id,
            l.customer_id,
            COALESCE(ind.name, corp.trading_name, corp.legal_name, '') AS customer_name,
            l.product_code,
            l.loan_type,
            l.scheme,
            l.status AS loan_status,
            l.remodified_in_place,
            l.originated_from_split,
            l.modification_topup_applied,
            l.agent_id,
            ag.name AS agent_name,
            sec.name AS sector_name,
            sub.name AS subsector_name,
            ind.employer_details AS employer_or_scheme_notes,
            lds.as_of_date AS state_as_of,
            lds.principal_not_due,
            lds.principal_arrears,
            lds.interest_accrued_balance,
            lds.interest_arrears_balance,
            COALESCE(lds.default_interest_balance, 0) AS default_interest_balance,
            COALESCE(lds.penalty_interest_balance, 0) AS penalty_interest_balance,
            COALESCE(lds.fees_charges_balance, 0) AS fees_charges_balance,
            COALESCE(lds.total_interest_in_suspense_balance, 0) AS total_interest_in_suspense_balance,
            lds.total_exposure,
            lds.days_overdue
        FROM loans l
        LEFT JOIN customers c ON c.id = l.customer_id
        LEFT JOIN sectors sec ON sec.id = c.sector_id
        LEFT JOIN subsectors sub ON sub.id = c.subsector_id
        LEFT JOIN agents ag ON ag.id = l.agent_id
        LEFT JOIN individuals ind ON ind.customer_id = c.id
        LEFT JOIN corporates corp ON corp.customer_id = c.id
        LEFT JOIN LATERAL (
            SELECT *
            FROM loan_daily_state x
            WHERE x.loan_id = l.id AND x.as_of_date <= %s
            ORDER BY x.as_of_date DESC
            LIMIT 1
        ) lds ON TRUE
        WHERE 1=1 {status_clause}
        {rs_clause}
        ORDER BY l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=_portfolio_real_dict_cursor()) as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall() or []
    df = pd.DataFrame(rows)
    st.metric("Rows", len(df))
    if df.empty:
        st.info("No loans match the filter.")
        return
    st.dataframe(df, hide_index=True, width="stretch", height=400)
    _df_download(df, f"master_loan_listing_{as_of.isoformat()}.csv", button_key="port_r31_csv")


def _report_disbursed(
    period_start: date,
    period_end: date,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Disbursed loans (period)")
    st.caption("Loans with `disbursement_date` in the selected inclusive range (booked activation date).")
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        SELECT
            l.id AS loan_id,
            l.disbursement_date,
            l.created_at,
            l.principal AS original_principal,
            l.annual_rate,
            l.monthly_rate,
            l.term AS tenor_periods,
            l.loan_type,
            l.product_code,
            l.remodified_in_place,
            l.originated_from_split,
            l.modification_topup_applied,
            l.agent_id,
            ag.name AS agent_name,
            sec.name AS sector_name,
            sub.name AS subsector_name
        FROM loans l
        LEFT JOIN agents ag ON ag.id = l.agent_id
        LEFT JOIN customers c ON c.id = l.customer_id
        LEFT JOIN sectors sec ON sec.id = c.sector_id
        LEFT JOIN subsectors sub ON sub.id = c.subsector_id
        WHERE l.disbursement_date IS NOT NULL
          AND l.disbursement_date >= %s
          AND l.disbursement_date <= %s
          {rs_clause}
        ORDER BY l.disbursement_date, l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=_portfolio_real_dict_cursor()) as cur:
            cur.execute(sql, (period_start, period_end))
            rows = cur.fetchall() or []
    df = pd.DataFrame(rows)
    st.metric("Disbursements in range", len(df))
    if df.empty:
        st.info("No disbursements in this period.")
        return
    st.dataframe(df, hide_index=True, width="stretch", height=360)
    _df_download(df, f"disbursed_loans_{period_start}_{period_end}.csv", button_key="port_r32_csv")


def _report_iis_movement(
    period_start: date,
    period_end: date,
    nonzero_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Interest in suspense (IIS) movement")
    st.caption(
        "Compare latest `loan_daily_state` on or before **period start** vs **period end** "
        "(requires EOD rows for both dates)."
    )
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        WITH start_state AS (
            SELECT DISTINCT ON (loan_id)
                loan_id,
                as_of_date AS as_of_start,
                COALESCE(total_interest_in_suspense_balance, 0) AS tis_start,
                COALESCE(regular_interest_in_suspense_balance, 0) AS reg_start
            FROM loan_daily_state
            WHERE as_of_date <= %s
            ORDER BY loan_id, as_of_date DESC
        ),
        end_state AS (
            SELECT DISTINCT ON (loan_id)
                loan_id,
                as_of_date AS as_of_end,
                COALESCE(total_interest_in_suspense_balance, 0) AS tis_end,
                COALESCE(regular_interest_in_suspense_balance, 0) AS reg_end
            FROM loan_daily_state
            WHERE as_of_date <= %s
            ORDER BY loan_id, as_of_date DESC
        )
        SELECT
            s.loan_id,
            s.as_of_start,
            e.as_of_end,
            s.tis_start,
            e.tis_end,
            (e.tis_end - s.tis_start) AS delta_total_iis,
            s.reg_start,
            e.reg_end,
            (e.reg_end - s.reg_start) AS delta_regular_iis
        FROM start_state s
        JOIN end_state e ON e.loan_id = s.loan_id
        JOIN loans l ON l.id = s.loan_id
        WHERE 1=1
        {rs_clause}
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=_portfolio_real_dict_cursor()) as cur:
            cur.execute(sql, (period_start, period_end))
            rows = cur.fetchall() or []
    df = pd.DataFrame(rows)
    if df.empty:
        st.warning("No overlapping loan daily state for these dates.")
        return
    if nonzero_only:
        df = df[df["delta_total_iis"].astype(float).abs() > 1e-9]
    st.metric("Loans compared", len(df))
    _s = df["delta_total_iis"].astype(float)
    df_sorted = df.assign(_abs=_s.abs()).sort_values("_abs", ascending=False).drop(columns="_abs")
    st.dataframe(df_sorted, hide_index=True, width="stretch", height=400)
    _df_download(df, f"iis_movement_{period_start}_{period_end}.csv", button_key="port_r42_csv")


def _report_concentration(
    as_of: date,
    active_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Portfolio concentration")
    st.caption("Total exposure from latest daily state, grouped by dimension.")
    st.markdown("**By product_code**")
    st.dataframe(
        _concentration_query(as_of, active_only, "l.product_code", restructure_scope),
        hide_index=True,
        width="stretch",
    )
    st.markdown("**By sector**")
    st.dataframe(
        _concentration_query(as_of, active_only, "sec.name", restructure_scope),
        hide_index=True,
        width="stretch",
    )
    st.markdown("**By agent**")
    st.dataframe(
        _concentration_query(as_of, active_only, "ag.name", restructure_scope),
        hide_index=True,
        width="stretch",
    )
    st.markdown("**By scheme (loan.scheme)**")
    st.dataframe(
        _concentration_query(as_of, active_only, "l.scheme", restructure_scope),
        hide_index=True,
        width="stretch",
    )


def _concentration_query(
    as_of: date,
    active_only: bool,
    dim_sql: str,
    restructure_scope: frozenset[str] | None,
) -> pd.DataFrame:
    status_clause = "AND l.status = 'active'" if active_only else ""
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        WITH latest AS (
            SELECT DISTINCT ON (loan_id)
                loan_id,
                total_exposure
            FROM loan_daily_state
            WHERE as_of_date <= %s
            ORDER BY loan_id, as_of_date DESC
        )
        SELECT COALESCE(NULLIF(TRIM(CAST({dim_sql} AS TEXT)), ''), '(blank)') AS bucket,
               SUM(COALESCE(latest.total_exposure, 0)) AS sum_exposure,
               COUNT(*) AS loan_count
        FROM loans l
        LEFT JOIN latest ON latest.loan_id = l.id
        LEFT JOIN customers c ON c.id = l.customer_id
        LEFT JOIN sectors sec ON sec.id = c.sector_id
        LEFT JOIN subsectors sub ON sub.id = c.subsector_id
        LEFT JOIN agents ag ON ag.id = l.agent_id
        WHERE 1=1 {status_clause}
        {rs_clause}
        GROUP BY 1
        ORDER BY sum_exposure DESC NULLS LAST
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=_portfolio_real_dict_cursor()) as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall() or []
    return pd.DataFrame(rows)


def _report_ecl_provision(
    as_of: date,
    active_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Expected credit loss (ECL) — provisions view")
    st.caption(
        "Same logic as **IFRS Provisions (single loan)**: haircut on collateral, **provision = unsecured × PD%**. "
        "**PD%** is the **standard provision %** for the loan’s **IFRS grade** (**System configurations → Loan grade scales**). "
        "If no grade matches, **PD%** falls back to the **IFRS PD band** table by DPD."
    )
    try:
        from provisions.config import list_pd_bands, list_security_subtypes, provision_schema_ready
        from grade_scale_config import grade_scale_schema_ready, provision_pct_from_value, resolve_loan_grade
        from provisions.engine import compute_security_provision_breakdown
    except ImportError as e:
        st.error(f"Provisions config unavailable: {e}")
        return

    ok, msg = provision_schema_ready()
    if not ok:
        st.warning(msg)
        st.caption("Run **scripts/run_migration_53.py** to enable collateral + PD tables.")
        return

    bands = list_pd_bands(active_only=True)
    _gs_ok, _gs_msg = grade_scale_schema_ready()
    if not _gs_ok:
        st.warning(f"Loan grade scales: {_gs_msg}")
    if not bands and not _gs_ok:
        st.error(
            "Configure **Loan grade scales** (standard DPD + provision %) and/or **IFRS provision config → PD bands**."
        )
        return

    subtypes = list_security_subtypes(active_only=False)
    sub_by_id = {int(r["id"]): r for r in subtypes}

    status_clause = "AND l.status = 'active'" if active_only else ""
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        SELECT
            l.id AS loan_id,
            l.collateral_security_subtype_id,
            l.collateral_charge_amount,
            l.collateral_valuation_amount,
            lds.days_overdue,
            lds.total_exposure,
            COALESCE(lds.total_interest_in_suspense_balance, 0) AS total_interest_in_suspense_balance
        FROM loans l
        INNER JOIN LATERAL (
            SELECT days_overdue, total_exposure, total_interest_in_suspense_balance
            FROM loan_daily_state x
            WHERE x.loan_id = l.id AND x.as_of_date <= %s
            ORDER BY x.as_of_date DESC
            LIMIT 1
        ) lds ON TRUE
        WHERE 1=1 {status_clause}
        {rs_clause}
        ORDER BY l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=_portfolio_real_dict_cursor()) as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall() or []

    out: list[dict] = []
    for r in rows:
        sid = r.get("collateral_security_subtype_id")
        stype = sub_by_id.get(int(sid)) if sid is not None else None
        hc = Decimal(str(stype.get("typical_haircut_pct") or 0)) if stype else Decimal(0)
        ch = r.get("collateral_charge_amount")
        va = r.get("collateral_valuation_amount")
        ch_d = Decimal(str(ch)) if ch is not None else Decimal(0)
        va_d = Decimal(str(va)) if va is not None else Decimal(0)
        dpd_i = int(r.get("days_overdue") or 0)
        sg = resolve_loan_grade(dpd_i, scale="standard") if _gs_ok else None
        pd_ov = None
        pd_lbl = None
        if sg:
            pd_ov = provision_pct_from_value(sg.get("standard_provision_pct"))
            pd_lbl = f"IFRS grade · {str(sg.get('grade_name') or '—')}"
        br = compute_security_provision_breakdown(
            dpd=dpd_i,
            total_balance=Decimal(str(r.get("total_exposure") or 0)),
            interest_in_suspense=Decimal(str(r.get("total_interest_in_suspense_balance") or 0)),
            charge=ch_d,
            valuation=va_d,
            haircut_pct=hc,
            pd_bands=bands,
            pd_rate_pct_override=pd_ov,
            pd_status_label_override=pd_lbl,
        )
        out.append(
            {
                "loan_id": r["loan_id"],
                "days_overdue": dpd_i,
                "ifrs_grade": (sg or {}).get("grade_name") or "—",
                "ifrs_performance_status": (sg or {}).get("performance_status") or "—",
                "pd_source": br["status_label"],
                "pd_rate_pct": float(br["pd_rate_pct"]),
                "total_exposure": float(r.get("total_exposure") or 0),
                "interest_in_suspense": float(r.get("total_interest_in_suspense_balance") or 0),
                "collateral_after_haircut": float(br["collateral_value"]),
                "unsecured_exposure": float(br["unsecured_exposure"]),
                "provision_amount": float(br["provision"]),
            }
        )

    df = pd.DataFrame(out)
    st.metric("Loans in scope", len(df))
    if df.empty:
        st.info("No loans with daily state in scope.")
        return
    total_prov = float(df["provision_amount"].sum())
    st.metric("Sum of provision (IFRS)", f"{total_prov:,.2f}")
    st.dataframe(df, hide_index=True, width="stretch", height=400)
    _df_download(df, f"ecl_provision_ifrs_{as_of.isoformat()}.csv", button_key="port_r52_csv")


def _report_regulatory_classification(
    as_of: date,
    active_only: bool,
    restructure_scope: frozenset[str] | None,
) -> None:
    render_sub_sub_header("Loan classification (regulatory scale)")
    st.caption(
        "Each loan is classified using **System configurations → Loan grade scales → Regulatory** DPD bands. "
        "Exposure = `loan_daily_state.total_exposure` (latest on or before as-of). "
        "**Supervisory provision** = exposure × **regulatory provision %** per grade (also set under Loan grade scales). "
        "This does not change IFRS PD-based provision math."
    )
    try:
        from grade_scale_config import (
            grade_scale_schema_ready,
            list_loan_grade_scale_rules,
            provision_pct_from_value,
            resolve_loan_grade,
        )
    except ImportError as e:
        st.error(f"Grade scale config unavailable: {e}")
        return

    ok, msg = grade_scale_schema_ready()
    if not ok:
        st.warning(msg)
        st.caption("Run **scripts/run_migration_63.py** to create the table.")
        return
    if not list_loan_grade_scale_rules(active_only=True):
        st.error("No active loan grade rules — configure under **System configurations → Loan grade scales**.")
        return

    status_clause = "AND l.status = 'active'" if active_only else ""
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        SELECT
            l.id AS loan_id,
            lds.days_overdue,
            lds.total_exposure
        FROM loans l
        INNER JOIN LATERAL (
            SELECT days_overdue, total_exposure
            FROM loan_daily_state x
            WHERE x.loan_id = l.id AND x.as_of_date <= %s
            ORDER BY x.as_of_date DESC
            LIMIT 1
        ) lds ON TRUE
        WHERE 1=1 {status_clause}
        {rs_clause}
        ORDER BY l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=_portfolio_real_dict_cursor()) as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall() or []

    out: list[dict] = []
    for r in rows:
        dpd_i = int(r.get("days_overdue") or 0)
        rg = resolve_loan_grade(dpd_i, scale="regulatory")
        exp = float(r.get("total_exposure") or 0)
        exp_d = as_10dp(Decimal(str(r.get("total_exposure") or 0)))
        r_pct_d = provision_pct_from_value((rg or {}).get("regulatory_provision_pct"))
        prov_sup = as_10dp(exp_d * r_pct_d / Decimal(100))
        out.append(
            {
                "loan_id": r["loan_id"],
                "days_overdue": dpd_i,
                "regulatory_grade": (rg or {}).get("grade_name") or "—",
                "performance_status": (rg or {}).get("performance_status") or "—",
                "regulatory_provision_pct": float(r_pct_d),
                "supervisory_provision_on_exposure": float(prov_sup),
                "total_exposure": exp,
            }
        )

    df = pd.DataFrame(out)
    st.metric("Loans in scope", len(df))
    if df.empty:
        st.info("No loans with daily state in scope.")
        return

    grp = (
        df.groupby(["regulatory_grade", "performance_status"], dropna=False)
        .agg(
            loan_count=("loan_id", "count"),
            total_exposure=("total_exposure", "sum"),
            supervisory_provision_on_exposure=("supervisory_provision_on_exposure", "sum"),
        )
        .reset_index()
        .sort_values(["regulatory_grade", "performance_status"])
    )
    tot_exp = float(grp["total_exposure"].sum())
    tot_n = int(grp["loan_count"].sum())
    tot_sup = float(df["supervisory_provision_on_exposure"].sum())
    grp = pd.concat(
        [
            grp,
            pd.DataFrame(
                [
                    {
                        "regulatory_grade": "TOTAL",
                        "performance_status": "",
                        "loan_count": tot_n,
                        "total_exposure": tot_exp,
                        "supervisory_provision_on_exposure": tot_sup,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    st.markdown("**Summary by grade**")
    st.dataframe(grp, hide_index=True, width="stretch", height=260)
    st.markdown("**Loan detail**")
    st.dataframe(df, hide_index=True, width="stretch", height=320)
    _df_download(df, f"loan_classification_regulatory_{as_of.isoformat()}.csv", button_key="port_r53_csv")


def _render_portfolio_data_export_block() -> None:
    st.divider()
    render_sub_sub_header("Data Export")
    if st.session_state.pop(_PORTFOLIO_CLEAR_EXPORT_LOAN_ID_NEXT_RUN, False):
        prev = int(st.session_state.get(_EXPORT_LOAN_ID_INPUT_NONCE_KEY, 0))
        st.session_state[_EXPORT_LOAN_ID_INPUT_NONCE_KEY] = prev + 1
        st.session_state.pop(f"export_loan_id_text_{prev}", None)
    _loan_id_nonce = int(st.session_state.get(_EXPORT_LOAN_ID_INPUT_NONCE_KEY, 0))
    _loan_id_widget_key = f"export_loan_id_text_{_loan_id_nonce}"
    _export_ok_msg = st.session_state.pop("portfolio_export_success_detail", None)
    if _export_ok_msg:
        st.success(_export_ok_msg)
        _log = st.session_state.get("portfolio_export_last_stdout")
        if _log is not None:
            with st.expander("View export log"):
                st.code(_log or "(no stdout)")
    st.caption(
        "Export low-level database tables to CSV in the folder you set below (default: project farndacred_exports/). "
        "Optional filters narrow rows at the database (product and/or loan). "
        "Large exports auto-create a ZIP when total CSV size exceeds the threshold "
        "(FARNDACRED_EXPORT_ZIP_MIN_BYTES, default 12 MiB). "
        "Streamlit has no folder picker — paste a full path from Explorer."
    )

    product_labels: list[str] = ["All"]
    product_codes: list[str | None] = [None]
    try:
        from loan_management.product_catalog import list_products

        for p in list_products(active_only=False):
            code = (p.get("code") or "").strip()
            if not code:
                continue
            name = (p.get("name") or "").strip() or code
            product_labels.append(f"{code} — {name}")
            product_codes.append(code)
    except Exception:
        pass

    ex_c1, ex_c2, ex_c3, ex_c4 = st.columns([1, 1, 1.1, 1.1], vertical_alignment="bottom")
    with ex_c1:
        exp_start = st.date_input(
            "Start Date", value=date(date.today().year, 1, 1), key="export_start"
        )
    with ex_c2:
        exp_end = st.date_input("End Date", value=date.today(), key="export_end")
    with ex_c3:
        pick_i = st.selectbox(
            "Product",
            options=list(range(len(product_labels))),
            format_func=lambda i: product_labels[i],
            key="export_product_idx",
        )
    with ex_c4:
        loan_id_raw = st.text_input(
            "Loan ID",
            value="",
            placeholder="All loans",
            key=_loan_id_widget_key,
            help="Leave empty for all loans, or enter a numeric loan id.",
        )
    if _EXPORT_OUTPUT_DIR_WIDGET_KEY not in st.session_state:
        st.session_state[_EXPORT_OUTPUT_DIR_WIDGET_KEY] = os.path.abspath(
            os.path.join(_project_root_for_export(), "farndacred_exports")
        )
    out_col, btn_col = st.columns([3.2, 0.9], vertical_alignment="bottom")
    with out_col:
        export_out_raw = st.text_input(
            "Output folder",
            key=_EXPORT_OUTPUT_DIR_WIDGET_KEY,
            help="Absolute path or ~ ; created if missing. Override with env FARNDACRED_EXPORT_DIR when using CLI only.",
        )
    with btn_col:
        run_export = st.button("Run Data Export", type="primary", key="portfolio_run_data_export")

    if run_export:
        import subprocess
        import sys

        exp_product = product_codes[int(pick_i)] if pick_i < len(product_codes) else None
        loan_id_arg: int | None = None
        lid_s = (loan_id_raw or "").strip()
        loan_id_invalid = False
        if lid_s:
            try:
                loan_id_arg = int(lid_s)
                if loan_id_arg <= 0:
                    st.error("Loan ID must be a positive integer.")
                    loan_id_invalid = True
            except ValueError:
                st.error("Loan ID must be a whole number.")
                loan_id_invalid = True

        out_s = (export_out_raw or "").strip()
        out_bad = False
        if "\n" in out_s or "\r" in out_s:
            st.error("Output folder must be a single-line path.")
            out_bad = True
        elif not out_s:
            st.error("Output folder cannot be empty.")
            out_bad = True
        else:
            export_output_abs = os.path.abspath(os.path.expandvars(os.path.expanduser(out_s)))

        if loan_id_invalid:
            pass
        elif out_bad:
            pass
        elif exp_start > exp_end:
            st.error("Start Date must be on or before End Date.")
        else:
            cmd = [
                sys.executable,
                "scripts/export_loan_tables.py",
                "--start-date",
                exp_start.strftime("%Y-%m-%d"),
                "--end-date",
                exp_end.strftime("%Y-%m-%d"),
                "--output-dir",
                export_output_abs,
            ]
            if exp_product:
                cmd.extend(["--product-code", exp_product])
            if loan_id_arg is not None:
                cmd.extend(["--loan-id", str(loan_id_arg)])

            with st.spinner("Running export script…"):
                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        cwd=_project_root_for_export(),
                        check=True,
                    )
                    scope_bits: list[str] = []
                    if loan_id_arg is not None:
                        scope_bits.append(f"loan **{loan_id_arg}**")
                    if exp_product:
                        scope_bits.append(f"product **{exp_product}**")
                    scope_txt = (
                        "Confirmed: export completed for " + ", ".join(scope_bits)
                        if scope_bits
                        else "Confirmed: export completed (all loans, all products)"
                    )
                    detail = (
                        f"{scope_txt} for **{exp_start}** – **{exp_end}**. "
                        f"Output folder: `{export_output_abs}` (see script log in expander below)."
                    )
                    st.session_state["portfolio_export_success_detail"] = detail
                    st.session_state[_PORTFOLIO_CLEAR_EXPORT_LOAN_ID_NEXT_RUN] = True
                    st.session_state["portfolio_export_last_stdout"] = result.stdout or ""
                    st.rerun()
                except subprocess.CalledProcessError as e:
                    st.error("Export failed.")
                    _err_txt = (e.stderr or e.stdout or "").lower()
                    if "disk full" in _err_txt or "no space" in _err_txt:
                        st.warning(
                            "Output disk may be full. Free space on the drive for your **Output folder**, "
                            "delete old exports/ZIPs there, choose another folder, or narrow dates and filters."
                        )
                    with st.expander("View error"):
                        st.code(e.stderr or e.stdout or str(e))
                except Exception as e:
                    st.error(f"Error executing script: {e}")


def render_portfolio_reports_ui() -> None:
    try:
        from loan_management.schema_ddl import _ensure_loans_schema_for_save_loan

        with _connection() as conn:
            _ensure_loans_schema_for_save_loan(conn)
    except Exception:
        pass

    try:
        from rbac.subfeature_access import portfolio_can_data_exports, portfolio_can_view_reports
    except Exception:

        def portfolio_can_view_reports(user=None) -> bool:  # type: ignore[misc]
            return True

        def portfolio_can_data_exports(user=None) -> bool:  # type: ignore[misc]
            return True

    groups_visible = [
        g
        for g in _PORTFOLIO_GROUPS
        if (g[1] == "g_export" and portfolio_can_data_exports())
        or (g[1] != "g_export" and portfolio_can_view_reports())
    ]
    if not groups_visible:
        st.warning("No portfolio report areas are enabled for your role.")
        return
    _gid_to_reports_live: dict[str, list[tuple[str, str]]] = {g[1]: g[2] for g in groups_visible}

    st.session_state.setdefault("portfolio_nav_group", groups_visible[0][1])
    st.session_state.setdefault("portfolio_sel_rk", "")

    _legacy_pick = st.session_state.pop("portfolio_report_pick", None)
    if _legacy_pick and _legacy_pick != _PORTFOLIO_REPORT_PLACEHOLDER:
        _mrk = _LEGACY_PORTFOLIO_LABEL_TO_RK.get(_legacy_pick)
        if _mrk:
            st.session_state["portfolio_nav_group"] = _portfolio_group_id_for_rk(_mrk)
            st.session_state["portfolio_sel_rk"] = _mrk

    def _on_portfolio_group_change() -> None:
        st.session_state["portfolio_sel_rk"] = ""

    if st.session_state.pop(_PORTFOLIO_CLOSE_NEXT_RUN_KEY, False):
        st.session_state["portfolio_sel_rk"] = ""

    _group_id = st.session_state["portfolio_nav_group"]
    if _group_id not in _gid_to_reports_live:
        _group_id = groups_visible[0][1]
        st.session_state["portfolio_nav_group"] = _group_id
    _items_cur = _gid_to_reports_live.get(_group_id, [])
    _valid_rks = {b for _a, b in _items_cur}
    _sr = (st.session_state.get("portfolio_sel_rk") or "").strip()
    if _group_id == "g_export" and _sr:
        st.session_state["portfolio_sel_rk"] = ""
        _sr = ""
    elif _sr and _sr not in _valid_rks:
        st.session_state["portfolio_sel_rk"] = ""
        _sr = ""

    _gopts = [g[1] for g in groups_visible]
    _gfmt = {g[1]: g[0] for g in groups_visible}
    st.caption("Report area")
    st.radio(
        "Report area",
        options=_gopts,
        format_func=lambda x: _gfmt[x],
        key="portfolio_nav_group",
        horizontal=True,
        label_visibility="collapsed",
        on_change=_on_portfolio_group_change,
    )

    _group_id = st.session_state["portfolio_nav_group"]
    _items_cur = _gid_to_reports_live.get(_group_id, [])

    as_of_default = _default_as_of()
    as_of = as_of_default
    active_only = True
    par_d = 30

    c1, c2, c3, c4 = st.columns([2, 1, 1, 1], gap="xxsmall", vertical_alignment="top")
    with c1:
        if _group_id == "g_export":
            st.caption("Data export")
            st.caption("Bulk table export — panel below.")
        else:
            st.caption("Report")
            _rk_opts = [""] + [b for _a, b in _items_cur]
            _rk_lbl = {b: a for a, b in _items_cur}
            st.selectbox(
                "Report",
                options=_rk_opts,
                format_func=lambda x: "— Select report —" if not x else _rk_lbl.get(x, x),
                key="portfolio_sel_rk",
                label_visibility="collapsed",
            )

    if _group_id == "g_export":
        rk = None
        report_open = False
    else:
        _sr2 = (st.session_state.get("portfolio_sel_rk") or "").strip()
        rk = _sr2 if _sr2 else None
        report_open = bool(rk)

    _no_snap = rk in ("ifrs", "r32", "r42", "shell_12", "shell_23", "shell_41") if rk else True
    with c2:
        if report_open and not _no_snap:
            st.caption("As-of")
            as_of = st.date_input(
                "As-of",
                value=as_of_default,
                key="port_rep_as_of",
                label_visibility="collapsed",
            )
    with c3:
        if report_open and not _no_snap:
            st.caption("Scope")
            active_only = st.checkbox(
                "Active loans only",
                value=True,
                key="port_rep_active",
                help="When on, only loans with status active are included.",
            )
    with c4:
        if rk == "r22":
            st.caption("PAR > DPD")
            par_d = st.number_input(
                "PAR DPD",
                min_value=0,
                value=30,
                step=1,
                key="par_thresh",
                label_visibility="collapsed",
                help="Exposure in numerator when days_overdue is strictly greater than this.",
            )

    restructure_scope: frozenset[str] | None = None
    if report_open and rk in _REPORT_KEYS_WITH_RESTRUCTURE_FILTER:
        _lab_list = [t[0] for t in _RESTRUCTURE_TAG_LABELS]
        _lab_to_tag = dict(_RESTRUCTURE_TAG_LABELS)
        st.caption(
            "Restructure tags — loans matching **any** selected tag; leave empty for the full portfolio."
        )
        _picked = st.multiselect(
            "Restructure filter",
            options=_lab_list,
            key="portfolio_restructure_filter_tags",
            label_visibility="collapsed",
        )
        restructure_scope = frozenset(_lab_to_tag[x] for x in _picked) if _picked else None

    if report_open:
        bar_l, bar_r = st.columns([6, 1], gap="small", vertical_alignment="center")
        with bar_l:
            st.caption("Close report to return to the portfolio home screen.")
        with bar_r:
            if st.button("✕", key="portfolio_close_report", help="Close report"):
                st.session_state[_PORTFOLIO_CLOSE_NEXT_RUN_KEY] = True
                st.rerun()

    if not report_open and _group_id != "g_export":
        st.info("Select a report from the dropdown above to view it.")
    elif rk == "ifrs":
        from provisions.ui import render_ifrs_provision_calculator

        render_ifrs_provision_calculator()
    elif rk == "mat_11":
        _report_debtor_maturity(as_of, active_only, restructure_scope)
    elif rk == "reg_mat":
        _report_regulatory_maturity_profile(as_of, active_only, restructure_scope)
    elif rk == "shell_12":
        _report_creditor_maturity_profile(as_of, active_only)
    elif rk == "treas_cred_arrears":
        _report_creditor_arrears_aging(as_of, active_only)
    elif rk == "shell_gap":
        _report_gap_analysis(as_of, active_only, restructure_scope)
    elif rk == "shell_23":
        _shell_report(
            "Migration analysis (roll rates)",
            [
                "Compare **`loan_daily_state`** (or DPD buckets) at **two** as-of dates (e.g. month start vs end).",
                "Matrix: transitions between delinquency buckets (e.g. current → 30, 30 → 60).",
            ],
        )
    elif rk == "shell_41":
        _shell_report(
            "Collection efficiency",
            [
                "Ratio: **actual receipts** (principal + interest in period) ÷ **scheduled due** from **`schedule_lines`** for the same window.",
                "Must align **`loan_repayments`** / allocation with schedule periods (incl. reschedules).",
            ],
        )
    elif rk == "r21":
        _report_arrears_aging(as_of, active_only, restructure_scope)
    elif rk == "r22":
        _report_par(as_of, active_only, par_d, restructure_scope)
    elif rk == "r31":
        _report_master_listing(as_of, active_only, restructure_scope)
    elif rk == "r32":
        p1, p2, _, _ = st.columns(4, gap="xxsmall", vertical_alignment="top")
        with p1:
            st.caption("Period from")
            p0 = st.date_input(
                "From",
                value=as_of_default.replace(day=1),
                key="disb_start",
                label_visibility="collapsed",
            )
        with p2:
            st.caption("Period to")
            p1 = st.date_input(
                "To",
                value=as_of_default,
                key="disb_end",
                label_visibility="collapsed",
            )
        _report_disbursed(p0, p1, restructure_scope)
    elif rk == "r42":
        p1, p2, p3, _ = st.columns(4, gap="xxsmall", vertical_alignment="top")
        with p1:
            st.caption("IIS from")
            p0 = st.date_input(
                "IIS from",
                value=as_of_default.replace(day=1),
                key="iis_start",
                label_visibility="collapsed",
            )
        with p2:
            st.caption("IIS to")
            p1 = st.date_input(
                "IIS to",
                value=as_of_default,
                key="iis_end",
                label_visibility="collapsed",
            )
        with p3:
            st.caption("Filter")
            nz = st.checkbox("Non-zero Δ only", value=False, key="iis_nz")
        if p1 < p0:
            st.error("Period end must be on or after period start.")
        else:
            _report_iis_movement(p0, p1, nz, restructure_scope)
    elif rk == "r51":
        _report_concentration(as_of, active_only, restructure_scope)
    elif rk == "r52":
        _report_ecl_provision(as_of, active_only, restructure_scope)
    elif rk == "r53":
        _report_regulatory_classification(as_of, active_only, restructure_scope)

    if _group_id == "g_export":
        _render_portfolio_data_export_block()
