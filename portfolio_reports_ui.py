"""
Portfolio reporting: credit risk, listings, IIS movement, concentration, ECL (provisions).
Shell placeholders: creditor mirror, roll rates, collection efficiency.
Debtor maturity + bucketed arrears ageing: see portfolio_reporting.py (read-only).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from io import StringIO

import pandas as pd
import streamlit as st
from psycopg2.extras import RealDictCursor

from loan_management import _connection
from portfolio_reporting import (
    ARREARS_BUCKET_KEYS,
    ARREARS_BUCKET_LABELS,
    MATURITY_BUCKET_KEYS,
    MATURITY_BUCKET_LABELS,
    build_arrears_aging_report,
    build_maturity_profile_report,
)
from provision_engine import compute_security_provision_breakdown

try:
    from system_business_date import get_effective_date
except ImportError:

    def get_effective_date() -> date:
        return date.today()


def _default_as_of() -> date:
    try:
        return get_effective_date()
    except Exception:
        return date.today()


def _df_download(df: pd.DataFrame, filename: str, *, button_key: str) -> None:
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


def _shell_report(title: str, planned: list[str]) -> None:
    st.subheader(title)
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


def _report_arrears_aging(as_of: date, active_only: bool) -> None:
    st.subheader("Debtor loans arrears (aging)")
    try:
        df = build_arrears_aging_report(as_of, active_only=active_only)
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


def _report_debtor_maturity(as_of: date, active_only: bool) -> None:
    st.subheader("Debtor maturity profile")
    st.caption("Principal-Only: Principal not due is spread across future instalments in proportion to scheduled principal. "
               "Full Cash Flow: Scheduled principal + interest are spread across future instalments. "
               "Buckets use days from as-of to due date. Unparseable dates go to Unallocated.")
    
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
        df = build_maturity_profile_report(as_of, active_only=active_only, view_type=view_type)
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


def _report_par(as_of: date, active_only: bool, par_threshold_days: int) -> None:
    st.subheader("Portfolio at risk (PAR)")
    st.caption(
        f"**PAR ({par_threshold_days}+)** = sum of `total_exposure` where `days_overdue` > {par_threshold_days} "
        "÷ sum of `total_exposure` for the same portfolio base. Uses latest daily state per loan."
    )
    status_clause = "AND l.status = 'active'" if active_only else ""
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
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
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


def _report_master_listing(as_of: date, active_only: bool) -> None:
    st.subheader("Master loan listing")
    st.caption("Flat snapshot: loan + customer dimensions + latest daily state on or before as-of.")
    status_clause = "AND l.status = 'active'" if active_only else ""
    sql = f"""
        SELECT
            l.id AS loan_id,
            l.customer_id,
            COALESCE(ind.name, corp.trading_name, corp.legal_name, '') AS customer_name,
            l.product_code,
            l.loan_type,
            l.scheme,
            l.status AS loan_status,
            l.agent_id,
            ag.name AS agent_name,
            sec.name AS sector_name,
            sub.name AS subsector_name,
            ind.employer_details AS employer_or_scheme_notes,
            lds.as_of_date AS state_as_of,
            lds.principal_not_due,
            lds.principal_arrears,
            lds.interest_accrued_balance,
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
        ORDER BY l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall() or []
    df = pd.DataFrame(rows)
    st.metric("Rows", len(df))
    if df.empty:
        st.info("No loans match the filter.")
        return
    st.dataframe(df, hide_index=True, width="stretch", height=400)
    _df_download(df, f"master_loan_listing_{as_of.isoformat()}.csv", button_key="port_r31_csv")


def _report_disbursed(period_start: date, period_end: date) -> None:
    st.subheader("Disbursed loans (period)")
    st.caption("Loans with `disbursement_date` in the selected inclusive range (booked activation date).")
    sql = """
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
        ORDER BY l.disbursement_date, l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (period_start, period_end))
            rows = cur.fetchall() or []
    df = pd.DataFrame(rows)
    st.metric("Disbursements in range", len(df))
    if df.empty:
        st.info("No disbursements in this period.")
        return
    st.dataframe(df, hide_index=True, width="stretch", height=360)
    _df_download(df, f"disbursed_loans_{period_start}_{period_end}.csv", button_key="port_r32_csv")


def _report_iis_movement(period_start: date, period_end: date, nonzero_only: bool) -> None:
    st.subheader("Interest in suspense (IIS) movement")
    st.caption(
        "Compare latest `loan_daily_state` on or before **period start** vs **period end** "
        "(requires EOD rows for both dates)."
    )
    sql = """
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
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
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


def _report_concentration(as_of: date, active_only: bool) -> None:
    st.subheader("Portfolio concentration")
    st.caption("Total exposure from latest daily state, grouped by dimension.")
    st.markdown("**By product_code**")
    st.dataframe(_concentration_query(as_of, active_only, "l.product_code"), hide_index=True, width="stretch")
    st.markdown("**By sector**")
    st.dataframe(_concentration_query(as_of, active_only, "sec.name"), hide_index=True, width="stretch")
    st.markdown("**By agent**")
    st.dataframe(_concentration_query(as_of, active_only, "ag.name"), hide_index=True, width="stretch")
    st.markdown("**By scheme (loan.scheme)**")
    st.dataframe(_concentration_query(as_of, active_only, "l.scheme"), hide_index=True, width="stretch")


def _concentration_query(as_of: date, active_only: bool, dim_sql: str) -> pd.DataFrame:
    status_clause = "AND l.status = 'active'" if active_only else ""
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
        GROUP BY 1
        ORDER BY sum_exposure DESC NULLS LAST
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall() or []
    return pd.DataFrame(rows)


def _report_ecl_provision(as_of: date, active_only: bool) -> None:
    st.subheader("Expected credit loss (ECL) — provisions view")
    st.caption(
        "Same logic as **IFRS Provisions (single loan)** and **System configurations → IFRS provision config**: "
        "PD band from DPD, haircut on collateral, "
        "provision = unsecured × PD%. One row per loan with daily state."
    )
    try:
        from provisions_config import list_pd_bands, list_security_subtypes, provision_schema_ready
    except ImportError as e:
        st.error(f"Provisions config unavailable: {e}")
        return

    ok, msg = provision_schema_ready()
    if not ok:
        st.warning(msg)
        st.caption("Run **scripts/run_migration_53.py** to enable collateral + PD tables.")
        return

    bands = list_pd_bands(active_only=True)
    if not bands:
        st.error("No active PD bands — configure under **System configurations → IFRS provision config**.")
        return

    subtypes = list_security_subtypes(active_only=False)
    sub_by_id = {int(r["id"]): r for r in subtypes}

    status_clause = "AND l.status = 'active'" if active_only else ""
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
        ORDER BY l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
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
        br = compute_security_provision_breakdown(
            dpd=int(r.get("days_overdue") or 0),
            total_balance=Decimal(str(r.get("total_exposure") or 0)),
            interest_in_suspense=Decimal(str(r.get("total_interest_in_suspense_balance") or 0)),
            charge=ch_d,
            valuation=va_d,
            haircut_pct=hc,
            pd_bands=bands,
        )
        out.append(
            {
                "loan_id": r["loan_id"],
                "days_overdue": int(r.get("days_overdue") or 0),
                "status_label": br["status_label"],
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
    st.metric("Sum of provision (IFRS-style)", f"{total_prov:,.2f}")
    st.dataframe(df, hide_index=True, width="stretch", height=400)
    _df_download(df, f"ecl_provision_ifrs_{as_of.isoformat()}.csv", button_key="port_r52_csv")


def render_portfolio_reports_ui() -> None:
    st.markdown(
        "<div style='color:#1D4ED8; font-weight:700; font-size:1.05rem; margin:0.08rem 0 0.25rem 0;'>"
        "Portfolio reports</div>",
        unsafe_allow_html=True,
    )
    report_keys = [
        ("IFRS Provisions (single loan)", "ifrs"),
        ("Debtor maturity profile", "mat_11"),
        ("Creditor maturity profile (shell)", "shell_12"),
        ("Debtor arrears (aging)", "r21"),
        ("Portfolio at risk (PAR)", "r22"),
        ("Migration / roll rates (shell)", "shell_23"),
        ("Master loan listing", "r31"),
        ("Disbursed loans (period)", "r32"),
        ("Collection efficiency (shell)", "shell_41"),
        ("IIS movement", "r42"),
        ("Concentration", "r51"),
        ("ECL / provisions (IFRS view)", "r52"),
    ]
    labels = [x[0] for x in report_keys]
    key_by_label = {x[0]: x[1] for x in report_keys}

    as_of_default = _default_as_of()
    as_of = as_of_default
    active_only = True
    par_d = 30

    c1, c2, c3, c4 = st.columns([2, 1, 1, 1], gap="xxsmall", vertical_alignment="top")
    with c1:
        st.caption("Report")
        choice = st.selectbox(
            "Report",
            labels,
            key="portfolio_report_pick",
            label_visibility="collapsed",
        )
    rk = key_by_label[choice]
    _no_snap = rk in ("ifrs", "r32", "r42", "shell_12", "shell_23", "shell_41")
    with c2:
        if not _no_snap:
            st.caption("As-of")
            as_of = st.date_input(
                "As-of",
                value=as_of_default,
                key="port_rep_as_of",
                label_visibility="collapsed",
            )
    with c3:
        if not _no_snap:
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

    if rk == "ifrs":
        from provisions_ui import render_ifrs_provision_calculator

        render_ifrs_provision_calculator()
    elif rk == "mat_11":
        _report_debtor_maturity(as_of, active_only)
    elif rk == "shell_12":
        _shell_report(
            "Creditor loans maturity profile",
            [
                "Mirror of debtor maturity for **liabilities you owe** — needs an outbound / creditor product model (not in this LMS yet).",
            ],
        )
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
        _report_arrears_aging(as_of, active_only)
    elif rk == "r22":
        _report_par(as_of, active_only, par_d)
    elif rk == "r31":
        _report_master_listing(as_of, active_only)
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
        _report_disbursed(p0, p1)
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
            _report_iis_movement(p0, p1, nz)
    elif rk == "r51":
        _report_concentration(as_of, active_only)
    elif rk == "r52":
        _report_ecl_provision(as_of, active_only)
