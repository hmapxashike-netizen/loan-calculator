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

from style import render_main_header, render_sub_header, render_sub_sub_header

from psycopg2.extras import RealDictCursor

from decimal_utils import as_10dp
from grade_scale_config import provision_pct_from_value
from loan_management import _connection
from reporting.portfolio_reporting import (
    ARREARS_BUCKET_KEYS,
    ARREARS_BUCKET_LABELS,
    MATURITY_BUCKET_KEYS,
    MATURITY_BUCKET_LABELS,
    REGULATORY_MATURITY_BUCKET_KEYS,
    REGULATORY_MATURITY_BUCKET_LABELS,
    build_arrears_aging_report,
    build_maturity_profile_report,
    build_regulatory_maturity_profile_report,
    build_regulatory_maturity_summary_table,
)
from provisions.engine import compute_security_provision_breakdown

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
    if not st.session_state.get("portfolio_exports_visible", False):
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


def _report_arrears_aging(as_of: date, active_only: bool) -> None:
    render_sub_sub_header("Debtor loans arrears (aging)")
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


def _report_regulatory_maturity_profile(as_of: date, active_only: bool) -> None:
    render_sub_sub_header("Regulatory maturity profile")
    st.caption(
        "Debtor **cash inflows** are the same engine as **Debtor maturity profile**: `principal_not_due` from "
        "latest `loan_daily_state` on/before as-of, spread across **future** instalments by scheduled principal "
        "(or principal + interest in cash-flow view). Amounts are **re-bucketed** into regulatory bands "
        "(0–7, 8–14, 15–30, …, 360+). **Cash outflows (maturing liabilities)** are **0** until creditor data is wired."
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
            as_of, active_only=active_only, view_type=view_type
        )
        detail_df = build_regulatory_maturity_profile_report(
            as_of, active_only=active_only, view_type=view_type
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


def _report_par(as_of: date, active_only: bool, par_threshold_days: int) -> None:
    render_sub_sub_header("Portfolio at risk (PAR)")
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
    render_sub_sub_header("Master loan listing")
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
    render_sub_sub_header("Disbursed loans (period)")
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
    render_sub_sub_header("Interest in suspense (IIS) movement")
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
    render_sub_sub_header("Portfolio concentration")
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
    render_sub_sub_header("Expected credit loss (ECL) — provisions view")
    st.caption(
        "Same logic as **IFRS Provisions (single loan)**: haircut on collateral, **provision = unsecured × PD%**. "
        "**PD%** is the **standard provision %** for the loan’s **IFRS grade** (**System configurations → Loan grade scales**). "
        "If no grade matches, **PD%** falls back to the **IFRS PD band** table by DPD."
    )
    try:
        from provisions.config import list_pd_bands, list_security_subtypes, provision_schema_ready
        from grade_scale_config import grade_scale_schema_ready, resolve_loan_grade
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


def _report_regulatory_classification(as_of: date, active_only: bool) -> None:
    render_sub_sub_header("Loan classification (regulatory scale)")
    st.caption(
        "Each loan is classified using **System configurations → Loan grade scales → Regulatory** DPD bands. "
        "Exposure = `loan_daily_state.total_exposure` (latest on or before as-of). "
        "**Supervisory provision** = exposure × **regulatory provision %** per grade (also set under Loan grade scales). "
        "This does not change IFRS PD-based provision math."
    )
    try:
        from grade_scale_config import grade_scale_schema_ready, list_loan_grade_scale_rules, resolve_loan_grade
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
        ORDER BY l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
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


def render_portfolio_reports_ui() -> None:
    st.session_state.setdefault("portfolio_exports_visible", False)
    report_keys = [
        ("IFRS Provisions (single loan)", "ifrs"),
        ("Debtor maturity profile", "mat_11"),
        ("Regulatory maturity profile", "reg_mat"),
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
        ("Loan classification (regulatory)", "r53"),
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

    _ex_vis = st.session_state.get("portfolio_exports_visible", False)
    if st.button(
        "Hide exports" if _ex_vis else "Data export",
        key="portfolio_exports_toggle",
        help="Show or hide CSV downloads for reports and the bulk data export panel at the bottom.",
    ):
        st.session_state["portfolio_exports_visible"] = not _ex_vis

    if rk == "ifrs":
        from provisions.ui import render_ifrs_provision_calculator

        render_ifrs_provision_calculator()
    elif rk == "mat_11":
        _report_debtor_maturity(as_of, active_only)
    elif rk == "reg_mat":
        _report_regulatory_maturity_profile(as_of, active_only)
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
    elif rk == "r53":
        _report_regulatory_classification(as_of, active_only)

    if st.session_state.get("portfolio_exports_visible", False):
        st.divider()
        render_sub_sub_header("Data Export")
        st.caption("Export low-level database tables to CSV for external analysis or auditing.")

        ex_c1, ex_c2, ex_c3 = st.columns([1, 1, 2], vertical_alignment="bottom")
        with ex_c1:
            exp_start = st.date_input("Start Date", value=date(date.today().year, 1, 1), key="export_start")
        with ex_c2:
            exp_end = st.date_input("End Date", value=date.today(), key="export_end")
        with ex_c3:
            if st.button("Run Data Export Script", type="primary", key="btn_run_export"):
                import subprocess
                import sys

                with st.spinner("Running export script..."):
                    try:
                        result = subprocess.run(
                            [
                                sys.executable,
                                "scripts/export_loan_tables.py",
                                "--start-date",
                                exp_start.strftime("%Y-%m-%d"),
                                "--end-date",
                                exp_end.strftime("%Y-%m-%d"),
                            ],
                            capture_output=True,
                            text=True,
                            check=True,
                        )
                        st.success("Export successful!")
                        with st.expander("View Output"):
                            st.code(result.stdout)
                    except subprocess.CalledProcessError as e:
                        st.error("Export failed!")
                        with st.expander("View Error"):
                            st.code(e.stderr or e.stdout)
                    except Exception as e:
                        st.error(f"Error executing script: {e}")
