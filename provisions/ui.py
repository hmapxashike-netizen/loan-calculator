"""
IFRS provisioning UI pieces:
- Config tables (security subtypes, PD bands): System configurations → IFRS provision config.
- Single-loan calculator: Portfolio reports → IFRS Provisions (single loan).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd
import streamlit as st

from decimal_utils import as_10dp
from display_formatting import format_display_amount
from loan_management import get_loan, get_loan_daily_state_balances
from provisions.engine import compute_security_provision_breakdown

try:
    from eod.system_business_date import get_effective_date
except ImportError:

    def get_effective_date() -> date:
        return date.today()


def _fmt_num(v: Decimal | float | int) -> str:
    return format_display_amount(v)


def _provisions_import_ok() -> tuple[bool, str]:
    try:
        import provisions.config  # noqa: F401

        return True, ""
    except Exception as e:
        return False, str(e)


def _ensure_provisions_schema() -> bool:
    """Return True if schema OK; otherwise show error and return False."""
    ok, err = _provisions_import_ok()
    if not ok:
        st.error(f"Provisions configuration is unavailable ({err}). Run **scripts/run_migration_53.py**.")
        return False

    from provisions.config import provision_schema_ready

    schema_ok, schema_msg = provision_schema_ready()
    if not schema_ok:
        st.error(schema_msg)
        st.caption(
            "This creates `provision_security_subtypes`, `provision_pd_bands`, and collateral columns on `loans` "
            "(see `schema/53_provisions_security_pd_collateral.sql`)."
        )
        return False
    return True


def render_provisions_config_tables() -> None:
    """Security subtypes + PD bands maintenance (System configurations)."""
    if not _ensure_provisions_schema():
        return

    from provisions.config import (
        delete_pd_band_hard,
        delete_security_subtype_hard,
        insert_pd_band,
        insert_security_subtype,
        list_pd_bands,
        list_security_subtypes,
        update_pd_band,
        update_security_subtype,
    )

    st.subheader("Security subtypes & haircuts")
    st.caption("Collateral sub-types and typical haircuts (applied to min(charge, valuation) for IFRS provision).")
    rows = list_security_subtypes(active_only=False)
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch", height=280)
    with st.expander("Add subtype", expanded=False):
        with st.form("prov_add_sec"):
            c1, c2 = st.columns(2)
            with c1:
                st_type = st.text_input("Security type", placeholder="e.g. Immovable")
                sub_nm = st.text_input("Subtype name", placeholder="e.g. Residential Property")
            with c2:
                hc = st.number_input("Typical haircut %", min_value=0.0, max_value=100.0, value=10.0, step=0.01)
                notes = st.text_area("System notes", height=68)
            so = st.number_input("Sort order", min_value=0, value=0, step=1)
            if st.form_submit_button("Add"):
                if st_type.strip() and sub_nm.strip():
                    try:
                        insert_security_subtype(
                            st_type, sub_nm, as_10dp(hc), system_notes=notes or None, sort_order=int(so)
                        )
                        st.success("Added.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
                else:
                    st.error("Security type and subtype name are required.")

    with st.expander("Edit / deactivate / delete subtype", expanded=False):
        if not rows:
            st.caption("No rows yet.")
        else:
            ids = [r["id"] for r in rows]
            pick = st.selectbox(
                "Subtype",
                ids,
                format_func=lambda i: next(f"{r['security_type']} — {r['subtype_name']}" for r in rows if r["id"] == i),
            )
            cur = next(r for r in rows if r["id"] == pick)
            with st.form("prov_edit_sec"):
                e_st = st.text_input("Security type", value=str(cur.get("security_type") or ""))
                e_sub = st.text_input("Subtype name", value=str(cur.get("subtype_name") or ""))
                e_hc = st.number_input(
                    "Haircut %", min_value=0.0, max_value=100.0, value=float(cur.get("typical_haircut_pct") or 0), step=0.01
                )
                e_notes = st.text_area("Notes", value=str(cur.get("system_notes") or ""), height=60)
                e_act = st.checkbox("Active", value=bool(cur.get("is_active", True)))
                e_so = st.number_input("Sort order", value=int(cur.get("sort_order") or 0), step=1)
                c1, c2, c3 = st.columns(3)
                with c1:
                    save_b = st.form_submit_button("Save changes")
                with c2:
                    del_b = st.form_submit_button("Delete row", type="primary")
                if save_b:
                    try:
                        update_security_subtype(
                            int(pick),
                            security_type=e_st,
                            subtype_name=e_sub,
                            typical_haircut_pct=as_10dp(e_hc),
                            system_notes=e_notes or None,
                            is_active=e_act,
                            sort_order=int(e_so),
                        )
                        st.success("Updated.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
                if del_b:
                    try:
                        delete_security_subtype_hard(int(pick))
                        st.success("Deleted.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))

    st.divider()
    st.subheader("Probability of default by DPD band")
    st.caption("Editable **PD %** per status. DPD comes from `loan_daily_state.days_overdue`.")
    bands = list_pd_bands(active_only=False)
    if bands:
        st.dataframe(pd.DataFrame(bands), hide_index=True, width="stretch", height=220)
    with st.expander("Add band", expanded=False):
        with st.form("prov_add_pd"):
            sl = st.text_input("Status label", placeholder="e.g. Standard")
            dmin = st.number_input("DPD min", min_value=0, value=0, step=1)
            dmax_open = st.checkbox("Open-ended upper DPD (e.g. 181+)", value=False)
            dmax_v = st.number_input("DPD max (inclusive)", min_value=0, value=30, step=1, disabled=dmax_open)
            pdr = st.number_input("PD rate %", min_value=0.0, max_value=100.0, value=1.0, step=0.01)
            pso = st.number_input("Sort order", min_value=0, value=0, step=1)
            if st.form_submit_button("Add band"):
                if sl.strip():
                    try:
                        insert_pd_band(
                            sl.strip(),
                            int(dmin),
                            None if dmax_open else int(dmax_v),
                            as_10dp(pdr),
                            sort_order=int(pso),
                        )
                        st.success("Added.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
                else:
                    st.error("Status label is required.")
    with st.expander("Edit / delete band", expanded=False):
        if not bands:
            st.caption("No bands.")
        else:
            bid = st.selectbox(
                "Band",
                [b["id"] for b in bands],
                format_func=lambda i: next(
                    f"{b['status_label']} (DPD {b['dpd_min']}-{(b['dpd_max'] if b['dpd_max'] is not None else '∞')})"
                    for b in bands
                    if b["id"] == i
                ),
            )
            bcur = next(b for b in bands if b["id"] == bid)
            with st.form("prov_edit_pd"):
                esl = st.text_input("Status label", value=str(bcur.get("status_label") or ""))
                edmin = st.number_input("DPD min", value=int(bcur.get("dpd_min") or 0), step=1)
                open_max = bcur.get("dpd_max") is None
                e_open = st.checkbox("Open-ended max", value=open_max)
                edmax = st.number_input("DPD max", value=int(bcur.get("dpd_max") or 180), step=1, disabled=e_open)
                epd = st.number_input("PD %", value=float(bcur.get("pd_rate_pct") or 0), step=0.01)
                eact = st.checkbox("Active", value=bool(bcur.get("is_active", True)))
                eso = st.number_input("Sort order", value=int(bcur.get("sort_order") or 0), step=1)
                c1, c2 = st.columns(2)
                with c1:
                    sb = st.form_submit_button("Save")
                with c2:
                    db = st.form_submit_button("Delete", type="primary")
                if sb:
                    try:
                        update_pd_band(
                            int(bid),
                            status_label=esl,
                            dpd_min=int(edmin),
                            dpd_max=None if e_open else int(edmax),
                            pd_rate_pct=as_10dp(epd),
                            is_active=eact,
                            sort_order=int(eso),
                        )
                        st.success("Saved.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
                if db:
                    try:
                        delete_pd_band_hard(int(bid))
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))


def render_ifrs_provision_calculator() -> None:
    """Single-loan IFRS / security-based provision preview (Portfolio reports)."""
    if not _ensure_provisions_schema():
        return

    from provisions.config import get_security_subtype, list_pd_bands

    try:
        from grade_scale_config import grade_scale_schema_ready, resolve_loan_grade
    except ImportError:
        grade_scale_schema_ready = None
        resolve_loan_grade = None

    try:
        as_of_default = get_effective_date()
    except Exception:
        as_of_default = date.today()

    st.markdown("##### IFRS Provisions (single loan)")
    st.caption(
        "**Total balance** = `loan_daily_state.total_exposure` (latest row on or before as-of date). "
        "**Interest in suspense** = `total_interest_in_suspense_balance`. "
        "**Collateral** = min(charge, valuation) × (1 − haircut%). **Provision** = unsecured × PD%. "
        "**PD%** uses **standard provision %** for the IFRS grade from **System configurations → Loan grade scales** "
        "when that grade applies; otherwise the IFRS **PD band** table by DPD."
    )
    ic1, ic2, ic3 = st.columns([1, 1, 1], gap="xxsmall", vertical_alignment="top")
    with ic1:
        st.caption("Loan ID")
        loan_id_in = st.number_input(
            "Loan ID", min_value=1, step=1, key="ifrs_prv_loan_id", label_visibility="collapsed"
        )
    with ic2:
        st.caption("As-of")
        as_of = st.date_input(
            "As-of", value=as_of_default, key="ifrs_prv_as_of", label_visibility="collapsed"
        )
    with ic3:
        st.caption("Run")
        run_ifrs = st.button("Compute", key="ifrs_prv_compute_btn")
    if run_ifrs:
        loan = get_loan(int(loan_id_in))
        if not loan:
            st.error("Loan not found.")
        else:
            bal = get_loan_daily_state_balances(int(loan_id_in), as_of)
            if not bal:
                st.warning("No `loan_daily_state` row on or before this date — run EOD or pick another date.")
            else:
                bands = list_pd_bands(active_only=True)
                dpd = int(bal.get("days_overdue") or 0)
                total_bal = Decimal(str(bal.get("total_exposure") or 0))
                iis = Decimal(str(bal.get("total_interest_in_suspense_balance") or 0))
                sid = loan.get("collateral_security_subtype_id")
                stype = get_security_subtype(int(sid)) if sid else None
                hc = Decimal(str(stype.get("typical_haircut_pct") or 0)) if stype else Decimal(0)
                charge = loan.get("collateral_charge_amount")
                valn = loan.get("collateral_valuation_amount")
                ch_d = Decimal(str(charge)) if charge is not None else Decimal(0)
                va_d = Decimal(str(valn)) if valn is not None else Decimal(0)

                _ifrs_grade = "—"
                _ifrs_perf = "—"
                _pd_override = None
                _pd_lbl_ov = None
                if grade_scale_schema_ready and resolve_loan_grade:
                    _gok, _ = grade_scale_schema_ready()
                    if _gok:
                        from grade_scale_config import provision_pct_from_value

                        _sg = resolve_loan_grade(dpd, scale="standard")
                        if _sg:
                            _ifrs_grade = str(_sg.get("grade_name") or "—")
                            _ifrs_perf = str(_sg.get("performance_status") or "—")
                            _pd_override = provision_pct_from_value(_sg.get("standard_provision_pct"))
                            _pd_lbl_ov = f"IFRS grade · {_ifrs_grade}"
                if _pd_override is None and not bands:
                    st.error(
                        "No **IFRS grade** match and no active **PD bands** — configure **Loan grade scales** "
                        "(standard DPD + provision %) and/or **IFRS provision config → PD bands**."
                    )
                else:
                    br = compute_security_provision_breakdown(
                        dpd=dpd,
                        total_balance=total_bal,
                        interest_in_suspense=iis,
                        charge=ch_d,
                        valuation=va_d,
                        haircut_pct=hc,
                        pd_bands=bands,
                        pd_rate_pct_override=_pd_override,
                        pd_status_label_override=_pd_lbl_ov,
                    )
                    st.markdown("**Inputs** (loan + daily state)")
                    subtype_lbl = (
                        f"{stype['security_type']} — {stype['subtype_name']}" if stype else "(not set on loan)"
                    )
                    inputs_df = pd.DataFrame(
                        [
                            ("DPD (days overdue)", str(dpd)),
                            ("IFRS grade (standard scale)", _ifrs_grade),
                            ("IFRS performance status", _ifrs_perf),
                            ("PD source", str(br["status_label"])),
                            ("PD % (applied)", _fmt_num(br["pd_rate_pct"])),
                            ("Total balance (total_exposure)", _fmt_num(total_bal)),
                            ("Interest in suspense", _fmt_num(iis)),
                            ("Collateral subtype", subtype_lbl),
                            ("Haircut %", _fmt_num(hc)),
                            ("Charge amount", _fmt_num(ch_d)),
                            ("Valuation amount", _fmt_num(va_d)),
                        ],
                        columns=["Field", "Value"],
                    )
                    st.dataframe(inputs_df, hide_index=True, width="stretch", height=340)

                    st.markdown("**Results**")
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Collateral (after haircut)", _fmt_num(br["collateral_value"]))
                    m2.metric("Unsecured exposure", _fmt_num(br["unsecured_exposure"]))
                    m3.metric("Provision", _fmt_num(br["provision"]))

