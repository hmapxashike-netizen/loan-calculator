import re
from html import escape as html_escape
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from display_formatting import (
    build_dataframe_money_column_config,
    format_display_amount,
    resolve_display_format,
)
from io import BytesIO
import numpy_financial as npf

from accounting.core import (
    MappingRegistry,
    coa_grandchild_prefix_matches_immediate_parent,
)

from loans import (
    add_months,
    days_in_month,
    is_last_day_of_month,
    format_schedule_display,
    repayment_dates,
    get_amortization_schedule,
    get_term_loan_amortization_schedule,
    get_bullet_schedule,
    recompute_customised_from_payments,
    parse_schedule_dates_from_table,
    schedule_dataframe_to_csv_bytes,
    schedule_dataframe_to_excel_bytes,
)

from core.config_manager import (
    ensure_core_session_state,
    get_fx_rates as _core_get_fx_rates,
    get_global_loan_settings as _core_get_global_loan_settings,
    get_mapping_registry as _core_get_mapping_registry,
    get_system_config as _core_get_system_config,
)
from utils.formatters import parse_display_substrings_csv as _parse_display_substrings_csv
from utils.rates import pct_to_monthly as _pct_to_monthly
from style import (
    format_navigation_label,
    inject_farnda_global_styles_once,
    render_main_page_title,
    render_sub_header,
)
from ui.components import inject_tertiary_hyperlink_css_once
from ui.system_configurations import render_system_configurations_ui

try:
    from customers.core import (
        create_individual,
        create_corporate,
        create_corporate_with_entities,
        list_customers,
        get_customer,
        update_individual,
        update_corporate,
        set_active,
        get_display_name,
        list_addresses,
        add_address,
        list_sectors,
        list_subsectors,
        create_sector,
        create_subsector,
        update_customer_sector,
    )
    _customers_available = True
    _customers_error = ""
except Exception as e:
    _customers_available = False
    _customers_error = str(e)

try:
    from agents import list_agents, get_agent, create_agent, update_agent
    _agents_available = True
    _agents_error = ""
except Exception as e:
    _agents_available = False
    _agents_error = str(e)

    def list_agents(*args, **kwargs):
        return []

    def get_agent(*_args, **_kwargs):
        return None

    def create_agent(**_kwargs):
        raise RuntimeError(f"Agents module is not available. ({_agents_error})")

    def update_agent(*_args, **_kwargs):
        raise RuntimeError(f"Agents module is not available. ({_agents_error})")

try:
    from dal import list_users_for_selection
    _users_for_rm_available = True
except Exception:
    _users_for_rm_available = False
    list_users_for_selection = lambda: []

try:
    from customers.documents import (
        list_document_classes,
        create_document_class,
        update_document_class,
        list_document_categories,
        create_document_category,
        update_document_category,
        upload_document,
        list_documents,
        get_document,
        delete_document,
    )
    _documents_available = True
    _documents_error = ""
except Exception as e:
    _documents_available = False
    _documents_error = str(e)

_loan_management_error: str = ""
try:
    from loan_management import (
        get_loan,
        get_loans_by_customer,
        get_amount_due_summary,
        get_schedule_lines,
        apply_unapplied_funds_recast,
        load_system_config_from_db,
        get_loan_daily_state_balances,
        list_products,
        get_product,
        get_product_by_code,
        create_product,
        update_product,
        delete_product,
        get_product_config_from_db,
        save_product_config_to_db,
        save_loan_approval_draft,
        save_loan,
        update_loan_approval_draft_staged,
        resubmit_loan_approval_draft,
        list_loan_approval_drafts,
        get_loan_approval_draft,
        approve_loan_approval_draft,
        send_back_loan_approval_draft,
        dismiss_loan_approval_draft,
        update_loan_safe_details,
        list_loan_purposes,
        get_loan_purpose_by_id,
        create_loan_purpose,
        set_loan_purpose_active,
        update_loan_purpose,
        count_loan_purposes_rows,
        clear_all_loan_purposes,
    )
    _loan_management_available = True
except Exception as e:
    _loan_management_available = False
    _loan_management_error = str(e)

try:
    from loan_management import get_cached_source_cash_account_entries
except Exception:
    def get_cached_source_cash_account_entries():
        return []

try:
    from provisions.config import list_security_subtypes as list_provision_security_subtypes
    from provisions.config import provision_schema_ready as _provision_schema_ready_fn

    _PROVISIONS_CONFIG_OK = True
except Exception:
    list_provision_security_subtypes = None
    _provision_schema_ready_fn = None
    _PROVISIONS_CONFIG_OK = False


# Same control as Teller: cached posting leaves under A100000 (per branch).
SOURCE_CASH_GL_WIDGET_LABEL = "Source cash / bank GL account (A100000 tree, cached list)"


def _source_cash_gl_cached_labels_and_ids() -> tuple[list[str], list[str]]:
    entries = get_cached_source_cash_account_entries()
    return (
        [f"{e['code']} — {e['name']}" for e in entries],
        [str(e["id"]) for e in entries],
    )


def _source_cash_gl_cache_empty_warning() -> None:
    st.warning(
        "The **source cash account cache** is empty. Rebuild it under **System configurations → "
        "Accounting configurations → Maintenance — source cash account cache**."
    )


# --- App state & global settings (UI) ---

def _get_system_date():
    from datetime import date as _date

    try:
        from eod.system_business_date import get_effective_date

        d = get_effective_date()
    except ImportError:
        d = datetime.now().date()
    fz = st.session_state.get("subscription_frozen_effective_date")
    if fz is not None and isinstance(fz, _date):
        return min(d, fz)
    return d


def _schedule_export_downloads(df: pd.DataFrame, *, file_stem: str, key_prefix: str) -> None:
    """
    Schedule downloads: CSV (2dp) + Excel (.xlsx) with real numeric cells.

    Use **Excel** if Microsoft flags CSV cells as “number stored as text” (green triangles).
    """
    if df is None or getattr(df, "empty", True):
        return
    inject_tertiary_hyperlink_css_once()
    c1, c2, _c_sp = st.columns([1, 1, 4], gap="small")
    with c1:
        st.download_button(
            label="Download CSV",
            data=schedule_dataframe_to_csv_bytes(df, amount_decimals=2),
            file_name=f"{file_stem}.csv",
            mime="text/csv",
            key=f"{key_prefix}_csv",
            type="tertiary",
            help="UTF-8 with BOM; amounts rounded to 2dp for readability.",
        )
    with c2:
        st.download_button(
            label="Download Excel",
            data=schedule_dataframe_to_excel_bytes(df, amount_decimals=2),
            file_name=f"{file_stem}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx",
            type="tertiary",
            help="Native Excel numbers (.xlsx); no text warnings.",
        )


def _get_mapping_registry() -> MappingRegistry:
    """Lazy-initialise an in-memory MappingRegistry stored in session state."""
    return _core_get_mapping_registry(key="accounting_mapping_registry")


def _get_fx_rates() -> list[dict]:
    """
    Simple FX rate store in session state.
    Each item: {"currency": str, "rate_to_base": float, "as_of": str}.
    """
    return _core_get_fx_rates(key="accounting_fx_rates")


def _get_consumer_schemes() -> list[dict]:
    """Consumer schemes from system config (managed in System configurations)."""
    return _get_system_config().get("consumer_schemes", [
        {"name": "SSB", "interest_rate_pct": 7.0, "admin_fee_pct": 7.0},
        {"name": "TPC", "interest_rate_pct": 7.0, "admin_fee_pct": 5.0},
    ])


def _consumer_schemes_admin_editor_ui(*, key_prefix: str) -> None:
    """
    Admin editor for `system_config.consumer_schemes`.

    Kept off by default and only shown on consumer-loan add/edit screens
    to avoid cluttering the rest of the UI.
    """
    expander_label = "Manage consumer loan schemes (admin)"
    with st.expander(expander_label, expanded=False):
        schemes_current = _get_consumer_schemes()
        draft_key = f"{key_prefix}_consumer_schemes_draft"
        if draft_key not in st.session_state:
            st.session_state[draft_key] = list(schemes_current) if schemes_current else []

        draft: list[dict] = st.session_state.get(draft_key, []) or []

        st.caption("Each scheme is used as a predefined interest/admin rate pair.")

        cs_add_name = st.text_input("Scheme name", key=f"{key_prefix}_cs_add_name")
        cs_add_interest = st.number_input(
            "Interest rate (%)",
            min_value=0.0,
            max_value=100.0,
            value=7.0,
            step=0.1,
            key=f"{key_prefix}_cs_add_interest",
        )
        cs_add_admin = st.number_input(
            "Admin fee (%)",
            min_value=0.0,
            max_value=100.0,
            value=5.0,
            step=0.1,
            key=f"{key_prefix}_cs_add_admin",
        )

        if st.button("Add / Update scheme", key=f"{key_prefix}_cs_add_update_btn"):
            name = (cs_add_name or "").strip()
            if not name:
                st.error("Scheme name is required.")
            elif name.lower() == "other":
                st.error("Scheme name 'Other' is reserved by the UI.")
            else:
                entry = {
                    "name": name,
                    "interest_rate_pct": float(cs_add_interest),
                    "admin_fee_pct": float(cs_add_admin),
                }
                replaced = False
                for i, s in enumerate(draft):
                    if (s or {}).get("name") == name:
                        draft[i] = entry
                        replaced = True
                        break
                if not replaced:
                    draft.append(entry)

                # De-dup by name while preserving order.
                seen: set[str] = set()
                deduped: list[dict] = []
                for s in draft:
                    n = (s or {}).get("name")
                    if n and n not in seen:
                        deduped.append(s)
                        seen.add(n)
                st.session_state[draft_key] = deduped
                st.success(f"Draft updated for scheme '{name}'.")
                st.rerun()

        if st.button("Reload from DB", key=f"{key_prefix}_cs_reload_btn"):
            st.session_state[draft_key] = list(schemes_current) if schemes_current else []
            st.success("Reloaded schemes from system config.")
            st.rerun()

        st.markdown("**Configured schemes:**")
        if not draft:
            st.info("No consumer schemes configured yet.")
        else:
            for s in draft:
                name = (s or {}).get("name") or ""
                if not name:
                    continue
                interest_pct = float((s or {}).get("interest_rate_pct", 0.0) or 0.0)
                admin_fee_pct = float((s or {}).get("admin_fee_pct", 0.0) or 0.0)
                cols = st.columns([3, 2, 2, 1])
                cols[0].markdown(f"**{name}**")
                cols[1].markdown(f"{interest_pct:.2f}%")
                cols[2].markdown(f"{admin_fee_pct:.2f}%")
                if cols[3].button("Remove", key=f"{key_prefix}_cs_remove_{name}"):
                    st.session_state[draft_key] = [x for x in draft if (x or {}).get("name") != name]
                    st.rerun()

        if st.button("Save schemes to system config (DB)", type="primary", key=f"{key_prefix}_cs_save_btn"):
            try:
                clean: list[dict] = []
                for s in st.session_state.get(draft_key, []) or []:
                    n = (s or {}).get("name")
                    if not n:
                        continue
                    clean.append(
                        {
                            "name": str(n),
                            "interest_rate_pct": float((s or {}).get("interest_rate_pct", 0.0) or 0.0),
                            "admin_fee_pct": float((s or {}).get("admin_fee_pct", 0.0) or 0.0),
                        }
                    )

                from loan_management import save_system_config_to_db

                sys_cfg_new = dict(_get_system_config())
                sys_cfg_new["consumer_schemes"] = clean
                if save_system_config_to_db(sys_cfg_new):
                    st.session_state["system_config"] = sys_cfg_new
                    st.success("Consumer schemes saved.")
                    st.rerun()
                else:
                    st.error("Failed to save consumer schemes.")
            except Exception as e:
                st.error(f"Failed to save: {e}")


def _get_global_loan_settings() -> dict:
    """Global assumptions: interest_method, interest_type, rate_basis (no principal_input - per loan)."""
    return _core_get_global_loan_settings(key="global_loan_settings")


def _get_product_rate_basis(
    product_cfg: dict | None,
    *,
    fallback: str | None = None,
) -> str:
    """
    Product-level rate basis for interest/penalty input interpretation.
    - "Per month": values are already monthly.
    - "Per annum": values must be converted to per-month for display and schedule math.
    """
    if not product_cfg:
        if fallback is None:
            st.error("Product config is missing; cannot resolve rate_basis.")
            st.stop()
        return fallback
    gls = product_cfg.get("global_loan_settings") or {}
    rb = gls.get("rate_basis")
    if rb in {"Per month", "Per annum"}:
        return rb
    if fallback is None:
        st.error(
            "Selected product must define product_config:product_code.global_loan_settings.rate_basis "
            "as either 'Per month' or 'Per annum'."
        )
        st.stop()
    return fallback


def _get_system_config() -> dict:
    """Penalty, waterfall, suspension, curing, compounding, default rates per loan type."""
    return _core_get_system_config(key="system_config")


def _money_df_column_config(df: pd.DataFrame, overrides: dict | None = None, **kwargs):
    """Streamlit column_config for money-like columns from system **display_format**."""
    return build_dataframe_money_column_config(
        df,
        st_column_config=st.column_config,
        system_config=_get_system_config(),
        overrides=overrides,
        **kwargs,
    )


_SCHEDULE_EDITOR_DISABLED_AMOUNTS = {
    "Interest": True,
    "Principal": True,
    "Principal Balance": True,
    "Total Outstanding": True,
}


def _format_schedule_df(df: pd.DataFrame):
    """Schedule table styling using configured grouping/decimals."""
    return format_schedule_display(df, system_config=_get_system_config())


def system_configurations_ui():
    """System configurations: sectors, EOD, accounting periods, products, and IFRS provision tables."""
    _list_sectors = (
        globals().get("list_sectors") if _customers_available else (lambda *a, **k: [])
    )
    _list_subsectors = (
        globals().get("list_subsectors")
        if _customers_available
        else (lambda *a, **k: [])
    )
    _create_sector = (
        globals().get("create_sector") if _customers_available else (lambda *a, **k: None)
    )
    _create_subsector = (
        globals().get("create_subsector")
        if _customers_available
        else (lambda *a, **k: None)
    )

    _list_products = (
        globals().get("list_products")
        if _loan_management_available
        else (lambda *a, **k: [])
    )
    _get_product_by_code = (
        globals().get("get_product_by_code")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _create_product = (
        globals().get("create_product")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _update_product = (
        globals().get("update_product")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _delete_product = (
        globals().get("delete_product")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _get_product = (
        globals().get("get_product")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _get_product_config_from_db = (
        globals().get("get_product_config_from_db")
        if _loan_management_available
        else (lambda *a, **k: {})
    )
    _save_product_config_to_db = (
        globals().get("save_product_config_to_db")
        if _loan_management_available
        else (lambda *a, **k: False)
    )

    _list_loan_purposes = (
        globals().get("list_loan_purposes")
        if _loan_management_available
        else (lambda *a, **k: [])
    )
    _get_loan_purpose_by_id = (
        globals().get("get_loan_purpose_by_id")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _create_loan_purpose = (
        globals().get("create_loan_purpose")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _set_loan_purpose_active = (
        globals().get("set_loan_purpose_active")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _update_loan_purpose = (
        globals().get("update_loan_purpose")
        if _loan_management_available
        else (lambda *a, **k: None)
    )
    _count_loan_purposes_rows = (
        globals().get("count_loan_purposes_rows")
        if _loan_management_available
        else (lambda *a, **k: 0)
    )
    _clear_all_loan_purposes = (
        globals().get("clear_all_loan_purposes")
        if _loan_management_available
        else (lambda *a, **k: (0, 0))
    )

    render_system_configurations_ui(
        get_system_config=_get_system_config,
        consumer_schemes_admin_editor_ui=_consumer_schemes_admin_editor_ui,
        parse_display_substrings_csv=_parse_display_substrings_csv,
        customers_available=_customers_available,
        list_sectors=_list_sectors,
        list_subsectors=_list_subsectors,
        create_sector=_create_sector,
        create_subsector=_create_subsector,
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error,
        list_products=_list_products,
        get_product_by_code=_get_product_by_code,
        create_product=_create_product,
        update_product=_update_product,
        delete_product=_delete_product,
        get_product=_get_product,
        get_product_config_from_db=_get_product_config_from_db,
        save_product_config_to_db=_save_product_config_to_db,
        list_loan_purposes=_list_loan_purposes,
        get_loan_purpose_by_id=_get_loan_purpose_by_id,
        create_loan_purpose=_create_loan_purpose,
        set_loan_purpose_active=_set_loan_purpose_active,
        update_loan_purpose=_update_loan_purpose,
        count_loan_purposes_rows=_count_loan_purposes_rows,
        clear_all_loan_purposes=_clear_all_loan_purposes,
    )


# --- MAIN APP ---

def eod_ui():
    """End-of-day processing configuration and manual run."""
    from ui.eod import render_eod_ui

    render_eod_ui(
        get_system_config=_get_system_config,
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error or "",
        load_system_config_from_db=globals().get("load_system_config_from_db"),
        is_admin=_user_is_admin(),
    )


def consumer_loan_ui():
    from ui.loan_calculators import render_consumer_loan_ui

    render_consumer_loan_ui(
        get_consumer_schemes=_get_consumer_schemes,
        get_system_config=_get_system_config,
        get_system_date=_get_system_date,
        get_global_loan_settings=_get_global_loan_settings,
        compute_consumer_schedule=compute_consumer_schedule,
        money_df_column_config=_money_df_column_config,
    )

def term_loan_ui():
    from ui.loan_calculators import render_term_loan_ui

    render_term_loan_ui(
        get_global_loan_settings=_get_global_loan_settings,
        get_system_config=_get_system_config,
        get_system_date=_get_system_date,
        compute_term_schedule=compute_term_schedule,
        money_df_column_config=_money_df_column_config,
    )

def bullet_loan_ui():
    from ui.loan_calculators import render_bullet_loan_ui

    render_bullet_loan_ui(
        get_global_loan_settings=_get_global_loan_settings,
        get_system_config=_get_system_config,
        get_system_date=_get_system_date,
        compute_bullet_schedule=compute_bullet_schedule,
        money_df_column_config=_money_df_column_config,
    )

def customised_repayments_ui():
    from ui.loan_calculators import render_customised_repayments_ui

    render_customised_repayments_ui(
        get_global_loan_settings=_get_global_loan_settings,
        get_system_config=_get_system_config,
        get_system_date=_get_system_date,
        money_df_column_config=_money_df_column_config,
        schedule_editor_disabled_amounts=_SCHEDULE_EDITOR_DISABLED_AMOUNTS,
        first_repayment_from_customised_table=_first_repayment_from_customised_table,
    )


def _first_repayment_from_customised_table(df_cap: pd.DataFrame):
    """First repayment date from first row with non-zero Payment; None if none."""
    for idx in range(1, len(df_cap)):
        try:
            payment = float(df_cap.at[idx, "Payment"]) if pd.notna(df_cap.at[idx, "Payment"]) else 0.0
            if payment > 0 and pd.notna(df_cap.at[idx, "Date"]):
                s = str(df_cap.at[idx, "Date"]).strip()[:32]
                return datetime.combine(datetime.strptime(s, "%d-%b-%Y").date(), datetime.min.time())
        except (ValueError, TypeError):
            continue
    return None


# --- Single calculator engine: used by both Loan calculators and Loan capture ---

def compute_consumer_schedule(
    loan_required: float,
    loan_term: int,
    start_date: datetime,
    base_rate: float,
    admin_fee: float,
    input_total_facility: bool,
    rate_basis: str,
    flat_rate: bool,
    scheme: str = "Other",
    additional_monthly_rate: float = 0.0,
    first_repayment_date: datetime | None = None,
    use_anniversary: bool = True,
) -> tuple[dict, pd.DataFrame]:
    """Compute consumer loan schedule. Returns (details dict for DB, schedule DataFrame).

    When first_repayment_date and use_anniversary are provided, repayment dates follow
    the same logic as Term Loan: anniversary (same day each month) or last day of month.
    """
    if input_total_facility:
        total_facility = loan_required
        amount_display = total_facility * (1.0 - admin_fee)
    else:
        total_facility = loan_required / (1.0 - admin_fee)
        amount_display = loan_required
    base_monthly = (base_rate / 12.0) if rate_basis == "Per annum" else base_rate
    total_monthly_rate = base_monthly + additional_monthly_rate
    monthly_installment = float(npf.pmt(total_monthly_rate, loan_term, -total_facility))

    if first_repayment_date is not None:
        schedule_dates = repayment_dates(start_date, first_repayment_date, int(loan_term), use_anniversary)
        end_date = schedule_dates[-1] if schedule_dates else add_months(start_date, loan_term) - timedelta(days=1)
        first_rep = first_repayment_date
    else:
        schedule_dates = None
        end_date = add_months(start_date, loan_term) - timedelta(days=1)
        first_rep = add_months(start_date, 1)

    df_schedule = get_amortization_schedule(
        total_facility, total_monthly_rate, int(loan_term), start_date, monthly_installment,
        flat_rate=flat_rate, schedule_dates=schedule_dates,
    )
    details = {
        "principal": total_facility, "disbursed_amount": amount_display, "term": loan_term,
        "monthly_rate": total_monthly_rate, "admin_fee": admin_fee, "scheme": scheme,
        "disbursement_date": start_date, "start_date": start_date, "end_date": end_date,
        "first_repayment_date": first_rep,
        "installment": monthly_installment,
        "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
    }
    return details, df_schedule


def compute_term_schedule(
    loan_required: float,
    loan_term: int,
    disbursement_date: datetime,
    rate_pct: float,
    drawdown_fee_pct: float,
    arrangement_fee_pct: float,
    input_total_facility: bool,
    grace_type: str,
    moratorium_months: int,
    first_repayment_date: datetime,
    use_anniversary: bool,
    rate_basis: str,
    flat_rate: bool,
) -> tuple[dict, pd.DataFrame]:
    """Compute term loan schedule. Returns (details dict for DB, schedule DataFrame)."""
    total_fee = drawdown_fee_pct + arrangement_fee_pct
    if total_fee >= 1.0:
        raise ValueError("Total upfront fees must be < 100% to compute net disbursed amount.")

    amount_display: float
    if input_total_facility:
        total_facility = loan_required
        # When user enters total principal/facility, cash disbursed is net of upfront fees.
        amount_display = total_facility * (1.0 - total_fee)
    else:
        amount_display = loan_required
        total_facility = loan_required / (1.0 - total_fee)
    annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
    schedule_dates = repayment_dates(disbursement_date, first_repayment_date, int(loan_term), use_anniversary)
    grace_key = "none"
    _gt = (grace_type or "").lower()
    if "principal and interest" in _gt:
        grace_key = "principal_and_interest"
    elif "principal" in _gt and "moratorium" in _gt:
        grace_key = "principal"
    df_schedule, installment = get_term_loan_amortization_schedule(
        total_facility, annual_rate, disbursement_date, schedule_dates, grace_key, moratorium_months, flat_rate=flat_rate
    )
    end_date = schedule_dates[-1] if schedule_dates else disbursement_date
    details = {
        "principal": total_facility, "disbursed_amount": amount_display, "term": loan_term,
        "annual_rate": annual_rate, "drawdown_fee": drawdown_fee_pct, "arrangement_fee": arrangement_fee_pct,
        "disbursement_date": disbursement_date, "start_date": disbursement_date, "end_date": end_date,
        "first_repayment_date": first_repayment_date, "installment": installment, "grace_type": grace_type,
        "moratorium_months": moratorium_months, "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
    }
    return details, df_schedule


def compute_bullet_schedule(
    loan_required: float,
    loan_term: int,
    disbursement_date: datetime,
    rate_pct: float,
    drawdown_fee_pct: float,
    arrangement_fee_pct: float,
    input_total_facility: bool,
    bullet_type: str,
    first_repayment_date: datetime | None,
    use_anniversary: bool,
    rate_basis: str,
    flat_rate: bool,
) -> tuple[dict, pd.DataFrame]:
    """Compute bullet loan schedule. Returns (details dict for DB, schedule DataFrame)."""
    total_fee = drawdown_fee_pct + arrangement_fee_pct
    if total_fee >= 1.0:
        raise ValueError("Total upfront fees must be < 100% to compute net disbursed amount.")

    amount_display: float
    if input_total_facility:
        total_facility = loan_required
        # When user enters total principal/facility, cash disbursed is net of upfront fees.
        amount_display = total_facility * (1.0 - total_fee)
    else:
        amount_display = loan_required
        total_facility = loan_required / (1.0 - total_fee)
    annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
    end_date = add_months(disbursement_date, loan_term)
    schedule_dates = None
    if first_repayment_date is not None:
        # Respect selected first repayment date and timing for both straight and with-interest bullet.
        schedule_dates = repayment_dates(disbursement_date, first_repayment_date, int(loan_term), use_anniversary)
        end_date = schedule_dates[-1] if schedule_dates else end_date
    df_schedule = get_bullet_schedule(
        total_facility, annual_rate, disbursement_date, end_date,
        "straight" if "Straight" in bullet_type else "with_interest",
        schedule_dates, flat_rate=flat_rate,
    )
    total_payment = float(df_schedule["Payment"].sum())
    details = {
        "principal": total_facility, "disbursed_amount": amount_display, "term": loan_term,
        "annual_rate": annual_rate, "drawdown_fee": drawdown_fee_pct, "arrangement_fee": arrangement_fee_pct,
        "disbursement_date": disbursement_date, "start_date": disbursement_date, "end_date": end_date,
        "total_payment": total_payment, "bullet_type": "straight" if "Straight" in bullet_type else "with_interest",
        "first_repayment_date": first_repayment_date, "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
    }
    return details, df_schedule


def capture_loan_ui():
    """Capture loan: single flat panel — details, schedule, review, documents; brand-styled section rules."""
    if not _customers_available:
        st.error("Customer module is required for Capture Loan. Check database connection.")
        return
    if not _loan_management_available:
        st.error(f"Loan management module is not available. ({_loan_management_error})")
        return

    from ui.capture_loan import render_capture_loan_ui

    render_capture_loan_ui(
        documents_available=_documents_available,
        list_document_categories=globals().get("list_document_categories")
        or (lambda active_only=True: []),
        upload_document=globals().get("upload_document"),
        loan_management_available=_loan_management_available,
        list_customers=list_customers,
        get_display_name=get_display_name,
        list_products=list_products,
        get_product_config_from_db=get_product_config_from_db,
        list_loan_purposes=list_loan_purposes,
        get_loan_purpose_by_id=get_loan_purpose_by_id,
        users_for_rm_available=_users_for_rm_available,
        list_users_for_selection=list_users_for_selection,
        agents_available=_agents_available,
        list_agents=list_agents,
        get_cached_source_cash_account_entries=get_cached_source_cash_account_entries,
        source_cash_gl_cached_labels_and_ids=_source_cash_gl_cached_labels_and_ids,
        source_cash_gl_cache_empty_warning=_source_cash_gl_cache_empty_warning,
        list_loan_approval_drafts=list_loan_approval_drafts,
        get_loan_approval_draft=get_loan_approval_draft,
        provisions_config_ok=_PROVISIONS_CONFIG_OK,
        list_provision_security_subtypes=list_provision_security_subtypes,
        provision_schema_ready_fn=_provision_schema_ready_fn,
        get_system_config=_get_system_config,
        get_consumer_schemes=_get_consumer_schemes,
        get_product_rate_basis=_get_product_rate_basis,
        get_system_date=_get_system_date,
        money_df_column_config=_money_df_column_config,
        schedule_editor_disabled_amounts=_SCHEDULE_EDITOR_DISABLED_AMOUNTS,
        compute_consumer_schedule=compute_consumer_schedule,
        compute_term_schedule=compute_term_schedule,
        compute_bullet_schedule=compute_bullet_schedule,
        first_repayment_from_customised_table=_first_repayment_from_customised_table,
        pct_to_monthly=_pct_to_monthly,
    )


def update_loans_ui():
    from ui.loan_management import render_update_loans_ui

    render_update_loans_ui(
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error,
        customers_available=_customers_available,
        list_customers=list_customers,
        get_display_name=get_display_name,
        get_loans_by_customer=get_loans_by_customer,
        update_loan_safe_details=update_loan_safe_details,
        save_loan_approval_draft=save_loan_approval_draft,
        provisions_config_ok=_PROVISIONS_CONFIG_OK,
        list_provision_security_subtypes=list_provision_security_subtypes,
    )

def approve_loans_ui():
    from ui.loan_management import render_approve_loans_ui

    render_approve_loans_ui(
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error,
        customers_available=_customers_available,
        documents_available=_documents_available,
        list_loan_approval_drafts=list_loan_approval_drafts,
        get_loan_approval_draft=get_loan_approval_draft,
        get_display_name=get_display_name,
        list_documents=globals().get("list_documents") or (lambda *a, **k: []),
        get_document=globals().get("get_document"),
        upload_document=globals().get("upload_document"),
        approve_loan_approval_draft=approve_loan_approval_draft,
        send_back_loan_approval_draft=send_back_loan_approval_draft,
        dismiss_loan_approval_draft=dismiss_loan_approval_draft,
        format_schedule_df=_format_schedule_df,
        money_df_column_config=_money_df_column_config,
    )


def batch_loans_ui():
    from customers.core import list_customers_for_loan_batch_link
    from ui.loan_management import render_batch_loan_capture_ui

    render_batch_loan_capture_ui(
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error,
        customers_available=_customers_available,
        list_customers=list_customers,
        get_display_name=get_display_name,
        save_loan=save_loan,
        list_customers_for_loan_batch_link=list_customers_for_loan_batch_link,
        source_cash_gl_cached_labels_and_ids=_source_cash_gl_cached_labels_and_ids,
    )


def customers_ui():
    """Web UI to add and manage customers (individuals and corporates)."""
    from ui.customers import render_customers_ui

    _list_document_categories = globals().get("list_document_categories") or (lambda *a, **k: [])
    _upload_document = globals().get("upload_document")

    render_customers_ui(
        customers_available=_customers_available,
        customers_error=_customers_error,
        documents_available=_documents_available,
        agents_available=_agents_available,
        agents_error=_agents_error,
        list_sectors=list_sectors,
        list_subsectors=list_subsectors,
        create_individual=create_individual,
        create_corporate_with_entities=create_corporate_with_entities,
        list_customers=list_customers,
        list_agents=list_agents,
        get_display_name=get_display_name,
        get_agent=get_agent,
        get_customer=get_customer,
        update_agent=update_agent,
        update_individual=update_individual,
        update_corporate=update_corporate,
        set_active=set_active,
        create_agent=create_agent,
        list_document_categories=_list_document_categories,
        upload_document=_upload_document,
    )


def view_schedule_ui():
    from ui.loan_management import render_view_schedule_ui

    render_view_schedule_ui(
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error,
        customers_available=_customers_available,
        list_customers=list_customers,
        get_display_name=get_display_name,
        get_loan=get_loan,
        get_loans_by_customer=get_loans_by_customer,
        get_schedule_lines=get_schedule_lines,
        format_schedule_df=_format_schedule_df,
        schedule_export_downloads=_schedule_export_downloads,
    )



def teller_ui():
    """Teller module: delegate to ui.teller (session/widget keys unchanged)."""
    if not _customers_available:
        st.error("Customer module is required for Teller. Check database connection.")
        return
    if not _loan_management_available:
        st.error(f"Loan management module is not available. ({_loan_management_error})")
        return

    from ui.teller import render_teller_ui

    render_teller_ui(
        customers_available=_customers_available,
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error,
        list_customers=list_customers,
        get_display_name=get_display_name,
        get_loans_by_customer=get_loans_by_customer,
        get_system_date=_get_system_date,
        source_cash_gl_cached_labels_and_ids=_source_cash_gl_cached_labels_and_ids,
        source_cash_gl_widget_label=SOURCE_CASH_GL_WIDGET_LABEL,
        source_cash_gl_cache_empty_warning=_source_cash_gl_cache_empty_warning,
    )


def _user_is_admin() -> bool:
    """True when the signed-in user is ADMIN or SUPERADMIN."""
    try:
        from middleware import get_current_user

        u = get_current_user()
        if not u:
            return False
        role = str(u.get("role") or "").strip().upper()
        return role in ("ADMIN", "SUPERADMIN")
    except Exception:
        return False


def _user_can_reamort_direct_principal_tab() -> bool:
    """Direct principal recast (no unapplied): admin-only extra tab."""
    return _user_is_admin()


def _reamod_created_by() -> str:
    try:
        from middleware import get_current_user

        u = get_current_user()
        if u:
            return str(u.get("email") or u.get("username") or u.get("id") or "user")
    except Exception:
        pass
    return "reamortisation_ui"


def reamortisation_ui():
    """Reamortisation: loan modification, recast, unapplied funds."""
    from ui.reamortisation import render_reamortisation_ui

    _cash_pairs = None
    if _loan_management_available:
        try:
            lbls, ids = _source_cash_gl_cached_labels_and_ids()
            if lbls and ids and len(lbls) == len(ids):
                _cash_pairs = list(zip(lbls, ids))
        except Exception:
            _cash_pairs = None

    render_reamortisation_ui(
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error or "",
        customers_available=_customers_available,
        customers_error=_customers_error or "",
        list_customers=globals().get("list_customers") or (lambda **k: []),
        get_display_name=globals().get("get_display_name") or (lambda _id: str(_id)),
        get_system_date=_get_system_date,
        format_schedule_df=_format_schedule_df,
        schedule_export_downloads=_schedule_export_downloads,
        money_df_column_config=_money_df_column_config,
        schedule_editor_disabled_amounts=_SCHEDULE_EDITOR_DISABLED_AMOUNTS,
        first_repayment_from_customised_table=_first_repayment_from_customised_table,
        apply_unapplied_funds_recast=globals().get("apply_unapplied_funds_recast"),
        list_products=list_products if _loan_management_available else (lambda active_only=True: []),
        get_product_config_from_db=get_product_config_from_db,
        get_system_config=_get_system_config,
        get_consumer_schemes=_get_consumer_schemes,
        get_product_rate_basis=_get_product_rate_basis,
        compute_consumer_schedule=compute_consumer_schedule,
        compute_term_schedule=compute_term_schedule,
        compute_bullet_schedule=compute_bullet_schedule,
        pct_to_monthly=_pct_to_monthly,
        save_loan_approval_draft=save_loan_approval_draft
        if _loan_management_available
        else (lambda *a, **k: 0),
        update_loan_approval_draft_staged=update_loan_approval_draft_staged
        if _loan_management_available
        else (lambda *a, **k: None),
        resubmit_loan_approval_draft=resubmit_loan_approval_draft
        if _loan_management_available
        else (lambda *a, **k: 0),
        list_loan_approval_drafts=list_loan_approval_drafts
        if _loan_management_available
        else (lambda *a, **k: []),
        get_loan_approval_draft=get_loan_approval_draft
        if _loan_management_available
        else (lambda *a, **k: None),
        approve_loan_approval_draft=approve_loan_approval_draft
        if _loan_management_available
        else (lambda *a, **k: 0),
        send_back_loan_approval_draft=send_back_loan_approval_draft
        if _loan_management_available
        else (lambda *a, **k: None),
        dismiss_loan_approval_draft=dismiss_loan_approval_draft
        if _loan_management_available
        else (lambda *a, **k: None),
        documents_available=_documents_available,
        list_document_categories=globals().get("list_document_categories")
        or (lambda active_only=True: []),
        upload_document=globals().get("upload_document"),
        provisions_config_ok=_PROVISIONS_CONFIG_OK,
        list_provision_security_subtypes=list_provision_security_subtypes,
        source_cash_gl_cached_labels_and_ids=_cash_pairs,
        created_by=_reamod_created_by(),
        direct_principal_tab=_user_can_reamort_direct_principal_tab(),
    )


def statements_ui():
    from ui.statements import render_statements_ui

    render_statements_ui(
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error or "",
        customers_available=_customers_available,
        customers_error=_customers_error or "",
        get_system_config=_get_system_config,
        get_system_date=_get_system_date,
        list_customers=list_customers,
        get_display_name=get_display_name,
        money_df_column_config=_money_df_column_config,
    )

def accounting_ui():
    """Database-backed Accounting Module."""
    from ui.accounting import render_accounting_ui

    try:
        from subscription.access import premium_bank_reconciliation_enabled

        _show_bank_recon = premium_bank_reconciliation_enabled()
    except Exception:
        _show_bank_recon = True

    render_accounting_ui(
        loan_management_available=_loan_management_available,
        list_products=globals().get("list_products") or (lambda **k: []),
        get_system_config=_get_system_config,
        get_system_date=_get_system_date,
        money_df_column_config=_money_df_column_config,
        show_bank_reconciliation_tab=_show_bank_recon,
    )


def notifications_ui():
    from ui.notifications import render_notifications_ui

    render_notifications_ui(
        customers_available=_customers_available,
        list_customers=globals().get("list_customers") or (lambda **k: []),
        get_display_name=globals().get("get_display_name") or (lambda _id: str(_id)),
    )



def journals_ui():
    """Standalone Journals module (manual journals + balance adjustments)."""
    from ui.journals import render_journals_ui

    render_journals_ui(get_system_date=_get_system_date)

def document_management_ui():
    from ui.document_management import render_document_management_ui

    render_document_management_ui(
        documents_available=_documents_available,
        documents_error=_documents_error,
        list_document_classes=globals().get("list_document_classes"),
        create_document_class=globals().get("create_document_class"),
        update_document_class=globals().get("update_document_class"),
        list_document_categories=globals().get("list_document_categories"),
        create_document_category=globals().get("create_document_category"),
        update_document_category=globals().get("update_document_category"),
        list_documents=globals().get("list_documents"),
        get_document=globals().get("get_document"),
    )


def main():
    # Stage 5: ensure core session state exists early.
    ensure_core_session_state()
    inject_farnda_global_styles_once()
    inject_tertiary_hyperlink_css_once()
    _nav_sections = get_loan_app_sections()
    if not _nav_sections:
        st.error("Navigation is not configured (no sections).")
        st.stop()
    render_sub_header("Navigation", sidebar=True)
    nav = st.sidebar.radio(
        "Section",
        _nav_sections,
        format_func=format_navigation_label,
        key="farnda_app_section_nav",
    )
    st.sidebar.divider()
    render_loan_app_section(nav)


LOAN_APP_SECTIONS = [
    "Customers",
    "Loan management",
    "Portfolio reports",
    "Teller",
    "Reamortisation",
    "Statements",
    "Accounting",
    "Journals",
    "Notifications",
    "Document Management",
    "End of day",
    "System configurations",
    "Subscription",
]


def get_loan_app_sections() -> list[str]:
    return list(LOAN_APP_SECTIONS)


def render_loan_app_section(nav: str) -> None:
    _get_global_loan_settings()  # ensure defaults exist
    from middleware import get_current_user

    from rbac.guards import enforce_nav_section_or_stop

    enforce_nav_section_or_stop(get_current_user(), nav)
    from subscription.access import check_access, get_subscription_snapshot

    check_access(nav_section=nav, snapshot=get_subscription_snapshot())
    render_main_page_title(nav)
    if nav == "Subscription":
        from middleware import get_current_user as _gcu
        from ui.subscription_user import render_subscription_user_ui

        render_subscription_user_ui(get_current_user=_gcu)
        return
    if nav == "Customers":
        customers_ui()
    elif nav == "Teller":
        teller_ui()
    elif nav == "Reamortisation":
        reamortisation_ui()
    elif nav == "Statements":
        statements_ui()
    elif nav == "Portfolio reports":
        from reporting.portfolio_reports_ui import render_portfolio_reports_ui

        render_portfolio_reports_ui()
    elif nav == "Loan management":
        # Use ``st.tabs`` (Baseweb tab list), not ``st.segmented_control``: theme styles on the
        # button-group widget override global CSS, so the segmented bar stayed “boxed”. Tabs share
        # the same underline rules as the rest of the app (see ``style.FARNDA_GLOBAL_CSS``).
        _lm_sections = [
            "Loan Capture",
            "Batch Capture",
            "View Schedule",
            "Loan Calculators",
            "Update Loans",
            "Interest In Suspense",
            "Approve Loans",
        ]
        _lm_legacy = {
            "Loan capture": "Loan Capture",
            "Batch capture": "Batch Capture",
            "View schedule": "View Schedule",
            "Loan calculators": "Loan Calculators",
            "Update loans": "Update Loans",
            "Interest in suspense": "Interest In Suspense",
            "Approve loans": "Approve Loans",
        }
        _cur_nav = st.session_state.get("loan_mgmt_subnav")
        if _cur_nav in _lm_legacy:
            st.session_state["loan_mgmt_subnav"] = _lm_legacy[_cur_nav]
        st.session_state.setdefault("loan_mgmt_subnav", "Loan Capture")
        if st.session_state["loan_mgmt_subnav"] not in _lm_sections:
            st.session_state["loan_mgmt_subnav"] = "Loan Capture"
        _lm_default = st.session_state["loan_mgmt_subnav"]
        (
            t_capture,
            t_batch,
            t_schedule,
            t_calc,
            t_update,
            t_suspense,
            t_approve,
        ) = st.tabs(_lm_sections, default=_lm_default)

        with t_capture:
            try:
                from subscription.access import basic_tier_hide_loan_capture

                if basic_tier_hide_loan_capture():
                    st.warning("Loan capture (origination) requires a **Premium** subscription.")
                    return
            except Exception:
                pass
            inject_tertiary_hyperlink_css_once()
            st.session_state.setdefault("capture_open_draft_panel", None)
            _cap_panel = st.session_state.get("capture_open_draft_panel")
            st.markdown(
                '<span class="farnda-lm-subnav-secondary" aria-hidden="true"></span>',
                unsafe_allow_html=True,
            )
            _sc1, _sc2, _sc_sp = st.columns([1.35, 1.45, 4], gap="small")
            with _sc1:
                if st.button(
                    "See Loans for Rework",
                    key="cap_open_rework_panel",
                    type="primary",
                    icon=":material/assignment_return:",
                    help="Open the list of drafts returned for rework",
                ):
                    st.session_state["capture_open_draft_panel"] = (
                        None if _cap_panel == "rework" else "rework"
                    )
                    st.rerun()
            with _sc2:
                if st.button(
                    "Resume Capture Draft",
                    key="cap_open_staged_panel",
                    type="primary",
                    icon=":material/edit_note:",
                    help="Open staged drafts to resume capture",
                ):
                    st.session_state["capture_open_draft_panel"] = (
                        None if _cap_panel == "staged" else "staged"
                    )
                    st.rerun()
            with _sc_sp:
                st.empty()
            capture_loan_ui()

        with t_schedule:
            view_schedule_ui()

        with t_batch:
            batch_loans_ui()

        with t_calc:
            _calc_types = [
                "Consumer Loan",
                "Term Loan",
                "Bullet Loan",
                "Customised Repayments",
            ]
            st.session_state.setdefault("loan_mgmt_calc_type", "Consumer Loan")
            if st.session_state.get("loan_mgmt_calc_type") not in _calc_types:
                st.session_state["loan_mgmt_calc_type"] = "Consumer Loan"
            _lt_lab, _lt_dd, _lt_sp = st.columns([1, 6, 5], gap="small")
            with _lt_lab:
                st.markdown(
                    '<p style="margin:0;padding-top:0.5rem;font-weight:600;">Loan Type</p>',
                    unsafe_allow_html=True,
                )
            with _lt_dd:
                st.selectbox(
                    "Loan Type",
                    _calc_types,
                    key="loan_mgmt_calc_type",
                    label_visibility="collapsed",
                )
            with _lt_sp:
                st.empty()
            _ct = st.session_state["loan_mgmt_calc_type"]
            if _ct == "Consumer Loan":
                consumer_loan_ui()
            elif _ct == "Term Loan":
                term_loan_ui()
            elif _ct == "Bullet Loan":
                bullet_loan_ui()
            else:
                customised_repayments_ui()

        with t_update:
            update_loans_ui()

        with t_suspense:
            from interest_suspense_ui import render_suspense_ui

            render_suspense_ui()

        with t_approve:
            approve_loans_ui()
    elif nav == "Accounting":
        accounting_ui()
    elif nav == "Journals":
        journals_ui()
    elif nav == "Notifications":
        notifications_ui()
    elif nav == "Document Management":
        document_management_ui()
    elif nav == "End of day":
        eod_ui()
    elif nav == "System configurations":
        system_configurations_ui()


if __name__ == "__main__":
    main()
