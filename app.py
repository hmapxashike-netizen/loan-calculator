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
from subscription.nav_sections import LOAN_APP_SIDEBAR_SECTIONS

LOAN_APP_SECTIONS = list(LOAN_APP_SIDEBAR_SECTIONS)


def get_loan_app_sections() -> list[str]:
    return list(LOAN_APP_SECTIONS)


try:
    from customers.core import (
        create_individual,
        create_corporate,
        create_corporate_with_entities,
        list_customers,
        search_customers_by_name,
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


def _consumer_scheme_rate_basis(product_cfg: dict | None) -> str:
    gls = (product_cfg or {}).get("global_loan_settings") or {}
    rb = gls.get("rate_basis")
    return rb if rb in {"Per month", "Per annum"} else "Per annum"


def _get_consumer_schemes() -> list[dict]:
    """
    Labels + rate pairs for consumer schedule matching (capture, calculators, modification).

    Built from each **active consumer-loan product**'s ``default_rates.consumer_loan`` and
    ``global_loan_settings.rate_basis`` — configure under **System configurations → Products**,
    not system-wide JSON.
    """
    if not _loan_management_available:
        return []
    rows: list[dict] = []
    try:
        products = list_products(active_only=True) or []
    except Exception:
        products = []
    for p in products:
        lt = str(p.get("loan_type") or "").strip().lower()
        if lt != "consumer_loan":
            continue
        code = str(p.get("code") or "").strip()
        if not code:
            continue
        try:
            cfg = get_product_config_from_db(code) or {}
        except Exception:
            cfg = {}
        dr = (cfg.get("default_rates") or {}).get("consumer_loan") or {}
        ip = dr.get("interest_pct")
        af = dr.get("admin_fee_pct")
        if ip is None or af is None:
            continue
        try:
            prb = _consumer_scheme_rate_basis(cfg)
            ip_m = _pct_to_monthly(float(ip), prb)
        except (TypeError, ValueError):
            continue
        if ip_m is None:
            continue
        pname = str(p.get("name") or code).strip() or code
        label = f"{pname} ({code})"
        rows.append(
            {
                "name": label,
                "product_code": code,
                "interest_rate_pct": float(ip_m),
                "admin_fee_pct": float(af),
            }
        )
    rows.sort(key=lambda r: str(r.get("name") or ""))
    return rows


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
    from ui.system_configurations import render_system_configurations_ui

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
    from loans import consumer_level_payment

    monthly_installment = consumer_level_payment(total_facility, total_monthly_rate, int(loan_term))

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

def loan_applications_ui():
    from ui.loan_applications import render_loan_applications_ui

    _scn = globals().get("search_customers_by_name")
    render_loan_applications_ui(
        loan_management_available=_loan_management_available,
        loan_management_error=_loan_management_error,
        customers_available=_customers_available,
        search_customers_by_name=_scn if callable(_scn) else (lambda *a, **k: []),
        get_customer=globals().get("get_customer"),
        get_display_name=globals().get("get_display_name") or (lambda _id: ""),
        create_individual=globals().get("create_individual"),
        create_corporate_with_entities=globals().get("create_corporate_with_entities"),
        get_consumer_schemes=_get_consumer_schemes if _loan_management_available else (lambda: []),
        list_sectors=globals().get("list_sectors") or (lambda: []),
        list_subsectors=globals().get("list_subsectors") or (lambda *a, **k: []),
        list_agents=list_agents,
        get_loan=globals().get("get_loan"),
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
    if not _user_can_loan_batch_capture_tab():
        st.error("You do not have permission for batch loan capture (migration).")
        st.stop()
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


def _user_is_superadmin() -> bool:
    """True when the signed-in user is SUPERADMIN."""
    try:
        from middleware import get_current_user

        u = get_current_user()
        if not u:
            return False
        role = str(u.get("role") or "").strip().upper()
        return role == "SUPERADMIN"
    except Exception:
        return False


def _user_can_loan_approve_tab() -> bool:
    """Approve Loans sub-tab: admins always; others need ``loan_management.approve_loans`` when RBAC is on."""
    if _user_is_admin():
        return True
    try:
        from middleware import get_current_user
        from rbac.service import rbac_tables_ready, get_permission_keys_for_role_key

        u = get_current_user()
        if not u:
            return False
        role = str(u.get("role") or "").strip().upper()
        if not rbac_tables_ready():
            return role in ("LOAN_OFFICER", "LOAN_SUPERVISOR", "ADMIN", "SUPERADMIN")
        return "loan_management.approve_loans" in get_permission_keys_for_role_key(role)
    except Exception:
        return True


def _user_can_loan_schedules_repayments_tab() -> bool:
    """Schedules & repayments (stored schedule lines): explicit key or legacy nav.loan_management."""
    try:
        from rbac.subfeature_access import loan_management_can_schedules_repayments

        return loan_management_can_schedules_repayments()
    except Exception:
        return True


def _user_can_loan_batch_capture_tab() -> bool:
    """Batch loan migration tab: explicit loan_management.batch_capture or SUPERADMIN when RBAC is on."""
    try:
        from rbac.subfeature_access import loan_management_can_batch_capture

        return loan_management_can_batch_capture()
    except Exception:
        return _user_is_superadmin()


def _user_can_reamort_direct_principal_tab() -> bool:
    """Direct principal recast (no unapplied): admin / explicit permission."""
    try:
        from rbac.subfeature_access import reamort_can_direct_principal

        return reamort_can_direct_principal()
    except Exception:
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
        list_products=globals().get("list_products"),
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
    # Loan applications "Add Individual/Corporate" sets this; must apply before sidebar.radio or widget state overrides it.
    from ui.loan_applications import consume_loan_app_navigation_intent

    consume_loan_app_navigation_intent()
    _nav_legacy = st.session_state.get("farnda_app_section_nav")
    if _nav_legacy == "Loan applications":
        st.session_state["farnda_app_section_nav"] = "Loan pipeline"
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
    elif nav == "Loan pipeline":
        loan_applications_ui()
    elif nav == "Teller":
        teller_ui()
    elif nav == "Reamortisation":
        reamortisation_ui()
    elif nav == "Statements":
        statements_ui()
    elif nav == "Portfolio reports":
        from reporting.portfolio_reports_ui import render_portfolio_reports_ui

        render_portfolio_reports_ui()
    elif nav == "Creditor loans":
        from ui.creditor_loans import render_creditor_loans_ui

        render_creditor_loans_ui(
            get_system_date=_get_system_date,
            get_cached_source_cash_account_entries=get_cached_source_cash_account_entries,
            documents_available=_documents_available,
            list_document_categories=globals().get("list_document_categories"),
            upload_document=globals().get("upload_document"),
            money_df_column_config=_money_df_column_config,
        )
    elif nav == "Loan management":
        # Single subnav + one active branch: avoids ``st.tabs`` running every tab body on each rerun.
        # Legacy session: old tab label inside Loan management — migrate to Loan Capture without
        # forcing the sidebar to Loan pipeline (that broke Loan Capture workflow / Jump from pipeline).
        if st.session_state.get("loan_mgmt_subnav") == "Loan applications":
            st.session_state["loan_mgmt_subnav"] = "Loan Capture"
            st.rerun()
        _lm_sections = ["Loan Capture"]
        if _user_can_loan_batch_capture_tab():
            _lm_sections.append("Batch Capture")
        if _user_can_loan_schedules_repayments_tab():
            _lm_sections.append("Schedules & repayments")
        _lm_sections.extend(
            [
                "Loan Calculators",
                "Update Loans",
                "Interest In Suspense",
            ]
        )
        if _user_can_loan_approve_tab():
            _lm_sections.append("Approve Loans")
        _lm_legacy = {
            "Loan capture": "Loan Capture",
            "Batch capture": "Batch Capture",
            "View schedule": "Schedules & repayments",
            "View Schedule": "Schedules & repayments",
            "Schedules repayments": "Schedules & repayments",
            "Schedules & repayments": "Schedules & repayments",
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

        st.markdown(
            '<p id="farnda-lm-tabbar" aria-hidden="true"></p>',
            unsafe_allow_html=True,
        )
        st.radio(
            "Loan management section",
            _lm_sections,
            key="loan_mgmt_subnav",
            horizontal=True,
            label_visibility="collapsed",
        )

        _lm_active = st.session_state["loan_mgmt_subnav"]

        if _lm_active == "Loan Capture":
            # Pipeline "Jump to Loan Capture" stores this flash, but Loan pipeline UI does not rerun here — show it once on capture.
            _lm_pipe_flash = st.session_state.pop("loan_apps_flash", None)
            if _lm_pipe_flash:
                st.success(_lm_pipe_flash)
            try:
                from subscription.access import basic_tier_hide_loan_capture

                _hide_capture = basic_tier_hide_loan_capture()
            except Exception:
                _hide_capture = False
            if _hide_capture:
                st.warning(
                    "Loan **capture** is off for your subscription tier — **Loan management** is not enabled "
                    "for this tier in **System configurations → Subscription vendor**. Ask your administrator "
                    "to tick **Loan management** for your tier (or upgrade tier under **Subscription**)."
                )
            else:
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

        elif _lm_active == "Batch Capture":
            if not _user_can_loan_batch_capture_tab():
                st.error("You do not have permission for batch loan capture (migration).")
                st.stop()
            batch_loans_ui()

        elif _lm_active == "Schedules & repayments":
            if not _user_can_loan_schedules_repayments_tab():
                st.warning("You do not have permission to open Schedules & repayments.")
                st.stop()
            view_schedule_ui()

        elif _lm_active == "Loan Calculators":
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

        elif _lm_active == "Update Loans":
            update_loans_ui()

        elif _lm_active == "Interest In Suspense":
            from interest_suspense_ui import render_suspense_ui

            render_suspense_ui()

        elif _lm_active == "Approve Loans":
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
