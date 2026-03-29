import re
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import numpy_financial as npf

from accounting_core import (
    Account,
    AccountCategory,
    EventAccountMapping,
    MappingCategory,
    MappingRegistry,
    PostingSide,
    SystemEventTag,
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

try:
    from customers import (
        create_individual,
        create_corporate,
        create_corporate_with_entities,
        list_customers,
        get_customer,
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
except Exception as e:
    _customers_available = False
    _customers_error = str(e)

try:
    from agents import list_agents, get_agent, create_agent, update_agent
    _agents_available = True
except Exception as e:
    _agents_available = False
    _agents_error = str(e)

try:
    from dal import list_users_for_selection
    _users_for_rm_available = True
except Exception:
    _users_for_rm_available = False
    list_users_for_selection = lambda: []

try:
    from documents import (
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
except Exception as e:
    _documents_available = False
    _documents_error = str(e)

# Document type name whitelists per entity (must match document_categories.name)
INDIVIDUAL_DOC_TYPES = {
    "National ID",
    "Payslip",
    "Proof of Residence",
    "Confirmation of Employment",
    "Other",
}

CORPORATE_DOC_TYPES = {
    "CR5",
    "CR6",
    "Memorandum and Articles",
    "Certificate of Incorporation",
    "CR2",
    "Other",
}

AGENT_INDIVIDUAL_DOC_TYPES = INDIVIDUAL_DOC_TYPES.union({"Tax Clearance"})

AGENT_CORPORATE_DOC_TYPES = CORPORATE_DOC_TYPES.union(
    {
        # Director KYC (same as individual)
        "National ID",
        "Payslip",
        "Proof of Residence",
        "Confirmation of Employment",
    }
)

_loan_management_error: str = ""
try:
    from loan_management import (
        save_loan as save_loan_to_db,
        record_repayment,
        record_repayments_batch,
        reverse_repayment,
        ReverseRepaymentResult,
        get_loan,
        get_loans_by_customer,
        get_amount_due_summary,
        get_schedule_lines,
        allocate_repayment_waterfall,
        apply_unapplied_funds_recast,
        load_system_config_from_db,
        get_loan_daily_state_balances,
        get_repayments_with_allocations,
        reallocate_repayment,
        get_repayment_ids_for_loan_and_date,
        list_products,
        get_product,
        get_product_by_code,
        create_product,
        update_product,
        delete_product,
        get_product_config_from_db,
        save_product_config_to_db,
        save_loan_approval_draft,
        update_loan_approval_draft_staged,
        resubmit_loan_approval_draft,
        list_loan_approval_drafts,
        get_loan_approval_draft,
        approve_loan_approval_draft,
        send_back_loan_approval_draft,
        dismiss_loan_approval_draft,
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
    from provisions_config import list_security_subtypes as list_provision_security_subtypes
    from provisions_config import provision_schema_ready as _provision_schema_ready_fn

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
    try:
        from system_business_date import get_effective_date
        return get_effective_date()
    except ImportError:
        return __import__('datetime').datetime.now().date()


def _schedule_export_downloads(df: pd.DataFrame, *, file_stem: str, key_prefix: str) -> None:
    """
    Schedule downloads: CSV (2dp) + Excel (.xlsx) with real numeric cells.

    Use **Excel** if Microsoft flags CSV cells as “number stored as text” (green triangles).
    """
    if df is None or getattr(df, "empty", True):
        return
    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            label="Download CSV (2 decimals)",
            data=schedule_dataframe_to_csv_bytes(df, amount_decimals=2),
            file_name=f"{file_stem}.csv",
            mime="text/csv",
            key=f"{key_prefix}_csv",
            help="UTF-8 with BOM; amounts rounded to 2dp for readability.",
        )
    with c2:
        st.download_button(
            label="Download Excel (.xlsx)",
            data=schedule_dataframe_to_excel_bytes(df, amount_decimals=2),
            file_name=f"{file_stem}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx",
            help="Native Excel numbers (no text warnings).",
        )


def _get_mapping_registry() -> MappingRegistry:
    """Lazy-initialise an in-memory MappingRegistry stored in session state."""
    if "accounting_mapping_registry" not in st.session_state:
        st.session_state["accounting_mapping_registry"] = MappingRegistry()
    return st.session_state["accounting_mapping_registry"]


def _get_fx_rates() -> list[dict]:
    """
    Simple FX rate store in session state.
    Each item: {"currency": str, "rate_to_base": float, "as_of": str}.
    """
    if "accounting_fx_rates" not in st.session_state:
        st.session_state["accounting_fx_rates"] = []
    return st.session_state["accounting_fx_rates"]


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
    if "global_loan_settings" not in st.session_state:
        st.session_state["global_loan_settings"] = {
            "interest_method": "Reducing balance",
            "interest_type": "Simple",
            "rate_basis": "Per month",
        }
    return st.session_state["global_loan_settings"]


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


def _pct_to_monthly(pct: float | int | None, rate_basis: str) -> float | None:
    if pct is None:
        return None
    pct_f = float(pct)
    return (pct_f / 12.0) if rate_basis == "Per annum" else pct_f


def _get_system_config() -> dict:
    """Penalty, waterfall, suspension, curing, compounding, default rates per loan type."""
    defaults = {
        # Waterfall configuration
        "waterfall_buckets": [
            "fees_charges_balance",
            "penalty_interest_balance",
            "default_interest_balance",
            "interest_arrears_balance",
            "interest_accrued_balance",
            "principal_arrears",
            "principal_not_due",
        ],
        "waterfall_profiles": {
            # Standard: Fees → Penalty → Default → Int arrears → Int accrued → Prin arrears → Prin not due
            "standard": [
                "fees_charges_balance",
                "penalty_interest_balance",
                "default_interest_balance",
                "interest_arrears_balance",
                "interest_accrued_balance",
                "principal_arrears",
                "principal_not_due",
            ],
            # Borrower-friendly: Principal not due → Principal arrears → all interest → fees
            "borrower_friendly": [
                "principal_not_due",
                "principal_arrears",
                "interest_accrued_balance",
                "interest_arrears_balance",
                "default_interest_balance",
                "penalty_interest_balance",
                "fees_charges_balance",
            ],
        },
        "base_currency": "USD",
        "accepted_currencies": ["USD"],
        "loan_default_currencies": {
            "consumer_loan": "USD",
            "term_loan": "USD",
            "bullet_loan": "USD",
            "customised_repayments": "USD",
        },
        "penalty_interest_quotation": "Absolute Rate",
        "penalty_balance_basis": "Arrears",
        "payment_waterfall": "Standard",
        "suspension_logic": "Manual",
        "suspension_auto_days": 90,
        "curing_logic": "Curing",
        "capitalization_of_unpaid_interest": False,
        "penalty_rates": {
            "consumer_loan": 2.0,
            "term_loan": 2.0,
            "bullet_loan": 2.0,
            "customised_repayments": 2.0,
        },
        "default_rates": {
            "consumer_loan": {"interest_pct": 7.0, "admin_fee_pct": 5.0},
            "term_loan": {"interest_pct": 7.0, "drawdown_pct": 2.5, "arrangement_pct": 2.5},
            "bullet_loan": {"interest_pct": 7.0, "drawdown_pct": 2.5, "arrangement_pct": 2.5},
            "customised_repayments": {"interest_pct": 7.0, "drawdown_pct": 2.5, "arrangement_pct": 2.5},
        },
        "consumer_schemes": [
            {"name": "SSB", "interest_rate_pct": 7.0, "admin_fee_pct": 7.0},
            {"name": "TPC", "interest_rate_pct": 7.0, "admin_fee_pct": 5.0},
        ],
        "consumer_default_additional_rate_pct": 0.0,
        # End-of-day processing defaults
        "eod_settings": {
            "mode": "manual",  # 'manual' or 'automatic'
            "automatic_time": "23:00",  # HH:MM (24h) preferred run time if external scheduler is used
            "tasks": {
                "run_loan_engine": True,
                "post_accounting_events": False,
                "generate_statements": False,
                "snapshot_financial_statements": True,
                "send_notifications": False,
            },
            "stage_policy": {
                "mode": "hybrid",  # strict | hybrid | best_effort
                "blocking_stages": [
                    "loan_engine",
                    "reallocate_after_reversals",
                    "apply_unapplied_to_arrears",
                    "accounting_events",
                    "statements",
                ],
                "advance_date_on_degraded": False,
            },
        },
        "accounting_periods": {
            "month_end_mode": "calendar",  # calendar | fixed_day
            "month_end_day": 31,  # used only when month_end_mode=fixed_day
            "fiscal_year_end_month": 12,  # 1=Jan ... 12=Dec
            "snapshot_max_rows": 100,
        },
    }
    try:
        from provision_config import PROVISION_RBZ_CONFIG_KEY, default_provision_rbz_config

        defaults[PROVISION_RBZ_CONFIG_KEY] = default_provision_rbz_config()
    except Exception:
        pass
    if "system_config" not in st.session_state:
        try:
            from loan_management import load_system_config_from_db
            db_cfg = load_system_config_from_db()
            if db_cfg:
                merged = defaults.copy()
                for k, v in db_cfg.items():
                    if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
                        merged[k] = {**merged[k], **v}
                    else:
                        merged[k] = v
                st.session_state["system_config"] = merged
            else:
                st.session_state["system_config"] = defaults.copy()
        except Exception:
            st.session_state["system_config"] = defaults.copy()
    cfg = st.session_state["system_config"]
    for k, v in defaults.items():
        if k not in cfg:
            cfg[k] = v
    return cfg


def system_configurations_ui():
    """System configurations: sectors, EOD, accounting periods, products, and IFRS provision tables."""
    st.markdown(
        "<div style='background-color: #16A34A; color: white; padding: 8px 12px; font-weight: bold; font-size: 1.1rem;'>System configurations</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)
    cfg = _get_system_config()

    eod_cfg = cfg.get("eod_settings", {}) or {}
    eod_mode = eod_cfg.get("mode", "manual")
    eod_time = eod_cfg.get("automatic_time", "23:00")
    eod_task_defaults = {
        "run_loan_engine": True,
        "post_accounting_events": False,
        "generate_statements": False,
        "snapshot_financial_statements": True,
        "send_notifications": False,
    }
    existing_tasks = eod_cfg.get("tasks") or {}
    eod_tasks: dict[str, bool] = {
        k: bool(existing_tasks.get(k, default)) for k, default in eod_task_defaults.items()
    }
    policy_cfg = eod_cfg.get("stage_policy", {}) or {}
    policy_mode = str(policy_cfg.get("mode") or "hybrid")
    blocking_stage_default = [
        "loan_engine",
        "reallocate_after_reversals",
        "apply_unapplied_to_arrears",
        "accounting_events",
        "statements",
    ]
    blocking_stages = policy_cfg.get("blocking_stages")
    if not isinstance(blocking_stages, list):
        blocking_stages = list(blocking_stage_default)
    advance_date_on_degraded = bool(policy_cfg.get("advance_date_on_degraded", False))

    tab_sectors, tab_eod, tab_accounting, tab_consumer_schemes, tab_products, tab_ifrs_prov = st.tabs(
        [
            "Sectors & subsectors",
            "EOD configurations",
            "Accounting configurations",
            "Consumer schemes",
            "Products",
            "IFRS provision config",
        ],
    )

    # ---------------- Sectors & subsectors tab ----------------
    with tab_sectors:
            st.subheader("Sectors & subsectors")
            st.caption("Configure sectors and subsectors for customer classification.")
            if _customers_available:
                sectors_list = list_sectors()
                if sectors_list:
                    for s in sectors_list:
                        with st.expander(f"Sector: {s.get('name', '')}", expanded=False):
                            subs = list_subsectors(s.get("id"))
                            for sub in subs:
                                st.caption(f"  • {sub.get('name', '')}")
                            new_sub = st.text_input("New subsector name", key=f"new_sub_{s['id']}", placeholder="Subsector name")
                            if st.button("Add subsector", key=f"add_sub_{s['id']}") and new_sub and new_sub.strip():
                                try:
                                    create_subsector(s["id"], new_sub.strip())
                                    st.success("Subsector added.")
                                    st.rerun()
                                except Exception as ex:
                                    st.error(str(ex))
                new_sector_name = st.text_input("New sector name", key="syscfg_new_sector", placeholder="e.g. Agriculture")
                if st.button("Add sector", key="syscfg_add_sector") and new_sector_name and new_sector_name.strip():
                    try:
                        create_sector(new_sector_name.strip())
                        st.success("Sector added.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
            else:
                st.info("Customer module required to manage sectors.")

    # ---------------- EOD configurations tab ----------------
    with tab_eod:
        st.subheader("System business date")
        st.caption("Accruals and Amount Due use the system date, not the calendar.")
        try:
            from system_business_date import get_system_business_config, set_system_business_config
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
            default_time = datetime.now().replace(hour=h, minute=m, second=s, microsecond=0).time()
            new_time = st.time_input(
                "EOD auto-run time (when enabled)",
                value=default_time,
                key="syscfg_eod_auto_time",
            )
            new_auto = st.checkbox("Enable auto EOD (trigger at configured time)", value=sb_cfg["is_auto_eod_enabled"], key="syscfg_auto_eod")
            if st.button("Save auto EOD settings", key="syscfg_save_auto"):
                if set_system_business_config(eod_auto_run_time=new_time, is_auto_eod_enabled=new_auto):
                    st.success("Auto EOD settings saved.")
                    st.rerun()
        except Exception as ex:
            st.warning("System business config not available (run migration 26): %s", ex)

        st.divider()
        st.subheader("End of day (EOD) settings")
        st.caption(
            "Configure how and when EOD runs, and which high-level tasks should be included. "
            "The detailed orchestration is fixed in code for safety and auditability."
        )

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
            # Parse existing time for default; fall back to 23:00.
            hours, minutes = 23, 0
            try:
                parts = (eod_time or "23:00").split(":")
                hours, minutes = int(parts[0]), int(parts[1])
            except Exception:
                pass
            time_value = st.time_input(
                "Preferred EOD time (24h, server local time)",
                datetime.now().replace(hour=hours, minute=minutes, second=0, microsecond=0).time(),
                key="syscfg_eod_time",
            )
            eod_time = time_value.strftime("%H:%M")

        st.markdown("**EOD tasks**")
        st.caption(
            "Choose which high-level tasks should run as part of EOD. "
            "The detailed sequence is fixed in code for safety and auditability."
        )
        # Core loan engine is always on to keep loan_daily_state coherent.
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

    with tab_accounting:
        st.subheader("Accounting periods")
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

        month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
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
            value=snapshot_max_rows_default if 10 <= snapshot_max_rows_default <= 1000 else 100,
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
                    disabled=(not _scc_confirm) or (_scc_type.strip().upper() != "REBUILD"),
                ):
                    try:
                        from accounting_service import AccountingService

                        _scc_block = AccountingService().refresh_source_cash_account_cache()
                        st.session_state.pop("system_config", None)
                        st.success(
                            f"Cache rebuilt at {_scc_block.get('refreshed_at')} — "
                            f"{len(_scc_block.get('entries') or [])} account(s)."
                        )
                        st.rerun()
                    except Exception as _scc_ex:
                        st.error(str(_scc_ex))

    with tab_consumer_schemes:
        st.subheader("Consumer schemes (admin)")
        st.caption(
            "Used for consumer loan schedule calculation. Normally you set rates at the product level; "
            "this list is mainly for enabling/disabling scheme names (SSB/TPC/future)."
        )
        _consumer_schemes_admin_editor_ui(key_prefix="syscfg_consumer_schemes")

    # ---------------- Products tab ----------------
    with tab_products:
        st.subheader("Products")
        st.caption("Products own loan config, currency, waterfall, suspension & curing. Loan type (on product) drives amortisation. System references products by code.")
        if not _loan_management_available:
            st.error("Loan management module is required for Products.")
        else:
            products_list = list_products(active_only=False)
            if products_list:
                product_options = [(0, "(Select product to edit)")] + [(p["id"], f"{p['code']} – {p['name']} (ID: {p['id']})") for p in products_list]
                option_labels = [t[1] for t in product_options]
                option_ids = [t[0] for t in product_options]

                st.markdown("**Products**")
                col_h1, col_h2, col_h3, col_h4, col_h5, col_h6 = st.columns([1.5, 2, 1.2, 1, 0.8, 0.8])
                with col_h1: st.caption("**Code**")
                with col_h2: st.caption("**Name**")
                with col_h3: st.caption("**Loan type**")
                with col_h4: st.caption("**Status**")
                with col_h5: st.caption("**Edit**")
                with col_h6: st.caption("**Delete**")
                for p in products_list:
                    c1, c2, c3, c4, c5, c6 = st.columns([1.5, 2, 1.2, 1, 0.8, 0.8])
                    with c1: st.text(p.get("code", ""))
                    with c2: st.text(p.get("name", ""))
                    with c3: st.text(p.get("loan_type", ""))
                    with c4: st.text("Active" if p.get("is_active", True) else "Inactive")
                    with c5:
                        if st.button("Edit", key=f"ptbl_edit_{p['id']}"):
                            idx = next((i for i, (oid, _) in enumerate(product_options) if oid == p["id"]), 0)
                            st.session_state["prod_edit_sel"] = idx
                            st.rerun()
                    with c6:
                        if st.button("Delete", key=f"ptbl_del_{p['id']}"):
                            try:
                                delete_product(p["id"])
                                st.success("Product deleted.")
                                st.rerun()
                            except ValueError as e:
                                st.error(str(e))
                                st.rerun()
                with st.expander("Add product", expanded=False):
                    p_code = st.text_input("Code", key="prod_add_code", max_chars=32, placeholder="e.g. TL-USD")
                    p_name = st.text_input("Name", key="prod_add_name", placeholder="Display name")
                    p_lt = st.selectbox("Loan type", ["term_loan", "consumer_loan", "bullet_loan", "customised_repayments"], key="prod_add_lt")
                    if st.button("Create", key="prod_add_btn") and p_code and p_name:
                        code_upper = p_code.strip().upper()
                        if get_product_by_code(code_upper):
                            st.error("Product code already exists.")
                        else:
                            try:
                                create_product(code_upper, p_name.strip(), p_lt)
                                st.success(f"Product **{code_upper}** created.")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                if "prod_edit_sel" not in st.session_state:
                    st.session_state["prod_edit_sel"] = 0
                if st.session_state.get("prod_edit_sel", 0) != 0:
                    st.divider()
                    st.markdown("**Edit product config**")
                    sel_idx = st.selectbox(
                        "Select product to edit config",
                        range(len(option_labels)),
                        format_func=lambda i: option_labels[i],
                        key="prod_edit_sel",
                    )
                    edit_id = option_ids[sel_idx] if sel_idx is not None else 0
                else:
                    edit_id = 0
                    st.caption("Click **Edit** on a product above to edit its config.")
                if edit_id:
                    prod = get_product(edit_id)
                    if prod:
                        p_cfg = get_product_config_from_db(prod["code"]) or {}
                        pid = edit_id
                        code_display = prod.get("code") or ""
                        name_display = prod.get("name") or ""
                        lt = prod.get("loan_type", "term_loan")
                        with st.expander(f"**{code_display}** – Rename & status", expanded=True):
                            col_rn, col_st = st.columns(2)
                            with col_rn:
                                new_name = st.text_input("Rename product", value=prod.get("name") or "", key="pedit_rename")
                                if st.button("Save name", key="pedit_save_name") and new_name.strip():
                                    update_product(edit_id, name=new_name.strip())
                                    st.success("Name updated.")
                                    st.rerun()
                            with col_st:
                                current_active = bool(prod.get("is_active", True))
                                status_choice = st.radio("Status", ["Active", "Inactive"], index=0 if current_active else 1, key="pedit_status")
                                if st.button("Update status", key="pedit_save_status"):
                                    update_product(edit_id, is_active=(status_choice == "Active"))
                                    st.success("Status updated.")
                                    st.rerun()
                        st.caption("Product config (overrides system config for loans using this product).")
                        st.markdown(f"**Changes apply only to this product:** **{code_display}**")
                        p_reg_tab, p_pen_tab, p_ccy_tab, p_wf_tab, p_sus_tab = st.tabs(["Regular interest", "Penalty", "Currency", "Waterfall", "Suspension & curing"])
                        with p_reg_tab:
                            glob_p = (p_cfg.get("global_loan_settings") or {}).copy()
                            for k, v in (cfg.get("global_loan_settings") or {}).items():
                                if k not in glob_p:
                                    glob_p[k] = v
                            im_opts, it_opts, rb_opts = ["Reducing balance", "Flat rate"], ["Simple", "Compound"], ["Per annum", "Per month"]
                            c1, c2, c3 = st.columns(3)
                            with c1:
                                im = st.radio("Interest method", im_opts, index=im_opts.index(glob_p.get("interest_method", "Reducing balance")) if glob_p.get("interest_method") in im_opts else 0, key=f"pedit_im_{pid}")
                            with c2:
                                it = st.radio("Interest type", it_opts, index=it_opts.index(glob_p.get("interest_type", "Simple")) if glob_p.get("interest_type") in it_opts else 0, key=f"pedit_it_{pid}")
                            with c3:
                                rb = st.radio("Rate basis", rb_opts, index=rb_opts.index(glob_p.get("rate_basis", "Per month")) if glob_p.get("rate_basis") in rb_opts else 1, key=f"pedit_rb_{pid}")
                            cap = st.radio("Capitalization", ["No", "Yes"], index=1 if p_cfg.get("capitalization_of_unpaid_interest", cfg.get("capitalization_of_unpaid_interest")) else 0, key=f"pedit_cap_{pid}")
                            st.markdown("**Default rates (this product type)**")
                            dr = p_cfg.get("default_rates") or cfg.get("default_rates") or {}
                            row = dr.get(lt, {})
                            if lt == "consumer_loan":
                                cr_def = dr.get("consumer_loan", {})
                                co1, co2 = st.columns(2)
                                with co1: dr_interest = st.number_input("Interest %", 0.0, 100.0, float(cr_def.get("interest_pct", 7)), step=0.1, key=f"pedit_dr_int_{pid}")
                                with co2: dr_admin = st.number_input("Admin %", 0.0, 100.0, float(cr_def.get("admin_fee_pct", 5)), step=0.1, key=f"pedit_dr_adm_{pid}")
                            else:
                                d1, d2, d3 = st.columns(3)
                                with d1: dr_interest = st.number_input("Interest %", 0.0, 100.0, float(row.get("interest_pct", 7)), step=0.1, key=f"pedit_dr_int_{pid}")
                                with d2: dr_drawdown = st.number_input("Drawdown %", 0.0, 100.0, float(row.get("drawdown_pct", 2.5)), step=0.1, key=f"pedit_dr_dd_{pid}")
                                with d3: dr_arr = st.number_input("Arrangement %", 0.0, 100.0, float(row.get("arrangement_pct", 2.5)), step=0.1, key=f"pedit_dr_arr_{pid}")
                            if st.button("Save Regular interest", key=f"pedit_save_reg_{pid}"):
                                merge = dict(p_cfg)
                                merge["global_loan_settings"] = {"interest_method": im, "interest_type": it, "rate_basis": rb}
                                merge["capitalization_of_unpaid_interest"] = cap == "Yes"
                                dr_merge = dict(merge.get("default_rates") or {})
                                if lt == "consumer_loan":
                                    dr_merge["consumer_loan"] = {"interest_pct": dr_interest, "admin_fee_pct": dr_admin}
                                else:
                                    dr_merge[lt] = {"interest_pct": dr_interest, "drawdown_pct": dr_drawdown, "arrangement_pct": dr_arr}
                                merge["default_rates"] = dr_merge
                                if save_product_config_to_db(prod["code"], merge):
                                    st.success("Saved.")
                                    st.rerun()
                        with p_pen_tab:
                            pr = p_cfg.get("penalty_rates") or cfg.get("penalty_rates") or {}
                            pq = p_cfg.get("penalty_interest_quotation") or cfg.get("penalty_interest_quotation") or "Absolute Rate"
                            pb = p_cfg.get("penalty_balance_basis") or cfg.get("penalty_balance_basis") or "Arrears"
                            col_q, col_b, col_p = st.columns(3)
                            with col_q:
                                penalty_quotation_p = st.radio("Quotation", ["Absolute Rate", "Margin"], index=0 if pq == "Absolute Rate" else 1, key=f"pedit_pq_{pid}")
                            with col_b:
                                penalty_balance_p = st.radio("Balance for penalty interest", ["Arrears", "Balance"], index=0 if pb == "Arrears" else 1, key=f"pedit_pb_{pid}")
                            with col_p:
                                pen_value = st.number_input("Default penalty %", 0.0, 100.0, float(pr.get(lt, 2)), step=0.5, key=f"pedit_pen_{pid}")
                            if st.button("Save Penalty", key=f"pedit_save_pen_{pid}"):
                                merge = dict(p_cfg)
                                merge["penalty_interest_quotation"] = penalty_quotation_p
                                merge["penalty_balance_basis"] = penalty_balance_p
                                merge["penalty_rates"] = {**(merge.get("penalty_rates") or {}), lt: pen_value}
                                if save_product_config_to_db(prod["code"], merge):
                                    st.success("Saved.")
                                    st.rerun()
                        with p_ccy_tab:
                            base_p = p_cfg.get("base_currency") or cfg.get("base_currency", "USD")
                            acc_p = p_cfg.get("accepted_currencies") or cfg.get("accepted_currencies", [base_p])
                            if isinstance(acc_p, list):
                                acc_p = ",".join(acc_p)
                            def_ccy_map = p_cfg.get("loan_default_currencies") or cfg.get("loan_default_currencies") or {}
                            def_ccy = def_ccy_map.get(prod.get("loan_type"), base_p)
                            base_in = st.text_input("Base currency", value=base_p, max_chars=8, key=f"pedit_base_{pid}")
                            acc_in = st.text_input("Accepted (comma)", value=acc_p if isinstance(acc_p, str) else ",".join(acc_p), key=f"pedit_acc_{pid}")
                            list_acc = [c.strip().upper() for c in (acc_in or base_in or "USD").split(",") if c.strip()] or [base_in or "USD"]
                            base_val = (base_in or "USD").strip().upper()
                            if base_val and base_val not in list_acc:
                                list_acc.insert(0, base_val)
                            def_in = st.selectbox("Default currency (this product)", list_acc, index=list_acc.index(def_ccy) if def_ccy in list_acc else 0, key=f"pedit_defccy_{pid}")
                            if st.button("Save Currency config", key=f"pedit_save_ccy_{pid}"):
                                merge = dict(p_cfg)
                                merge["base_currency"] = (base_in or "USD").strip().upper()
                                merge["accepted_currencies"] = list_acc
                                merge["loan_default_currencies"] = {**(merge.get("loan_default_currencies") or {}), prod["loan_type"]: def_in}
                                if save_product_config_to_db(prod["code"], merge):
                                    st.success("Saved.")
                                    st.rerun()
                        with p_wf_tab:
                            wf = p_cfg.get("payment_waterfall") or cfg.get("payment_waterfall", "Standard")
                            wf_choice = st.radio("Waterfall profile", ["Standard", "Borrower-friendly"], index=0 if wf.startswith("Standard") else 1, key=f"pedit_wf_{pid}")
                            if st.button("Save Waterfall config", key=f"pedit_save_wf_{pid}"):
                                merge = dict(p_cfg)
                                merge["payment_waterfall"] = wf_choice
                                if save_product_config_to_db(prod["code"], merge):
                                    st.success("Saved.")
                                    st.rerun()
                        with p_sus_tab:
                            sus = p_cfg.get("suspension_logic") or cfg.get("suspension_logic", "Manual")
                            auto_days = p_cfg.get("suspension_auto_days") or cfg.get("suspension_auto_days", 90)
                            cur = p_cfg.get("curing_logic") or cfg.get("curing_logic", "Curing")
                            
                            st.markdown("### Interest in Suspense")
                            sus_choice = st.radio("Suspension logic", ["Manual", "Automatic"], index=0 if sus == "Manual" else 1, key=f"pedit_sus_{pid}")
                            auto_days_choice = st.number_input("Days overdue for auto-suspension", min_value=1, value=int(auto_days), key=f"pedit_sus_days_{pid}") if sus_choice == "Automatic" else 90
                            
                            st.markdown("### Curing")
                            cur_choice = st.radio("Curing logic", ["Curing", "Yo-Yoing"], index=0 if cur == "Curing" else 1, key=f"pedit_cur_{pid}")
                            
                            if st.button("Save Suspension & curing", key=f"pedit_save_sus_{pid}"):
                                merge = dict(p_cfg)
                                merge["suspension_logic"] = sus_choice
                                if sus_choice == "Automatic":
                                    merge["suspension_auto_days"] = auto_days_choice
                                merge["curing_logic"] = cur_choice
                                if save_product_config_to_db(prod["code"], merge):
                                    st.success("Saved.")
                                    st.rerun()
                    else:
                        st.caption("No product with that ID.")
            else:
                st.info("No products yet. Add one below.")
                with st.expander("Add product", expanded=True):
                    p_code = st.text_input("Code", key="prod_add_code_empty", max_chars=32, placeholder="e.g. TL-USD")
                    p_name = st.text_input("Name", key="prod_add_name_empty", placeholder="Display name")
                    p_lt = st.selectbox("Loan type", ["term_loan", "consumer_loan", "bullet_loan", "customised_repayments"], key="prod_add_lt_empty")
                    if st.button("Create", key="prod_add_btn_empty") and p_code and p_name:
                        code_upper = p_code.strip().upper()
                        if get_product_by_code(code_upper):
                            st.error("Product code already exists.")
                        else:
                            try:
                                create_product(code_upper, p_name.strip(), p_lt)
                                st.success(f"Product **{code_upper}** created.")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))

    with tab_ifrs_prov:
        st.subheader("IFRS provision configuration")
        st.caption(
            "Collateral security subtypes, haircuts, and PD bands by DPD. Used by **Portfolio reports** "
            "(ECL / IFRS view and single-loan IFRS Provisions)."
        )
        from provisions_ui import render_provisions_config_tables

        render_provisions_config_tables()
        st.divider()
        st.markdown("**Regulatory (RBZ / non-IFRS)**")
        st.info(
            "Reserve Bank and other provisions that **do not** use the IFRS security haircut model "
            "will be added here when required."
        )

    # Keep existing config from DB; only EOD settings and accounting periods are edited directly in this UI.
    st.session_state["system_config"] = {
        **cfg,
        "eod_settings": {
            "mode": eod_mode,
            "automatic_time": eod_time,
            "tasks": {
                "run_loan_engine": True,
                "post_accounting_events": eod_tasks.get("post_accounting_events", False),
                "generate_statements": eod_tasks.get("generate_statements", False),
                "snapshot_financial_statements": eod_tasks.get("snapshot_financial_statements", True),
                "send_notifications": eod_tasks.get("send_notifications", False),
            },
            "stage_policy": {
                "mode": policy_mode,
                "blocking_stages": blocking_stages,
                "advance_date_on_degraded": bool(advance_date_on_degraded),
            },
        },
        "accounting_periods": {
            "month_end_mode": month_mode,
            "month_end_day": int(month_day),
            "fiscal_year_end_month": int(fiscal_year_end_month),
            "snapshot_max_rows": int(snapshot_max_rows),
        },
    }
    st.divider()
    if st.button("Update System Configurations", type="primary", key="syscfg_save_db"):
        try:
            from loan_management import save_system_config_to_db
            if save_system_config_to_db(st.session_state["system_config"]):
                st.success("System configurations saved to database for future reference.")
            else:
                st.error("Failed to save to database.")
        except Exception as e:
            st.error(f"Failed to save: {e}")


# --- MAIN APP ---

def eod_ui():
    """End-of-day processing configuration and manual run."""
    from eod import run_eod_for_date, run_single_loan_eod
    from eod_audit import is_another_eod_session_active
    from system_business_date import get_system_business_config, run_eod_process

    st.markdown(
        "<div style='color:#16A34A; font-weight:700; font-size:2rem; margin:0.25rem 0 0.75rem 0;'>End of day</div>",
        unsafe_allow_html=True,
    )

    sb_cfg = get_system_business_config()
    current_system_date = sb_cfg["current_system_date"]
    next_date = current_system_date + timedelta(days=1)

    d_col1, d_col2 = st.columns([1, 2])
    with d_col1:
        st.caption(f"Calendar date: {datetime.now().strftime('%Y-%m-%d')}")
    with d_col2:
        st.markdown(
            f"<div style='font-size: 1.6rem; font-weight: 700; text-align: right;'>System date: {current_system_date.isoformat()}</div>",
            unsafe_allow_html=True,
        )

    cfg = _get_system_config()
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
        eod_busy = False
        try:
            eod_busy = is_another_eod_session_active()
        except Exception:
            eod_busy = False
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

        st.subheader("Run EOD (advance system date)")
        st.caption(
            "Runs EOD for the current system date. On success, system date advances by +1 day. "
            "Accruals and Amount Due logic use the system date, not the calendar."
        )

        from loan_management import _connection
        from psycopg2.extras import RealDictCursor
        loans_with_state = 0
        active_loans = 0
        try:
            with _connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT loan_id) AS n
                        FROM loan_daily_state
                        WHERE as_of_date = %s
                        """,
                        (current_system_date,),
                    )
                    row = cur.fetchone() or {}
                    loans_with_state = int(row.get("n") or 0)
                    cur.execute(
                        "SELECT COUNT(*) AS n FROM loans WHERE status = 'active'"
                    )
                    row2 = cur.fetchone() or {}
                    active_loans = int(row2.get("n") or 0)
        except Exception:
            pass

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
            msg = (
                f"EOD completed for {last_eod['as_of_date']} "
                f"(status: {status_txt}). "
                f"System date advanced to {last_eod['new_system_date']}. "
                f"Real-world: {last_eod['real_world_time']}"
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
                result = run_eod_process()
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
            st.subheader("Backfill EOD (specific date, no system date advance)")
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
                        result = run_eod_for_date(backfill_date)
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
        st.subheader("Manual EOD run")
        st.info(
            "EOD is configured for **automatic** mode. Manual runs are disabled here. "
            "Use your scheduling/ops tooling to trigger EOD."
        )

    if fix_eod_issues:
        # Available in both manual and automatic EOD modes — does not advance system date.
        st.subheader("Reallocate receipts")
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
        if not _loan_management_available:
            st.warning(f"Loan management unavailable: {_loan_management_error}")
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
                    ids = get_repayment_ids_for_loan_and_date(int(realloc_loan), realloc_vd)
                    if not ids:
                        st.warning(
                            f"No posted receipts for loan_id={int(realloc_loan)} on {realloc_vd.isoformat()}."
                        )
                    else:
                        with st.spinner(f"Reallocating {len(ids)} receipt(s)…"):
                            ok, err = [], []
                            for rid in ids:
                                try:
                                    reallocate_repayment(rid, system_config=cfg)
                                    ok.append(rid)
                                except Exception as ex:
                                    err.append((rid, str(ex)))
                        if ok:
                            st.success(f"Reallocated repayment_id(s): {ok}")
                        if err:
                            for rid, msg in err:
                                st.error(f"repayment_id={rid}: {msg}")
                except Exception as e:
                    st.error(str(e))

            if run_by_ids:
                raw = (realloc_ids_text or "").replace(",", "\n").splitlines()
                parsed: list[int] = []
                bad_token = None
                for line in raw:
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        parsed.append(int(s))
                    except ValueError:
                        bad_token = s
                        break
                if bad_token is not None:
                    st.error(f"Not an integer: {bad_token!r}")
                elif not parsed:
                    st.warning("Enter at least one repayment ID.")
                else:
                    cfg = load_system_config_from_db() or {}
                    with st.spinner(f"Reallocating {len(parsed)} receipt(s)…"):
                        ok, err = [], []
                        for rid in parsed:
                            try:
                                reallocate_repayment(rid, system_config=cfg)
                                ok.append(rid)
                            except Exception as ex:
                                err.append((rid, str(ex)))
                    if ok:
                        st.success(f"Reallocated repayment_id(s): {ok}")
                    if err:
                        for rid, msg in err:
                            st.error(f"repayment_id={rid}: {msg}")

    if fix_eod_issues:
        st.caption(
            "**Reallocate** only works when there is at least one receipt for that date. "
            "If all receipts were deleted or you need to refresh `loan_daily_state` from the "
            "engine and prior day, use this instead (runs `run_single_loan_eod`)."
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
            cfg = load_system_config_from_db() or {}
            try:
                with st.spinner(f"Running engine for loan_id={int(rl_loan)} on {rl_date}…"):
                    run_single_loan_eod(int(rl_loan), rl_date, sys_cfg=cfg)
                st.success(
                    f"Updated `loan_daily_state` for loan_id={int(rl_loan)} as of {rl_date}."
                )
            except Exception as ex:
                st.error(str(ex))


def consumer_loan_ui():
    schemes = _get_consumer_schemes()
    scheme_names = [s["name"] for s in schemes]
    cfg = _get_system_config()
    default_additional_rate_pct = cfg.get("consumer_default_additional_rate_pct", 0.0)

    st.subheader("Consumer Loan Parameters")
    # Currency selection with system default + override
    accepted_currencies = cfg.get(
        "accepted_currencies", [cfg.get("base_currency", "USD")]
    )
    loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
    default_ccy = loan_curr_cfg.get("consumer_loan", cfg.get("base_currency", "USD"))
    if default_ccy not in accepted_currencies:
        accepted_currencies = [default_ccy, *accepted_currencies]
    glob = _get_global_loan_settings()
    scheme_options = scheme_names + ["Other"]
    p_col1, p_col2 = st.columns(2)
    with p_col1:
        currency = st.selectbox(
            "Currency",
            accepted_currencies,
            index=accepted_currencies.index(default_ccy)
            if default_ccy in accepted_currencies
            else 0,
            key="cl_currency",
        )
        principal_input_choice = st.radio(
            "What are you entering?",
            ["Net proceeds", "Principal (total loan amount)"],
            key="cl_principal_input",
        )
        input_total_facility = principal_input_choice == "Principal (total loan amount)"
        loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
        loan_required = st.number_input(
            loan_input_label,
            min_value=0.0,
            value=140.0,
            step=10.0,
            format="%.2f",
            key="cl_principal",
        )
        loan_term = st.number_input(
            "Term (Months)",
            min_value=1,
            max_value=60,
            value=6,
            step=1,
            key="cl_term",
        )
    with p_col2:
        st.caption("Schemes and default rates are managed in **System configurations**.")
        scheme = st.selectbox("Loan Scheme", scheme_options, key="cl_scheme")
        disbursement_input = st.date_input("Disbursement date", _get_system_date(), key="cl_start")
        disbursement_date = datetime.combine(disbursement_input, datetime.min.time())
        default_first_rep = add_months(disbursement_date, 1).date()
        first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="cl_first_rep")
        first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
        use_anniversary = st.radio(
            "Repayments on",
            ["Anniversary date (same day each month)", "Last day of each month"],
            key="cl_timing",
        ).startswith("Anniversary")
    if not use_anniversary and not is_last_day_of_month(first_repayment_date):
        st.error("When repayments are on last day of month, First Repayment Date must be the last day of that month.")

    # Future disbursement: prompt for additional rate when disbursement_date > next month
    today_normalized = datetime.combine(_get_system_date(), datetime.min.time()).replace(hour=0, minute=0, second=0, microsecond=0)
    next_month_limit = add_months(today_normalized, 1)
    additional_buffer_rate = 0.0

    if disbursement_date > next_month_limit:
        st.warning("Future date detected: additional interest rate applies per extra month.")
        additional_rate_pct = st.number_input(
            "Additional Monthly Rate (%) per extra month",
            min_value=0.0,
            max_value=100.0,
            value=float(default_additional_rate_pct),
            step=0.1,
            help="Rate applied for each month the disbursement date is beyond next month (0 is acceptable).",
            key="cl_add_rate",
        )
        months_excess = max(
            0,
            (disbursement_date.year - next_month_limit.year) * 12
            + (disbursement_date.month - next_month_limit.month),
        )
        additional_buffer_rate = (additional_rate_pct / 100.0) * months_excess

    # Base rates: from selected scheme or manual entry for Other
    if scheme != "Other":
        sch = next((s for s in schemes if s["name"] == scheme), None)
        base_rate = (sch["interest_rate_pct"] / 100.0) if sch else 0.07
        admin_fee = (sch["admin_fee_pct"] / 100.0) if sch else 0.07
    else:
        o_col1, o_col2 = st.columns(2)
        with o_col1:
            interest_rate_percent = st.number_input(
                "Interest rate (%)",
                min_value=0.0,
                max_value=100.0,
                value=0.0,
                step=0.1,
                key="cl_other_rate",
            )
        with o_col2:
            admin_fee_percent = st.number_input(
                "Administration fee (%)",
                min_value=0.0,
                max_value=100.0,
                value=0.0,
                step=0.1,
                key="cl_other_admin",
            )
        has_error = False
        if interest_rate_percent <= 0.0:
            st.error("Please enter an interest rate greater than 0% for the 'Other' scheme.")
            has_error = True
        if admin_fee_percent <= 0.0:
            st.error("Please enter an administration fee greater than 0% for the 'Other' scheme.")
            has_error = True
        if has_error:
            return
        base_rate = interest_rate_percent / 100.0
        admin_fee = admin_fee_percent / 100.0

    flat_rate = glob.get("interest_method") == "Flat rate"
    if not use_anniversary and not is_last_day_of_month(first_repayment_date):
        return
    details, df_schedule = compute_consumer_schedule(
        loan_required, loan_term, disbursement_date, base_rate, admin_fee, input_total_facility,
        glob.get("rate_basis", "Per month"), flat_rate, scheme=scheme,
        additional_monthly_rate=additional_buffer_rate,
        first_repayment_date=first_repayment_date, use_anniversary=use_anniversary,
    )
    details["currency"] = currency
    total_facility = details["principal"]
    amount_required_display = details["disbursed_amount"]
    total_monthly_rate = details["monthly_rate"]
    monthly_installment = details["installment"]
    end_date = details["end_date"]
    first_repayment_date = details["first_repayment_date"]

    st.markdown(
        "<div style='background-color: #16A34A; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Consumer Loan Calculator</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    calc_css = """
    <style>
    .calc-desc { font-size: 0.85rem; color: #64748B; margin-top: 2px; margin-bottom: 8px; }
    .calc-value-red { color: #DC2626; font-weight: bold; }
    </style>
    """
    st.markdown(calc_css, unsafe_allow_html=True)

    st.markdown(f"**a. Scheme:** {scheme}")
    st.markdown(f"**b. Net proceeds:** {amount_required_display:,.2f} US Dollars")
    st.markdown(f"**c. Interest Rate (% per month):** {total_monthly_rate * 100:.2f}%")
    st.markdown(
        f"<p class='calc-desc'>"
        f"{total_monthly_rate * 100:.2f}% per month accrued from day to day on principal balance"
        "</p>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**d. Administration Fees (%):** {admin_fee * 100:.2f}%")
    st.markdown(
        f"<p class='calc-desc'>"
        f"{admin_fee * 100:.2f}% once-off on total loan amount"
        "</p>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**e. Principal (total loan amount):** {total_facility:,.2f} US Dollars")
    st.markdown(
        f"<span class='calc-value-red'><strong>f. Monthly Instalment:</strong> US${monthly_installment:,.2f}</span>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**g. Disbursement date:** {disbursement_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**h. Loan Term (months):** {loan_term}")
    st.markdown(f"**j. First Repayment Date:** {first_repayment_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**k. No. of Repayments:** {loan_term} times")
    st.markdown(f"**i. End Date:** {end_date.strftime('%d-%b-%Y')}")

    with st.expander("Notes", expanded=False):
        st.markdown(
            "<div style='background-color: #F1F5F9; padding: 12px 16px; border-radius: 4px;'>"
            "<strong>Notes</strong><br>"
            "1. Select Scheme (a.). If the loan does not fall under a Scheme, select \"Other\"<br>"
            "2. Enter net proceeds in (b) or principal (total loan amount)<br>"
            "3. If you selected \"Other\", enter interest rate (c.) and administration fee (d.)<br>"
            "4. Enter the Loan Term in months (h.)<br>"
            "5. Monthly repayment (f.) assumes every month has 30 days<br>"
            "6. Default rates and schemes are in **System configurations**"
            "</div>",
            unsafe_allow_html=True,
        )
    with st.expander("Repayment schedule and downloads", expanded=False):
        st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True, height=240)
        _schedule_export_downloads(df_schedule, file_stem="consumer_loan_schedule", key_prefix="dl_sched_consumer")

    # 6. Save button - DB-ready structure (from shared engine)
    loan_record = {**details, "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
    for k in ("disbursement_date", "start_date", "end_date", "first_repayment_date"):
        if k in loan_record and hasattr(loan_record[k], "isoformat"):
            loan_record[k] = loan_record[k].isoformat()

    with st.expander("Save to system", expanded=False):
        if st.button("Save Loan Record to System", type="primary", key="cl_save"):
            # TODO: Replace with db.insert(loan_record) when DB is ready
            st.success(f"Loan for ${loan_required:,.2f} has been prepared for database sync.")
            with st.expander("Preview record (for DB insertion)"):
                st.json(loan_record)


def term_loan_ui():
    glob = _get_global_loan_settings()
    cfg = _get_system_config()
    # Optional product selector for calculator defaults (safe fallback to system defaults).
    rate_basis = glob.get("rate_basis", "Per month")
    product_cfg: dict[str, Any] = {}
    product_opts = []
    try:
        all_products = list_products(active_only=True) if _loan_management_available else []
        product_opts = [
            p for p in (all_products or [])
            if str(p.get("loan_type") or "").strip().lower() == "term_loan"
        ]
    except Exception:
        product_opts = []

    st.subheader("Term Loan Parameters")
    if product_opts:
        prod_labels = ["System defaults"] + [f"{p.get('code')} - {p.get('name')}" for p in product_opts]
        selected_label = st.selectbox("Product (optional)", prod_labels, key="term_product_pick")
        if selected_label != "System defaults":
            picked = product_opts[prod_labels.index(selected_label) - 1]
            product_code = str(picked.get("code") or "").strip()
            if product_code:
                product_cfg = get_product_config_from_db(product_code) or {}
                rate_basis = _get_product_rate_basis(product_cfg, fallback=rate_basis)

    # Currency selection with system default + override
    accepted_currencies = cfg.get(
        "accepted_currencies", [cfg.get("base_currency", "USD")]
    )
    loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
    default_ccy = loan_curr_cfg.get("term_loan", cfg.get("base_currency", "USD"))
    if default_ccy not in accepted_currencies:
        accepted_currencies = [default_ccy, *accepted_currencies]
    p_col1, p_col2 = st.columns(2)
    with p_col1:
        currency = st.selectbox(
            "Currency",
            accepted_currencies,
            index=accepted_currencies.index(default_ccy)
            if default_ccy in accepted_currencies
            else 0,
            key="term_currency",
        )
        principal_input_choice = st.radio(
            "What are you entering?",
            ["Net proceeds", "Principal (total loan amount)"],
            key="term_principal_input",
        )
        input_total_facility = principal_input_choice == "Principal (total loan amount)"
        loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
        loan_required = st.number_input(
            loan_input_label,
            min_value=0.0,
            value=1000.0,
            step=100.0,
            format="%.2f",
            key="term_principal",
        )
        loan_term = st.number_input(
            "Term (Months)",
            min_value=1,
            max_value=120,
            value=24,
            step=1,
            key="term_months",
        )
    with p_col2:
        disbursement_input = st.date_input("Disbursement date", _get_system_date(), key="term_disb")
        disbursement_date = datetime.combine(disbursement_input, datetime.min.time())

    # Term loan: defaults from selected product (if any), else system config; always safe.
    dr_sys = cfg.get("default_rates", {}).get("term_loan", {}) or {}
    dr_prod = (product_cfg.get("default_rates") or {}).get("term_loan") or {}
    dr = {**dr_sys, **dr_prod}
    default_interest = float(dr.get("interest_pct") or 7.0)
    default_drawdown = float(dr.get("drawdown_pct") or 2.5)
    default_arrangement = float(dr.get("arrangement_pct") or 2.5)
    rate_label = "Interest rate (% per annum)" if rate_basis == "Per annum" else "Interest rate (% per month)"
    fee_col1, fee_col2 = st.columns(2)
    with fee_col1:
        rate_pct = st.number_input(rate_label, 0.0, 100.0, default_interest, step=0.1, key="term_rate")
        drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, default_drawdown, step=0.1, key="term_drawdown") / 100.0
    with fee_col2:
        arrangement_fee_pct = st.number_input("Arrangement fee (%)", 0.0, 100.0, default_arrangement, step=0.1, key="term_arrangement") / 100.0
    total_fee = drawdown_fee_pct + arrangement_fee_pct
    if rate_pct <= 0:
        st.error("Please enter an interest rate greater than 0%.")
        return
    if total_fee < 0:
        st.error("Total of Drawdown and Arrangement fees cannot be negative.")
        return

    if input_total_facility:
        total_facility = loan_required
    else:
        total_facility = loan_required / (1.0 - total_fee)
    # Rate basis: per annum -> annual_rate = rate_pct/100; per month -> annual_rate = (rate_pct/100)*12
    annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
    flat_rate = glob.get("interest_method") == "Flat rate"

    # Grace period + repayment timing
    g_col1, g_col2 = st.columns(2)
    with g_col1:
        st.markdown("**Grace Period**")
        grace_type = st.radio(
            "Grace period type",
            ["No grace period", "Principal moratorium", "Principal and interest moratorium"],
            key="term_grace",
        )
        moratorium_months = 0
        if "Principal moratorium" in grace_type:
            moratorium_months = st.number_input("Moratorium length (months)", 1, 60, 3, key="term_moratorium_p")
        elif "Principal and interest" in grace_type:
            moratorium_months = st.number_input("Moratorium length (months)", 1, 60, 3, key="term_moratorium_pi")
    with g_col2:
        default_first_rep = add_months(disbursement_date, 1).date()
        first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="term_first_rep")
        first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
        st.markdown("**Repayment Timing**")
        use_anniversary = st.radio(
            "Repayments on",
            ["Anniversary date (same day each month)", "Last day of each month"],
            key="term_timing",
        ).startswith("Anniversary")

    today_norm = datetime.combine(_get_system_date(), datetime.min.time()).replace(hour=0, minute=0, second=0, microsecond=0)
    next_month_limit = add_months(today_norm, 1)

    if grace_type == "No grace period" and first_repayment_date > next_month_limit:
        st.error("No grace period: First Repayment Date must not be greater than next month.")
        return

    if "Principal" in grace_type and moratorium_months >= loan_term:
        st.error("Moratorium length must be less than loan term.")
        return

    if not use_anniversary and not is_last_day_of_month(first_repayment_date):
        last_day = days_in_month(first_repayment_date.year, first_repayment_date.month)
        example = datetime(first_repayment_date.year, first_repayment_date.month, last_day).strftime("%d-%b-%Y")
        st.error(
            "When repayments are on the **last day of each month**, the First Repayment Date must be the last day of its month. "
            f"For {first_repayment_date.strftime('%B %Y')} the last day is **{example}**."
        )
        return

    details, df_schedule = compute_term_schedule(
        loan_required, loan_term, disbursement_date, rate_pct, drawdown_fee_pct, arrangement_fee_pct,
        input_total_facility, grace_type, moratorium_months, first_repayment_date, use_anniversary,
        rate_basis, flat_rate,
    )
    details["currency"] = currency
    installment = details["installment"]
    end_date = details["end_date"]

    # Display
    st.markdown(
        "<div style='background-color: #16A34A; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Term Loan Calculator (Actual/360)</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    calc_css = """
    <style>
    .calc-desc { font-size: 0.85rem; color: #64748B; margin-top: 2px; margin-bottom: 8px; }
    .calc-value-red { color: #DC2626; font-weight: bold; }
    </style>
    """
    st.markdown(calc_css, unsafe_allow_html=True)

    net_proceeds_to_display = float(details.get("disbursed_amount", loan_required) or 0.0)
    st.markdown(f"**a. Net proceeds:** {net_proceeds_to_display:,.2f} US Dollars")
    st.markdown(f"**b. Interest Rate (annual, Actual/360):** {details['annual_rate'] * 100:.2f}%")
    st.markdown(
        "<p class='calc-desc'>Interest accrued on actual days / 360 basis</p>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**c. Drawdown fee (%):** {drawdown_fee_pct * 100:.2f}% | **Arrangement fee (%):** {arrangement_fee_pct * 100:.2f}%")
    total_fee = drawdown_fee_pct + arrangement_fee_pct
    st.markdown(
        f"<p class='calc-desc'>Total {total_fee * 100:.2f}% once-off on total facility</p>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**d. Principal (total loan amount):** {details['principal']:,.2f} US Dollars")
    st.markdown(
        f"<span class='calc-value-red'><strong>e. Installment (from first P&I period):</strong> US${installment:,.2f}</span>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**f. Disbursement Date:** {disbursement_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**g. Loan Term (months):** {loan_term}")
    st.markdown(f"**h. First Repayment Date:** {first_repayment_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**i. Grace period:** {grace_type}")
    st.markdown(f"**j. End Date:** {end_date.strftime('%d-%b-%Y')}")

    with st.expander("Repayment schedule and downloads", expanded=False):
        st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True, height=240)
        _schedule_export_downloads(df_schedule, file_stem="term_loan_schedule", key_prefix="dl_sched_term")

    loan_record = {**details, "loan_type": "term_loan", "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
    for k in ("disbursement_date", "start_date", "end_date", "first_repayment_date"):
        if k in loan_record and hasattr(loan_record[k], "isoformat"):
            loan_record[k] = loan_record[k].isoformat()

    with st.expander("Save to system", expanded=False):
        if st.button("Save Term Loan Record to System", type="primary", key="term_save"):
            st.success(f"Term loan for ${loan_required:,.2f} has been prepared for database sync.")
            with st.expander("Preview record (for DB insertion)"):
                st.json(loan_record)


def bullet_loan_ui():
    glob = _get_global_loan_settings()
    cfg = _get_system_config()
    st.subheader("Bullet Loan Parameters")
    # Currency selection with system default + override
    accepted_currencies = cfg.get(
        "accepted_currencies", [cfg.get("base_currency", "USD")]
    )
    loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
    default_ccy = loan_curr_cfg.get("bullet_loan", cfg.get("base_currency", "USD"))
    if default_ccy not in accepted_currencies:
        accepted_currencies = [default_ccy, *accepted_currencies]
    p_col1, p_col2 = st.columns(2)
    with p_col1:
        currency = st.selectbox(
            "Currency",
            accepted_currencies,
            index=accepted_currencies.index(default_ccy)
            if default_ccy in accepted_currencies
            else 0,
            key="bullet_currency",
        )
        principal_input_choice = st.radio(
            "What are you entering?",
            ["Net proceeds", "Principal (total loan amount)"],
            key="bullet_principal_input",
        )
        input_total_facility = principal_input_choice == "Principal (total loan amount)"
        loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
        loan_required = st.number_input(
            loan_input_label,
            min_value=0.0,
            value=1000.0,
            step=100.0,
            format="%.2f",
            key="bullet_principal",
        )
        loan_term = st.number_input(
            "Term (Months)",
            min_value=1,
            max_value=120,
            value=12,
            step=1,
            key="bullet_term",
        )
    with p_col2:
        bullet_type = st.radio(
            "Bullet type",
            ["Straight bullet (no interim payments)", "Bullet with interest payments"],
            key="bullet_type",
        )
        disbursement_input = st.date_input("Disbursement date", _get_system_date(), key="bullet_disb")
        disbursement_date = datetime.combine(disbursement_input, datetime.min.time())

    dr = cfg.get("default_rates", {}).get("bullet_loan", {})
    rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
    f_col1, f_col2 = st.columns(2)
    with f_col1:
        rate_pct = st.number_input(rate_label, min_value=0.0, max_value=100.0, value=float(dr.get("interest_pct", 7.0)), step=0.1, key="bullet_rate")
        drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="bullet_drawdown") / 100.0
    with f_col2:
        arrangement_fee_pct = st.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct", 2.5)), step=0.1, key="bullet_arrangement") / 100.0
    total_fee = drawdown_fee_pct + arrangement_fee_pct
    if rate_pct <= 0:
        st.error("Please enter an interest rate greater than 0%.")
        return
    if total_fee < 0:
        st.error("Total of Drawdown and Arrangement fees cannot be negative.")
        return

    flat_rate = glob.get("interest_method") == "Flat rate"
    first_repayment_date = None
    use_anniversary = True
    if "with interest" in bullet_type:
        t_col1, t_col2 = st.columns(2)
        with t_col1:
            default_first_rep = add_months(disbursement_date, 1).date()
            first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="bullet_first_rep")
            first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
        with t_col2:
            use_anniversary = st.radio(
                "Interest payments on",
                ["Anniversary date (same day each month)", "Last day of each month"],
                key="bullet_timing",
            ).startswith("Anniversary")
        if not use_anniversary and not is_last_day_of_month(first_repayment_date):
            last_day = days_in_month(first_repayment_date.year, first_repayment_date.month)
            example = datetime(first_repayment_date.year, first_repayment_date.month, last_day).strftime("%d-%b-%Y")
            st.error(
                "When interest payments are on the **last day of each month**, the First Repayment Date must be the last day of its month. "
                f"For {first_repayment_date.strftime('%B %Y')} the last day is **{example}**."
            )
            return

    details, df_schedule = compute_bullet_schedule(
        loan_required, loan_term, disbursement_date, rate_pct, drawdown_fee_pct, arrangement_fee_pct,
        input_total_facility, bullet_type, first_repayment_date, use_anniversary,
        glob.get("rate_basis", "Per month"), flat_rate,
    )
    details["currency"] = currency

    # Display
    st.markdown(
        "<div style='background-color: #16A34A; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Bullet Loan Calculator (Actual/360)</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    calc_css = """
    <style>
    .calc-desc { font-size: 0.85rem; color: #64748B; margin-top: 2px; margin-bottom: 8px; }
    .calc-value-red { color: #DC2626; font-weight: bold; }
    </style>
    """
    st.markdown(calc_css, unsafe_allow_html=True)

    net_proceeds_to_display = float(details.get("disbursed_amount", loan_required) or 0.0)
    st.markdown(f"**a. Net proceeds:** {net_proceeds_to_display:,.2f} US Dollars")
    st.markdown(f"**b. Interest Rate (annual, Actual/360):** {details['annual_rate'] * 100:.2f}%")
    st.markdown("<p class='calc-desc'>Interest on actual days / 360 basis</p>", unsafe_allow_html=True)
    st.markdown(f"**c. Drawdown fee (%):** {drawdown_fee_pct * 100:.2f}% | **Arrangement fee (%):** {arrangement_fee_pct * 100:.2f}%")
    st.markdown(f"<p class='calc-desc'>Total {total_fee * 100:.2f}% once-off on total facility</p>", unsafe_allow_html=True)
    st.markdown(f"**d. Principal (total loan amount):** {details['principal']:,.2f} US Dollars")
    st.markdown(
        f"<span class='calc-value-red'><strong>e. Total payment at maturity:</strong> US${details['total_payment']:,.2f}</span>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**f. Disbursement Date:** {disbursement_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**g. Term (months):** {loan_term}")
    st.markdown(f"**h. End date:** {details['end_date'].strftime('%d-%b-%Y')}")
    if details.get("first_repayment_date") is not None:
        st.markdown(f"**i. First interest payment:** {details['first_repayment_date'].strftime('%d-%b-%Y')}")

    with st.expander("Repayment schedule and downloads", expanded=False):
        st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True, height=240)
        _schedule_export_downloads(df_schedule, file_stem="bullet_loan_schedule", key_prefix="dl_sched_bullet")

    loan_record = {**details, "loan_type": "bullet_loan", "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
    for k in ("disbursement_date", "end_date", "first_repayment_date"):
        if k in loan_record and loan_record[k] is not None and hasattr(loan_record[k], "isoformat"):
            loan_record[k] = loan_record[k].isoformat()

    with st.expander("Save to system", expanded=False):
        if st.button("Save Bullet Loan Record to System", type="primary", key="bullet_save"):
            st.success(f"Bullet loan for ${net_proceeds_to_display:,.2f} has been prepared for database sync.")
            with st.expander("Preview record (for DB insertion)"):
                st.json(loan_record)


def customised_repayments_ui():
    glob = _get_global_loan_settings()
    cfg = _get_system_config()
    flat_rate = glob.get("interest_method") == "Flat rate"

    st.subheader("Customised Repayments Parameters")
    # Currency selection with system default + override
    accepted_currencies = cfg.get(
        "accepted_currencies", [cfg.get("base_currency", "USD")]
    )
    loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
    default_ccy = loan_curr_cfg.get(
        "customised_repayments", cfg.get("base_currency", "USD")
    )
    if default_ccy not in accepted_currencies:
        accepted_currencies = [default_ccy, *accepted_currencies]
    p_col1, p_col2 = st.columns(2)
    with p_col1:
        currency = st.selectbox(
            "Currency",
            accepted_currencies,
            index=accepted_currencies.index(default_ccy)
            if default_ccy in accepted_currencies
            else 0,
            key="cust_currency",
        )
        principal_input_choice = st.radio(
            "What are you entering?",
            ["Net proceeds", "Principal (total loan amount)"],
            key="cust_principal_input",
        )
        input_total_facility = principal_input_choice == "Principal (total loan amount)"
        loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
        loan_required = st.number_input(
            loan_input_label,
            min_value=0.0,
            value=1000.0,
            step=100.0,
            format="%.2f",
            key="cust_principal",
        )
        loan_term = st.number_input(
            "Term (Months)",
            min_value=1,
            max_value=120,
            value=12,
            step=1,
            key="cust_term",
        )
    with p_col2:
        disbursement_input = st.date_input("Disbursement date", _get_system_date(), key="cust_start")
        disbursement_date = datetime.combine(disbursement_input, datetime.min.time())
        irregular_calc = st.checkbox("Irregular", value=False, key="cust_irregular", help="Allow editing dates and adding rows; schedule recomputes from table.")
        use_anniversary = st.radio(
            "Repayments on",
            ["Anniversary date (same day each month)", "Last day of each month"],
            key="cust_timing",
        ).startswith("Anniversary")
    default_first_rep = add_months(disbursement_date, 1).date()
    if not use_anniversary:
        default_first_rep = default_first_rep.replace(day=days_in_month(default_first_rep.year, default_first_rep.month))
    existing_cust = st.session_state.get("customised_repayments_df")
    first_rep_calc = _first_repayment_from_customised_table(existing_cust) if existing_cust is not None and len(existing_cust) > 1 else None
    first_rep_display_calc = (first_rep_calc.date() if first_rep_calc else default_first_rep)
    st.date_input("First repayment date (from table)", first_rep_display_calc, key="cust_first_rep", disabled=True, help="From first row with non-zero payment.")
    first_repayment_date = datetime.combine(first_rep_display_calc, datetime.min.time())
    dr = cfg.get("default_rates", {}).get("customised_repayments", {})
    rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
    f_col1, f_col2 = st.columns(2)
    with f_col1:
        rate_pct = st.number_input(rate_label, 0.0, 100.0, float(dr.get("interest_pct", 7.0)), step=0.1, key="cust_rate")
        drawdown_fee_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="cust_drawdown") / 100.0
    with f_col2:
        arrangement_fee_pct = st.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct", 2.5)), step=0.1, key="cust_arrangement") / 100.0
    total_fee = drawdown_fee_pct + arrangement_fee_pct
    if total_fee < 0:
        st.error("Total of Drawdown and Arrangement fees cannot be negative.")
        return

    if input_total_facility:
        total_facility = loan_required
    else:
        total_facility = loan_required / (1.0 - total_fee)
    annual_rate = (rate_pct / 100.0) * 12.0 if glob.get("rate_basis") == "Per month" else (rate_pct / 100.0)

    session_key = "customised_repayments_df"
    params_key = (round(total_facility, 2), loan_term, disbursement_date.strftime("%Y-%m-%d"), irregular_calc)
    if session_key not in st.session_state or st.session_state.get("customised_params") != params_key:
        st.session_state["customised_params"] = params_key
        schedule_dates_init = repayment_dates(disbursement_date, first_repayment_date, int(loan_term), use_anniversary)
        rows = [{"Period": 0, "Date": disbursement_date.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": round(total_facility, 2), "Total Outstanding": round(total_facility, 2)}]
        for i, dt in enumerate(schedule_dates_init, 1):
            rows.append({"Period": i, "Date": dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0})
        st.session_state[session_key] = pd.DataFrame(rows)

    df = st.session_state[session_key].copy()
    schedule_dates = parse_schedule_dates_from_table(df, start_date=disbursement_date)
    df = recompute_customised_from_payments(df, total_facility, schedule_dates, annual_rate, flat_rate, disbursement_date)
    st.session_state[session_key] = df

    st.markdown(
        "<div style='background-color: #16A34A; color: white; padding: 8px 12px; font-weight: bold; font-size: 1.1rem;'>Customised Repayments (Actual/360)</div>",
        unsafe_allow_html=True,
    )
    if irregular_calc:
        if st.button("Add row", key="cust_add_row"):
            last_df = st.session_state[session_key]
            if len(last_df) > 0:
                try:
                    last_date_str = str(last_df.at[len(last_df) - 1, "Date"]).strip()[:32]
                    last_dt = datetime.combine(datetime.strptime(last_date_str, "%d-%b-%Y").date(), datetime.min.time())
                except (ValueError, TypeError):
                    last_dt = add_months(disbursement_date, len(last_df))
                next_dt = add_months(last_dt, 1)
                if not use_anniversary:
                    next_dt = next_dt.replace(day=days_in_month(next_dt.year, next_dt.month))
                new_row = {"Period": len(last_df), "Date": next_dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0}
                st.session_state[session_key] = pd.concat([last_df, pd.DataFrame([new_row])], ignore_index=True)
                st.rerun()
        st.caption("Irregular: edit **Date** and **Payment**; add rows with the button above. Schedule recomputes from table dates.")
    else:
        st.caption("Edit the **Payment** column; interest and balances update automatically. Save only when the loan is fully cleared (Total Outstanding = $0).")
    date_editable_calc = irregular_calc
    with st.expander("Repayment editor and schedule", expanded=False):
        edited = st.data_editor(
            df,
            column_config={
                "Period": st.column_config.NumberColumn(disabled=True),
                "Date": st.column_config.TextColumn(disabled=not date_editable_calc, help="Format: DD-Mon-YYYY" if date_editable_calc else None),
                "Interest": st.column_config.NumberColumn(disabled=True, format="%.2f"),
                "Principal": st.column_config.NumberColumn(disabled=True, format="%.2f"),
                "Principal Balance": st.column_config.NumberColumn(disabled=True, format="%.2f"),
                "Total Outstanding": st.column_config.NumberColumn(disabled=True, format="%.2f"),
                "Payment": st.column_config.NumberColumn(format="%.2f"),
            },
            width="stretch",
            hide_index=True,
            key="cust_editor",
            height=260,
        )
        if not edited.equals(df):
            schedule_dates_edit = parse_schedule_dates_from_table(edited, start_date=disbursement_date)
            df_updated = recompute_customised_from_payments(edited, total_facility, schedule_dates_edit, annual_rate, flat_rate, disbursement_date)
            st.session_state[session_key] = df_updated
            st.rerun()

    final_total_outstanding = float(df.at[len(df) - 1, "Total Outstanding"]) if len(df) > 1 and "Total Outstanding" in df.columns else total_facility
    if abs(final_total_outstanding) < 0.01:
        st.success("Loan cleared. You may save this record.")
    else:
        st.warning(f"Total outstanding at end: **${final_total_outstanding:,.2f}**. Adjust payments so Total Outstanding is $0 to save.")

    can_save = abs(final_total_outstanding) < 0.01
    with st.expander("Save to system", expanded=False):
        if st.button("Save Customised Repayments to System", type="primary", key="cust_save", disabled=not can_save):
            if can_save:
                st.success(f"Customised loan for ${loan_required:,.2f} has been prepared for database sync.")
                with st.expander("Preview record (for DB insertion)"):
                    st.json({
                        "loan_type": "customised_repayments",
                        "timestamp": datetime.now().isoformat(),
                        "principal": float(total_facility),
                        "disbursed_amount": float(loan_required),
                        "term": int(loan_term),
                        "annual_rate": float(annual_rate),
                        "drawdown_fee": float(drawdown_fee_pct),
                        "arrangement_fee": float(arrangement_fee_pct),
                        "disbursement_date": disbursement_date.isoformat(),
                        "currency": currency,
                        "schedule": df.to_dict(orient="records"),
                    })


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
    if "Principal moratorium" in grace_type:
        grace_key = "principal"
    elif "Principal and interest" in grace_type:
        grace_key = "principal_and_interest"
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
    if first_repayment_date is not None and "with interest" in bullet_type.lower():
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
    """Capture loan flow: 3-step wizard — Key details → Build schedule → Review & approve."""
    if not _customers_available:
        st.error("Customer module is required for Capture Loan. Check database connection.")
        return
    if not _loan_management_available:
        st.error(f"Loan management module is not available. ({_loan_management_error})")
        return

    if "capture_loan_step" not in st.session_state:
        st.session_state["capture_loan_step"] = 0

    def _stage1_session_details() -> dict:
        return {
            "agent_id": st.session_state.get("capture_agent_id"),
            "relationship_manager_id": st.session_state.get("capture_relationship_manager_id"),
            "disbursement_bank_option_id": None,
            "cash_gl_account_id": st.session_state.get("capture_cash_gl_account_id"),
            "collateral_security_subtype_id": st.session_state.get("capture_collateral_subtype_pick"),
            "collateral_charge_amount": st.session_state.get("capture_collateral_charge"),
            "collateral_valuation_amount": st.session_state.get("capture_collateral_valuation"),
        }

    def _step1_source_cash_gl_valid() -> bool:
        entries = get_cached_source_cash_account_entries()
        if not entries:
            return True
        cid = st.session_state.get("capture_cash_gl_account_id")
        if cid is None or str(cid).strip() == "":
            st.error(
                "Select **Source cash / bank GL** below. When the **source cash account cache** is configured, "
                "this operating account is required for disbursement and **LOAN_CAPTURE** on `cash_operating`. "
                "Rebuild the cache under **System configurations → Accounting configurations → "
                "Maintenance — source cash account cache** if the list is empty."
            )
            return False
        allowed = {str(e.get("id")) for e in entries if e.get("id")}
        if str(cid).strip() not in allowed:
            st.error(
                "The selected cash GL is not in the source cash cache. Rebuild the cache after chart changes, "
                "or pick an account from the dropdown."
            )
            return False
        return True

    flash_msg = st.session_state.pop("capture_flash_message", None)
    if flash_msg:
        st.success(str(flash_msg))
    step = int(st.session_state["capture_loan_step"])
    if step > 2:
        step = 2
        st.session_state["capture_loan_step"] = 2
    if step < 0:
        step = 0
        st.session_state["capture_loan_step"] = 0
    step_labels = ["Key loan details", "Build schedule", "Review & submit"]
    progress = " · ".join(
        [
            f"**{i + 1}. {step_labels[i]}**" if i == step else f"{i + 1}. {step_labels[i]}"
            for i in range(len(step_labels))
        ]
    )
    st.caption(f"Step {step + 1} of {len(step_labels)} — {progress}")
    with st.popover("See loans for rework"):
        srch = st.text_input(
            "Search rework drafts",
            placeholder="Draft ID / Customer ID / Product / Loan type",
            key="cap_rework_search",
        )
        rework_rows = list_loan_approval_drafts(
            status="REWORK",
            search=srch.strip() or None,
            limit=200,
        )
        if not rework_rows:
            st.caption("No drafts currently in rework.")
        else:
            rw_df = pd.DataFrame(rework_rows)
            rw_cols = [c for c in ["id", "customer_id", "loan_type", "product_code", "assigned_approver_id", "submitted_at"] if c in rw_df.columns]
            st.dataframe(rw_df[rw_cols], width="stretch", hide_index=True, height=160)
            rw_options = [int(r["id"]) for r in rework_rows]
            pick_rw = st.selectbox("Select rework draft", rw_options, key="cap_rework_pick")
            if st.button("Load selected draft", key="cap_rework_load_btn", width="stretch"):
                draft = get_loan_approval_draft(int(pick_rw))
                if not draft:
                    st.error(f"Draft #{pick_rw} not found.")
                else:
                    draft_loan_type = str(draft.get("loan_type") or "")
                    type_map = {
                        "consumer_loan": "Consumer Loan",
                        "term_loan": "Term Loan",
                        "bullet_loan": "Bullet Loan",
                        "customised_repayments": "Customised Repayments",
                    }
                    display_type = type_map.get(draft_loan_type, draft_loan_type)
                    det = draft.get("details_json") or {}
                    sched = draft.get("schedule_json") or []
                    st.session_state["capture_customer_id"] = int(draft.get("customer_id"))
                    st.session_state["capture_loan_type"] = display_type
                    st.session_state["capture_product_code"] = draft.get("product_code")
                    st.session_state["capture_loan_details"] = det
                    st.session_state["capture_loan_schedule_df"] = pd.DataFrame(sched)
                    st.session_state["capture_approval_assigned_to"] = draft.get("assigned_approver_id")
                    st.session_state["capture_agent_id"] = det.get("agent_id")
                    st.session_state["capture_relationship_manager_id"] = det.get("relationship_manager_id")
                    st.session_state.pop("capture_disbursement_bank_option_id", None)
                    st.session_state["capture_cash_gl_account_id"] = det.get("cash_gl_account_id")
                    _cs = det.get("collateral_security_subtype_id")
                    if _cs is not None:
                        try:
                            st.session_state["capture_collateral_subtype_pick"] = int(_cs)
                        except (TypeError, ValueError):
                            st.session_state.pop("capture_collateral_subtype_pick", None)
                    else:
                        st.session_state.pop("capture_collateral_subtype_pick", None)
                    st.session_state["capture_collateral_charge"] = float(det.get("collateral_charge_amount") or 0)
                    st.session_state["capture_collateral_valuation"] = float(det.get("collateral_valuation_amount") or 0)
                    st.session_state["capture_rework_source_draft_id"] = int(draft.get("id"))
                    st.session_state.pop("capture_stage1_draft_id", None)
                    st.session_state["capture_loan_step"] = 1
                    st.session_state["capture_flash_message"] = f"Loaded rework draft #{draft.get('id')} for editing."
                    st.rerun()

    with st.popover("Resume saved (step 1)"):
        st.caption("Incomplete captures saved with **Save & continue later** (not yet sent for approval).")
        stg_srch = st.text_input(
            "Search staged drafts",
            placeholder="Draft ID / Customer ID / Product",
            key="cap_staged_search",
        )
        staged_rows = list_loan_approval_drafts(
            status="STAGED",
            search=stg_srch.strip() or None,
            limit=200,
        )
        if not staged_rows:
            st.caption("No staged step-1 drafts.")
        else:
            stg_df = pd.DataFrame(staged_rows)
            stg_cols = [
                c
                for c in [
                    "id",
                    "customer_id",
                    "loan_type",
                    "product_code",
                    "assigned_approver_id",
                    "submitted_at",
                ]
                if c in stg_df.columns
            ]
            st.dataframe(stg_df[stg_cols], width="stretch", hide_index=True, height=140)
            stg_options = [int(r["id"]) for r in staged_rows]
            pick_stg = st.selectbox("Select draft", stg_options, key="cap_staged_pick")
            if st.button("Load staged draft", key="cap_staged_load_btn", width="stretch"):
                draft_s = get_loan_approval_draft(int(pick_stg))
                if not draft_s:
                    st.error(f"Draft #{pick_stg} not found.")
                else:
                    draft_loan_type_s = str(draft_s.get("loan_type") or "")
                    type_map_s = {
                        "consumer_loan": "Consumer Loan",
                        "term_loan": "Term Loan",
                        "bullet_loan": "Bullet Loan",
                        "customised_repayments": "Customised Repayments",
                    }
                    display_type_s = type_map_s.get(draft_loan_type_s, draft_loan_type_s)
                    det_s = draft_s.get("details_json") or {}
                    st.session_state["capture_customer_id"] = int(draft_s.get("customer_id"))
                    st.session_state["capture_loan_type"] = display_type_s
                    st.session_state["capture_product_code"] = draft_s.get("product_code")
                    st.session_state["capture_agent_id"] = det_s.get("agent_id")
                    st.session_state["capture_relationship_manager_id"] = det_s.get("relationship_manager_id")
                    st.session_state.pop("capture_disbursement_bank_option_id", None)
                    st.session_state["capture_cash_gl_account_id"] = det_s.get("cash_gl_account_id")
                    _cs_s = det_s.get("collateral_security_subtype_id")
                    if _cs_s is not None:
                        try:
                            st.session_state["capture_collateral_subtype_pick"] = int(_cs_s)
                        except (TypeError, ValueError):
                            st.session_state.pop("capture_collateral_subtype_pick", None)
                    else:
                        st.session_state.pop("capture_collateral_subtype_pick", None)
                    st.session_state["capture_collateral_charge"] = float(det_s.get("collateral_charge_amount") or 0)
                    st.session_state["capture_collateral_valuation"] = float(det_s.get("collateral_valuation_amount") or 0)
                    st.session_state["capture_approval_assigned_to"] = draft_s.get("assigned_approver_id")
                    st.session_state["capture_stage1_draft_id"] = int(draft_s.get("id"))
                    st.session_state.pop("capture_rework_source_draft_id", None)
                    st.session_state.pop("capture_loan_details", None)
                    st.session_state.pop("capture_loan_schedule_df", None)
                    st.session_state["capture_loan_step"] = 0
                    st.session_state["capture_flash_message"] = f"Resumed staged draft #{draft_s.get('id')} (step 1)."
                    st.rerun()

    # -------- Window 1: Key loan details --------
    if step == 0:
        st.caption("1. Key loan details — Select customer, product and optional RM/agent.")
        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.warning("No active customers. Add a customer first under **Customers**.")
        else:
            col_a, col_b = st.columns([1, 1])
            with col_a:
                options = [(c["id"], get_display_name(c["id"]) or f"Customer #{c['id']}") for c in customers_list]
                choice = st.selectbox(
                    "Customer",
                    range(len(options)),
                    format_func=lambda i: options[i][1],
                    key="cap_customer_sel",
                )
                if choice is not None:
                    st.session_state["capture_customer_id"] = options[choice][0]
                    product_opts = list_products(active_only=True) if _loan_management_available else []
                    if not product_opts:
                        st.warning("No products. Create products under **System configurations → Products**.")
                    product_labels = [f"{p['code']} – {p['name']}" for p in product_opts]
                    lt_display = {"consumer_loan": "Consumer Loan", "term_loan": "Term Loan", "bullet_loan": "Bullet Loan", "customised_repayments": "Customised Repayments"}
                    prod_options = list(range(len(product_labels))) if product_labels else [0]
                    prod_format = (lambda i: product_labels[i]) if product_labels else (lambda i: "(No products)")
                    product_sel_idx = st.selectbox(
                        "Product",
                        prod_options,
                        format_func=prod_format,
                        key="cap_product_sel",
                    )
                    if product_opts and product_sel_idx is not None and 0 <= product_sel_idx < len(product_opts):
                        st.session_state["capture_product_code"] = product_opts[product_sel_idx]["code"]
                        st.session_state["capture_loan_type"] = lt_display.get(product_opts[product_sel_idx]["loan_type"], product_opts[product_sel_idx]["loan_type"])
                    else:
                        st.session_state["capture_product_code"] = None
                        st.session_state["capture_loan_type"] = "Term Loan"
                    if product_opts and product_sel_idx is not None and 0 <= product_sel_idx < len(product_opts):
                        st.caption(f"Loan type: **{lt_display.get(product_opts[product_sel_idx]['loan_type'], product_opts[product_sel_idx]['loan_type'])}** (from product)")
            with col_b:
                if _users_for_rm_available:
                    users_rm = list_users_for_selection()
                    rm_opts = [(None, "(None)")] + [(u["id"], f"{u['full_name']} ({u['email']})") for u in users_rm]
                    rm_labels = [t[1] for t in rm_opts]
                    rm_ids = [t[0] for t in rm_opts]
                    rm_sel = st.selectbox("Relationship manager (internal)", rm_labels, key="cap_rm_t1")
                    st.session_state["capture_relationship_manager_id"] = rm_ids[rm_labels.index(rm_sel)] if rm_sel else None
                else:
                    st.session_state["capture_relationship_manager_id"] = None
                if _agents_available:
                    try:
                        agents_list_cap = list_agents(status="active") or []
                    except Exception:
                        agents_list_cap = []
                    agent_labels_cap = ["(None)"] + [a["name"] for a in agents_list_cap]
                    agent_ids_cap = [None] + [a["id"] for a in agents_list_cap]
                    default_agent_idx = 0
                    if st.session_state.get("capture_agent_id") is not None:
                        try:
                            default_agent_idx = agent_ids_cap.index(st.session_state["capture_agent_id"])
                        except ValueError:
                            pass
                    sel_agent_label = st.selectbox(
                        "Agent (external broker)",
                        agent_labels_cap,
                        index=default_agent_idx,
                        key="cap_agent_sel_t0",
                    )
                    st.session_state["capture_agent_id"] = agent_ids_cap[agent_labels_cap.index(sel_agent_label)] if sel_agent_label else None
                else:
                    st.session_state["capture_agent_id"] = None
            st.divider()
            st.caption(
                "**Source cash / bank GL (operating account)** — same cached list as **Teller**. "
                "Defines the loan’s operating bank for **disbursement** and **LOAN_CAPTURE** resolution on "
                "`cash_operating`. The list is built under **System configurations → Accounting configurations → "
                "Maintenance — source cash account cache** (not a separate disbursement-bank screen). "
                "**Receipts** still use the account chosen in Teller per payment."
            )
            _cg_lab, _cg_ids = _source_cash_gl_cached_labels_and_ids()
            if _cg_ids:
                _cg_default = 0
                _prev_cg = st.session_state.get("capture_cash_gl_account_id")
                if _prev_cg and str(_prev_cg) in _cg_ids:
                    _cg_default = _cg_ids.index(str(_prev_cg))
                _cg_i = st.selectbox(
                    SOURCE_CASH_GL_WIDGET_LABEL,
                    range(len(_cg_lab)),
                    format_func=lambda i: _cg_lab[i],
                    index=_cg_default,
                    key="cap_cash_gl_sel_t0",
                )
                st.session_state["capture_cash_gl_account_id"] = _cg_ids[_cg_i]
            else:
                st.session_state["capture_cash_gl_account_id"] = None
                _source_cash_gl_cache_empty_warning()

            with st.expander("Collateral (IFRS provision inputs)", expanded=False):
                st.caption(
                    "Subtype and amounts are stored on the loan at approval. **DPD**, **total balance**, and "
                    "**interest in suspense** for live provision come from **loan_daily_state** — use **Portfolio reports → "
                    "IFRS Provisions** or **ECL / provisions (IFRS view)** after booking."
                )
                if (
                    not _PROVISIONS_CONFIG_OK
                    or list_provision_security_subtypes is None
                    or _provision_schema_ready_fn is None
                ):
                    st.warning("Collateral tables unavailable — run **scripts/run_migration_53.py**.")
                else:
                    _sch_ok, _sch_msg = _provision_schema_ready_fn()
                    if not _sch_ok:
                        st.warning(_sch_msg)
                    else:
                        _subs = list_provision_security_subtypes(active_only=True) or []
                        if not _subs:
                            st.info("Add subtypes under **System configurations → IFRS provision config**.")
                        else:
                            _sid_opts = [int(s["id"]) for s in _subs]
                            _pick_cur = st.session_state.get("capture_collateral_subtype_pick")
                            if _pick_cur is not None and int(_pick_cur) not in _sid_opts:
                                st.session_state.pop("capture_collateral_subtype_pick", None)
                            _sid_lbl = {
                                int(s["id"]): f"{s['security_type']} · {s['subtype_name']} (haircut {s['typical_haircut_pct']}%)" for s in _subs
                            }
                            st.selectbox(
                                "Collateral subtype",
                                _sid_opts,
                                format_func=lambda i, m=_sid_lbl: m.get(int(i), str(i)),
                                key="capture_collateral_subtype_pick",
                            )
                            _cca, _cva = st.columns(2)
                            with _cca:
                                st.number_input(
                                    "Charge amount (registered / agreed)",
                                    min_value=0.0,
                                    step=0.01,
                                    key="capture_collateral_charge",
                                )
                            with _cva:
                                st.number_input(
                                    "Valuation amount (market)",
                                    min_value=0.0,
                                    step=0.01,
                                    key="capture_collateral_valuation",
                                )

        btn_save1, btn_clear, btn_next, _ = st.columns([1, 1, 1, 1])
        with btn_save1:
            if st.button("Save & continue later", key="cap_save_stage1"):
                cid_sv = st.session_state.get("capture_customer_id")
                ltype_sv = st.session_state.get("capture_loan_type")
                pcode_sv = st.session_state.get("capture_product_code")
                if not cid_sv or not ltype_sv:
                    st.error("Select **customer** and **product** before saving.")
                elif not _step1_source_cash_gl_valid():
                    pass
                else:
                    try:
                        appr_sv = st.session_state.get("capture_approval_assigned_to")
                        empty_df = pd.DataFrame()
                        sid_existing = st.session_state.get("capture_stage1_draft_id")
                        if sid_existing is not None:
                            update_loan_approval_draft_staged(
                                int(sid_existing),
                                int(cid_sv),
                                str(ltype_sv),
                                _stage1_session_details(),
                                empty_df,
                                product_code=pcode_sv,
                                assigned_approver_id=str(appr_sv) if appr_sv is not None else None,
                            )
                            st.session_state["capture_flash_message"] = (
                                f"Updated staged draft #{int(sid_existing)}. Resume anytime from **Resume saved (step 1)**."
                            )
                        else:
                            new_sid = save_loan_approval_draft(
                                int(cid_sv),
                                str(ltype_sv),
                                _stage1_session_details(),
                                empty_df,
                                product_code=pcode_sv,
                                assigned_approver_id=str(appr_sv) if appr_sv is not None else None,
                                created_by="capture_ui",
                                status="STAGED",
                            )
                            st.session_state["capture_stage1_draft_id"] = int(new_sid)
                            st.session_state["capture_flash_message"] = (
                                f"Saved step 1 as draft **#{int(new_sid)}**. Continue later via **Resume saved (step 1)**."
                            )
                        st.rerun()
                    except Exception as _ex:
                        st.error(str(_ex))
        with btn_clear:
            if st.button("Clear selection", key="cap_clear_t1"):
                for k in list(st.session_state.keys()):
                    if k.startswith("capture_"):
                        st.session_state.pop(k, None)
                st.rerun()
        with btn_next:
            if st.button("Next →", type="primary", key="cap_next_0"):
                if not _step1_source_cash_gl_valid():
                    pass
                else:
                    st.session_state["capture_loan_step"] = 1
                    st.rerun()

    # -------- Window 2: Build schedule --------
    elif step == 1:
        st.caption("2. Build schedule — Enter loan parameters and generate the repayment schedule.")
        cid = st.session_state.get("capture_customer_id")
        ltype = st.session_state.get("capture_loan_type")
        product_code = st.session_state.get("capture_product_code") or "—"
        if not cid or not ltype:
            st.info("Complete **Step 1 — Key loan details** first.")
            if st.button("← Back", key="cap_back_1_empty"):
                st.session_state["capture_loan_step"] = 0
                st.rerun()
        else:
            st.caption(f"**Customer:** {get_display_name(cid)} (ID {cid}) · **Product:** {product_code} · **Loan type:** {ltype}")
            if st.session_state.get("capture_loan_details") is not None or st.session_state.get("capture_loan_schedule_df") is not None:
                if st.button("Clear saved schedule", key="cap_clear_t2"):
                    st.session_state.pop("capture_loan_details", None)
                    st.session_state.pop("capture_loan_schedule_df", None)
                    st.rerun()
            product_cfg_for_basis = get_product_config_from_db(product_code) or {}
            product_rate_basis = _get_product_rate_basis(product_cfg_for_basis, fallback=None)
            product_gls = product_cfg_for_basis.get("global_loan_settings") or {}
            interest_method = product_gls.get("interest_method")
            if interest_method not in {"Reducing balance", "Flat rate"}:
                st.error(
                    f"Selected product `{product_code}` must define "
                    f"product_config:{product_code}.global_loan_settings.interest_method as "
                    f"'Reducing balance' or 'Flat rate'."
                )
                st.stop()
            flat_rate = interest_method == "Flat rate"
            rate_label = (
                "Interest rate (% per annum)"
                if product_rate_basis == "Per annum"
                else "Interest rate (% per month)"
            )
            payment_timing_anniversary = True  # will set from form

            if ltype == "Consumer Loan":
                cfg = _get_system_config()
                schemes = _get_consumer_schemes()
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("consumer_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]

                cl_col1, cl_col2, cl_col3 = st.columns(3)
                with cl_col1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_cl_currency",
                    )
                    principal_input = st.radio(
                        "What are you entering?",
                        ["Net proceeds", "Principal (total loan amount)"],
                        key="cap_cl_principal_input",
                        horizontal=True,
                    )
                    input_tf = principal_input == "Principal (total loan amount)"
                with cl_col2:
                    loan_required = st.number_input(
                        "Loan amount",
                        min_value=0.0,
                        value=140.0,
                        step=10.0,
                        format="%.2f",
                        key="cap_cl_principal",
                    )
                    loan_term = st.number_input("Term (months)", 1, 60, 6, key="cap_cl_term")
                with cl_col3:
                    disbursement_date = datetime.combine(
                        st.date_input("Disbursement date", _get_system_date(), key="cap_cl_start"),
                        datetime.min.time(),
                    )
                    default_first_rep = add_months(disbursement_date, 1).date()
                    first_rep_input = st.date_input("First Repayment Date", default_first_rep, key="cap_cl_first_rep")
                    first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
                    use_anniversary = st.radio(
                        "Repayments on",
                        ["Anniversary date (same day each month)", "Last day of each month"],
                        key="cap_cl_timing",
                        horizontal=True,
                    ).startswith("Anniversary")
                cl_schedule_valid = use_anniversary or is_last_day_of_month(first_repayment_date)
                if not cl_schedule_valid:
                    last_day = days_in_month(first_repayment_date.year, first_repayment_date.month)
                    example = datetime(first_repayment_date.year, first_repayment_date.month, last_day).strftime("%d-%b-%Y")
                    st.error(
                        "When repayments are on the **last day of each month**, the First Repayment Date must be the last day of its month. "
                        f"For {first_repayment_date.strftime('%B %Y')} the last day is **{example}**."
                    )
                # Product-per-scheme: derive consumer schedule rates from selected product.
                # This removes redundancy in the capture flow (scheme is implicit in product_code).
                product_cfg = get_product_config_from_db(product_code) or {}
                product_rate_basis = _get_product_rate_basis(product_cfg)
                default_rates = (product_cfg.get("default_rates") or {}).get("consumer_loan") or {}
                interest_pct = default_rates.get("interest_pct")
                admin_fee_pct = default_rates.get("admin_fee_pct")

                if interest_pct is not None and admin_fee_pct is not None:
                    interest_pct_month = _pct_to_monthly(interest_pct, product_rate_basis)
                    if interest_pct_month is None:
                        st.error(
                            f"Selected product `{product_code}` has invalid interest_pct for consumer_loan (must be numeric)."
                        )
                        st.stop()

                    base_rate = float(interest_pct_month) / 100.0
                    admin_fee = float(admin_fee_pct) / 100.0

                    matched = next(
                        (
                            s
                            for s in schemes
                            if abs(float(s.get("interest_rate_pct", 0.0)) - float(interest_pct_month)) < 1e-6
                            and abs(float(s.get("admin_fee_pct", 0.0)) - float(admin_fee_pct)) < 1e-6
                        ),
                        None,
                    )
                    scheme = str(matched["name"]) if matched and matched.get("name") else "Other"
                else:
                    st.error(
                        f"Selected product `{product_code}` must define "
                        f"`product_config:{product_code}.default_rates.consumer_loan.interest_pct` and "
                        f"`product_config:{product_code}.default_rates.consumer_loan.admin_fee_pct`."
                    )
                    st.stop()

                # Product-per-scheme: regular interest/admin come from product defaults.
                # Penalty/default interest is also derived from product config, but we do NOT expose
                # a penalty override field in the consumer capture flow.
                penalty_pct = (product_cfg.get("penalty_rates") or {}).get("consumer_loan")
                if penalty_pct is None:
                    st.error(
                        f"Selected product `{product_code}` must define "
                        f"`product_config:{product_code}.penalty_rates.consumer_loan`."
                    )
                    st.stop()

                penalty_pct_month = _pct_to_monthly(penalty_pct, product_rate_basis)
                if penalty_pct_month is None:
                    st.error(
                        f"Selected product `{product_code}` has invalid penalty_rates.consumer_loan (must be numeric)."
                    )
                    st.stop()

                penalty_pct = float(penalty_pct_month or 0.0)
                penalty_quotation_product = product_cfg.get("penalty_interest_quotation")
                if not penalty_quotation_product:
                    st.error(
                        f"Selected product `{product_code}` must define "
                        f"`product_config:{product_code}.penalty_interest_quotation`."
                    )
                    st.stop()

                st.caption(f"Derived from product `{product_code}` → Scheme: `{scheme}`")

                override_rates = st.checkbox(
                    "Override regular interest rate and administration fee",
                    value=False,
                    key="cap_cl_override_rates",
                )
                if override_rates:
                    override_interest_label = (
                        "Regular interest rate (% per annum)"
                        if product_rate_basis == "Per annum"
                        else "Regular interest rate (% per month)"
                    )
                    override_interest_pct = st.number_input(
                        override_interest_label,
                        min_value=0.0,
                        max_value=100.0,
                        value=round(float(interest_pct or 0.0), 4),
                        step=0.1,
                        key="cap_cl_override_interest_pct",
                    )
                    override_admin_fee_pct = st.number_input(
                        "Administration fee (%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=round(float(admin_fee) * 100.0, 4),
                        step=0.1,
                        key="cap_cl_override_admin_fee_pct",
                    )

                    # Convert the overridden rate into the internal "Per month" form
                    # because the consumer schedule computation uses that basis internally.
                    override_interest_pct_month = _pct_to_monthly(override_interest_pct, product_rate_basis)
                    if override_interest_pct_month is None:
                        st.error("Invalid override interest rate for the selected product rate basis.")
                        st.stop()
                    base_rate = float(override_interest_pct_month) / 100.0
                    admin_fee = float(override_admin_fee_pct) / 100.0

                    # Remap scheme name based on overridden rates (or use "Other").
                    scheme_interest_pct_for_match = override_interest_pct_month
                    matched_override = next(
                        (
                            s
                            for s in schemes
                            if abs(float(s.get("interest_rate_pct", 0.0)) - float(scheme_interest_pct_for_match)) < 1e-6
                            and abs(float(s.get("admin_fee_pct", 0.0)) - float(override_admin_fee_pct)) < 1e-6
                        ),
                        None,
                    )
                    scheme = (
                        str(matched_override["name"])
                        if matched_override and matched_override.get("name")
                        else "Other"
                    )

                    st.caption(
                        f"Overrides applied → Scheme: `{scheme}` "
                        f"(interest={override_interest_pct:.2f}%, admin={override_admin_fee_pct:.2f}%)."
                    )
                if cl_schedule_valid:
                    details, df_schedule = compute_consumer_schedule(
                        loan_required, loan_term, disbursement_date, base_rate, admin_fee, input_tf,
                        "Per month", flat_rate, scheme=scheme,
                        first_repayment_date=first_repayment_date, use_anniversary=use_anniversary,
                    )
                    details["currency"] = currency
                    details["penalty_rate_pct"] = penalty_pct
                    details["penalty_quotation"] = penalty_quotation_product
                    st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True)
                    _schedule_export_downloads(
                        df_schedule, file_stem="capture_consumer_schedule", key_prefix="dl_cap_sched_consumer"
                    )
                    if st.button("Use this schedule", key="cap_cl_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_schedule
                        st.session_state["capture_loan_step"] = 2
                        st.session_state["capture_require_docs_prompt"] = True
                        st.success("Schedule saved. Continue with document upload and approval.")
                        st.rerun()

            elif ltype == "Term Loan":
                cfg = _get_system_config()
                product_cfg = get_product_config_from_db(product_code) or {}
                product_rate_basis = _get_product_rate_basis(product_cfg)
                dr = (product_cfg.get("default_rates") or {}).get("term_loan") or {}
                required = ["interest_pct", "drawdown_pct", "arrangement_pct"]
                missing = [k for k in required if dr.get(k) is None]
                if missing:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.default_rates.term_loan "
                        f"keys: {', '.join(missing)}."
                    )
                    st.stop()
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("term_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]

                top_tog1, top_tog2, top_tog3 = st.columns(3)
                with top_tog1:
                    principal_input = st.radio(
                        "What are you entering?",
                        ["Net proceeds", "Principal (total loan amount)"],
                        key="cap_term_principal_input",
                        horizontal=True,
                    )
                    input_tf = principal_input == "Principal (total loan amount)"
                with top_tog2:
                    grace_type = st.radio(
                        "Grace period",
                        ["No grace period", "Principal moratorium", "Principal and interest moratorium"],
                        key="cap_term_grace",
                        horizontal=True,
                    )
                with top_tog3:
                    use_anniversary = st.radio(
                        "Repayments on",
                        ["Anniversary date", "Last day of month"],
                        key="cap_term_timing",
                        horizontal=True,
                    ).startswith("Anniversary")

                tcol1, tcol2, tcol3 = st.columns(3)
                with tcol1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_term_currency",
                    )
                    loan_required = st.number_input(
                        "Loan amount",
                        min_value=0.0,
                        value=1000.0,
                        step=100.0,
                        format="%.2f",
                        key="cap_term_principal",
                    )
                with tcol2:
                    loan_term = st.number_input("Term (months)", 1, 120, 24, key="cap_term_months")
                    disbursement_date = datetime.combine(
                        st.date_input("Disbursement date", _get_system_date(), key="cap_term_disb"),
                        datetime.min.time(),
                    )
                with tcol3:
                    drawdown_pct = (
                        st.number_input(
                            "Drawdown fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("drawdown_pct")),
                            step=0.1,
                            key="cap_term_drawdown",
                        )
                        / 100.0
                    )
                    arrangement_pct = (
                        st.number_input(
                            "Arrangement fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("arrangement_pct")),
                            step=0.1,
                            key="cap_term_arrangement",
                        )
                        / 100.0
                    )
                def_penalty = (product_cfg.get("penalty_rates") or {}).get("term_loan")
                if def_penalty is None:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_rates.term_loan."
                    )
                    st.stop()
                penalty_quotation_product = product_cfg.get("penalty_interest_quotation")
                if not penalty_quotation_product:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_interest_quotation."
                    )
                    st.stop()
                tpen1, tpen2, tpen3 = st.columns(3)
                with tpen1:
                    rate_pct = st.number_input(
                        rate_label,
                        0.0,
                        100.0,
                        float(dr.get("interest_pct") or 0.0),
                        step=0.1,
                        key="cap_term_rate",
                    )
                with tpen2:
                    penalty_label = (
                        "Penalty interest (% per annum)"
                        if product_rate_basis == "Per annum"
                        else "Penalty interest (% per month)"
                    )
                    penalty_pct = st.number_input(
                        penalty_label,
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_term_penalty",
                        help="Required. 0% is acceptable. Will be converted to a per-month penalty rate for EOD.",
                    )
                with tpen3:
                    default_first = add_months(disbursement_date, 1).date()
                    first_rep = datetime.combine(
                        st.date_input("First repayment date", default_first, key="cap_term_first_rep"),
                        datetime.min.time(),
                    )
                moratorium_months = 0
                if "Principal moratorium" in grace_type:
                    mcol, _ = st.columns([1, 1])
                    with mcol:
                        moratorium_months = st.number_input(
                            "Moratorium (months)", 1, 60, 3, key="cap_term_moratorium_p"
                        )
                elif "Principal and interest" in grace_type:
                    mcol, _ = st.columns([1, 1])
                    with mcol:
                        moratorium_months = st.number_input(
                            "Moratorium (months)", 1, 60, 3, key="cap_term_moratorium_pi"
                        )
                if not use_anniversary and not is_last_day_of_month(first_rep):
                    st.error("When repayments are on last day of month, first repayment date must be the last day of that month.")
                else:
                    details, df_schedule = compute_term_schedule(
                        loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                        input_tf, grace_type, moratorium_months, first_rep, use_anniversary,
                        product_rate_basis, flat_rate,
                    )
                    details["currency"] = currency
                    penalty_pct_monthly = _pct_to_monthly(penalty_pct, product_rate_basis)
                    if penalty_pct_monthly is None:
                        st.error("Invalid penalty interest for the selected product rate basis.")
                        st.stop()
                    details["penalty_rate_pct"] = float(penalty_pct_monthly)
                    details["penalty_quotation"] = penalty_quotation_product
                    st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True)
                    _schedule_export_downloads(
                        df_schedule, file_stem="capture_term_schedule", key_prefix="dl_cap_sched_term"
                    )
                    if st.button("Use this schedule", key="cap_term_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_schedule
                        st.session_state["capture_loan_step"] = 2
                        st.session_state["capture_require_docs_prompt"] = True
                        st.success("Schedule saved. Continue with document upload and approval.")
                        st.rerun()

            elif ltype == "Bullet Loan":
                cfg = _get_system_config()
                product_cfg = get_product_config_from_db(product_code) or {}
                dr = (product_cfg.get("default_rates") or {}).get("bullet_loan") or {}
                required = ["interest_pct", "drawdown_pct", "arrangement_pct"]
                missing = [k for k in required if dr.get(k) is None]
                if missing:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.default_rates.bullet_loan "
                        f"keys: {', '.join(missing)}."
                    )
                    st.stop()
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("bullet_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]
                bcol1, bcol2, bcol3 = st.columns(3)
                with bcol1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_bullet_currency",
                    )
                    bullet_type = st.radio(
                        "Bullet type",
                        ["Straight bullet (no interim payments)", "Bullet with interest payments"],
                        key="cap_bullet_type",
                        horizontal=True,
                    )
                    principal_input = st.radio(
                        "What are you entering?",
                        ["Net proceeds", "Principal (total loan amount)"],
                        key="cap_bullet_principal_input",
                        horizontal=True,
                    )
                    input_tf = principal_input == "Principal (total loan amount)"
                    loan_required = st.number_input(
                        "Loan amount",
                        min_value=0.0,
                        value=1000.0,
                        step=100.0,
                        format="%.2f",
                        key="cap_bullet_principal",
                    )
                with bcol2:
                    loan_term = st.number_input("Term (months)", 1, 120, 12, key="cap_bullet_term")
                    disbursement_date = datetime.combine(
                        st.date_input("Disbursement date", _get_system_date(), key="cap_bullet_disb"),
                        datetime.min.time(),
                    )
                with bcol3:
                    rate_pct = st.number_input(
                        rate_label,
                        0.0,
                        100.0,
                        float(dr.get("interest_pct")),
                        step=0.1,
                        key="cap_bullet_rate",
                    )
                    drawdown_pct = (
                        st.number_input(
                            "Drawdown fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("drawdown_pct")),
                            step=0.1,
                            key="cap_bullet_drawdown",
                        )
                        / 100.0
                    )
                    arrangement_pct = (
                        st.number_input(
                            "Arrangement fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("arrangement_pct")),
                            step=0.1,
                            key="cap_bullet_arrangement",
                        )
                        / 100.0
                    )
                def_penalty = (product_cfg.get("penalty_rates") or {}).get("bullet_loan")
                if def_penalty is None:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_rates.bullet_loan."
                    )
                    st.stop()
                penalty_quotation_product = product_cfg.get("penalty_interest_quotation")
                if not penalty_quotation_product:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_interest_quotation."
                    )
                    st.stop()
                bpen1, bpen2, _ = st.columns(3)
                with bpen1:
                    penalty_label = (
                        "Penalty interest (% per annum)"
                        if product_rate_basis == "Per annum"
                        else "Penalty interest (% per month)"
                    )
                    penalty_pct = st.number_input(
                        penalty_label,
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_bullet_penalty",
                        help="Required. 0% is acceptable. Will be converted to a per-month penalty rate for EOD.",
                    )
                penalty_pct_monthly = _pct_to_monthly(penalty_pct, product_rate_basis)
                if penalty_pct_monthly is None:
                    st.error("Invalid penalty interest for the selected product rate basis.")
                    st.stop()
                first_rep = None
                use_anniversary = True
                if "with interest" in bullet_type:
                    with bpen2:
                        default_first = add_months(disbursement_date, 1).date()
                        first_rep = datetime.combine(st.date_input("First repayment date", default_first, key="cap_bullet_first_rep"), datetime.min.time())
                        use_anniversary = st.radio(
                            "Interest payments on",
                            ["Anniversary date", "Last day of month"],
                            key="cap_bullet_timing",
                            horizontal=True,
                        ).startswith("Anniversary")
                    if not use_anniversary and not is_last_day_of_month(first_rep):
                        st.error("First repayment date must be last day of month when using last day of month.")
                    else:
                        details, df_schedule = compute_bullet_schedule(
                            loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                            input_tf, bullet_type, first_rep, use_anniversary, product_rate_basis, flat_rate,
                        )
                        details["currency"] = currency
                        details["penalty_rate_pct"] = float(penalty_pct_monthly)
                        details["penalty_quotation"] = penalty_quotation_product
                        st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True)
                        _schedule_export_downloads(
                            df_schedule, file_stem="capture_bullet_schedule", key_prefix="dl_cap_sched_bullet_i"
                        )
                        if st.button("Use this schedule", key="cap_bullet_use"):
                            st.session_state["capture_loan_details"] = details
                            st.session_state["capture_loan_schedule_df"] = df_schedule
                            st.session_state["capture_loan_step"] = 2
                            st.session_state["capture_require_docs_prompt"] = True
                            st.success("Schedule saved. Continue with document upload and approval.")
                            st.rerun()
                else:
                    details, df_schedule = compute_bullet_schedule(
                        loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                        input_tf, bullet_type, None, True, product_rate_basis, flat_rate,
                    )
                    details["currency"] = currency
                    details["penalty_rate_pct"] = float(penalty_pct_monthly)
                    details["penalty_quotation"] = penalty_quotation_product
                    st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True)
                    _schedule_export_downloads(
                        df_schedule, file_stem="capture_bullet_schedule", key_prefix="dl_cap_sched_bullet_s"
                    )
                    if st.button("Use this schedule", key="cap_bullet_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_schedule
                        st.session_state["capture_loan_step"] = 2
                        st.session_state["capture_require_docs_prompt"] = True
                        st.success("Schedule saved. Continue with document upload and approval.")
                        st.rerun()

            else:
                # Customised Repayments
                cfg = _get_system_config()
                product_cfg = get_product_config_from_db(product_code) or {}
                dr = (product_cfg.get("default_rates") or {}).get("customised_repayments") or {}
                required = ["interest_pct", "drawdown_pct", "arrangement_pct"]
                missing = [k for k in required if dr.get(k) is None]
                if missing:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.default_rates.customised_repayments "
                        f"keys: {', '.join(missing)}."
                    )
                    st.stop()
                accepted_currencies = cfg.get(
                    "accepted_currencies", [cfg.get("base_currency", "USD")]
                )
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get(
                    "customised_repayments", cfg.get("base_currency", "USD")
                )
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]
                ccol1, ccol2, ccol3 = st.columns(3)
                with ccol1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_cust_currency",
                    )
                    principal_input = st.radio(
                        "What are you entering?",
                        ["Net proceeds", "Principal (total loan amount)"],
                        key="cap_cust_principal_input",
                        horizontal=True,
                    )
                    input_tf = principal_input == "Principal (total loan amount)"
                    loan_required = st.number_input("Loan amount", min_value=0.0, value=1000.0, step=100.0, format="%.2f", key="cap_cust_principal")
                with ccol2:
                    loan_term = st.number_input("Term (months)", 1, 120, 12, key="cap_cust_term")
                    disbursement_date = datetime.combine(st.date_input("Disbursement date", _get_system_date(), key="cap_cust_start"), datetime.min.time())
                    irregular = st.checkbox("Irregular", value=False, key="cap_cust_irregular", help="Allow editing dates and adding rows; schedule recomputes from table dates.")
                    use_anniversary = st.radio(
                        "Repayments on",
                        ["Anniversary date", "Last day of month"],
                        key="cap_cust_timing",
                        horizontal=True,
                    ).startswith("Anniversary")
                default_first = add_months(disbursement_date, 1).date()
                if not use_anniversary:
                    default_first = default_first.replace(day=days_in_month(default_first.year, default_first.month))
                # first_rep for initial schedule build: use stored derived if set, else default
                first_rep_derived = st.session_state.get("cap_cust_first_rep_derived")
                first_rep_display = (first_rep_derived.date() if first_rep_derived is not None else default_first)
                first_rep = datetime.combine(first_rep_display, datetime.min.time())

                with ccol3:
                    rate_pct = st.number_input(
                        rate_label,
                        0.0,
                        100.0,
                        float(dr.get("interest_pct")),
                        step=0.1,
                        key="cap_cust_rate",
                    )
                    drawdown_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct")), step=0.1, key="cap_cust_drawdown") / 100.0
                    arrangement_pct = st.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct")), step=0.1, key="cap_cust_arrangement") / 100.0

                def_penalty = (product_cfg.get("penalty_rates") or {}).get("customised_repayments")
                if def_penalty is None:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_rates.customised_repayments."
                    )
                    st.stop()

                penalty_quotation_product = product_cfg.get("penalty_interest_quotation")
                if not penalty_quotation_product:
                    st.error(
                        f"Selected product `{product_code}` must define product_config:{product_code}.penalty_interest_quotation."
                    )
                    st.stop()

                penalty_label = (
                    "Penalty interest (% per annum)"
                    if product_rate_basis == "Per annum"
                    else "Penalty interest (% per month)"
                )
                cpen1, cpen2, cpen3 = st.columns(3)
                with cpen1:
                    penalty_pct = st.number_input(
                        penalty_label,
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_cust_penalty",
                        help="Required. 0% is acceptable. Converted to a per-month penalty rate for EOD.",
                    )
                penalty_pct_monthly = _pct_to_monthly(penalty_pct, product_rate_basis)
                if penalty_pct_monthly is None:
                    st.error("Invalid penalty interest for the selected product rate basis.")
                    st.stop()
                total_fee = drawdown_pct + arrangement_pct
                if input_tf:
                    total_facility = loan_required
                else:
                    total_facility = loan_required / (1.0 - total_fee)
                annual_rate = (
                    (rate_pct / 100.0) * 12.0
                    if product_rate_basis == "Per month"
                    else (rate_pct / 100.0)
                )

                cap_key = "cap_cust_df"
                cap_params = (round(total_facility, 2), loan_term, disbursement_date.strftime("%Y-%m-%d"), irregular)
                if cap_key not in st.session_state or st.session_state.get("cap_cust_params") != cap_params:
                    st.session_state["cap_cust_params"] = cap_params
                    schedule_dates_init = repayment_dates(disbursement_date, first_rep, int(loan_term), use_anniversary)
                    rows = [{"Period": 0, "Date": disbursement_date.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": round(total_facility, 2), "Total Outstanding": round(total_facility, 2)}]
                    for i, dt in enumerate(schedule_dates_init, 1):
                        rows.append({"Period": i, "Date": dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0})
                    st.session_state[cap_key] = pd.DataFrame(rows)
                    st.session_state.pop("cap_cust_first_rep_derived", None)

                df_cap = st.session_state[cap_key].copy()
                # Always derive schedule_dates from table so recompute matches displayed dates
                schedule_dates = parse_schedule_dates_from_table(df_cap, start_date=disbursement_date)
                df_cap = recompute_customised_from_payments(df_cap, total_facility, schedule_dates, annual_rate, flat_rate, disbursement_date)
                st.session_state[cap_key] = df_cap
                st.session_state["cap_cust_first_rep_derived"] = _first_repayment_from_customised_table(df_cap)

                date_editable = irregular
                if irregular:
                    if st.button("Add row", key="cap_cust_add_row"):
                        last_df = st.session_state[cap_key]
                        if len(last_df) > 0:
                            try:
                                last_date_str = str(last_df.at[len(last_df) - 1, "Date"]).strip()[:32]
                                last_dt = datetime.combine(datetime.strptime(last_date_str, "%d-%b-%Y").date(), datetime.min.time())
                            except (ValueError, TypeError):
                                last_dt = add_months(disbursement_date, len(last_df))
                            next_dt = add_months(last_dt, 1)
                            if not use_anniversary:
                                next_dt = next_dt.replace(day=days_in_month(next_dt.year, next_dt.month))
                            new_row = {"Period": len(last_df), "Date": next_dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0}
                            st.session_state[cap_key] = pd.concat([last_df, pd.DataFrame([new_row])], ignore_index=True)
                            st.rerun()
                    st.caption("Irregular: edit **Date** and **Payment**; add rows with the button above. Schedule recomputes from table dates.")

                edited = st.data_editor(
                    df_cap,
                    column_config={
                        "Period": st.column_config.NumberColumn(disabled=True),
                        "Date": st.column_config.TextColumn(disabled=not date_editable, help="Format: DD-Mon-YYYY (e.g. 01-Jan-2025)" if date_editable else None),
                        "Interest": st.column_config.NumberColumn(disabled=True, format="%.2f"),
                        "Principal": st.column_config.NumberColumn(disabled=True, format="%.2f"),
                        "Principal Balance": st.column_config.NumberColumn(disabled=True, format="%.2f"),
                        "Total Outstanding": st.column_config.NumberColumn(disabled=True, format="%.2f"),
                        "Payment": st.column_config.NumberColumn(format="%.2f"),
                    },
                    width="stretch",
                    hide_index=True,
                    key="cap_cust_editor",
                )
                if not edited.equals(df_cap):
                    schedule_dates_edit = parse_schedule_dates_from_table(edited, start_date=disbursement_date)
                    df_cap = recompute_customised_from_payments(edited, total_facility, schedule_dates_edit, annual_rate, flat_rate, disbursement_date)
                    st.session_state[cap_key] = df_cap
                    st.session_state["cap_cust_first_rep_derived"] = _first_repayment_from_customised_table(df_cap)
                    st.rerun()

                # Show first repayment date from current table (first row with payment > 0)
                first_rep_from_current = _first_repayment_from_customised_table(df_cap)
                first_rep_label = first_rep_from_current.strftime("%d-%b-%Y") if first_rep_from_current else default_first.strftime("%d-%b-%Y") + " (no payment yet)"
                st.markdown(f"**First repayment date (from table):** {first_rep_label}")

                # For save: first repayment = first row with non-zero payment; end = last date in table
                first_rep_for_save = _first_repayment_from_customised_table(df_cap) or first_rep
                end_date_from_table = schedule_dates[-1] if schedule_dates else disbursement_date

                final_to = float(df_cap.at[len(df_cap) - 1, "Total Outstanding"]) if len(df_cap) > 1 else total_facility
                if abs(final_to) < 0.01:
                    details = {
                        "principal": total_facility, "disbursed_amount": loan_required, "term": loan_term,
                        "annual_rate": annual_rate, "drawdown_fee": drawdown_pct, "arrangement_fee": arrangement_pct,
                        "disbursement_date": disbursement_date, "end_date": end_date_from_table,
                        "first_repayment_date": first_rep_for_save, "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
                        "penalty_rate_pct": float(penalty_pct_monthly),
                        "penalty_quotation": penalty_quotation_product,
                        "currency": currency,
                    }
                    if st.button("Use this schedule", key="cap_cust_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_cap
                        st.session_state["capture_loan_step"] = 2
                        st.session_state["capture_require_docs_prompt"] = True
                        st.success("Schedule saved. Continue with document upload and approval.")
                        st.rerun()
                else:
                    st.warning("Clear the schedule (Total Outstanding = $0) before using it.")
        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("Loan Documents")
        st.write("Upload supporting loan documents before saving.")
        if "loan_docs_staged" not in st.session_state:
            st.session_state["loan_docs_staged"] = []
        staged_loan_docs = st.session_state["loan_docs_staged"]
        if _documents_available:
            doc_cats = list_document_categories(active_only=True)
            LOAN_DOC_TYPES = {
                "Signed Loan Agreement",
                "Facility Letter",
                "Term Sheet",
                "Business Plan",
                "Application Form",
                "Application Letter",
                "Purchase Orders",
                "Offtake Agreement",
                "Supply Agreement",
                "Other",
            }
            name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in LOAN_DOC_TYPES}
            if not name_to_cat:
                st.info("No matching loan document categories configured.")
            else:
                dcol1, dcol2, dcol3 = st.columns(3)
                with dcol1:
                    doc_type = st.selectbox(
                        "Doc type",
                        sorted(name_to_cat.keys()),
                        key="loan_doc_type",
                    )
                    other_label = ""
                    if doc_type == "Other":
                        other_label = st.text_input(
                            "If Other, describe the document",
                            key="loan_doc_other_label",
                        )
                with dcol2:
                    f = st.file_uploader("Choose file", type=["pdf", "png", "jpg", "jpeg"], key="loan_doc_file")
                with dcol3:
                    notes = st.text_input("Notes (optional)", key="loan_doc_notes")
                if st.button("Save document to list", key="loan_doc_add") and f is not None:
                    cat = name_to_cat[doc_type]
                    label = other_label.strip() if doc_type == "Other" else notes.strip()
                    staged_loan_docs.append(
                        {
                            "category_id": cat["id"],
                            "category_name": doc_type,
                            "file": f,
                            "notes": label or "",
                        }
                    )
                    st.session_state["loan_docs_staged"] = staged_loan_docs
                    st.success(f"Staged {f.name} as {doc_type}.")
            if staged_loan_docs:
                st.markdown("**Staged documents:**")
                for idx, row in enumerate(staged_loan_docs, start=1):
                    cat_name = row.get("category_name") or "Document"
                    st.write(f"{idx}. {row['file'].name} · {cat_name} ({row.get('notes') or 'no notes'})")
        else:
            st.info("Document module is unavailable.")

        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("Approval routing")
        if _users_for_rm_available:
            approver_users = list_users_for_selection() or []
            if approver_users:
                approver_opts = [(u["id"], f"{u['full_name']} ({u['email']})") for u in approver_users]
                approver_labels = [x[1] for x in approver_opts]
                default_approver_idx = 0
                prev_approver_id = st.session_state.get("capture_approval_assigned_to")
                if prev_approver_id is not None:
                    try:
                        default_approver_idx = next(
                            i for i, (uid, _lbl) in enumerate(approver_opts) if str(uid) == str(prev_approver_id)
                        )
                    except Exception:
                        default_approver_idx = 0
                approver_label = st.selectbox(
                    "Assign approver",
                    approver_labels,
                    index=default_approver_idx,
                    key="cap_assigned_approver_select_stage2",
                )
                assigned_approver_id = approver_opts[approver_labels.index(approver_label)][0] if approver_label else None
                st.session_state["capture_approval_assigned_to"] = assigned_approver_id
                st.caption("Captured in draft and used in Stage 3 approval save.")

        has_schedule = st.session_state.get("capture_loan_details") is not None and st.session_state.get("capture_loan_schedule_df") is not None
        if not has_schedule:
            st.caption("Click **Use this schedule** above, then **Send for approval**.")
        else:
            st.caption("Draft is in session. Send for approval to persist it to the approval queue.")
        btn_b, btn_submit, _ = st.columns([1, 1, 2])
        with btn_b:
            if st.button("← Back", key="cap_back_1"):
                st.session_state["capture_loan_step"] = 0
                st.rerun()
        with btn_submit:
            if st.button("Send for approval", type="primary", key="cap_send_for_approval", disabled=not has_schedule):
                try:
                    details = st.session_state.get("capture_loan_details") or {}
                    df_schedule = st.session_state.get("capture_loan_schedule_df")
                    cid = st.session_state.get("capture_customer_id")
                    ltype = st.session_state.get("capture_loan_type")
                    pcode = st.session_state.get("capture_product_code")
                    if not details or df_schedule is None or not cid or not ltype:
                        st.error("Draft is incomplete. Please rebuild schedule first.")
                    elif not _step1_source_cash_gl_valid():
                        pass
                    else:
                        base_meta = details.get("metadata") or {}
                        assigned_approver_id = st.session_state.get("capture_approval_assigned_to")
                        if assigned_approver_id is not None:
                            base_meta["approval_assigned_to_user_id"] = str(assigned_approver_id)
                        details_to_queue = {
                            **details,
                            "agent_id": st.session_state.get("capture_agent_id"),
                            "relationship_manager_id": st.session_state.get("capture_relationship_manager_id"),
                            "disbursement_bank_option_id": None,
                            "cash_gl_account_id": st.session_state.get("capture_cash_gl_account_id"),
                            "collateral_security_subtype_id": st.session_state.get("capture_collateral_subtype_pick"),
                            "collateral_charge_amount": st.session_state.get("capture_collateral_charge"),
                            "collateral_valuation_amount": st.session_state.get("capture_collateral_valuation"),
                            "metadata": base_meta,
                        }
                        source_draft_id = st.session_state.get("capture_rework_source_draft_id")
                        stage1_draft_id = st.session_state.get("capture_stage1_draft_id")
                        if source_draft_id is not None:
                            draft_id = resubmit_loan_approval_draft(
                                int(source_draft_id),
                                int(cid),
                                str(ltype),
                                details_to_queue,
                                df_schedule,
                                product_code=pcode,
                                assigned_approver_id=str(assigned_approver_id) if assigned_approver_id is not None else None,
                                created_by="capture_ui",
                            )
                        elif stage1_draft_id is not None:
                            draft_id = resubmit_loan_approval_draft(
                                int(stage1_draft_id),
                                int(cid),
                                str(ltype),
                                details_to_queue,
                                df_schedule,
                                product_code=pcode,
                                assigned_approver_id=str(assigned_approver_id) if assigned_approver_id is not None else None,
                                created_by="capture_ui",
                            )
                        else:
                            draft_id = save_loan_approval_draft(
                                int(cid),
                                str(ltype),
                                details_to_queue,
                                df_schedule,
                                product_code=pcode,
                                assigned_approver_id=str(assigned_approver_id) if assigned_approver_id is not None else None,
                                created_by="capture_ui",
                            )
                        doc_count = 0
                        staged_loan_docs = st.session_state.get("loan_docs_staged") or []
                        if _documents_available and staged_loan_docs:
                            for row in staged_loan_docs:
                                cat_id = row["category_id"]
                                f = row["file"]
                                notes = row.get("notes") or ""
                                try:
                                    upload_document(
                                        "loan_approval_draft",
                                        int(draft_id),
                                        int(cat_id),
                                        f.name,
                                        f.type,
                                        f.size,
                                        f.getvalue(),
                                        uploaded_by="System User",
                                        notes=notes,
                                    )
                                    doc_count += 1
                                except Exception as de:
                                    st.error(f"Failed to attach {f.name}: {de}")
                        if source_draft_id is not None:
                            st.session_state["capture_flash_message"] = (
                                f"Draft #{draft_id} re-submitted for approval. "
                                f"Attached documents: {doc_count}."
                            )
                        else:
                            st.session_state["capture_flash_message"] = (
                                f"Draft sent for approval. Draft ID: {draft_id}. "
                                f"Attached documents: {doc_count}."
                            )
                        for k in list(st.session_state.keys()):
                            if k.startswith("capture_"):
                                st.session_state.pop(k, None)
                        st.session_state["loan_docs_staged"] = []
                        st.rerun()
                except Exception as e:
                    st.error(f"Could not send draft for approval: {e}")

    # -------- Window 3: View, approve, rework, dismiss --------
    elif step == 2:
        st.caption("3. View, approve, save & send back for rework, or dismiss.")
        # Show save result from previous run (success or failure)
        save_result = st.session_state.pop("capture_last_save_result", None)
        if save_result is not None:
            if save_result.get("success"):
                doc_msg = f" Also uploaded {save_result.get('doc_count', 0)} document(s)." if save_result.get('doc_count', 0) > 0 else ""
                st.success(f"**Loan saved successfully to the database.** Loan ID: **{save_result.get('loan_id', '—')}**{doc_msg}")
            else:
                st.error(f"**Save to database failed.** {save_result.get('error', 'Unknown error')}")

        details = st.session_state.get("capture_loan_details")
        df_schedule = st.session_state.get("capture_loan_schedule_df")
        cid = st.session_state.get("capture_customer_id")
        ltype = st.session_state.get("capture_loan_type")
        if st.session_state.pop("capture_require_docs_prompt", False):
            st.info("Please upload supporting loan documents before approval.")
        if st.session_state.get("capture_rework_note"):
            st.warning(str(st.session_state.pop("capture_rework_note")))
        if not details or df_schedule is None or not cid or not ltype:
            if save_result is None:
                st.info("Complete **Step 1 — Key loan details** and **Step 2 — Build schedule** first.")
            col_clr, col_b = st.columns(2)
            with col_clr:
                if st.button("Clear and start over", key="cap_clear_t3_empty"):
                    for k in list(st.session_state.keys()):
                        if k.startswith("capture_"):
                            st.session_state.pop(k, None)
                    st.rerun()
            with col_b:
                if st.button("← Back", key="cap_back_2_empty"):
                    st.session_state["capture_loan_step"] = 1
                    st.rerun()
        else:
            st.subheader("View")
            st.markdown("**Loan summary**")
            sum_col1, sum_col2, sum_col3 = st.columns(3)
            with sum_col1:
                st.markdown(f"**Customer:** {get_display_name(cid)} (ID {cid})")
                st.markdown(f"**Product:** {st.session_state.get('capture_product_code') or '—'} · **Loan type:** {ltype}")
            with sum_col2:
                st.markdown(f"**Principal:** {details.get('principal', 0):,.2f}")
                st.markdown(f"**Disbursed amount:** {details.get('disbursed_amount', 0):,.2f} | **Term:** {details.get('term', 0)} months")
            with sum_col3:
                product_code_for_rate = st.session_state.get("capture_product_code")
                product_cfg_for_rate = get_product_config_from_db(product_code_for_rate) or {}
                rate_basis_for_display = (
                    (product_cfg_for_rate.get("global_loan_settings") or {}).get("rate_basis")
                )
                if rate_basis_for_display not in {"Per month", "Per annum"}:
                    st.error(
                        f"Selected product `{product_code_for_rate}` must define "
                        "product_config:{product_code_for_rate}.global_loan_settings.rate_basis "
                        "as either 'Per month' or 'Per annum'."
                    )
                    st.stop()

                monthly_dec = None
                annual_dec = None
                if details.get("monthly_rate") is not None:
                    monthly_dec = float(details.get("monthly_rate") or 0.0)
                    annual_dec = monthly_dec * 12.0
                if details.get("annual_rate") is not None:
                    annual_dec = float(details.get("annual_rate") or 0.0)
                    monthly_dec = annual_dec / 12.0

                if rate_basis_for_display == "Per month":
                    rate_display_pct = (monthly_dec or 0.0) * 100.0
                    st.markdown(f"**Interest Rate (per month):** {rate_display_pct:.2f}%")
                else:
                    rate_display_pct = (annual_dec or 0.0) * 100.0
                    st.markdown(f"**Interest Rate (per annum):** {rate_display_pct:.2f}%")

                pen_rate_pct = details.get("metadata", {}).get(
                    "penalty_rate_pct", details.get("penalty_rate_pct", 0)
                )
                # penalty_rate_pct is stored in details in a per-month basis in our capture flow.
                if rate_basis_for_display == "Per month":
                    pen_display_pct = float(pen_rate_pct or 0.0)
                    st.markdown(f"**Penalty Rate (per month):** {pen_display_pct:.2f}%")
                else:
                    pen_display_pct = float(pen_rate_pct or 0.0) * 12.0
                    st.markdown(f"**Penalty Rate (per annum):** {pen_display_pct:.2f}%")
                
                # Try explicit amounts first, fallback to calculating from rates
                d_fee_amt = details.get('drawdown_fee_amount')
                a_fee_amt = details.get('arrangement_fee_amount')
                adm_fee_amt = details.get('admin_fee_amount')
                
                prin_raw = details.get("principal", details.get("facility", 0))
                if d_fee_amt is None: d_fee_amt = float(prin_raw) * float(details.get("drawdown_fee") or 0)
                if a_fee_amt is None: a_fee_amt = float(prin_raw) * float(details.get("arrangement_fee") or 0)
                if adm_fee_amt is None: adm_fee_amt = float(prin_raw) * float(details.get("admin_fee") or 0)
                
                fees = float(d_fee_amt) + float(a_fee_amt) + float(adm_fee_amt)
                st.markdown(f"**Total Fees:** {fees:,.2f}")
            st.divider()
            
            st.markdown("**Journal preview (on approval)**")
            from accounting_service import AccountingService
            from loan_management import build_loan_approval_journal_payload
            try:
                payload_preview = build_loan_approval_journal_payload(details)
                _cash_gl_prev = (details or {}).get("cash_gl_account_id") or st.session_state.get(
                    "capture_cash_gl_account_id"
                )
                if _cash_gl_prev:
                    _ao_prev = dict(payload_preview.get("account_overrides") or {})
                    _ao_prev["cash_operating"] = str(_cash_gl_prev).strip()
                    payload_preview["account_overrides"] = _ao_prev
                sim = AccountingService().simulate_event("LOAN_APPROVAL", payload=payload_preview)
                if sim.lines:
                    if not sim.balanced and sim.warning:
                        st.warning(sim.warning)
                    else:
                        st.caption("Double-entry check (2dp): debits = credits ✓")
                    df_preview = pd.DataFrame([{
                        "Account": f"{line['account_name']} ({line['account_code']})",
                        "Debit": float(line['debit']),
                        "Credit": float(line['credit'])
                    } for line in sim.lines])
                    st.dataframe(df_preview, use_container_width=True, hide_index=True)
                else:
                    st.info("No transaction templates found for LOAN_APPROVAL. No automated journals will be posted.")
            except Exception as e:
                st.warning(f"Could not preview journals: {e}")
            st.divider()

            st.markdown("**Schedule**")
            st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True)
            _schedule_export_downloads(
                df_schedule, file_stem="loan_schedule_review", key_prefix="dl_cap_sched_review"
            )
            st.divider()
            
            st.subheader("Documents")
            view_docs = st.checkbox("View documents", value=False, key="cap_view_docs_toggle")
            if view_docs:
                staged_loan_docs = st.session_state.get("loan_docs_staged") or []
                if not staged_loan_docs:
                    st.info("No staged documents available from Stage 2.")
                else:
                    st.markdown("**Staged documents (view only):**")
                    for idx, row in enumerate(staged_loan_docs, start=1):
                        cat_name = row.get("category_name") or "Document"
                        st.write(f"{idx}. {row['file'].name} · {cat_name} ({row.get('notes') or 'no notes'})")
            
            st.divider()
            st.subheader("Decision")
            assigned_approver_id = st.session_state.get("capture_approval_assigned_to")
            if assigned_approver_id is not None:
                st.caption(f"Approver assigned in Stage 2: user ID {assigned_approver_id}")

            col_save, col_rework, col_dismiss, col_back = st.columns([2, 2, 1, 1])
            with col_save:
                if st.button("Approve & save to database", type="primary", key="cap_save_btn"):
                    try:
                        if not _step1_source_cash_gl_valid():
                            pass
                        else:
                            base_meta = details.get("metadata") or {}
                            if assigned_approver_id is not None:
                                base_meta["approval_assigned_to_user_id"] = str(assigned_approver_id)
                            details_with_agent = {
                                **details,
                                "agent_id": st.session_state.get("capture_agent_id"),
                                "relationship_manager_id": st.session_state.get("capture_relationship_manager_id"),
                                "disbursement_bank_option_id": None,
                                "cash_gl_account_id": details.get("cash_gl_account_id")
                                if details.get("cash_gl_account_id") is not None
                                else st.session_state.get("capture_cash_gl_account_id"),
                                "collateral_security_subtype_id": st.session_state.get("capture_collateral_subtype_pick")
                                if st.session_state.get("capture_collateral_subtype_pick") is not None
                                else details.get("collateral_security_subtype_id"),
                                "collateral_charge_amount": st.session_state.get("capture_collateral_charge")
                                if st.session_state.get("capture_collateral_charge") is not None
                                else details.get("collateral_charge_amount"),
                                "collateral_valuation_amount": st.session_state.get("capture_collateral_valuation")
                                if st.session_state.get("capture_collateral_valuation") is not None
                                else details.get("collateral_valuation_amount"),
                                "metadata": base_meta,
                            }
                            loan_id = save_loan_to_db(
                                cid, ltype, details_with_agent, df_schedule,
                                product_code=st.session_state.get("capture_product_code"),
                            )

                            doc_count = 0
                            staged_loan_docs = st.session_state.get("loan_docs_staged") or []
                            if _documents_available and staged_loan_docs:
                                for row in staged_loan_docs:
                                    cat_id = row["category_id"]
                                    f = row["file"]
                                    notes = row.get("notes") or ""
                                    try:
                                        upload_document(
                                            "loan",
                                            loan_id,
                                            cat_id,
                                            f.name,
                                            f.type,
                                            f.size,
                                            f.getvalue(),
                                            uploaded_by="System User",
                                            notes=notes,
                                        )
                                        doc_count += 1
                                    except Exception as e:
                                        st.error(f"Failed to upload {f.name}: {e}")

                            st.session_state["capture_last_save_result"] = {
                                "success": True,
                                "loan_id": loan_id,
                                "doc_count": doc_count,
                            }
                            st.session_state["loan_docs_staged"] = []
                            for k in ["capture_loan_details", "capture_loan_schedule_df"]:
                                st.session_state.pop(k, None)
                            st.rerun()
                    except Exception as e:
                        st.session_state["capture_last_save_result"] = {"success": False, "error": str(e)}
                        st.rerun()
            with col_rework:
                if st.button("← Back to schedule builder", key="cap_back_sched_from_review"):
                    st.session_state["capture_loan_step"] = 1
                    st.rerun()
            with col_dismiss:
                if st.button("Dismiss", key="cap_dismiss_t3"):
                    for k in list(st.session_state.keys()):
                        if k.startswith("capture_"):
                            st.session_state.pop(k, None)
                    st.session_state["loan_docs_staged"] = []
                    st.success("Loan draft dismissed and removed from the capture flow.")
                    st.rerun()
            with col_back:
                if st.button("← Back", key="cap_back_2"):
                    st.session_state["capture_loan_step"] = 1
                    st.rerun()

def approve_loans_ui():
    """Approval inbox for loan drafts submitted from capture Stage 2."""
    st.subheader("Approve loans")
    if not _loan_management_available:
        st.error(f"Loan management module is not available. ({_loan_management_error})")
        return
    approve_flash = st.session_state.pop("approve_loans_flash_message", None)
    if approve_flash:
        st.success(str(approve_flash))

    # Small, compact search/filter row.
    f1, f2, f3, f4 = st.columns([2, 1, 1, 1])
    with f1:
        search_txt = st.text_input(
            "Search draft",
            placeholder="Draft ID / Customer ID / Product / Loan type",
            key="approve_loan_search",
        )
    with f2:
        show_status = st.selectbox(
            "Status",
            ["PENDING", "REWORK", "APPROVED", "DISMISSED"],
            index=0,
            key="approve_loan_status",
        )
    with f3:
        assigned_only = st.checkbox("Assigned to me", value=False, key="approve_assigned_only")
    with f4:
        st.write("")
        st.write("")
        if st.button("Clear selection", key="approve_clear_selection", width="stretch"):
            st.session_state.pop("approve_selected_draft_id", None)
            st.rerun()

    assigned_filter = None
    if assigned_only:
        current_uid = st.session_state.get("user_id")
        if current_uid is not None:
            assigned_filter = str(current_uid)

    drafts = list_loan_approval_drafts(
        status=show_status,
        search=search_txt.strip() or None,
        assigned_approver_id=assigned_filter,
        limit=500,
    )
    if not drafts:
        st.info("No loan drafts found for the selected filters.")
        return

    draft_options = [int(r["id"]) for r in drafts]
    selected_id = st.session_state.get("approve_selected_draft_id")
    if selected_id is not None and int(selected_id) not in draft_options:
        selected_id = None
        st.session_state.pop("approve_selected_draft_id", None)

    # When selected, show inspection panel FIRST (top), then keep inbox table below.
    if selected_id is not None:
        draft = get_loan_approval_draft(int(selected_id))
        if draft:
            details = draft.get("details_json") or {}
            schedule_rows = draft.get("schedule_json") or []
            df_schedule = pd.DataFrame(schedule_rows) if schedule_rows else pd.DataFrame()
            customer_name = (
                get_display_name(int(draft["customer_id"]))
                if _customers_available
                else f"Customer #{draft['customer_id']}"
            )

            st.markdown("### Draft inspection")
            p1, p2, p3, p4 = st.columns(4)
            with p1:
                st.caption("Identity")
                st.write(f"Draft: **{draft.get('id')}**")
                st.write(f"Customer: **{customer_name}**")
                st.write(f"Loan type: **{draft.get('loan_type')}**")
                st.write(f"Product: **{draft.get('product_code') or '—'}**")
            with p2:
                st.caption("Amounts")
                st.write(f"Principal: **{float(details.get('principal') or 0):,.2f}**")
                st.write(f"Disbursed: **{float(details.get('disbursed_amount') or 0):,.2f}**")
                st.write(f"Installment: **{float(details.get('installment') or 0):,.2f}**")
                st.write(f"Total payment: **{float(details.get('total_payment') or 0):,.2f}**")
            with p3:
                st.caption("Pricing")
                st.write(f"Annual rate: **{float(details.get('annual_rate') or 0) * 100:.2f}%**")
                st.write(f"Monthly rate: **{float(details.get('monthly_rate') or 0) * 100:.2f}%**")
                st.write(f"Penalty: **{float(details.get('penalty_rate_pct') or 0):.2f}%**")
                st.write(f"Fees: **{float(details.get('drawdown_fee') or 0) * 100:.2f}% / {float(details.get('arrangement_fee') or 0) * 100:.2f}%**")
            with p4:
                st.caption("Dates & status")
                st.write(f"Tenor: **{int(details.get('term') or 0)} months**")
                st.write(f"First repay: **{details.get('first_repayment_date') or '—'}**")
                st.write(f"Disbursed on: **{details.get('disbursement_date') or '—'}**")
                st.write(f"Status: **{draft.get('status')}**")

            with st.expander("View documents", expanded=False):
                if _documents_available:
                    docs = list_documents(entity_type="loan_approval_draft", entity_id=int(selected_id))
                    if not docs:
                        st.info("No documents attached to this draft.")
                    else:
                        doc_df = pd.DataFrame(docs)
                        show_doc_cols = [
                            c for c in ["category_name", "file_name", "file_size", "uploaded_by", "uploaded_at", "notes"]
                            if c in doc_df.columns
                        ]
                        st.dataframe(doc_df[show_doc_cols], width="stretch", hide_index=True, height=180)
                else:
                    st.info("Document module is unavailable.")

            with st.expander("View schedule", expanded=False):
                if df_schedule.empty:
                    st.info("No schedule found for this draft.")
                else:
                    st.dataframe(format_schedule_display(df_schedule), width="stretch", hide_index=True, height=220)

            note = st.text_input("Reviewer note (optional)", key="approve_reviewer_note")
            a1, a2, a3 = st.columns(3)
            with a1:
                if st.button("Approve and create loan", type="primary", key="approve_create_loan_btn"):
                    try:
                        loan_id = approve_loan_approval_draft(int(selected_id), approved_by="approver_ui")
                        # Copy draft documents to final loan entity.
                        doc_count = 0
                        if _documents_available:
                            docs = list_documents(entity_type="loan_approval_draft", entity_id=int(selected_id))
                            for row in docs:
                                full = get_document(int(row["id"]))
                                if not full:
                                    continue
                                upload_document(
                                    "loan",
                                    int(loan_id),
                                    int(full["category_id"]),
                                    str(full["file_name"]),
                                    str(full["file_type"]),
                                    int(full["file_size"]),
                                    full["file_content"],
                                    uploaded_by="System User",
                                    notes=str(full.get("notes") or ""),
                                )
                                doc_count += 1
                        st.session_state["approve_loans_flash_message"] = (
                            f"Loan approved successfully. Loan #{loan_id} created. "
                            f"{doc_count} document(s) copied."
                        )
                        st.session_state.pop("approve_selected_draft_id", None)
                        st.session_state["loan_mgmt_subnav"] = "Approve loans"
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not approve draft: {e}")
            with a2:
                if st.button("Send back for rework", key="approve_send_back_btn"):
                    try:
                        send_back_loan_approval_draft(int(selected_id), note=note or "", actor="approver_ui")
                        st.session_state["approve_loans_flash_message"] = (
                            f"Draft #{selected_id} sent back for rework."
                        )
                        st.session_state.pop("approve_selected_draft_id", None)
                        st.session_state["loan_mgmt_subnav"] = "Approve loans"
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not send back draft: {e}")
            with a3:
                if st.button("Dismiss draft", key="approve_dismiss_btn"):
                    try:
                        dismiss_loan_approval_draft(int(selected_id), note=note or "", actor="approver_ui")
                        st.session_state["approve_loans_flash_message"] = (
                            f"Draft #{selected_id} dismissed."
                        )
                        st.session_state.pop("approve_selected_draft_id", None)
                        st.session_state["loan_mgmt_subnav"] = "Approve loans"
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not dismiss draft: {e}")

            st.divider()

    # Inbox table always visible; select a row via compact "Open draft" controls.
    st.markdown("### Draft inbox")
    df = pd.DataFrame(drafts)
    show_cols = [
        c
        for c in [
            "id",
            "customer_id",
            "loan_type",
            "product_code",
            "assigned_approver_id",
            "status",
            "submitted_at",
        ]
        if c in df.columns
    ]
    st.dataframe(df[show_cols], width="stretch", hide_index=True, height=280)

    o1, o2, o3 = st.columns([2, 1, 1])
    with o1:
        open_label_map = {}
        open_labels = []
        for r in drafts:
            rid = int(r["id"])
            lbl = f"Draft {rid} · Cust {r.get('customer_id')} · {r.get('loan_type')} · {r.get('status')}"
            open_labels.append(lbl)
            open_label_map[lbl] = rid
        draft_pick = st.selectbox("Open draft", open_labels, key="approve_open_pick")
    with o2:
        manual_id = st.number_input("Draft ID", min_value=1, step=1, value=int(open_label_map.get(draft_pick, draft_options[0])), key="approve_open_manual_id")
    with o3:
        st.write("")
        st.write("")
        if st.button("Inspect draft", key="approve_open_btn", width="stretch"):
            st.session_state["approve_selected_draft_id"] = int(manual_id)
            st.rerun()


def customers_ui():
    """Web UI to add and manage customers (individuals and corporates)."""
    if not _customers_available:
        st.error(f"Customer module is not available. Check database connection and install: psycopg2-binary. ({_customers_error})")
        return

    # Match `st.header` visual scale used in Interest-in-Suspense; use logo-green font
    st.markdown(
        "<div style='color:#16A34A; font-weight:700; font-size:2rem; margin:0.25rem 0 0.75rem 0;'>Customers</div>",
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3, tab4 = st.tabs(["Add Individual", "Add Corporate", "View & manage", "Agents"])

    with tab1:
        st.subheader("New individual customer")
        col_main, _ = st.columns([1, 1])
        with col_main:
            with st.form("individual_form", clear_on_submit=True):
                col_id1, col_id2 = st.columns(2)
                with col_id1:
                    name = st.text_input("Full name *", placeholder="e.g. John Doe", key="ind_full_name")
                with col_id2:
                    national_id = st.text_input("National ID", placeholder="Optional", key="ind_national_id")
                sector_id, subsector_id = None, None
                if _customers_available:
                    sectors_list = list_sectors()
                    subsectors_list = list_subsectors()
                    if sectors_list:
                        sector_names = ["(None)"] + [s["name"] for s in sectors_list]
                        sel_sector_name = st.selectbox("Sector", sector_names, key="ind_sector")
                        sector_id = next((s["id"] for s in sectors_list if s["name"] == sel_sector_name), None) if sel_sector_name != "(None)" else None
                        subs_by_sector = [ss for ss in subsectors_list if sector_id and ss["sector_id"] == sector_id]
                        sub_names = ["(None)"] + [s["name"] for s in subs_by_sector]
                        sel_subsector_name = st.selectbox("Subsector", sub_names, key="ind_subsector")
                        subsector_id = next((s["id"] for s in subs_by_sector if s["name"] == sel_subsector_name), None) if sel_subsector_name != "(None)" else None
                col1, col2 = st.columns(2)
                with col1:
                    phone1 = st.text_input("Phone 1", placeholder="Optional", key="ind_phone1")
                    email1 = st.text_input("Email 1", placeholder="Optional", key="ind_email1")
                with col2:
                    phone2 = st.text_input("Phone 2", placeholder="Optional", key="ind_phone2")
                    email2 = st.text_input("Email 2", placeholder="Optional", key="ind_email2")
                employer_details = st.text_area("Employer details", placeholder="Optional", key="ind_employer_details", height=80)
                with st.expander("Addresses (optional)"):
                    addr_type = st.text_input("Address type", placeholder="e.g. physical, postal", key="ind_addr_type")
                    line1 = st.text_input("Address line 1", key="ind_addr_line1")
                    line2 = st.text_input("Address line 2", key="ind_addr_line2")
                    city = st.text_input("City", key="ind_addr_city")
                    region = st.text_input("Region", key="ind_addr_region")
                    postal_code = st.text_input("Postal code", key="ind_addr_postal_code")
                    country = st.text_input("Country", key="ind_addr_country")
                    use_addr = st.checkbox("Include this address", value=False, key="ind_use_addr")
                
                # Individual customer documents: single dropdown + uploader + staged list
                if "ind_docs_staged" not in st.session_state:
                    st.session_state["ind_docs_staged"] = []
                with st.expander("Documents (optional)"):
                    staged_ind_docs = st.session_state["ind_docs_staged"]
                    if _documents_available:
                        st.write("Upload individual KYC documents here. Max size 200MB per file.")
                        doc_cats = list_document_categories(active_only=True)
                        name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                        if not name_to_cat:
                            st.info("No matching document categories (Individual KYC) configured.")
                        else:
                            doc_type = st.selectbox(
                                "Document type",
                                sorted(name_to_cat.keys()),
                                key="ind_doc_type",
                            )
                            other_label = ""
                            if doc_type == "Other":
                                other_label = st.text_input(
                                    "If Other, describe the document",
                                    key="ind_doc_other_label",
                                )
                            f = st.file_uploader(
                                "Choose file",
                                type=["pdf", "png", "jpg", "jpeg"],
                                key="ind_doc_file",
                            )
                            notes = st.text_input("Notes (optional)", key="ind_doc_notes")
                            doc_add = st.form_submit_button("Save document to list", key="ind_doc_add")
                            if doc_add and f is not None:
                                cat = name_to_cat[doc_type]
                                label = other_label.strip() if doc_type == "Other" else notes.strip()
                                staged_ind_docs.append(
                                    {
                                        "category_id": cat["id"],
                                        "file": f,
                                        "notes": label or "",
                                    }
                                )
                                st.session_state["ind_docs_staged"] = staged_ind_docs
                                st.success(f"Staged {f.name} as {doc_type}.")
                        if staged_ind_docs:
                            st.markdown("**Staged documents:**")
                            for idx, row in enumerate(staged_ind_docs, start=1):
                                st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
                    else:
                        st.info("Document module is unavailable.")

                submitted = st.form_submit_button("Create individual")
                if submitted and name.strip():
                    addresses = None
                    if use_addr and line1.strip():
                        addresses = [{"address_type": addr_type or None, "line1": line1 or None, "line2": line2 or None, "city": city or None, "region": region or None, "postal_code": postal_code or None, "country": country or None}]
                    try:
                        cid = create_individual(
                            name=name.strip(),
                            national_id=national_id.strip() or None,
                            employer_details=employer_details.strip() or None,
                            phone1=phone1.strip() or None,
                            phone2=phone2.strip() or None,
                            email1=email1.strip() or None,
                            email2=email2.strip() or None,
                            addresses=addresses,
                            sector_id=sector_id,
                            subsector_id=subsector_id,
                        )
                        st.success(f"Individual customer created. Customer ID: **{cid}**.")
                        
                        staged_ind_docs = st.session_state.get("ind_docs_staged") or []
                        if _documents_available and staged_ind_docs:
                            doc_count = 0
                            for row in staged_ind_docs:
                                cat_id = row["category_id"]
                                f = row["file"]
                                notes = row.get("notes") or ""
                                try:
                                    upload_document(
                                        "customer",
                                        cid,
                                        cat_id,
                                        f.name,
                                        f.type,
                                        f.size,
                                        f.getvalue(),
                                        uploaded_by="System User",
                                        notes=notes,
                                    )
                                    doc_count += 1
                                except Exception as e:
                                    st.error(f"Failed to upload {f.name}: {e}")
                            if doc_count > 0:
                                st.success(f"Successfully uploaded {doc_count} documents.")
                        st.session_state["ind_docs_staged"] = []
                                
                    except Exception as e:
                        st.error(f"Could not create customer: {e}")
                elif submitted and not name.strip():
                    st.warning("Please enter a name.")

    with tab2:
        st.subheader("New corporate customer")
        col_main2, _ = st.columns([1, 1])
        with col_main2:
            with st.form("corporate_form", clear_on_submit=True):
                corp_top1, corp_top2 = st.columns(2)
                with corp_top1:
                    legal_name = st.text_input("Legal name *", placeholder="Company Ltd", key="corp_legal_name")
                    reg_number = st.text_input("Registration number", placeholder="Optional", key="corp_reg_number")
                with corp_top2:
                    trading_name = st.text_input("Trading name", placeholder="Optional", key="corp_trading_name")
                tin = st.text_input("TIN", placeholder="Optional", key="corp_tin")
                corp_sector_id, corp_subsector_id = None, None
                if _customers_available:
                    corp_sectors_list = list_sectors()
                    corp_subsectors_list = list_subsectors()
                    if corp_sectors_list:
                        corp_sector_names = ["(None)"] + [s["name"] for s in corp_sectors_list]
                        corp_sel_sector = st.selectbox("Sector", corp_sector_names, key="corp_sector")
                        corp_sector_id = next((s["id"] for s in corp_sectors_list if s["name"] == corp_sel_sector), None) if corp_sel_sector != "(None)" else None
                        corp_subs = [ss for ss in corp_subsectors_list if corp_sector_id and ss["sector_id"] == corp_sector_id]
                        corp_sub_names = ["(None)"] + [s["name"] for s in corp_subs]
                        corp_sel_subsector = st.selectbox("Subsector", corp_sub_names, key="corp_subsector")
                        corp_subsector_id = next((s["id"] for s in corp_subs if s["name"] == corp_sel_subsector), None) if corp_sel_subsector != "(None)" else None
                with st.expander("Addresses (optional)"):
                    addr_type = st.text_input("Address type", placeholder="e.g. registered, physical", key="corp_addr_type")
                    line1 = st.text_input("Address line 1", key="corp_addr_line1")
                    line2 = st.text_input("Address line 2", key="corp_addr_line2")
                    city = st.text_input("City", key="corp_addr_city")
                    region = st.text_input("Region", key="corp_addr_region")
                    postal_code = st.text_input("Postal code", key="corp_addr_postal_code")
                    country = st.text_input("Country", key="corp_addr_country")
                    use_addr = st.checkbox("Include this address", value=False, key="corp_use_addr")
                with st.expander("Contact person (optional)"):
                    cp_name = st.text_input("Full name", key="corp_cp_name")
                    cp_national_id = st.text_input("National ID", key="corp_cp_national_id")
                    cp_designation = st.text_input("Designation", key="corp_cp_designation")
                    cp_phone1 = st.text_input("Phone 1", key="corp_cp_phone1")
                    cp_phone2 = st.text_input("Phone 2", key="corp_cp_phone2")
                    cp_email = st.text_input("Email", key="corp_cp_email")
                    cp_addr1 = st.text_input("Address line 1", key="corp_cp_addr1")
                    cp_addr2 = st.text_input("Address line 2", key="corp_cp_addr2")
                    cp_city = st.text_input("City", key="corp_cp_city")
                    cp_country = st.text_input("Country", key="corp_cp_country")
                    use_cp = st.checkbox("Include contact person", value=False, key="corp_use_cp")
                    if "corp_contact_docs_staged" not in st.session_state:
                        st.session_state["corp_contact_docs_staged"] = []
                    staged_contact_docs = st.session_state["corp_contact_docs_staged"]
                    if _documents_available:
                        st.caption("Contact person documents")
                        doc_cats = list_document_categories(active_only=True)
                        name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                        if not name_to_cat:
                            st.info("No matching document categories (Contact person KYC) configured.")
                        else:
                            cp_doc_type = st.selectbox(
                                "Document type",
                                sorted(name_to_cat.keys()),
                                key="cp_doc_type",
                            )
                            cp_other_label = ""
                            if cp_doc_type == "Other":
                                cp_other_label = st.text_input(
                                    "If Other, describe the document",
                                    key="cp_doc_other_label",
                                )
                            cp_f = st.file_uploader(
                                "Choose file",
                                type=["pdf", "png", "jpg", "jpeg"],
                                key="cp_doc_file",
                            )
                            cp_notes = st.text_input("Notes (optional)", key="cp_doc_notes")
                            cp_add = st.form_submit_button("Save contact person document", key="cp_doc_add")
                            if cp_add and cp_f is not None:
                                cat = name_to_cat[cp_doc_type]
                                label = cp_other_label.strip() if cp_doc_type == "Other" else cp_notes.strip()
                                staged_contact_docs.append({"category_id": cat["id"], "file": cp_f, "notes": label or ""})
                                st.session_state["corp_contact_docs_staged"] = staged_contact_docs
                                st.success(f"Staged {cp_f.name} for contact person.")
                        if staged_contact_docs:
                            for idx, row in enumerate(staged_contact_docs, start=1):
                                st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
                with st.expander("Directors (optional)"):
                    dir_name = st.text_input("Director full name", key="corp_dir_name")
                    dir_national_id = st.text_input("Director national ID", key="corp_dir_national_id")
                    dir_designation = st.text_input("Director designation", key="corp_dir_designation")
                    dir_phone1 = st.text_input("Director phone 1", key="corp_dir_phone1")
                    dir_phone2 = st.text_input("Director phone 2", key="corp_dir_phone2")
                    dir_email = st.text_input("Director email", key="corp_dir_email")
                    use_dir = st.checkbox("Include this director", value=False, key="corp_use_dir")
                    if "corp_director_docs_staged" not in st.session_state:
                        st.session_state["corp_director_docs_staged"] = []
                    staged_director_docs = st.session_state["corp_director_docs_staged"]
                    if _documents_available:
                        st.caption("Director documents")
                        doc_cats = list_document_categories(active_only=True)
                        name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                        if not name_to_cat:
                            st.info("No matching document categories (Director KYC) configured.")
                        else:
                            dir_doc_type = st.selectbox(
                                "Document type",
                                sorted(name_to_cat.keys()),
                                key="dir_doc_type",
                            )
                            dir_other_label = ""
                            if dir_doc_type == "Other":
                                dir_other_label = st.text_input(
                                    "If Other, describe the document",
                                    key="dir_doc_other_label",
                                )
                            dir_f = st.file_uploader(
                                "Choose file",
                                type=["pdf", "png", "jpg", "jpeg"],
                                key="dir_doc_file",
                            )
                            dir_notes = st.text_input("Notes (optional)", key="dir_doc_notes")
                            dir_add = st.form_submit_button("Save director document", key="dir_doc_add")
                            if dir_add and dir_f is not None:
                                cat = name_to_cat[dir_doc_type]
                                label = dir_other_label.strip() if dir_doc_type == "Other" else dir_notes.strip()
                                staged_director_docs.append({"category_id": cat["id"], "file": dir_f, "notes": label or ""})
                                st.session_state["corp_director_docs_staged"] = staged_director_docs
                                st.success(f"Staged {dir_f.name} for director.")
                        if staged_director_docs:
                            for idx, row in enumerate(staged_director_docs, start=1):
                                st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
                with st.expander("Shareholders (optional)"):
                    sh_name = st.text_input("Shareholder full name", key="corp_sh_name")
                    sh_national_id = st.text_input("Shareholder national ID", key="corp_sh_national_id")
                    sh_designation = st.text_input("Shareholder designation", key="corp_sh_designation")
                    sh_phone1 = st.text_input("Shareholder phone 1", key="corp_sh_phone1")
                    sh_phone2 = st.text_input("Shareholder phone 2", key="corp_sh_phone2")
                    sh_email = st.text_input("Shareholder email", key="corp_sh_email")
                    sh_pct = st.number_input("Shareholding %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, key="corp_sh_pct")
                    use_sh = st.checkbox("Include this shareholder", value=False, key="corp_use_sh")
                
                # Corporate customer documents: single dropdown + uploader + staged list
                if "corp_docs_staged" not in st.session_state:
                    st.session_state["corp_docs_staged"] = []
                with st.expander("Documents (optional)"):
                    staged_corp_docs = st.session_state["corp_docs_staged"]
                    if _documents_available:
                        st.write("Upload corporate registration documents here. Max size 200MB per file.")
                        doc_cats = list_document_categories(active_only=True)
                        name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in CORPORATE_DOC_TYPES}
                        if not name_to_cat:
                            st.info("No matching document categories (Corporate KYC) configured.")
                        else:
                            doc_type = st.selectbox(
                                "Document type",
                                sorted(name_to_cat.keys()),
                                key="corp_doc_type",
                            )
                            other_label = ""
                            if doc_type == "Other":
                                other_label = st.text_input(
                                    "If Other, describe the document",
                                    key="corp_doc_other_label",
                                )
                            f = st.file_uploader(
                                "Choose file",
                                type=["pdf", "png", "jpg", "jpeg"],
                                key="corp_doc_file",
                            )
                            notes = st.text_input("Notes (optional)", key="corp_doc_notes")
                            doc_add_corp = st.form_submit_button("Save document to list", key="corp_doc_add")
                            if doc_add_corp and f is not None:
                                cat = name_to_cat[doc_type]
                                label = other_label.strip() if doc_type == "Other" else notes.strip()
                                staged_corp_docs.append(
                                    {
                                        "category_id": cat["id"],
                                        "file": f,
                                        "notes": label or "",
                                    }
                                )
                                st.session_state["corp_docs_staged"] = staged_corp_docs
                                st.success(f"Staged {f.name} as {doc_type}.")
                        if staged_corp_docs:
                            st.markdown("**Staged documents:**")
                            for idx, row in enumerate(staged_corp_docs, start=1):
                                st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
                    else:
                        st.info("Document module is unavailable.")

                submitted = st.form_submit_button("Create corporate")
                if submitted and legal_name.strip():
                    addresses = [{"address_type": addr_type or None, "line1": line1 or None, "line2": line2 or None, "city": city or None, "region": region or None, "postal_code": postal_code or None, "country": country or None}] if use_addr and line1.strip() else None
                    contact_person = None
                    if use_cp and cp_name.strip():
                        contact_person = {"full_name": cp_name.strip(), "national_id": cp_national_id.strip() or None, "designation": cp_designation.strip() or None, "phone1": cp_phone1.strip() or None, "phone2": cp_phone2.strip() or None, "email": cp_email.strip() or None, "address_line1": cp_addr1.strip() or None, "address_line2": cp_addr2.strip() or None, "city": cp_city.strip() or None, "country": cp_country.strip() or None}
                    directors = [{"full_name": dir_name.strip(), "national_id": dir_national_id.strip() or None, "designation": dir_designation.strip() or None, "phone1": dir_phone1.strip() or None, "phone2": dir_phone2.strip() or None, "email": dir_email.strip() or None, "address_line1": None, "address_line2": None, "city": None, "country": None}] if use_dir and dir_name.strip() else None
                    shareholders = [{"full_name": sh_name.strip(), "national_id": sh_national_id.strip() or None, "designation": sh_designation.strip() or None, "phone1": sh_phone1.strip() or None, "phone2": sh_phone2.strip() or None, "email": sh_email.strip() or None, "address_line1": None, "address_line2": None, "city": None, "country": None, "shareholding_pct": sh_pct}] if use_sh and sh_name.strip() else None
                    try:
                        created = create_corporate_with_entities(
                            legal_name=legal_name.strip(),
                            trading_name=trading_name.strip() or None,
                            reg_number=reg_number.strip() or None,
                            tin=tin.strip() or None,
                            addresses=addresses,
                            contact_person=contact_person,
                            directors=directors,
                            shareholders=shareholders,
                            sector_id=corp_sector_id,
                            subsector_id=corp_subsector_id,
                        )
                        cid = int(created["customer_id"])
                        st.success(f"Corporate customer created. Customer ID: **{cid}**.")
                        
                        staged_corp_docs = st.session_state.get("corp_docs_staged") or []
                        if _documents_available and staged_corp_docs:
                            doc_count = 0
                            for row in staged_corp_docs:
                                cat_id = row["category_id"]
                                f = row["file"]
                                notes = row.get("notes") or ""
                                try:
                                    upload_document(
                                        "customer",
                                        cid,
                                        cat_id,
                                        f.name,
                                        f.type,
                                        f.size,
                                        f.getvalue(),
                                        uploaded_by="System User",
                                        notes=notes,
                                    )
                                    doc_count += 1
                                except Exception as e:
                                    st.error(f"Failed to upload {f.name}: {e}")
                            if doc_count > 0:
                                st.success(f"Successfully uploaded {doc_count} documents.")
                        # Upload contact person docs to their own bucket/entity id.
                        staged_contact_docs = st.session_state.get("corp_contact_docs_staged") or []
                        contact_ids = created.get("contact_person_ids") or []
                        if _documents_available and staged_contact_docs and contact_ids:
                            cp_id = int(contact_ids[0])
                            cp_count = 0
                            for row in staged_contact_docs:
                                try:
                                    upload_document(
                                        "contact_person",
                                        cp_id,
                                        row["category_id"],
                                        row["file"].name,
                                        row["file"].type,
                                        row["file"].size,
                                        row["file"].getvalue(),
                                        uploaded_by="System User",
                                        notes=row.get("notes") or "",
                                    )
                                    cp_count += 1
                                except Exception as e:
                                    st.error(f"Failed to upload contact person doc {row['file'].name}: {e}")
                            if cp_count > 0:
                                st.success(f"Uploaded {cp_count} contact person document(s).")
                        # Upload director docs to their own bucket/entity id (first director from this form).
                        staged_director_docs = st.session_state.get("corp_director_docs_staged") or []
                        director_ids = created.get("director_ids") or []
                        if _documents_available and staged_director_docs and director_ids:
                            dir_id = int(director_ids[0])
                            dir_count = 0
                            for row in staged_director_docs:
                                try:
                                    upload_document(
                                        "director",
                                        dir_id,
                                        row["category_id"],
                                        row["file"].name,
                                        row["file"].type,
                                        row["file"].size,
                                        row["file"].getvalue(),
                                        uploaded_by="System User",
                                        notes=row.get("notes") or "",
                                    )
                                    dir_count += 1
                                except Exception as e:
                                    st.error(f"Failed to upload director doc {row['file'].name}: {e}")
                            if dir_count > 0:
                                st.success(f"Uploaded {dir_count} director document(s).")
                        st.session_state["corp_docs_staged"] = []
                        st.session_state["corp_contact_docs_staged"] = []
                        st.session_state["corp_director_docs_staged"] = []
                                
                    except Exception as e:
                        st.error(f"Could not create customer: {e}")
                        st.exception(e)
                elif submitted and not legal_name.strip():
                    st.warning("Please enter a legal name.")

    with tab3:
        st.subheader("View & manage customers")
        col_main3, _ = st.columns([1, 1])
        with col_main3:
            status_filter = st.selectbox("Status", ["all", "active", "inactive"], key="cust_status_filter")
            type_filter = st.selectbox("Type", ["all", "individual", "corporate"], key="cust_type_filter")
            status = None if status_filter == "all" else status_filter
            customer_type = None if type_filter == "all" else type_filter
            try:
                customers_list = list_customers(status=status, customer_type=customer_type)
            except Exception as e:
                st.error(f"Could not load customers: {e}")
                customers_list = []
            # Compact actions: keep toggles at top; only then ask for customer selection.
            action_col1, action_col2 = st.columns(2)
            with action_col1:
                show_status_tools = st.checkbox(
                    "Change customer status",
                    value=False,
                    key="cust_show_status_tools_top",
                )
            with action_col2:
                show_contact_docs_tools = st.checkbox(
                    "Contact person documents",
                    value=False,
                    key="cust_show_contact_docs_tools_top",
                )
            if not customers_list:
                st.info("No customers found. Add one in the tabs above.")
            loaded_id = None
            if (show_status_tools or show_contact_docs_tools) and customers_list:
                cust_options = [(int(c["id"]), get_display_name(int(c["id"])) or f"Customer #{c['id']}") for c in customers_list]
                labels = [f"{name} (ID {cid})" for cid, name in cust_options]
                sel_idx = 0
                if st.session_state.get("cust_loaded_id") is not None:
                    try:
                        prev_id = int(st.session_state["cust_loaded_id"])
                        sel_idx = next(i for i, (cid, _n) in enumerate(cust_options) if cid == prev_id)
                    except Exception:
                        sel_idx = 0
                st.divider()
                selected_label = st.selectbox(
                    "Select customer for selected action(s)",
                    labels,
                    index=sel_idx,
                    key="cust_action_select",
                )
                loaded_id = cust_options[labels.index(selected_label)][0] if selected_label else None
                st.session_state["cust_loaded_id"] = loaded_id
            elif not (show_status_tools or show_contact_docs_tools):
                st.caption("Enable an action above to select and manage a customer.")
                st.session_state.pop("cust_loaded_id", None)

            if loaded_id is not None:
                rec = get_customer(loaded_id)
                if not rec:
                    st.warning("Customer not found.")
                    st.session_state.pop("cust_loaded_id", None)
                else:
                    st.subheader(f"Customer #{loaded_id}")
                    # Human-readable profile view (avoid dumping raw JSON/object repr in UI).
                    ctype = rec.get("type") or "—"
                    cstatus = rec.get("status") or "—"
                    sector_id = rec.get("sector_id")
                    subsector_id = rec.get("subsector_id")
                    if ctype == "individual":
                        ind = rec.get("individual") or {}
                        cname = ind.get("name") or "—"
                        st.markdown(f"**Name:** {cname}")
                        st.caption(
                            f"Type: {ctype} · Status: {cstatus} · "
                            f"Sector: {sector_id if sector_id is not None else '—'} · "
                            f"Subsector: {subsector_id if subsector_id is not None else '—'}"
                        )
                        c1, c2 = st.columns(2)
                        with c1:
                            st.write(f"**National ID:** {ind.get('national_id') or '—'}")
                            st.write(f"**Phone 1:** {ind.get('phone1') or '—'}")
                            st.write(f"**Email 1:** {ind.get('email1') or '—'}")
                        with c2:
                            st.write(f"**Employer details:** {ind.get('employer_details') or '—'}")
                            st.write(f"**Phone 2:** {ind.get('phone2') or '—'}")
                            st.write(f"**Email 2:** {ind.get('email2') or '—'}")
                    else:
                        corp = rec.get("corporate") or {}
                        cname = corp.get("trading_name") or corp.get("legal_name") or "—"
                        st.markdown(f"**Corporate name:** {cname}")
                        st.caption(
                            f"Type: {ctype} · Status: {cstatus} · "
                            f"Sector: {sector_id if sector_id is not None else '—'} · "
                            f"Subsector: {subsector_id if subsector_id is not None else '—'}"
                        )
                        c1, c2 = st.columns(2)
                        with c1:
                            st.write(f"**Legal name:** {corp.get('legal_name') or '—'}")
                            st.write(f"**Registration number:** {corp.get('reg_number') or '—'}")
                        with c2:
                            st.write(f"**Trading name:** {corp.get('trading_name') or '—'}")
                            st.write(f"**TIN:** {corp.get('tin') or '—'}")

                    addrs = rec.get("addresses") or []
                    if addrs:
                        st.markdown("**Addresses**")
                        for i, a in enumerate(addrs, start=1):
                            line = ", ".join(
                                str(x).strip()
                                for x in [
                                    a.get("line1"),
                                    a.get("line2"),
                                    a.get("city"),
                                    a.get("region"),
                                    a.get("postal_code"),
                                    a.get("country"),
                                ]
                                if x
                            )
                            atype = a.get("address_type") or f"Address {i}"
                            st.write(f"- {atype}: {line or '—'}")

                    if show_status_tools:
                        current_status = rec.get("status", "active")
                        new_active = st.radio(
                            "Set status",
                            ["active", "inactive"],
                            index=0 if current_status == "active" else 1,
                            key="cust_set_status",
                        )
                        if st.button("Update status", key="cust_update_status"):
                            set_active(loaded_id, new_active == "active")
                            st.success(f"Status set to **{new_active}**.")
                            st.session_state["cust_loaded_id"] = loaded_id
                            st.rerun()

                    # Direct document upload to corporate sub-entities (separate buckets/IDs).
                    if show_contact_docs_tools and rec.get("type") == "corporate" and _documents_available:
                        doc_cats = list_document_categories(active_only=True) or []
                        # Contact person + directors share Individual KYC types plus Other.
                        name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}

                        if not name_to_cat:
                            st.info("No matching document categories configured for contact/director KYC.")
                        else:
                            cp_list = rec.get("contact_persons") or []
                            dir_list = rec.get("directors") or []

                            if cp_list:
                                st.divider()
                                st.subheader("Contact person documents")
                                cp_options = [(cp["id"], cp.get("full_name") or f"Contact #{cp['id']}") for cp in cp_list]
                                cp_id = st.selectbox(
                                    "Select contact person",
                                    options=[x[0] for x in cp_options],
                                    format_func=lambda i: next((n for (cid, n) in cp_options if cid == i), str(i)),
                                    key=f"cp_doc_pick_{loaded_id}",
                                )

                                cp_doc_type = st.selectbox(
                                    "Document type",
                                    sorted(name_to_cat.keys()),
                                    key=f"cp_doc_type_{loaded_id}",
                                )
                                cp_other_desc = ""
                                if cp_doc_type == "Other":
                                    cp_other_desc = st.text_input(
                                        "Other document name",
                                        key=f"cp_doc_other_{loaded_id}",
                                    )
                                cp_notes = st.text_input(
                                    "Notes (optional)",
                                    key=f"cp_doc_notes_{loaded_id}",
                                )
                                cp_file = st.file_uploader(
                                    "Choose file",
                                    type=["pdf", "png", "jpg", "jpeg"],
                                    key=f"cp_doc_file_{loaded_id}",
                                )
                                if st.button("Upload contact document", key=f"cp_doc_upload_{loaded_id}") and cp_file is not None:
                                    cat = name_to_cat[cp_doc_type]
                                    stored_notes = cp_other_desc.strip() if cp_doc_type == "Other" else cp_notes.strip()
                                    upload_document(
                                        "contact_person",
                                        int(cp_id),
                                        cat["id"],
                                        cp_file.name,
                                        cp_file.type,
                                        cp_file.size,
                                        cp_file.getvalue(),
                                        uploaded_by="System User",
                                        notes=stored_notes or "",
                                    )
                                    st.success("Contact person document uploaded.")

                            if dir_list:
                                st.divider()
                                st.subheader("Director documents")
                                dir_options = [(d["id"], d.get("full_name") or f"Director #{d['id']}") for d in dir_list]
                                dir_id = st.selectbox(
                                    "Select director",
                                    options=[x[0] for x in dir_options],
                                    format_func=lambda i: next((n for (did, n) in dir_options if did == i), str(i)),
                                    key=f"dir_doc_pick_{loaded_id}",
                                )

                                dir_doc_type = st.selectbox(
                                    "Document type",
                                    sorted(name_to_cat.keys()),
                                    key=f"dir_doc_type_{loaded_id}",
                                )
                                dir_other_desc = ""
                                if dir_doc_type == "Other":
                                    dir_other_desc = st.text_input(
                                        "Other document name",
                                        key=f"dir_doc_other_{loaded_id}",
                                    )
                                dir_notes = st.text_input(
                                    "Notes (optional)",
                                    key=f"dir_doc_notes_{loaded_id}",
                                )
                                dir_file = st.file_uploader(
                                    "Choose file",
                                    type=["pdf", "png", "jpg", "jpeg"],
                                    key=f"dir_doc_file_{loaded_id}",
                                )
                                if st.button("Upload director document", key=f"dir_doc_upload_{loaded_id}") and dir_file is not None:
                                    cat = name_to_cat[dir_doc_type]
                                    stored_notes = dir_other_desc.strip() if dir_doc_type == "Other" else dir_notes.strip()
                                    upload_document(
                                        "director",
                                        int(dir_id),
                                        cat["id"],
                                        dir_file.name,
                                        dir_file.type,
                                        dir_file.size,
                                        dir_file.getvalue(),
                                        uploaded_by="System User",
                                        notes=stored_notes or "",
                                    )
                                    st.success("Director document uploaded.")

            st.divider()
            if customers_list:
                df = pd.DataFrame(customers_list)
                df["display_name"] = df["id"].apply(lambda i: get_display_name(i))
                st.dataframe(
                    df[["id", "type", "status", "display_name", "created_at"]],
                    width="stretch",
                    hide_index=True,
                )

    with tab4:
        st.subheader("Agents")
        col_main4, _ = st.columns([1, 1])
        with col_main4:
            if not _agents_available:
                st.error(f"Agents module is not available. ({_agents_error})")
            else:
                status_agent = st.selectbox("Filter by status", ["active", "inactive", "all"], key="agent_status_filter")
                status_val = None if status_agent == "all" else status_agent
                try:
                    agents_list = list_agents(status=status_val)
                except Exception as e:
                    st.error(f"Could not load agents: {e}")
                    agents_list = []
                if agents_list:
                    df_agents = pd.DataFrame(agents_list)
                    cols_show = ["id", "name", "id_number", "phone1", "email", "commission_rate_pct", "tax_clearance_expiry", "status"]
                    cols_show = [c for c in cols_show if c in df_agents.columns]
                    st.dataframe(df_agents[cols_show], width="stretch", hide_index=True)
                else:
                    st.info("No agents found.")
                st.divider()
                ag_col1, ag_col2 = st.columns(2)
                with ag_col1:
                    show_add_agent = st.checkbox(
                        "Add Agent",
                        value=False,
                        key="agent_show_add_toggle",
                    )
                with ag_col2:
                    show_edit_agent = st.checkbox(
                        "Edit Agent",
                        value=False,
                        key="agent_show_edit_toggle",
                    )

                if not show_add_agent and not show_edit_agent:
                    st.caption("Enable an action above to add or edit an agent.")

                if show_add_agent:
                    st.subheader("Add agent")
                    with st.form("add_agent_form", clear_on_submit=True):
                        col_a1, col_a2 = st.columns(2)
                        with col_a1:
                            aname = st.text_input("Agent name *", key="agent_name")
                            atype_label = st.selectbox("Agent type", ["Individual", "Corporate"], key="agent_type")
                            aid_number = st.text_input("ID number", placeholder="e.g. 111111111x11", key="agent_id_number")
                            aaddr1 = st.text_input("Address line 1", key="agent_addr1")
                            acity = st.text_input("City", key="agent_city")
                            aphone1 = st.text_input("Phone 1", key="agent_phone1")
                            aemail = st.text_input("Email", key="agent_email")
                        with col_a2:
                            aaddr2 = st.text_input("Address line 2", key="agent_addr2")
                            acountry = st.text_input("Country", key="agent_country")
                            aphone2 = st.text_input("Phone 2", key="agent_phone2")
                        acommission = st.number_input("Commission rate %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, format="%.2f", key="agent_commission")
                        atin = st.text_input("TIN number", key="agent_tin")
                        atax_expiry = st.date_input("Tax clearance expiry", value=None, key="agent_tax_expiry")
                        # Agent documents (optional)
                        if "agent_docs_staged" not in st.session_state:
                            st.session_state["agent_docs_staged"] = []
                        staged_agent_docs = st.session_state["agent_docs_staged"]
                        with st.expander("Agent documents (optional)"):
                            if _documents_available:
                                atype_internal = "individual" if atype_label.lower().startswith("individual") else "corporate"
                                doc_cats = list_document_categories(active_only=True)
                                allowed = AGENT_INDIVIDUAL_DOC_TYPES if atype_internal == "individual" else AGENT_CORPORATE_DOC_TYPES
                                name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in allowed}
                                if not name_to_cat:
                                    st.info("No matching document categories configured for agents.")
                                else:
                                    doc_type = st.selectbox(
                                        "Document type",
                                        sorted(name_to_cat.keys()),
                                        key="agent_doc_type",
                                    )
                                    other_label = ""
                                    if doc_type == "Other":
                                        other_label = st.text_input(
                                            "If Other, describe the document",
                                            key="agent_doc_other_label",
                                        )
                                    f = st.file_uploader(
                                        "Choose file",
                                        type=["pdf", "png", "jpg", "jpeg"],
                                        key="agent_doc_file",
                                    )
                                    notes = st.text_input("Notes (optional)", key="agent_doc_notes")
                                    add_agent_doc = st.form_submit_button("Save document to list", key="agent_doc_add")
                                    if add_agent_doc and f is not None:
                                        cat = name_to_cat[doc_type]
                                        label = other_label.strip() if doc_type == "Other" else notes.strip()
                                        staged_agent_docs.append(
                                            {
                                                "category_id": cat["id"],
                                                "file": f,
                                                "notes": label or "",
                                            }
                                        )
                                        st.session_state["agent_docs_staged"] = staged_agent_docs
                                        st.success(f"Staged {f.name} as {doc_type}.")
                                if staged_agent_docs:
                                    st.markdown("**Staged documents:**")
                                    for idx, row in enumerate(staged_agent_docs, start=1):
                                        st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
                            else:
                                st.info("Document module is unavailable.")

                        submitted_create_agent = st.form_submit_button("Create agent")
                        if submitted_create_agent and aname.strip():
                            try:
                                atype_internal = "individual" if atype_label.lower().startswith("individual") else "corporate"
                                aid = create_agent(
                                    name=aname.strip(),
                                    agent_type=atype_internal,
                                    id_number=aid_number.strip() or None,
                                    address_line1=aaddr1.strip() or None,
                                    address_line2=aaddr2.strip() or None,
                                    city=acity.strip() or None,
                                    country=acountry.strip() or None,
                                    phone1=aphone1.strip() or None,
                                    phone2=aphone2.strip() or None,
                                    email=aemail.strip() or None,
                                    commission_rate_pct=acommission if acommission else None,
                                    tin_number=atin.strip() or None,
                                    tax_clearance_expiry=atax_expiry,
                                )
                                # Upload any staged agent documents
                                staged_agent_docs = st.session_state.get("agent_docs_staged") or []
                                if _documents_available and staged_agent_docs:
                                    doc_count = 0
                                    for row in staged_agent_docs:
                                        cat_id = row["category_id"]
                                        f = row["file"]
                                        notes = row.get("notes") or ""
                                        try:
                                            upload_document(
                                                "agent",
                                                aid,
                                                cat_id,
                                                f.name,
                                                f.type,
                                                f.size,
                                                f.getvalue(),
                                                uploaded_by="System User",
                                                notes=notes,
                                            )
                                            doc_count += 1
                                        except Exception as e:
                                            st.error(f"Failed to upload {f.name}: {e}")
                                    if doc_count > 0:
                                        st.success(f"Successfully uploaded {doc_count} agent document(s).")
                                st.session_state["agent_docs_staged"] = []
                                st.success(f"Agent created. Agent ID: **{aid}**.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Could not create agent: {e}")
                        elif submitted_create_agent and not aname.strip():
                            st.warning("Please enter agent name.")

                if show_edit_agent:
                    st.divider()
                    st.subheader("Edit agent")
                    edit_agent_id = st.number_input("Agent ID to edit", min_value=1, value=1, step=1, key="edit_agent_id")
                    if st.button("Load agent", key="agent_load_btn"):
                        st.session_state["agent_edit_loaded_id"] = edit_agent_id
                    loaded_agent_id = st.session_state.get("agent_edit_loaded_id")
                    if loaded_agent_id is not None:
                        arec = get_agent(loaded_agent_id)
                        if not arec:
                            st.warning("Agent not found.")
                            st.session_state.pop("agent_edit_loaded_id", None)
                        else:
                            with st.form("edit_agent_form"):
                                ename = st.text_input("Agent name *", value=arec.get("name") or "", key="edit_agent_name")
                                eid_number = st.text_input("ID number", value=arec.get("id_number") or "", key="edit_agent_id_number")
                                eaddr1 = st.text_input("Address line 1", value=arec.get("address_line1") or "", key="edit_agent_addr1")
                                eaddr2 = st.text_input("Address line 2", value=arec.get("address_line2") or "", key="edit_agent_addr2")
                                ecity = st.text_input("City", value=arec.get("city") or "", key="edit_agent_city")
                                ecountry = st.text_input("Country", value=arec.get("country") or "", key="edit_agent_country")
                                ephone1 = st.text_input("Phone 1", value=arec.get("phone1") or "", key="edit_agent_phone1")
                                ephone2 = st.text_input("Phone 2", value=arec.get("phone2") or "", key="edit_agent_phone2")
                                eemail = st.text_input("Email", value=arec.get("email") or "", key="edit_agent_email")
                                ecommission = st.number_input("Commission rate %", min_value=0.0, max_value=100.0, value=float(arec.get("commission_rate_pct") or 0), step=0.5, format="%.2f", key="edit_agent_commission")
                                etin = st.text_input("TIN number", value=arec.get("tin_number") or "", key="edit_agent_tin")
                                etax_expiry = st.date_input("Tax clearance expiry", value=arec.get("tax_clearance_expiry"), key="edit_agent_tax_expiry")
                                estatus = st.selectbox("Status", ["active", "inactive"], index=0 if (arec.get("status") or "active") == "active" else 1, key="edit_agent_status")
                                e_agent_type_label = st.selectbox(
                                    "Agent type",
                                    ["Individual", "Corporate"],
                                    index=0 if (arec.get("agent_type") or "individual") == "individual" else 1,
                                    key="edit_agent_type",
                                )
                                submitted_update_agent = st.form_submit_button("Update agent")
                                if submitted_update_agent and ename.strip():
                                    try:
                                        update_agent(
                                            loaded_agent_id,
                                            name=ename.strip(),
                                            id_number=eid_number.strip() or None,
                                            address_line1=eaddr1.strip() or None,
                                            address_line2=eaddr2.strip() or None,
                                            city=ecity.strip() or None,
                                            country=ecountry.strip() or None,
                                            phone1=ephone1.strip() or None,
                                            phone2=ephone2.strip() or None,
                                            email=eemail.strip() or None,
                                            commission_rate_pct=ecommission if ecommission else None,
                                            tin_number=etin.strip() or None,
                                            tax_clearance_expiry=etax_expiry,
                                            status=estatus,
                                            agent_type="individual" if e_agent_type_label.lower().startswith("individual") else "corporate",
                                        )
                                        st.success("Agent updated.")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Could not update agent: {e}")
                                elif submitted_update_agent and not ename.strip():
                                    st.warning("Please enter agent name.")


def view_schedule_ui():
    """View the amortization schedule of an existing loan."""
    if not _loan_management_available:
        st.error(f"Loan management module is not available. ({_loan_management_error})")
        return

    st.caption("Select a loan by ID or customer to view the stored repayment schedule.")

    loan_id = None
    search_by = st.radio("Find loan by", ["Loan ID", "Customer"], key="view_sched_by", horizontal=True)

    if search_by == "Loan ID":
        id_col, btn_col = st.columns([2, 1])
        with id_col:
            lid_input = st.number_input("Loan ID", min_value=1, value=1, step=1, key="view_sched_loan_id")
        with btn_col:
            st.write("")
            st.write("")
            load_by_id = st.button("Load schedule", key="view_sched_load_by_id", use_container_width=True)
        if load_by_id:
            loan = get_loan(int(lid_input)) if _loan_management_available else None
            if not loan:
                st.warning(f"Loan #{lid_input} not found.")
            else:
                loan_id = int(lid_input)
                st.session_state["view_schedule_loan_id"] = loan_id
        loan_id = st.session_state.get("view_schedule_loan_id")
    else:
        if not _customers_available:
            st.info("Customer module is required to select by customer.")
        else:
            customers_list = list_customers(status="active") or []
            if not customers_list:
                st.info("No customers found.")
            else:
                cust_options = [(c["id"], get_display_name(c["id"]) or f"Customer #{c['id']}") for c in customers_list]
                cust_labels = [t[1] for t in cust_options]
                cust_sel = st.selectbox("Customer", cust_labels, key="view_sched_cust")
                cid = cust_options[cust_labels.index(cust_sel)][0] if cust_sel else None
                if cid:
                    loans_list = get_loans_by_customer(cid)
                    if not loans_list:
                        st.info("No loans for this customer.")
                    else:
                        loan_options = [(l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}") for l in loans_list]
                        loan_labels = [t[1] for t in loan_options]
                        loan_sel = st.selectbox("Loan", loan_labels, key="view_sched_loan_sel")
                        if loan_sel:
                            loan_id = loan_options[loan_labels.index(loan_sel)][0]

    if loan_id:
        try:
            lines = get_schedule_lines(loan_id)
        except Exception as e:
            st.error(f"Could not load schedule: {e}")
            lines = []

        if not lines:
            st.info("No schedule stored for this loan (or loan has no instalments yet).")
        else:
            loan_info = get_loan(loan_id)
            if loan_info:
                st.markdown(f"**Loan #{loan_id}** · {loan_info.get('loan_type', '')} · Principal: {loan_info.get('principal', 0):,.2f} · Customer: {get_display_name(loan_info.get('customer_id')) if _customers_available else loan_info.get('customer_id')}")
            df = pd.DataFrame(lines)
            # Map DB column names to display names used by format_schedule_display
            col_map = {
                "payment": "Payment",
                "principal": "Principal",
                "interest": "Interest",
                "principal_balance": "Principal Balance",
                "total_outstanding": "Total Outstanding",
            }
            df = df.rename(columns=col_map)
            display_cols = [c for c in ["Period", "Date", "Payment", "Principal", "Interest", "Principal Balance", "Total Outstanding"] if c in df.columns]
            df_display = df[display_cols] if display_cols else df
            st.dataframe(format_schedule_display(df_display), width="stretch", hide_index=True)
            _schedule_export_downloads(
                df_display, file_stem=f"loan_{loan_id}_schedule", key_prefix=f"dl_sched_loan_view_{loan_id}"
            )


def teller_ui():
    """Teller module: single repayment capture and batch payments."""
    if not _customers_available:
        st.error("Customer module is required for Teller. Check database connection.")
        return
    if not _loan_management_available:
        st.error(f"Loan management module is not available. ({_loan_management_error})")
        return

    st.markdown(
        "<div style='color:#16A34A; font-weight:700; font-size:2rem; margin:0.25rem 0 0.75rem 0;'>Teller</div>",
        unsafe_allow_html=True,
    )

    # Extend Teller module with dedicated GL tabs for borrowings and write‑off recoveries.
    from accounting_service import AccountingService
    from decimal import Decimal

    acct_svc = AccountingService()

    tab_single, tab_batch, tab_reverse, tab_borrowing_payment, tab_writeoff_recovery = st.tabs(
        [
            "Single repayment",
            "Batch payments",
            "Reverse receipt",
            "Payment of borrowings",
            "Receipt from fully written-off loan",
        ]
    )

    with tab_single:
        st.subheader("Single repayment capture")
        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            options = [(c["id"], get_display_name(c["id"])) for c in customers_list]
            labels = [f"{name} (ID {cid})" for cid, name in options]
            idx = 0
            if "teller_customer_id" in st.session_state:
                try:
                    idx = next(i for i, (cid, _) in enumerate(options) if cid == st.session_state["teller_customer_id"])
                except StopIteration:
                    pass
            pick_col1, pick_col2 = st.columns(2)
            with pick_col1:
                sel = st.selectbox("Select customer", labels, index=idx, key="teller_cust_select")
            cid = options[labels.index(sel)][0] if sel and labels else None
            st.session_state["teller_customer_id"] = cid

            if cid:
                loans_list = get_loans_by_customer(cid)
                loans_active = [l for l in loans_list if l.get("status") == "active"]
                if not loans_active:
                    st.info("No active loans for this customer.")
                else:
                    loan_options = [(l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}") for l in loans_active]
                    loan_labels = [t[1] for t in loan_options]
                    with pick_col2:
                        loan_sel = st.selectbox("Select loan", loan_labels, key="teller_loan_select")
                    loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None

                    if loan_id:
                        # Amount due preview
                        try:
                            from loan_management import get_teller_amount_due_today
                            summary = get_teller_amount_due_today(loan_id)
                            amount_due = summary["amount_due_today"]
                            scheduled_total = None
                            repaid_total = None
                        except Exception:
                            amount_due = None
                            scheduled_total = None
                            repaid_total = None

                        if amount_due is not None:
                            help_text = (
                                f"Base arrears as at {summary.get('base_as_of_date')}: {float(summary.get('base_total_delinquency_arrears') or 0):,.2f}\n"
                                f"Less today's allocations to arrears buckets: {float(summary.get('today_allocations_to_delinquency') or 0):,.2f}\n"
                                f"Method: {summary.get('method')}"
                            )
                            st.metric(
                                label="Amount Due Today",
                                value=f"{amount_due:,.2f}",
                                help=help_text,
                            )

                        now = datetime.now()
                        _sys = _get_system_date()
                        st.caption(
                            "**Source cash / bank GL** — same control as **loan capture** step 1. "
                            "This choice applies to **this receipt only** (not the loan’s disbursement cash)."
                        )
                        _t_cash_lab, _t_cash_ids = _source_cash_gl_cached_labels_and_ids()
                        with st.form("teller_single_form", clear_on_submit=True):
                            if _t_cash_ids:
                                _t_sel = st.selectbox(
                                    SOURCE_CASH_GL_WIDGET_LABEL,
                                    range(len(_t_cash_lab)),
                                    format_func=lambda i: _t_cash_lab[i],
                                    key="teller_source_cash_gl",
                                )
                                _src_cash_gl = _t_cash_ids[_t_sel]
                            else:
                                _source_cash_gl_cache_empty_warning()
                                _src_cash_gl = None
                            f_col1, f_col2 = st.columns(2)
                            with f_col1:
                                amount = st.number_input("Amount", min_value=0.00, value=0.00, step=100.0, format="%.2f", key="teller_amount")
                                customer_ref = st.text_input("Customer reference (appears on loan statement)", placeholder="e.g. Receipt #123", key="teller_cust_ref")
                            with f_col2:
                                company_ref = st.text_input("Company reference (appears in general ledger)", placeholder="e.g. GL ref", key="teller_company_ref")
                            col1, col2 = st.columns(2)
                            with col1:
                                value_date = st.date_input("Value date", value=_sys, key="teller_value_date")
                            with col2:
                                system_date = st.date_input("System date", value=_sys, key="teller_system_date")
                            submitted = st.form_submit_button("Record repayment")
                            if submitted and amount > 0:
                                if not _src_cash_gl:
                                    st.error(
                                        "No source cash account is available. Rebuild the **source cash account cache** "
                                        "(System configurations → Accounting configurations), then try again."
                                    )
                                else:
                                    try:
                                        rid = record_repayment(
                                            loan_id=loan_id,
                                            amount=amount,
                                            payment_date=value_date,
                                            source_cash_gl_account_id=_src_cash_gl,
                                            customer_reference=customer_ref.strip() or None,
                                            company_reference=company_ref.strip() or None,
                                            value_date=value_date,
                                            system_date=datetime.combine(system_date, now.time()),
                                        )
                                        cfg = load_system_config_from_db() if _loan_management_available else {}
                                        allocate_repayment_waterfall(rid, system_config=cfg)
                                        st.success(
                                            f"Repayment recorded. **Repayment ID: {rid}**. "
                                            "Any overpayment was credited to Unapplied Funds."
                                        )
                                    except Exception as e:
                                        st.error(f"Could not record repayment: {e}")
                                        st.exception(e)

    with tab_batch:
        st.subheader("Batch payments")
        st.caption(
            "Upload an Excel file with repayment rows. **source_cash_gl_account_id** must be a UUID that appears in the "
            "**source cash account cache** (same list as Teller — leaves under **A100000**). Rebuild the cache under "
            "**System configurations → Accounting configurations** when the chart changes."
        )

        # Template download
        template_df = pd.DataFrame(columns=[
            "loan_id",
            "amount",
            "payment_date",
            "value_date",
            "customer_reference",
            "company_reference",
            "source_cash_gl_account_id",
        ])
        today = _get_system_date().isoformat()
        template_df.loc[0] = [1, 100.00, today, today, "Receipt-001", "GL-001", ""]
        buf = BytesIO()
        template_df.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        b_col1, b_col2 = st.columns(2)
        with b_col1:
            st.download_button(
                "Download template (Excel)",
                data=buf,
                file_name="teller_batch_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="teller_download_template",
            )
        with b_col2:
            uploaded = st.file_uploader("Upload Excel file", type=["xlsx", "xls"], key="teller_batch_upload")
        if uploaded:
            try:
                df = pd.read_excel(uploaded, engine="openpyxl")
                required = ["loan_id", "amount", "source_cash_gl_account_id"]
                missing = [c for c in required if c not in df.columns]
                if missing:
                    st.error(f"Missing columns: {', '.join(missing)}. Use the template.")
                else:
                    st.dataframe(df.head(20), width="stretch", hide_index=True)
                    if len(df) > 20:
                        st.caption(f"Showing first 20 of {len(df)} rows.")
                    p_col1, p_col2 = st.columns(2)
                    with p_col1:
                        process_batch = st.button("Process batch", type="primary", key="teller_batch_process")
                    with p_col2:
                        st.caption(f"Rows loaded: {len(df)}")
                    if process_batch:
                        valid_rows = []
                        parse_errors = []
                        for i, r in df.iterrows():
                            try:
                                lid = int(r["loan_id"])
                                amt = float(r["amount"])
                                if lid <= 0 or amt <= 0:
                                    parse_errors.append(f"Row {i + 2}: loan_id and amount must be positive")
                                    continue
                                pdate = r.get("payment_date")
                                if pd.isna(pdate):
                                    pdate = _get_system_date().isoformat()
                                elif hasattr(pdate, "date"):
                                    pdate = pdate.date().isoformat()
                                else:
                                    pdate = str(pdate)[:10]
                                vdate = r.get("value_date")
                                if pd.notna(vdate) and hasattr(vdate, "date"):
                                    vdate = vdate.date().isoformat()
                                elif pd.notna(vdate):
                                    vdate = str(vdate)[:10]
                                else:
                                    vdate = pdate
                                raw_gl = r.get("source_cash_gl_account_id")
                                if raw_gl is None or (isinstance(raw_gl, float) and pd.isna(raw_gl)):
                                    src_gl = None
                                else:
                                    src_gl = str(raw_gl).strip() or None
                                if not src_gl:
                                    parse_errors.append(
                                        f"Row {i + 2}: source_cash_gl_account_id is required (posting account UUID from chart)"
                                    )
                                    continue
                                valid_rows.append({
                                    "loan_id": lid,
                                    "amount": amt,
                                    "payment_date": pdate,
                                    "value_date": vdate,
                                    "customer_reference": str(r.get("customer_reference", "")).strip() or None,
                                    "company_reference": str(r.get("company_reference", "")).strip() or None,
                                    "source_cash_gl_account_id": src_gl,
                                })
                            except (ValueError, TypeError) as e:
                                parse_errors.append(f"Row {i + 2}: {e}")
                        if parse_errors:
                            st.warning(f"Parse issues: {len(parse_errors)} row(s) skipped.")
                            with st.expander("Parse errors"):
                                for err in parse_errors:
                                    st.text(err)
                        if not valid_rows:
                            st.error("No valid rows to process. Ensure loan_id and amount are numeric and positive.")
                        else:
                            success, fail, errors = record_repayments_batch(valid_rows)
                            st.success(f"Batch complete: **{success}** repaid, **{fail}** failed.")
                            if errors:
                                with st.expander("Processing errors"):
                                    for err in errors:
                                        st.text(err)
            except Exception as e:
                st.error(f"Could not read file: {e}")
                st.exception(e)

    with tab_reverse:
        st.subheader("Reverse receipt")
        st.caption("Select a customer and loan, then enter a receipt ID or choose one from the list to reverse it.")

        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            options = [(c["id"], get_display_name(c["id"])) for c in customers_list]
            labels = [f"{name} (ID {cid})" for cid, name in options]
            idx = 0
            if "teller_rev_customer_id" in st.session_state:
                try:
                    idx = next(i for i, (cid, _) in enumerate(options) if cid == st.session_state["teller_rev_customer_id"])
                except StopIteration:
                    pass
            rev_col1, rev_col2 = st.columns(2)
            with rev_col1:
                sel = st.selectbox("Select customer", labels, index=idx, key="teller_rev_cust_select")
            cid = options[labels.index(sel)][0] if sel and labels else None
            st.session_state["teller_rev_customer_id"] = cid

            if cid:
                loans_list = get_loans_by_customer(cid)
                loans_active = [l for l in loans_list if l.get("status") == "active"]
                if not loans_active:
                    st.info("No active loans for this customer.")
                else:
                    loan_options = [
                        (l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}")
                        for l in loans_active
                    ]
                    loan_labels = [t[1] for t in loan_options]
                    with rev_col2:
                        loan_sel = st.selectbox("Select loan", loan_labels, key="teller_rev_loan_select")
                    loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None

                    if loan_id:
                        # Fetch recent receipts for this loan (last 12 months)
                        today = _get_system_date()
                        start_date = today - timedelta(days=365)
                        try:
                            receipts = get_repayments_with_allocations(loan_id, start_date, today)
                        except Exception:
                            receipts = []

                        col_id, col_list = st.columns(2)
                        with col_id:
                            manual_id = st.text_input("Receipt ID (optional)", key="teller_rev_manual_id")
                        with col_list:
                            if receipts:
                                receipt_options = []
                                for r in receipts:
                                    rid = int(r.get("id"))
                                    amt = float(r.get("amount") or 0)
                                    vdate = r.get("value_date") or r.get("payment_date")
                                    label = f"ID {rid} | {vdate} | Amount {amt:,.2f}"
                                    receipt_options.append((rid, label))
                                rec_labels = [t[1] for t in receipt_options]
                                sel_label = st.selectbox(
                                    "Or select from recent receipts",
                                    rec_labels if rec_labels else ["(No receipts)"],
                                    key="teller_rev_receipt_select",
                                )
                                selected_id = None
                                if rec_labels and sel_label in rec_labels:
                                    selected_id = receipt_options[rec_labels.index(sel_label)][0]
                            else:
                                st.info("No receipts found for this loan in the last 12 months.")
                                selected_id = None

                        if st.button("Reverse receipt", type="primary", key="teller_rev_button"):
                            target_id = None
                            if manual_id.strip():
                                try:
                                    target_id = int(manual_id.strip())
                                except ValueError:
                                    st.error("Receipt ID must be a number.")
                            elif selected_id is not None:
                                target_id = selected_id

                            if not target_id:
                                st.error("Enter a valid receipt ID or select a receipt from the list.")
                            else:
                                try:
                                    st.info(
                                        f"**Reversal in progress** for receipt **{target_id}**. "
                                        "This will (1) save the reversal and (2) replay/reallocate EOD for the loan."
                                    )
                                    with st.spinner("Reversing receipt and recalculating loan state…"):
                                        rev_result: ReverseRepaymentResult = reverse_repayment(target_id)

                                    st.success(
                                        f"Reversal saved for receipt **{target_id}**. "
                                        f"Reversal repayment id **{rev_result.reversal_repayment_id}**."
                                    )

                                    if rev_result.eod_rerun_success:
                                        st.success(
                                            f"Re-allocation/EOD replay **successful** for loan **{rev_result.loan_id}** "
                                            f"from **{rev_result.eod_from_date.isoformat()}** through "
                                            f"**{rev_result.eod_to_date.isoformat()}**."
                                        )
                                    else:
                                        st.error(
                                            f"Reversal was saved, but **re-allocation/EOD replay failed** for loan "
                                            f"**{rev_result.loan_id}** (window "
                                            f"**{rev_result.eod_from_date.isoformat()}** → "
                                            f"**{rev_result.eod_to_date.isoformat()}**). "
                                            f"**Failed stage:** `{(rev_result.eod_rerun_error or 'unknown')}`"
                                        )
                                except Exception as e:
                                    st.error(
                                        f"Could not reverse receipt **{target_id}**. "
                                        f"**Failed stage:** `reverse_repayment` | **Error:** {e}"
                                    )
                                    st.exception(e)

    with tab_borrowing_payment:
        st.subheader("Payment of borrowings")
        st.caption(
            "Use this tab to post payments made to external lenders/borrowings. "
            "This uses the configured 'BORROWING_REPAYMENT' journal template."
        )

        _sys = _get_system_date()
        now = datetime.now()

        with st.form("teller_borrowing_payment_form"):
            bw_col1, bw_col2 = st.columns(2)
            with bw_col1:
                value_date = st.date_input("Payment value date", value=_sys, key="teller_borrowing_value_date")
                amount = st.number_input(
                    "Payment amount",
                    min_value=0.01,
                    value=1000.00,
                    step=100.00,
                    format="%.2f",
                    key="teller_borrowing_amount",
                )
                reference = st.text_input(
                    "Reference",
                    placeholder="e.g. Borrowing repayment ref",
                    key="teller_borrowing_ref",
                )
            with bw_col2:
                system_date = st.date_input("System date", value=_sys, key="teller_borrowing_system_date")
                description = st.text_input(
                    "Narration (Description)",
                    placeholder="e.g. Payment of borrowing to financier X",
                    key="teller_borrowing_desc",
                )

            submitted = st.form_submit_button("Post borrowing payment")
            if submitted:
                try:
                    acct_svc.post_event(
                        event_type="BORROWING_REPAYMENT",
                        reference=reference.strip() or None,
                        description=description.strip() or "Payment of borrowings",
                        event_id="BORROWING",  # high-level tag; not a customer loan
                        created_by="teller_ui",
                        entry_date=value_date,
                        amount=Decimal(str(amount)),
                        payload=None,
                        is_reversal=False,
                    )
                    st.success("Borrowing payment journal posted successfully.")
                except Exception as e:
                    st.error(f"Error posting borrowing payment journal: {e}")
                    st.exception(e)

    with tab_writeoff_recovery:
        st.subheader("Receipt from a fully written-off loan")
        st.caption(
            "Use this tab when you receive a recovery on a loan that has been fully written off. "
            "This uses the configured 'WRITEOFF_RECOVERY' journal template "
            "(Debit: CASH AND CASH EQUIVALENTS, Credit: BAD DEBTS RECOVERED)."
        )

        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.info("No active customers. Add customers first.")
        else:
            options = [(c["id"], get_display_name(c["id"])) for c in customers_list]
            labels = [f"{name} (ID {cid})" for cid, name in options]
            idx = 0
            if "teller_wr_customer_id" in st.session_state:
                try:
                    idx = next(
                        i for i, (cid, _) in enumerate(options) if cid == st.session_state["teller_wr_customer_id"]
                    )
                except StopIteration:
                    pass
            wr_col1, wr_col2 = st.columns(2)
            with wr_col1:
                sel = st.selectbox("Select customer", labels, index=idx, key="teller_wr_cust_select")
            cid = options[labels.index(sel)][0] if sel and labels else None
            st.session_state["teller_wr_customer_id"] = cid

            if cid:
                loans_list = get_loans_by_customer(cid)
                # Include all loans; recoveries can apply to closed/written-off loans.
                if not loans_list:
                    st.info("No loans found for this customer.")
                else:
                    loan_options = [
                        (
                            l["id"],
                            f"Loan #{l['id']} | Status: {l.get('status', 'unknown')} | Principal: {l.get('principal', 0):,.2f}",
                        )
                        for l in loans_list
                    ]
                    loan_labels = [t[1] for t in loan_options]
                    with wr_col2:
                        loan_sel = st.selectbox(
                            "Select written-off loan (or target loan)", loan_labels, key="teller_wr_loan"
                        )
                    loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None

                    if loan_id:
                        _sys = _get_system_date()
                        now = datetime.now()

                        with st.form("teller_writeoff_recovery_form"):
                            wrf_col1, wrf_col2 = st.columns(2)
                            with wrf_col1:
                                value_date = st.date_input(
                                    "Receipt value date", value=_sys, key="teller_wr_value_date"
                                )
                                amount = st.number_input(
                                    "Recovery amount",
                                    min_value=0.01,
                                    value=100.00,
                                    step=10.00,
                                    format="%.2f",
                                    key="teller_wr_amount",
                                )
                                customer_ref = st.text_input(
                                    "Customer reference (optional)",
                                    placeholder="e.g. Recovery receipt #123",
                                    key="teller_wr_cust_ref",
                                )
                            with wrf_col2:
                                system_date = st.date_input(
                                    "System date", value=_sys, key="teller_wr_system_date"
                                )
                                company_ref = st.text_input(
                                    "Company reference (optional)",
                                    placeholder="e.g. GL ref",
                                    key="teller_wr_company_ref",
                                )
                            submitted = st.form_submit_button("Post recovery receipt")

                            if submitted and amount > 0:
                                try:
                                    acct_svc.post_event(
                                        event_type="WRITEOFF_RECOVERY",
                                        reference=company_ref.strip() or customer_ref.strip() or None,
                                        description=(
                                            f"Recovery on written-off loan #{loan_id}"
                                            if not company_ref and not customer_ref
                                            else (company_ref or customer_ref)
                                        ),
                                        event_id=str(loan_id),
                                        created_by="teller_ui",
                                        entry_date=value_date,
                                        amount=Decimal(str(amount)),
                                        payload=None,
                                        is_reversal=False,
                                        loan_id=int(loan_id),
                                    )
                                    st.success(
                                        f"Recovery receipt posted successfully for loan #{loan_id}. "
                                        "The GL will debit CASH AND CASH EQUIVALENTS and credit BAD DEBTS RECOVERED."
                                    )
                                except Exception as e:
                                    st.error(f"Error posting recovery receipt journal: {e}")
                                    st.exception(e)


def reamortisation_ui():
    """
    Reamortisation: Loan Modification (new terms/agreement) and Loan Recast (prepayment → new instalment).
    """
    st.markdown(
        "<div style='background-color: #1E40AF; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Reamortisation</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    if not _loan_management_available:
        st.error(_loan_management_error or "Loan management not available.")
        return

    try:
        from loan_management import get_loan, get_loans_by_customer, get_latest_schedule_version
        from reamortisation import (
            get_loan_for_modification,
            preview_loan_modification,
            execute_loan_modification,
            preview_loan_recast,
            execute_loan_recast,
            list_unapplied_funds,
        )
    except ImportError as e:
        st.error(f"Reamortisation module not available: {e}")
        return

    tab_mod, tab_recast, tab_unapplied = st.tabs(
        ["Loan Modification", "Loan Recast", "Unapplied Funds"]
    )
    customers = list_customers() if _customers_available else []

    with tab_mod:
        st.subheader("Loan Modification (New Terms / Agreement)")
        st.caption(
            "Select an existing loan and apply new terms (rate, term, loan type). "
            "Restructure date cannot be in the future or before the last due date. "
            "Outstanding interest can be capitalised or written off."
        )
        if not customers:
            st.info("No customers. Create a customer first.")
        else:
            cust_sel = st.selectbox(
                "Customer",
                [get_display_name(c["id"]) for c in customers],
                key="reamod_cust",
            )
            cust_id = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel)
            loans = get_loans_by_customer(cust_id)
            loans_active = [l for l in loans if l.get("status") == "active"]
            if not loans_active:
                st.info("No active loans for this customer.")
            else:
                loan_options = [(l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}") for l in loans_active]
                loan_labels = [t[1] for t in loan_options]
                loan_sel = st.selectbox("Select loan", loan_labels, key="reamod_loan")
                loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None
                if loan_id:
                    info = get_loan_for_modification(loan_id)
                    if not info:
                        st.warning("Could not load loan details.")
                    else:
                        loan = info["loan"]
                        last_due = info.get("last_due_date")
                        st.caption(f"Current schedule version: {info['schedule_version']}. Last due date: {last_due}.")
                        restructure_date = st.date_input(
                            "Restructure date (not future, not before last due)",
                            value=datetime.now().date(),
                            max_value=datetime.now().date(),
                            key="reamod_date",
                        )
                        if last_due and restructure_date > last_due:
                            st.error("Restructure date cannot be after the last due date.")
                        elif last_due and restructure_date < _get_system_date() and restructure_date < last_due:
                            pass
                        new_loan_type = st.selectbox(
                            "Modified loan type",
                            ["consumer_loan", "term_loan", "bullet_loan", "customised_repayments"],
                            key="reamod_type",
                        )
                        new_term = st.number_input("New term (months)", min_value=1, value=12, key="reamod_term")
                        new_annual_rate = st.number_input("New annual rate (%)", min_value=0.0, value=float(loan.get("annual_rate") or 0), step=0.1, key="reamod_rate")
                        outstanding_interest = st.selectbox(
                            "Outstanding interest",
                            ["capitalise", "write_off"],
                            key="reamod_interest",
                        )

                        def _reamod_params():
                            p = {"term": new_term, "annual_rate": new_annual_rate}
                            if new_loan_type == "consumer_loan":
                                p["monthly_rate"] = new_annual_rate / 12.0
                                import numpy_financial as npf
                                p["installment"] = float(npf.pmt(new_annual_rate / 1200, new_term, -float(loan.get("principal") or loan.get("disbursed_amount") or 0)))
                            elif new_loan_type == "term_loan":
                                p["grace_type"] = loan.get("grace_type") or "none"
                                p["moratorium_months"] = loan.get("moratorium_months") or 0
                            elif new_loan_type == "bullet_loan":
                                from datetime import datetime as dt
                                p["end_date"] = dt.combine(restructure_date, dt.min.time())
                                p["bullet_type"] = loan.get("bullet_type") or "with_interest"
                            return p

                        preview_key = "reamod_preview"
                        if st.button("Preview schedule", type="secondary", key="reamod_preview_btn"):
                            try:
                                new_params = _reamod_params()
                                preview = preview_loan_modification(
                                    loan_id,
                                    restructure_date,
                                    new_loan_type,
                                    new_params,
                                    outstanding_interest,
                                )
                                st.session_state[preview_key] = {
                                    **preview,
                                    "loan_id": loan_id,
                                    "restructure_date": restructure_date,
                                    "new_params": new_params,
                                    "outstanding_interest": outstanding_interest,
                                }
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))

                        if st.session_state.get(preview_key) and st.session_state[preview_key].get("loan_id") == loan_id:
                            pr = st.session_state[preview_key]
                            st.subheader("Proposed schedule (inspect before commit)")
                            cap = f"New principal: **{pr['new_principal']:,.2f}**"
                            if pr.get("new_installment") is not None:
                                cap += f" | New instalment: **{pr['new_installment']:,.2f}**"
                            st.caption(cap)
                            df_preview = pr["schedule_df"]
                            st.dataframe(
                                format_schedule_display(df_preview),
                                width="stretch",
                                hide_index=True,
                            )
                            _schedule_export_downloads(
                                df_preview,
                                file_stem=f"loan_{loan_id}_modification_preview_schedule",
                                key_prefix=f"dl_reamod_sched_{loan_id}",
                            )
                            if st.button("Commit modification", type="primary", key="reamod_commit"):
                                try:
                                    v = execute_loan_modification(
                                        pr["loan_id"],
                                        pr["restructure_date"],
                                        pr["new_loan_type"],
                                        pr["new_params"],
                                        pr["outstanding_interest"],
                                    )
                                    if preview_key in st.session_state:
                                        del st.session_state[preview_key]
                                    st.success(f"Loan modification applied. New schedule version: {v}.")
                                    st.rerun()
                                except Exception as ex:
                                    st.error(str(ex))
                            if st.button("Cancel preview", key="reamod_cancel_preview"):
                                if preview_key in st.session_state:
                                    del st.session_state[preview_key]
                                st.rerun()

    with tab_recast:
        st.subheader("Loan Recast (Prepayment → New Instalment)")
        st.caption(
            "Re-amortise the loan from a given date to original maturity with a new principal balance. "
            "Same rate and type; only the instalment changes. Use when the borrower has made a lump-sum payment."
        )
        if _customers_available and customers:
            cust_sel_r = st.selectbox("Customer", [get_display_name(c["id"]) for c in customers], key="recast_cust")
            cust_id_r = next(c["id"] for c in customers if get_display_name(c["id"]) == cust_sel_r)
            loans_r = get_loans_by_customer(cust_id_r)
            loans_active_r = [l for l in loans_r if l.get("status") == "active"]
            if not loans_active_r:
                st.info("No active loans.")
            else:
                loan_opts = [(l["id"], f"Loan #{l['id']}") for l in loans_active_r]
                loan_labels_r = [t[1] for t in loan_opts]
                loan_sel_r = st.selectbox("Select loan", loan_labels_r, key="recast_loan")
                loan_id_r = loan_opts[loan_labels_r.index(loan_sel_r)][0] if loan_sel_r else None
                if loan_id_r:
                    recast_date = st.date_input("Recast effective date", value=_get_system_date(), key="recast_date")
                    from loan_management import get_loan_daily_state_balances
                    bal = get_loan_daily_state_balances(loan_id_r, recast_date)
                    new_principal = st.number_input(
                        "New principal balance (after prepayment)",
                        min_value=0.01,
                        value=round((bal["principal_not_due"] + bal["principal_arrears"]) if bal else 0, 2) or 1000.0,
                        step=100.0,
                        key="recast_principal",
                    )

                    recast_preview_key = "recast_preview"
                    if st.button("Preview recast", type="secondary", key="recast_preview_btn"):
                        try:
                            preview = preview_loan_recast(loan_id_r, recast_date, new_principal)
                            st.session_state[recast_preview_key] = {
                                **preview,
                                "loan_id": loan_id_r,
                                "recast_date": recast_date,
                                "new_principal_balance": new_principal,
                            }
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))

                    if st.session_state.get(recast_preview_key) and st.session_state[recast_preview_key].get("loan_id") == loan_id_r:
                        rp = st.session_state[recast_preview_key]
                        st.subheader("Proposed recast schedule (inspect before commit)")
                        st.caption(f"New instalment: **{rp['new_installment']:,.2f}**")
                        st.dataframe(
                            format_schedule_display(rp["schedule_df"]),
                            width="stretch",
                            hide_index=True,
                        )
                        _schedule_export_downloads(
                            rp["schedule_df"],
                            file_stem=f"loan_{loan_id_r}_recast_preview_schedule",
                            key_prefix=f"dl_recast_sched_{loan_id_r}",
                        )
                        if st.button("Commit recast", type="primary", key="recast_commit"):
                            try:
                                inst = execute_loan_recast(
                                    rp["loan_id"],
                                    rp["recast_date"],
                                    rp["new_principal_balance"],
                                )
                                if recast_preview_key in st.session_state:
                                    del st.session_state[recast_preview_key]
                                st.success(f"Recast applied. New instalment: {inst:,.2f}.")
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
                        if st.button("Cancel preview", key="recast_cancel_preview"):
                            if recast_preview_key in st.session_state:
                                del st.session_state[recast_preview_key]
                            st.rerun()

    with tab_unapplied:
        st.subheader("Unapplied Funds (Suspense)")
        st.caption("Overpayments credited here. Apply to the loan via recast (reclassify accrued→arrears, principal not due→arrears, then apply). Recast is only available after funds are in Unapplied.")
        rows = list_unapplied_funds(status="pending")
        if not rows:
            st.info("No pending unapplied funds.")
        else:
            df_ua = pd.DataFrame(rows)
            cols = [c for c in ["id", "loan_id", "amount", "currency", "value_date", "status", "created_at"] if c in df_ua.columns]
            st.dataframe(df_ua[cols] if cols else df_ua, width="stretch", hide_index=True)
            st.markdown("**Apply via recast** (applies this entry to the loan: accrued interest→interest arrears, then principal not due→principal arrears).")
            for r in rows:
                uf_id = r.get("id")
                loan_id_ua = r.get("loan_id")
                amt = r.get("amount", 0)
                vd = r.get("value_date", "")
                with st.container():
                    c1, c2 = st.columns([3, 1])
                    with c1:
                        st.caption(f"Entry {uf_id}: Loan {loan_id_ua} · {amt:,.2f} · {vd}")
                    with c2:
                        if st.button("Apply via recast", key=f"unapplied_recast_{uf_id}"):
                            try:
                                apply_unapplied_funds_recast(uf_id)
                                st.success(f"Unapplied entry {uf_id} applied to loan {loan_id_ua}.")
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))


def _make_statement_pdf(df, customer_name, cust_id, loan_id, start_fmt, end_fmt, statement_title):
    """Build PDF bytes for statement with header (customer, ID, period) and table. statement_title e.g. 'Loan Statement (Internal – Daily)' or 'Customer loan statement'."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
    except ImportError:
        return None
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    styles = getSampleStyleSheet()
    story = []
    story.append(Paragraph(statement_title, styles["Title"]))
    story.append(Paragraph(f"<b>Customer:</b> {customer_name}", styles["Normal"]))
    story.append(Paragraph(f"<b>Customer ID:</b> {cust_id or '—'}", styles["Normal"]))
    story.append(Paragraph(f"<b>Loan ID:</b> {loan_id}", styles["Normal"]))
    story.append(Paragraph(f"<b>Period covered:</b> {start_fmt} to {end_fmt}", styles["Normal"]))
    story.append(Spacer(1, 16))
    # Table: header row + data rows (stringify for reportlab)
    df_str = df.fillna("").astype(str)
    table_data = [df_str.columns.tolist()] + df_str.values.tolist()
    t = Table(table_data, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    story.append(t)
    doc.build(story)
    buf.seek(0)
    return buf.getvalue()


def _statement_table_html(df, display_headers: dict[str, str], center_columns: list[str] | None = None) -> str:
    """Build a full-width HTML table from the statement dataframe. display_headers maps column name -> display label.
    center_columns: optional list of column names to center (e.g. last 4 columns for customer statement)."""
    import html
    center_set = set(center_columns or [])
    def cell(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        if isinstance(v, (int, float)):
            try:
                return f"{float(v):.2f}"
            except (TypeError, ValueError):
                return str(v)
        return html.escape(str(v))
    cols = df.columns.tolist()
    headers = [display_headers.get(c, c) for c in cols]
    th_parts = []
    for i, h in enumerate(headers):
        cname = cols[i] if i < len(cols) else None
        cls = ' class="center"' if cname and cname in center_set else ""
        th_parts.append(f"<th{cls}>{html.escape(h)}</th>")
    th = "".join(th_parts)
    rows = []
    for _, r in df.iterrows():
        td_parts = []
        for i, c in enumerate(cols):
            cls = ' class="center"' if c in center_set else ""
            td_parts.append(f"<td{cls}>{cell(r.get(c))}</td>")
        rows.append(f"<tr>{''.join(td_parts)}</tr>")
    tbody = "\n".join(rows)
    return f'<table class="stmt-table"><thead><tr>{th}</tr></thead><tbody>{tbody}</tbody></table>'


def statements_ui():
    """
    Generate statements on demand (no persistence).
    Customer loan statement: select customer/loan, date range; search by customer name or Loan ID.
    GL / ledger statements (later).
    """
    import pandas as pd
    st.markdown(
        "<div style='background-color: #0F766E; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Statements</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    if not _loan_management_available:
        st.error(_loan_management_error or "Loan management not available.")
        return
    if not _customers_available:
        st.error(_customers_error or "Customers module not available.")
        return

    try:
        from statements import generate_customer_facing_statement
    except ImportError as e:
        st.error(f"Statements module not available: {e}")
        return

    # Short labels for allocation columns (display only)
    _alloc_display = {
        "Portion of Credit Allocated to Interest": "Credit to Interest",
        "Credit Allocated to Fees": "Credit to Fees",
        "Credit Allocated to Capital": "Credit to Principal",
        "Arrears": "Arrears (total delinquency, incl. fees)",
    }

    def _normalize_customer_id(v):
        if isinstance(v, dict):
            v = v.get("id")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _customer_label(cid):
        cid_n = _normalize_customer_id(cid)
        if cid_n is None:
            return f"Customer #{cid}"
        try:
            nm = get_display_name(cid_n) if _customers_available else ""
        except Exception:
            nm = ""
        # Defensive: if a dict leaks through from any upstream helper/session state, avoid raw JSON UI labels.
        if isinstance(nm, dict):
            nm = (
                (nm.get("individual") or {}).get("name")
                or (nm.get("corporate") or {}).get("trading_name")
                or (nm.get("corporate") or {}).get("legal_name")
                or ""
            )
        nm_s = str(nm or "").strip()
        return nm_s if nm_s else f"Customer #{cid_n}"

    tab_loan, tab_gl = st.tabs(["Customer loan statement", "General Ledger"])
    with tab_loan:
        st.subheader("Customer loan statement")
        st.caption(
            "Search by customer or Loan ID. Select loan and dates. "
            "**Arrears** = principal in arrears + interest in arrears + default + penalty + fees (PDF/CSV/Print). "
            "**Balance** = loan total outstanding (facility buckets only); on each schedule due date it is set to the "
            "stored closing position for that date, so it may differ from a manual running total of Debits minus Credits on "
            "that row. **Unapplied funds** is cash held pending allocation and is separate from Balance."
        )
        search = st.text_input(
            "Search by customer name or Loan ID",
            placeholder="e.g. Smith or 42",
            key="stmt_search",
        ).strip()

        customers = list_customers() if _customers_available else []
        preselect_cust_id = None
        preselect_loan_id = None

        if search:
            try:
                lid = int(search)
                from loan_management import get_loan
                loan = get_loan(lid)
                if loan and loan.get("customer_id"):
                    preselect_cust_id = _normalize_customer_id(loan["customer_id"])
                    preselect_loan_id = lid
            except ValueError:
                pass
            if preselect_loan_id is None:
                search_lower = search.lower()
                customers = [c for c in customers if search_lower in _customer_label(c.get("id")).lower()]

        if not customers and preselect_cust_id is None:
            st.info("No customers found. Create a customer or enter a valid Loan ID.")
        else:
            cust_options = [(_normalize_customer_id(c.get("id")), _customer_label(c.get("id"))) for c in customers]
            cust_options = [t for t in cust_options if t[0] is not None]
            cust_labels = [t[1] for t in cust_options]
            default_idx = 0
            if preselect_cust_id is not None:
                try:
                    default_idx = next(i for i, t in enumerate(cust_options) if t[0] == preselect_cust_id)
                except StopIteration:
                    cust_options.insert(0, (preselect_cust_id, _customer_label(preselect_cust_id)))
                    cust_labels.insert(0, cust_options[0][1])
                    default_idx = 0
            fc1, fc2 = st.columns(2)
            with fc1:
                cust_sel = st.selectbox(
                    "Customer",
                    cust_labels,
                    index=default_idx,
                    key="stmt_cust",
                )
                cust_id = cust_options[cust_labels.index(cust_sel)][0]

            from loan_management import get_loans_by_customer
            loans = get_loans_by_customer(cust_id)
            if not loans:
                st.info("No loans for this customer.")
            else:
                loan_options = [(l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('principal', 0):,.2f}") for l in loans]
                loan_labels = [t[1] for t in loan_options]
                default_loan_idx = 0
                if preselect_loan_id is not None:
                    try:
                        default_loan_idx = next(i for i, t in enumerate(loan_options) if t[0] == preselect_loan_id)
                    except StopIteration:
                        default_loan_idx = 0
                with fc2:
                    loan_sel = st.selectbox(
                        "Loan",
                        loan_labels,
                        index=default_loan_idx,
                        key="stmt_loan",
                    )
                    loan_id = loan_options[loan_labels.index(loan_sel)][0]

                from loan_management import get_loan
                loan_info = get_loan(loan_id)
                disbursement = loan_info.get("disbursement_date") or loan_info.get("start_date")
                if hasattr(disbursement, "date"):
                    disbursement = disbursement.date()
                elif isinstance(disbursement, str):
                    disbursement = datetime.fromisoformat(disbursement[:10]).date()
                start_default = disbursement or _get_system_date()
                fd1, fd2 = st.columns(2)
                with fd1:
                    start_date = st.date_input(
                        "Start date",
                        value=start_default,
                        key=f"stmt_start_{loan_id}",
                        disabled=True,
                        help="Fixed to disbursement date.",
                    )
                with fd2:
                    end_date = st.date_input("End date (optional)", value=_get_system_date(), key="stmt_end")
                st.caption("Start date is fixed to disbursement. Adjust end date as needed.")
                tcol1, tcol2, tcol3 = st.columns(3)
                with tcol1:
                    show_pa_billing = st.checkbox(
                        "Show principal arrears billing lines",
                        value=True,
                        key="stmt_show_pa_billing",
                        help="Adds non-cash informational rows: 'Principal arrears billing (amount)'.",
                    )
                with tcol2:
                    show_arrears_col = st.checkbox(
                        "Show arrears column",
                        value=True,
                        key="stmt_show_arrears_col",
                    )
                with tcol3:
                    show_unapplied_col = st.checkbox(
                        "Show unapplied funds column",
                        value=True,
                        key="stmt_show_unapplied_col",
                    )

                if st.button("Generate statement", type="primary", key="stmt_gen"):
                    try:
                        rows, meta = generate_customer_facing_statement(
                            loan_id,
                            start_date=start_date,
                            end_date=end_date,
                            include_principal_arrears_billing=show_pa_billing,
                        )
                        if not rows:
                            st.info("No statement lines for this period.")
                        else:
                            df = pd.DataFrame(rows)
                            start = meta.get("start_date")
                            end = meta.get("end_date")
                            cust_id = _normalize_customer_id(meta.get("customer_id"))
                            customer_name = _customer_label(cust_id) if cust_id is not None else "—"
                            start_fmt = start.strftime("%d%b%Y") if hasattr(start, "strftime") else str(start)
                            end_fmt = end.strftime("%d%b%Y") if hasattr(end, "strftime") else str(end)
                            gen = meta.get("generated_at")
                            generated_fmt = gen.strftime("%d %b %Y, %H:%M:%S") if gen and hasattr(gen, "strftime") else (str(gen) if gen else "")

                            statement_title = "Customer loan statement"
                            numeric_cols = ["Debits", "Credits", "Balance", "Arrears", "Unapplied funds"]
                            for c in numeric_cols:
                                if c in df.columns:
                                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

                            visible_df = df.copy()
                            if not show_arrears_col and "Arrears" in visible_df.columns:
                                visible_df = visible_df.drop(columns=["Arrears"])
                            if not show_unapplied_col and "Unapplied funds" in visible_df.columns:
                                visible_df = visible_df.drop(columns=["Unapplied funds"])

                            # Full-width statement: HTML table (no Streamlit dataframe width limits)
                            display_headers = {**_alloc_display}
                            closing_row = None
                            if len(visible_df) > 0:
                                last_narr = str(visible_df.iloc[-1].get("Narration") or "")
                                if "Total outstanding" in last_narr:
                                    closing_row = visible_df.iloc[-1]
                                    stmt_df = visible_df.iloc[:-1]
                                else:
                                    stmt_df = visible_df
                            else:
                                stmt_df = visible_df
                            center_cols = [
                                c for c in ["Debits", "Credits", "Balance", "Arrears", "Unapplied funds"]
                                if c in stmt_df.columns
                            ]
                            table_html = _statement_table_html(stmt_df, display_headers, center_columns=center_cols)
                            closing_html = ""
                            if closing_row is not None:
                                due_d = closing_row.get("Due Date")
                                bal = closing_row.get("Balance")
                                unapp = closing_row.get("Unapplied funds")
                                due_fmt = due_d.strftime("%d %b %Y") if due_d and hasattr(due_d, "strftime") else str(due_d or "")
                                try:
                                    bal_fmt = f"{float(bal):,.2f}" if bal is not None else "0.00"
                                    unapp_fmt = f"{float(unapp):,.2f}" if unapp is not None else "0.00"
                                except (TypeError, ValueError):
                                    bal_fmt = str(bal or "0.00")
                                    unapp_fmt = str(unapp or "0.00")
                                if show_unapplied_col:
                                    closing_html = f"<div class='stmt-closing'><strong>Closing balance as at {due_fmt}:</strong> {bal_fmt}  &nbsp;|&nbsp;  <strong>Unapplied funds:</strong> {unapp_fmt}</div>"
                                else:
                                    closing_html = f"<div class='stmt-closing'><strong>Closing balance as at {due_fmt}:</strong> {bal_fmt}</div>"
                            stmt_html = (
                                "<style>"
                                "main .block-container { max-width: 100% !important; padding-left: 1.5rem; padding-right: 1.5rem; } "
                                "[data-testid='stSidebar'] { width: 16rem !important; } "
                                ".stmt-view { width: 100%; max-width: 100%; overflow-x: auto; margin-top: 1rem; } "
                                ".stmt-view .stmt-header { margin-bottom: 1rem; padding: 1rem 1.25rem; border: 1px solid #e2e8f0; border-radius: 6px; background: #f8fafc; font-size: 1rem; } "
                                ".stmt-view .stmt-table { width: 100%; border-collapse: collapse; font-size: 0.95rem; background: #fff; } "
                                ".stmt-view .stmt-table th, .stmt-view .stmt-table td { border: 1px solid #e2e8f0; padding: 0.5rem 0.6rem; text-align: left; } "
                                ".stmt-view .stmt-table th { background: #f1f5f9; font-weight: 600; } "
                                ".stmt-view .stmt-table td.num, .stmt-view .stmt-table th.num { text-align: right; } "
                                ".stmt-view .stmt-table td.center, .stmt-view .stmt-table th.center { text-align: center; } "
                                ".stmt-view .stmt-table tbody tr:nth-child(even) { background: #f8fafc; } "
                                ".stmt-closing { margin-top: 1.5rem; text-align: center; font-size: 1rem; padding: 1rem; border-top: 1px solid #e2e8f0; color: #334155; } "
                                "</style>"
                                "<div class='stmt-view'>"
                                "<div class='stmt-header'>"
                                f"<strong style='font-size: 1.25rem; display: block; margin-bottom: 0.5rem;'>{statement_title}</strong>"
                                f"<span style='display: block;'><strong>Customer:</strong> {customer_name}</span>"
                                f"<span style='display: block;'><strong>Customer ID:</strong> {cust_id if cust_id is not None else '—'}</span>"
                                f"<span style='display: block;'><strong>Loan ID:</strong> {loan_id}</span>"
                                f"<span style='display: block; margin-top: 0.25rem;'><strong>Period covered:</strong> {start_fmt} to {end_fmt}</span>"
                                "</div>"
                                + table_html
                                + closing_html
                                + "</div>"
                            )
                            st.markdown(stmt_html, unsafe_allow_html=True)

                            for note in meta.get("notifications") or []:
                                st.warning(note)

                            st.markdown(
                                "<div style='margin-top: 1rem; padding-top: 0.75rem; border-top: 1px solid #e2e8f0; color: #64748b; font-size: 0.9rem;'>"
                                f"For the period from {start_fmt} to {end_fmt}<br>"
                                f"<strong>Generated:</strong> {generated_fmt}"
                                "</div>",
                                unsafe_allow_html=True,
                            )
                            # CSV with header (comment lines at top) so all formats include header
                            stmt_slug = "customer"
                            csv_header_lines = [
                                f"# {statement_title}",
                                f"# Customer: {customer_name}",
                                f"# Customer ID: {cust_id if cust_id is not None else '—'}",
                                f"# Loan ID: {loan_id}",
                                f"# Period covered: {start_fmt} to {end_fmt}",
                                "#",
                            ]
                            buf = BytesIO()
                            buf.write(("\n".join(csv_header_lines) + "\n").encode("utf-8"))
                            visible_df.to_csv(
                                buf,
                                index=False,
                                date_format="%Y-%m-%d",
                                float_format="%.2f",
                            )
                            buf.seek(0)
                            col_csv, col_pdf, col_print = st.columns([1, 1, 1])
                            with col_csv:
                                st.download_button(
                                    "Download as CSV",
                                    data=buf,
                                    file_name=f"loan_statement_{stmt_slug}_{loan_id}_{start_date}_{end_date}.csv",
                                    mime="text/csv",
                                    key="stmt_download",
                                )
                            with col_pdf:
                                pdf_bytes = _make_statement_pdf(visible_df, customer_name, cust_id, loan_id, start_fmt, end_fmt, statement_title)
                                if pdf_bytes:
                                    st.download_button(
                                        "Download as PDF",
                                        data=pdf_bytes,
                                        file_name=f"loan_statement_{stmt_slug}_{loan_id}_{start_date}_{end_date}.pdf",
                                        mime="application/pdf",
                                        key="stmt_download_pdf",
                                    )
                                else:
                                    st.caption("Install reportlab for PDF download.")
                            with col_print:
                                # Open browser print dialog (works via embedded iframe calling parent)
                                st.components.v1.html(
                                    """
                                    <script>
                                    function doPrint() {
                                        try { (window.top || window.parent).print(); } catch (e) { window.print(); }
                                    }
                                    </script>
                                    <button onclick="doPrint()" style="padding: 0.35rem 0.75rem; border-radius: 4px; border: 1px solid #ccc; background: #f0f0f0; color: #333; font-size: 0.9rem; cursor: pointer;">
                                    Print
                                    </button>
                                    """,
                                    height=40,
                                )
                            st.caption("CSV and PDF include the header. **Print** opens the browser print dialog; the header is included when printing.")
                    except Exception as ex:
                        st.error(str(ex))
                        st.exception(ex)

    with tab_gl:
        st.subheader("General Ledger Statement")
        
        from accounting_service import AccountingService
        svc = AccountingService()
        
        gl_col1, gl_col2, gl_col3 = st.columns(3)
        with gl_col1:
            sys_date = _get_system_date()
            gl_start = st.date_input("Start Date", value=sys_date.replace(day=1), key="stmt_gl_start")
        with gl_col2:
            gl_end = st.date_input("End Date", value=sys_date, key="stmt_gl_end")
        with gl_col3:
            all_accounts = svc.list_accounts()
            account_options = ["All"] + [f"{a['code']} - {a['name']}" for a in all_accounts]
            gl_account_sel = st.selectbox("Filter by Account", account_options, key="stmt_gl_acct")
            
        account_filter = None if gl_account_sel == "All" else gl_account_sel.split(" - ")[0]
        
        if account_filter:
            # If a parent account is selected, show one summary line per child (net movement for the period).
            if svc.is_parent_account(account_filter):
                st.markdown(f"#### Account Statement (Parent Summary): {gl_account_sel}")
                child_rows = svc.get_child_account_summaries(account_filter, gl_start, gl_end)

                def _fmt_bal(d, c):
                    net = float(d or 0) - float(c or 0)
                    if net > 0:
                        return f"{net:,.2f}", "Dr"
                    elif net < 0:
                        return f"{-net:,.2f}", "Cr"
                    return "0.00", "-"

                summary_rows = []
                total_dr = 0.0
                total_cr = 0.0
                for ch in child_rows:
                    d = float(ch["debit"] or 0)
                    c = float(ch["credit"] or 0)
                    total_dr += d
                    total_cr += c
                    bal_val, bal_side = _fmt_bal(d, c)
                    summary_rows.append(
                        {
                            "Child Account": f"{ch['code']} - {ch['name']}",
                            "Debit": f"{d:,.2f}" if d else "",
                            "Credit": f"{c:,.2f}" if c else "",
                            "Net Balance": bal_val,
                            "Dr/Cr": bal_side,
                        }
                    )

                import pandas as pd

                df_summary = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(
                    columns=["Child Account", "Debit", "Credit", "Net Balance", "Dr/Cr"]
                )
                st.dataframe(df_summary, use_container_width=True, hide_index=True)
                if summary_rows:
                    st.caption(f"Totals for period: Debit {total_dr:,.2f} | Credit {total_cr:,.2f}")

            else:
                ledger = svc.get_account_ledger(account_filter, start_date=gl_start, end_date=gl_end)
                if ledger:
                    st.markdown(f"#### Account Statement: {ledger['account']['code']} - {ledger['account']['name']}")
                
                    rows = []
                    # 1. Opening Balance Row
                    ob_debit = float(ledger['opening_balance']['ob_debit'] or 0)
                    ob_credit = float(ledger['opening_balance']['ob_credit'] or 0)
                    
                    running_net = ob_debit - ob_credit
                    
                    def format_bal(net):
                        if net > 0:
                            return f"{net:,.2f}", "Dr"
                        elif net < 0:
                            return f"{-net:,.2f}", "Cr"
                        else:
                            return "0.00", "-"
                    
                    ob_val, ob_type = format_bal(running_net)
                    
                    rows.append({
                        "Date": gl_start.strftime("%Y-%m-%d") if gl_start else "",
                        "Reference": "",
                        "Description": "Opening Balance",
                        "Debit": f"{ob_debit:,.2f}" if ob_debit else "",
                        "Credit": f"{ob_credit:,.2f}" if ob_credit else "",
                        "Balance": ob_val,
                        "Dr/Cr": ob_type
                    })
                    
                    total_dr = ob_debit
                    total_cr = ob_credit
                    
                    for tx in ledger['transactions']:
                        dr = float(tx['debit'] or 0)
                        cr = float(tx['credit'] or 0)
                        total_dr += dr
                        total_cr += cr
                        running_net += (dr - cr)
                        
                        b_val, b_type = format_bal(running_net)
                        event_id = str(tx.get("event_id") or "")
                        is_reversal = event_id.startswith("REV-")
                        # If this is a reversal journal, show the explicit description
                        # (we set those on reversal postings). Otherwise, keep template memo.
                        desc = tx['description'] if (is_reversal or "Reversal of" in str(tx.get("description") or "")) else (tx['memo'] if tx.get('memo') else tx.get('description'))
                        
                        rows.append({
                            "Date": tx['entry_date'].strftime("%Y-%m-%d") if tx['entry_date'] else "",
                            "Reference": tx['reference'] or "",
                            "Description": desc or "",
                            "Debit": f"{dr:,.2f}" if dr else "",
                            "Credit": f"{cr:,.2f}" if cr else "",
                            "Balance": b_val,
                            "Dr/Cr": b_type
                        })
                        
                    # Calculate closing (only show balance & Dr/Cr, not totals as a row)
                    cb_val, cb_type = format_bal(running_net)
                    rows.append({
                        "Date": gl_end.strftime("%Y-%m-%d") if gl_end else "",
                        "Reference": "",
                        "Description": "Closing Balance",
                        "Debit": "",
                        "Credit": "",
                        "Balance": cb_val,
                        "Dr/Cr": cb_type
                    })
                    
                    df_ledger = pd.DataFrame(rows)
                    st.dataframe(df_ledger, use_container_width=True, hide_index=True)
                    st.caption(f"Totals for period: Debit {total_dr:,.2f} | Credit {total_cr:,.2f}")
                    
                else:
                    st.info("Account not found.")
        else:
            entries = svc.get_journal_entries(start_date=gl_start, end_date=gl_end, account_code=account_filter)
            if entries:
                flat_rows = []
                for entry in entries:
                    ok = entry.get("double_entry_balanced", True)
                    for line in entry["lines"]:
                        flat_rows.append({
                            "Date": entry["entry_date"],
                            "Reference": entry["reference"],
                            "Event": entry["event_tag"],
                            "Balanced": "OK" if ok else "NO",
                            "Account": f"{line['account_name']} ({line['account_code']})",
                            "Debit": float(line["debit"]),
                            "Credit": float(line["credit"]),
                        })

                df_all = pd.DataFrame(flat_rows) if flat_rows else pd.DataFrame(
                    columns=["Date", "Reference", "Event", "Balanced", "Account", "Debit", "Credit"]
                )
                st.dataframe(df_all, use_container_width=True, hide_index=True)

                if not df_all.empty:
                    st.caption(
                        f"Totals for period: Debit {df_all['Debit'].sum():.2f} | Credit {df_all['Credit'].sum():.2f}"
                    )
                    if "Balanced" in df_all.columns and (df_all["Balanced"] == "NO").any():
                        st.warning(
                            "Some rows are from journal headers that fail double-entry at **2dp** "
                            "(see **Balanced** column). New postings are blocked if materially unbalanced; these may be historical."
                        )
            else:
                st.info("No journal entries found for the selected filters.")


def accounting_ui():
    """
    Database-backed Accounting Module.
    """
    from accounting_service import AccountingService
    from config import get_database_url
    import psycopg2
    import psycopg2.extras
    import pandas as pd
    from datetime import datetime
    import streamlit as st

    svc = AccountingService()

    def _coa_parent_display_label(acct_list: list, parent_id) -> str:
        """Human-readable parent for COA edit confirmations."""
        if parent_id is None:
            return "(None — top level)"
        for a in acct_list:
            if str(a.get("id")) == str(parent_id):
                c = (a.get("code") or "").strip()
                n = (a.get("name") or "").strip()
                return f"{c} — {n}" if (c or n) else str(parent_id)
        return f"(unknown id {parent_id})"

    st.markdown(
        "<div style='background-color: #0F766E; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Accounting Module</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    tab_coa, tab_templates, tab_mapping, tab_manual, tab_reports = st.tabs(
        ["Chart of Accounts", "Transaction Templates", "Receipt → GL Mapping", "Manual Journals", "Financial Reports"]
    )

    # 1. Chart of Accounts
    with tab_coa:
        _coa_banner = st.session_state.pop("coa_parent_edit_banner", None)
        if _coa_banner:
            _bkind, _btext = _coa_banner
            if _bkind == "success":
                st.success(_btext)
            else:
                st.info(_btext)
        st.subheader("Chart of Accounts")
        if not svc.is_coa_initialized():
            st.warning("Chart of Accounts is not initialized.")
            if st.button("Initialize Default Chart of Accounts"):
                svc.initialize_default_coa()
                st.success(
                    "Chart of Accounts initialized from bundled defaults "
                    "(accounting_defaults/chart_of_accounts.json when present; else built-in)."
                )
                st.rerun()
        
        accounts = svc.list_accounts()
        if accounts:
            df_accounts = pd.DataFrame([{
                "Code": a["code"],
                "Name": a["name"],
                "Category": a["category"],
                "System Tag": a["system_tag"] or "",
                "Parent Code": a["parent_code"] or "",
                "Resolve subaccounts": (a.get("subaccount_resolution") or ""),
            } for a in accounts])
            st.dataframe(df_accounts, use_container_width=True, hide_index=True)

        with st.expander("Subaccount resolution (on tagged **parent** rows)", expanded=False):
            st.caption(
                "When a **system_tag** sits on an account that has **child GL accounts**, set how automated "
                "posting picks the leaf: **PRODUCT** (maps per `loans.product_code`), **LOAN_CAPTURE** (`loans.cash_gl_account_id` "
                "at capture — same accounts as **Maintenance — source cash account cache** / Teller; **cash_operating** only), "
                "**JOURNAL** (pass `account_overrides` in API/UI). "
                "Leave blank if the tagged "
                "account has **no** children — behaviour is unchanged from before."
            )
            _tagged = [a for a in (accounts or []) if (a.get("system_tag") or "").strip()]
            if _tagged:
                _rlab = [f"{a['code']} — {a['name']} ({a['system_tag']})" for a in _tagged]
                _rids = [a["id"] for a in _tagged]
                ri = st.selectbox("Account", range(len(_rlab)), format_func=lambda i: _rlab[i], key="coa_res_pick")
                cur_mode = (_tagged[ri].get("subaccount_resolution") or "") or "(not set)"
                st.caption(f"Current: **{cur_mode}**")
                new_mode = st.selectbox(
                    "Subaccount resolution",
                    ["(clear)", "PRODUCT", "LOAN_CAPTURE", "JOURNAL"],
                    key="coa_res_mode",
                )
                if st.button("Save resolution mode", key="coa_res_save"):
                    val = None if new_mode == "(clear)" else new_mode
                    try:
                        svc.update_account_subaccount_resolution(_rids[ri], val)
                        st.success("Saved.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
            else:
                st.info("No accounts with a system tag yet.")

        with st.expander("Product → GL subaccounts (PRODUCT resolution)", expanded=False):
            st.caption("Maps `products.code` + template `system_tag` → leaf GL account for that loan’s product.")
            try:
                _prods = list_products(active_only=False) if _loan_management_available else []
            except Exception:
                _prods = []
            _plab = [f"{p['code']} — {p['name']}" for p in _prods] if _prods else []
            _pcodes = [p["code"] for p in _prods] if _prods else []
            _tmpl = svc.list_all_transaction_templates() or []
            _tags = sorted({t["system_tag"] for t in _tmpl if t.get("system_tag")})
            try:
                _mrows = svc.list_product_gl_subaccount_map() or []
            except Exception as ex:
                _mrows = []
                st.warning(f"Could not load map: {ex}")
            if _mrows:
                st.dataframe(
                    pd.DataFrame([{
                        "id": m["id"],
                        "product": m["product_code"],
                        "system_tag": m["system_tag"],
                        "GL": f"{m.get('gl_account_code')} — {m.get('gl_account_name')}",
                    } for m in _mrows]),
                    hide_index=True,
                    use_container_width=True,
                )
            c1, c2, c3 = st.columns(3)
            with c1:
                if _plab:
                    pi = st.selectbox("Product", range(len(_plab)), format_func=lambda i: _plab[i], key="coa_pmap_prod")
                    pmap_pc = _pcodes[pi]
                else:
                    pmap_pc = st.text_input("Product code", key="coa_pmap_pc_manual")
            with c2:
                pmap_tag = st.selectbox("System tag", _tags, key="coa_pmap_tag") if _tags else st.text_input(
                    "System tag", key="coa_pmap_tag_txt"
                )
            with c3:
                pai = (
                    st.selectbox("Leaf GL", range(len(_alab)), format_func=lambda i: _alab[i], key="coa_pmap_acct")
                    if _alab
                    else None
                )
            if not _alab:
                st.warning("No GL accounts to map.")
            if st.button("Save product GL map", key="coa_pmap_save"):
                tag_u = str(pmap_tag or "").strip()
                if (pmap_pc or "").strip() and tag_u and _aids and pai is not None:
                    try:
                        svc.upsert_product_gl_subaccount_map(str(pmap_pc).strip(), str(tag_u).strip(), _aids[pai])
                        st.success("Saved map row.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
                else:
                    st.error("Product code, tag, and account are required.")
        
        st.divider()
        _show_add_acct = st.checkbox(
            "Show **Add Custom Account**",
            value=False,
            key="acco_coa_show_add_account",
            help="Create a new GL account. Hidden by default to reduce clutter.",
        )
        if _show_add_acct:
            st.subheader("Add Custom Account")
            _coa_accounts = svc.list_accounts() or []
            _parent_labels = ["(None — top level)"]
            _parent_ids: list = [None]
            for _a in sorted(_coa_accounts, key=lambda x: (x.get("code") or "")):
                _pid = _a.get("id")
                if _pid is not None:
                    _parent_labels.append(f"{_a.get('code', '')} — {_a.get('name', '')}")
                    _parent_ids.append(_pid)
            with st.form("add_account_form"):
                use_grand = st.checkbox(
                    "Use next **grandchild** code (`BASE-NN`) under selected parent",
                    value=False,
                    key="add_acct_grandchild",
                    help="Requires a parent account. Ignores the manual code field and allocates the next free -NN suffix.",
                )
                code = st.text_input("Account Code (7-char or grandchild e.g. A100001-02)", key="add_acct_code_in")
                name = st.text_input("Account Name")
                category = st.selectbox("Category", ["ASSET", "LIABILITY", "EQUITY", "INCOME", "EXPENSE"])
                _pi = st.selectbox(
                    "Parent account (optional)",
                    options=list(range(len(_parent_labels))),
                    format_func=lambda i: _parent_labels[i],
                    help="Choose a parent for roll-up reporting. The parent must already exist in the chart. "
                    "Typically the category matches the parent’s category.",
                    key="add_acct_parent_sel",
                )
                parent_id = _parent_ids[_pi]
                system_tag = st.text_input("System Tag (Optional)")
                res_opts = ["(none)", "PRODUCT", "LOAN_CAPTURE", "JOURNAL"]
                res_i = st.selectbox(
                    "Subaccount resolution (optional; for tagged parents with children)",
                    range(len(res_opts)),
                    format_func=lambda i: res_opts[i],
                    key="add_acct_subres",
                )
                submitted = st.form_submit_button("Create Account")
                if submitted:
                    eff_code = (code or "").strip()
                    if use_grand and parent_id:
                        try:
                            eff_code = svc.suggest_next_grandchild_code_for_parent_id(str(parent_id))
                        except Exception as e:
                            st.error(str(e))
                            eff_code = ""
                    subres = None if res_opts[res_i] == "(none)" else res_opts[res_i]
                    if eff_code and name:
                        try:
                            svc.create_account(
                                eff_code,
                                name,
                                category,
                                system_tag=system_tag.strip() if system_tag else None,
                                parent_id=parent_id,
                                subaccount_resolution=subres,
                            )
                            st.success("Account created!")
                            st.rerun()
                        except ValueError as e:
                            st.error(str(e))
                        except Exception as e:
                            st.error(f"Could not create account: {e}")
                    else:
                        st.error("Code and Name are required.")

        st.divider()
        _show_edit_parent = st.checkbox(
            "Show **Edit account parent**",
            value=False,
            key="acco_coa_show_edit_parent",
            help="Change the parent of an existing account. Hidden by default to reduce clutter.",
        )
        if _show_edit_parent:
            st.subheader("Edit account parent")
            st.caption(
                "Set or change the parent for an **existing** account. "
                "This account and its descendants cannot be chosen as parent (prevents cycles)."
            )
            _edit_list = svc.list_accounts() or []
            if _edit_list:
                _sorted_edit = sorted(_edit_list, key=lambda x: (x.get("code") or ""))
                _elabels = [f"{a.get('code', '')} — {a.get('name', '')}" for a in _sorted_edit]
                _eids = [a["id"] for a in _sorted_edit]
                st.caption(
                    "Pick the account, choose the new parent, then click **Update parent**. "
                    "(Not inside a form so your parent choice is saved to session state before the button runs.)"
                )
                _ei = st.selectbox(
                    "Account to edit",
                    options=list(range(len(_elabels))),
                    format_func=lambda i: _elabels[i],
                    key="edit_acct_pick",
                )
                selected_id = _eids[_ei]
                acct_row = _sorted_edit[_ei]
                current_parent_id = acct_row.get("parent_id")
                # String IDs: UUID vs str from DB/cursors can break `x in set` and hide valid parents.
                _subtree_raw = svc.get_account_subtree_ids(selected_id)
                subtree = {str(x) for x in _subtree_raw}
                _plabels: list[str] = ["(None — top level)"]
                _pids: list = [None]
                for _a in _sorted_edit:
                    if str(_a["id"]) not in subtree:
                        _plabels.append(f"{_a.get('code', '')} — {_a.get('name', '')}")
                        _pids.append(_a["id"])
                default_pi = 0
                for _i, _pid in enumerate(_pids):
                    if current_parent_id is not None and str(_pid) == str(current_parent_id):
                        default_pi = _i
                        break
                default_pi = min(default_pi, max(0, len(_plabels) - 1))
                # Integer index into _pids (avoids label→id dict collisions if two rows share same label text).
                _sk_idx = f"coa_edit_parent_idx_{str(selected_id)}"
                if _sk_idx not in st.session_state:
                    st.session_state[_sk_idx] = default_pi
                else:
                    _cur = st.session_state[_sk_idx]
                    if not isinstance(_cur, int) or _cur < 0 or _cur >= len(_plabels):
                        st.session_state[_sk_idx] = default_pi
                st.selectbox(
                    "New parent",
                    options=list(range(len(_plabels))),
                    format_func=lambda i: _plabels[i],
                    key=_sk_idx,
                    help="Choose (None — top level) to clear the parent. "
                    "If an account is missing here, it may be under this account in the tree (cannot be parent).",
                )
                if st.button("Update parent", key="coa_btn_update_parent", type="primary"):
                    try:
                        _idx = st.session_state.get(_sk_idx, default_pi)
                        try:
                            _idx = int(_idx)
                        except (TypeError, ValueError):
                            _idx = default_pi
                        _idx = max(0, min(_idx, len(_pids) - 1))
                        new_parent_id = _pids[_idx]
                        old_lbl = _coa_parent_display_label(_sorted_edit, current_parent_id)
                        new_lbl = _coa_parent_display_label(_sorted_edit, new_parent_id)
                        acct_code = (acct_row.get("code") or "").strip() or "?"
                        acct_name = (acct_row.get("name") or "").strip()
                        acct_title = f"{acct_code} — {acct_name}" if acct_name else acct_code
                        if str(current_parent_id or "") == str(new_parent_id or ""):
                            st.session_state["coa_parent_edit_banner"] = (
                                "info",
                                f"**No change saved.** **{acct_title}** already has parent **{old_lbl}**.",
                            )
                        else:
                            svc.update_account_parent(selected_id, new_parent_id)
                            st.session_state.pop(_sk_idx, None)
                            st.session_state["coa_parent_edit_banner"] = (
                                "success",
                                f"**Saved.** **{acct_title}**: parent **{old_lbl}** → **{new_lbl}**.",
                            )
                        st.rerun()
                    except ValueError as e:
                        st.error(str(e))
                    except Exception as e:
                        st.error(f"Could not update parent: {e}")
            else:
                st.caption("Initialize the chart of accounts above before editing parents.")

    # 2. Transaction Templates
    with tab_templates:
        st.subheader("Transaction Templates (Journal Links)")

        if st.session_state.pop("acco_tt_restored_ok", None):
            st.success("Transaction templates were replaced from bundled defaults.")

        # Show current template counts (helps confirm reset)
        _templates_now = svc.list_all_transaction_templates()
        _event_count = len(set([t["event_type"] for t in _templates_now])) if _templates_now else 0
        st.caption(f"Currently loaded: {_event_count} event types / {len(_templates_now)} journal legs.")

        templates = svc.list_all_transaction_templates()
        if templates:
            evt_options = sorted(list({t["event_type"] for t in templates}))
            evt_sel = st.selectbox("Filter by Event Type", ["(All)"] + evt_options, key="tt_edit_evt")
            rows = [
                t
                for t in templates
                if evt_sel == "(All)" or t["event_type"] == evt_sel
            ]

            # Build dropdown options from DB for edit form
            accounts = svc.list_accounts() or []
            system_tags_from_accounts = sorted({a["system_tag"] for a in accounts if a.get("system_tag")})
            system_tags_from_templates = sorted({t["system_tag"] for t in templates})
            all_system_tags = sorted(set(system_tags_from_accounts) | set(system_tags_from_templates))

            # Journal number lookup for display only.
            journal_numbers = {
                "LOAN_APPROVAL": "1",
                "FEE_AMORTISATION_DRAWDOWN": "2",
                "FEE_AMORTISATION_ARRANGEMENT": "2a",
                "FEE_AMORTISATION_ADMIN": "2b",
            }

            # Table header
            h0, h1, h2, h3, h4, h5, h6, h7 = st.columns([1, 2, 2, 1, 2, 1, 1, 1])
            with h0:
                st.markdown("**Journal #**")
            with h1:
                st.markdown("**Event Type**")
            with h2:
                st.markdown("**System Tag**")
            with h3:
                st.markdown("**Dr/Cr**")
            with h4:
                st.markdown("**Description**")
            with h5:
                st.markdown("**Trigger**")
            with h6:
                st.markdown("**Edit**")
            with h7:
                st.markdown("**Delete**")

            editing_id = st.session_state.get("tt_editing_id")

            for t in rows:
                col0, col1, col2, col3, col4, col5, col6, col7 = st.columns([1, 2, 2, 1, 2, 1, 1, 1])
                with col0:
                    st.text(journal_numbers.get(t["event_type"], ""))
                with col1:
                    st.text(t["event_type"])
                with col2:
                    st.text(t["system_tag"])
                with col3:
                    st.text(t["direction"][:1] if t.get("direction") else "-")
                with col4:
                    st.text((t.get("description") or "")[:40] + ("..." if len(t.get("description") or "") > 40 else ""))
                with col5:
                    st.text(t.get("trigger_type") or "EVENT")
                with col6:
                    if st.button("Edit", key=f"tt_edit_{t['id']}"):
                        st.session_state["tt_editing_id"] = str(t["id"])
                        st.rerun()
                with col7:
                    if st.button("Delete", key=f"tt_del_{t['id']}"):
                        svc.delete_transaction_template(t["id"])
                        st.session_state.pop("tt_editing_id", None)
                        st.success("Template deleted.")
                        st.rerun()

            # Edit form (shown when editing a template)
            if editing_id:
                t_edit = next((x for x in templates if str(x["id"]) == editing_id), None)
                if t_edit:
                    st.divider()
                    st.markdown("**Edit template**")
                    with st.form("tt_edit_form"):
                        new_desc = st.text_input(
                            "Description",
                            value=t_edit.get("description") or "",
                            key="tt_edit_desc",
                        )
                        col_a, col_b = st.columns(2)
                        with col_a:
                            new_trigger = st.selectbox(
                                "Trigger Type",
                                ["EVENT", "EOD", "EOM"],
                                index=["EVENT", "EOD", "EOM"].index(t_edit.get("trigger_type", "EVENT")),
                                key="tt_edit_trig",
                            )
                            current_tag = t_edit["system_tag"]
                            tag_options = [current_tag] if current_tag and current_tag not in all_system_tags else []
                            tag_options.extend(all_system_tags)
                            tag_idx = tag_options.index(current_tag) if current_tag in tag_options else 0
                            new_system_tag = st.selectbox(
                                "System Tag (GL account)",
                                tag_options,
                                index=tag_idx,
                                key="tt_edit_tag",
                            )
                        with col_b:
                            new_direction = st.selectbox(
                                "Direction",
                                ["DEBIT", "CREDIT"],
                                index=0 if t_edit["direction"] == "DEBIT" else 1,
                                key="tt_edit_dir",
                            )
                        col_save, col_cancel = st.columns(2)
                        with col_save:
                            save_btn = st.form_submit_button("Save")
                        with col_cancel:
                            cancel_btn = st.form_submit_button("Cancel")
                        if save_btn:
                            svc.update_transaction_template(
                                t_edit["id"],
                                system_tag=new_system_tag.strip(),
                                direction=new_direction,
                                description=new_desc.strip() or None,
                                trigger_type=new_trigger,
                            )
                            st.session_state.pop("tt_editing_id", None)
                            st.success("Template updated.")
                            st.rerun()
                        elif cancel_btn:
                            st.session_state.pop("tt_editing_id", None)
                            st.rerun()
        else:
            st.info("No transaction templates defined.")

        st.divider()
        _show_link_journal = st.checkbox(
            "Show **Link New Journal (Double Entry)**",
            value=False,
            key="acco_tt_show_link_journal",
            help="Adds a debit and credit template row for one event type. Hidden by default to reduce clutter.",
        )
        if _show_link_journal:
            st.subheader("Link New Journal (Double Entry)")
            st.caption(
                "Adds one **debit** and one **credit** template row for an **event_type**. "
                "Posting code (e.g. `AccountingService.post_event`) must use the **exact same** event type string. "
                "Choose **Use existing** or **Define new**; the new name field appears as soon as you pick Define new."
            )
            _accounts = svc.list_accounts() or []
            _all_system_tags = sorted(set(a["system_tag"] for a in _accounts if a.get("system_tag")))
            _all_system_tags = _all_system_tags or ["cash_operating", "loan_principal", "deferred_fee_liability"]
            _templates_for_evt = svc.list_all_transaction_templates() or []
            _event_types = sorted(set(t["event_type"] for t in _templates_for_evt))

            _evt_mode = st.radio(
                "Event type",
                ["Use existing", "Define new"],
                horizontal=True,
                key="link_evt_mode",
                help="Define new: enter the identifier your posting code will use (same spelling/casing after normalization).",
            )
            evt_resolved = ""
            if _evt_mode == "Use existing":
                if _event_types:
                    evt_resolved = st.selectbox(
                        "Existing event type",
                        _event_types,
                        key="link_evt_existing",
                    )
                else:
                    st.info("No event types in the database yet. Switch to **Define new** above.")
            else:
                _new_evt = st.text_input(
                    "New event type name",
                    placeholder="e.g. LOAN_DISBURSEMENT",
                    key="link_evt_new_name",
                    help="Use the same identifier your code will pass to post_event(event_type=...). "
                    "Stored uppercase with spaces → underscores.",
                )
                evt_resolved = (_new_evt or "").strip().upper().replace(" ", "_")

            trigger_type = st.selectbox("Trigger Type", ["EVENT", "EOD", "EOM"], index=0, key="link_trig")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Debit leg**")
                debit_tag = st.selectbox("System tag to debit", _all_system_tags, key="link_debit_tag")
            with col2:
                st.markdown("**Credit leg**")
                credit_tag = st.selectbox("System tag to credit", _all_system_tags, key="link_credit_tag")

            desc = st.text_input("Description / memo", key="link_desc")

            if st.button("Add journal link", key="link_journal_btn", type="primary"):
                if not evt_resolved or not debit_tag or not credit_tag:
                    st.error("Event type (existing or new name), debit tag, and credit tag are required.")
                elif _evt_mode == "Define new" and len(evt_resolved) < 2:
                    st.error("Enter a valid new event type name (at least 2 characters after cleanup).")
                else:
                    try:
                        svc.link_journal(evt_resolved, debit_tag, "DEBIT", desc, trigger_type)
                        svc.link_journal(evt_resolved, credit_tag, "CREDIT", desc, trigger_type)
                        st.success(f"Double-entry template for **{evt_resolved}** added.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))

        st.divider()
        with st.expander(
            "Danger zone — replace ALL transaction templates from bundled defaults",
            expanded=False,
        ):
            st.error(
                "This **permanently deletes every row** in transaction templates and reloads from "
                "`accounting_defaults/transaction_templates.json` when present, otherwise built-in definitions. "
                "Export the live database first: `python scripts/export_accounting_defaults.py`."
            )
            _tt_danger_ack = st.checkbox(
                "I understand all existing journal template rows will be deleted and replaced.",
                key="tt_danger_ack",
            )
            _tt_danger_phrase = st.text_input(
                "Type the phrase **REPLACE TEMPLATES** exactly (case-sensitive) to enable the action.",
                key="tt_danger_phrase",
            )
            _tt_danger_ready = _tt_danger_ack and (_tt_danger_phrase.strip() == "REPLACE TEMPLATES")
            if st.button(
                "Replace all templates from bundled defaults",
                key="tt_danger_restore_btn",
                type="primary",
                disabled=not _tt_danger_ready,
                help="Requires the checkbox and exact phrase above.",
            ):
                try:
                    svc.initialize_default_transaction_templates()
                    st.session_state["acco_tt_restored_ok"] = True
                    st.rerun()
                except Exception as e:
                    st.error(f"Restore failed: {e}")

    # 3. Receipt → GL Mapping (dedicated tab)
    with tab_mapping:
        st.subheader("Receipt Allocation → Accounting Events")
        if st.session_state.pop("acco_rgl_restored_ok", None):
            st.success("Receipt → GL mappings were updated from bundled defaults.")

        st.caption(
            "This table tells the system how to translate repayment allocations "
            "into accounting events (and therefore GL postings)."
        )

        _table_exists = True
        try:
            mappings = svc.list_receipt_gl_mappings()
        except Exception as e:
            if "receipt_gl_mapping" in str(e) and "does not exist" in str(e).lower():
                _table_exists = False
                st.warning(
                    "The `receipt_gl_mapping` table has not been created yet. "
                    "Click the button below to create it (uses the same database connection as the app)."
                )
                if st.button("Create receipt_gl_mapping table"):
                    try:
                        import psycopg2
                        from pathlib import Path
                        sql_path = Path(__file__).parent / "schema" / "38_receipt_gl_mapping.sql"
                        if not sql_path.exists():
                            st.error(f"Migration file not found: {sql_path}")
                        else:
                            sql = sql_path.read_text(encoding="utf-8")
                            from config import get_database_url
                            conn = psycopg2.connect(get_database_url())
                            try:
                                with conn.cursor() as cur:
                                    cur.execute(sql)
                                conn.commit()
                                st.success("Table created. Refreshing...")
                                st.rerun()
                            finally:
                                conn.close()
                    except Exception as ex:
                        st.error(f"Could not create table: {ex}")
                        st.exception(ex)
                mappings = []
            else:
                raise

        st.caption(
            "Bundled defaults live in `accounting_defaults/receipt_gl_mapping.json` (active rows only), "
            "else built-in. Export from live DB: `python scripts/export_accounting_defaults.py`. "
            "Loading or replacing defaults is only available at the bottom of this tab, inside **Danger zone**."
        )

        if mappings:
            df_map = pd.DataFrame(mappings)
            st.dataframe(
                df_map[
                    [
                        "id",
                        "trigger_source",
                        "allocation_key",
                        "event_type",
                        "amount_source",
                        "amount_sign",
                        "is_active",
                        "priority",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No receipt GL mappings defined yet.")

        st.divider()
        _show_rgl_form = st.checkbox(
            "Show **Add / Edit Mapping**",
            value=False,
            key="acco_rgl_show_form",
            help="Create or update receipt → GL mapping rows. Hidden by default to reduce clutter.",
        )
        if _show_rgl_form:
            st.subheader("Add / Edit Mapping")

            # Dropdown options from DB for Receipt GL Mapping form
            _templates_for_events = svc.list_all_transaction_templates()
            _event_types_for_mapping = sorted(set(t["event_type"] for t in (_templates_for_events or [])))
            _predefined_allocation_keys = [
                "alloc_principal_arrears", "alloc_principal_not_due",
                "alloc_interest_arrears", "alloc_interest_accrued",
                "alloc_penalty_interest", "alloc_default_interest",
                "alloc_regular_interest", "alloc_fees_charges", "amount",
            ]
            _allocation_keys_from_db = sorted(set(m["allocation_key"] for m in (mappings or [])))
            _amount_sources_from_db = sorted(set(m["amount_source"] for m in (mappings or [])))
            _allocation_key_options = sorted(set(_predefined_allocation_keys + _allocation_keys_from_db))
            _amount_source_options = sorted(set(_predefined_allocation_keys + _amount_sources_from_db))

            with st.form("receipt_gl_mapping_form"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    mapping_options = ["New mapping"]
                    mapping_options += [f"Edit id={m['id']} ({m['trigger_source']} / {m['allocation_key']} → {m['event_type']})" for m in (mappings or [])]
                    edit_sel = st.selectbox(
                        "Mapping (choose existing to update or New mapping to create)",
                        mapping_options,
                        key="rgl_edit_sel",
                    )
                    edit_id = ""
                    if edit_sel != "New mapping" and "id=" in edit_sel:
                        edit_id = edit_sel.replace("Edit id=", "").split(" (")[0].strip()
                    trigger_source = st.selectbox(
                        "Trigger Source",
                        ["SAVE_RECEIPT", "SAVE_REVERSAL", "APPLY_UNAPPLIED"],
                        index=0,
                        key="rgl_trigger",
                    )
                with col2:
                    allocation_key = st.selectbox(
                        "Allocation Key",
                        _allocation_key_options,
                        key="rgl_alloc_key",
                        help="Allocation bucket from repayment engine.",
                    )
                    event_type = st.selectbox(
                        "Accounting Event Type",
                        _event_types_for_mapping if _event_types_for_mapping else ["PAYMENT_PRINCIPAL", "PAYMENT_REGULAR_INTEREST", "WRITEOFF_RECOVERY"],
                        key="rgl_event_type",
                    )
                with col3:
                    amount_source = st.selectbox(
                        "Amount Source",
                        _amount_source_options,
                        key="rgl_amount_source",
                        help="Usually same as allocation key.",
                    )
                    amount_sign = st.selectbox(
                        "Sign",
                        [1, -1],
                        index=0,
                        format_func=lambda x: "Normal (+1)" if x == 1 else "Reversal (-1)",
                        key="rgl_sign",
                    )

                col4, col5 = st.columns(2)
                with col4:
                    is_active = st.checkbox("Active", value=True, key="rgl_active")
                with col5:
                    priority = st.number_input(
                        "Priority (lower runs first)",
                        min_value=0,
                        max_value=1000,
                        value=100,
                        step=10,
                        key="rgl_priority",
                    )

                col_save, col_del = st.columns(2)
                with col_save:
                    submit_map = st.form_submit_button("Save Mapping")
                with col_del:
                    delete_map = st.form_submit_button("Delete Mapping")

                if submit_map:
                    if not allocation_key or not event_type or not amount_source:
                        st.error("Allocation Key, Event Type, and Amount Source are required.")
                    else:
                        mapping_id = int(edit_id) if edit_id.strip() else None
                        svc.upsert_receipt_gl_mapping(
                            mapping_id=mapping_id,
                            trigger_source=trigger_source,
                            allocation_key=allocation_key.strip(),
                            event_type=event_type.strip(),
                            amount_source=amount_source.strip(),
                            amount_sign=int(amount_sign),
                            is_active=is_active,
                            priority=int(priority),
                        )
                        st.success("Mapping saved.")
                        st.rerun()
                if delete_map and edit_id.strip():
                    svc.delete_receipt_gl_mapping(int(edit_id))
                    st.success("Mapping deleted.")
                    st.rerun()

        if _table_exists:
            st.divider()
            with st.expander(
                "Danger zone — load or replace ALL receipt → GL mappings from bundled defaults",
                expanded=False,
            ):
                if not mappings:
                    st.warning(
                        "The table is **empty**. This action inserts the bundled default mapping set "
                        "(from JSON or built-in). It does not delete existing rows."
                    )
                    _rgl_ack = st.checkbox(
                        "I want to insert the bundled default receipt → GL mappings.",
                        key="rgl_danger_ack_init",
                    )
                    _rgl_phrase = st.text_input(
                        "Type **LOAD DEFAULT MAPPINGS** exactly (case-sensitive).",
                        key="rgl_danger_phrase_init",
                    )
                    _rgl_ready = _rgl_ack and (_rgl_phrase.strip() == "LOAD DEFAULT MAPPINGS")
                    if st.button(
                        "Load bundled default mappings (empty table only)",
                        key="rgl_danger_init_btn",
                        type="primary",
                        disabled=not _rgl_ready,
                    ):
                        try:
                            if svc.initialize_default_receipt_gl_mappings():
                                st.session_state["acco_rgl_restored_ok"] = True
                                st.rerun()
                            else:
                                st.info("Mappings were already present; nothing to load.")
                        except Exception as ex:
                            st.error(f"Could not load defaults: {ex}")
                            st.exception(ex)
                else:
                    st.error(
                        "This **permanently deletes every row** in `receipt_gl_mapping` and reloads bundled defaults. "
                        "Export first if you need to keep the current configuration."
                    )
                    _rgl_ack = st.checkbox(
                        "I understand all existing receipt → GL mapping rows will be deleted and replaced.",
                        key="rgl_danger_ack_reset",
                    )
                    _rgl_phrase = st.text_input(
                        "Type **REPLACE RECEIPT MAPPINGS** exactly (case-sensitive).",
                        key="rgl_danger_phrase_reset",
                    )
                    _rgl_ready = _rgl_ack and (_rgl_phrase.strip() == "REPLACE RECEIPT MAPPINGS")
                    if st.button(
                        "Replace all receipt → GL mappings from bundled defaults",
                        key="rgl_danger_reset_btn",
                        type="primary",
                        disabled=not _rgl_ready,
                    ):
                        try:
                            svc.reset_receipt_gl_mappings_to_defaults()
                            st.session_state["acco_rgl_restored_ok"] = True
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Could not replace mappings: {ex}")
                            st.exception(ex)

    # 4. Manual Journals
    with tab_manual:
        st.subheader("Manual Journals")
        st.info("Day-to-day manual postings should now be done via the standalone **Journals** menu in the left navigation.")

    # 5. Reports
    with tab_reports:
        st.subheader("Financial Reports")
        try:
            from accounting_periods import (
                normalize_accounting_period_config,
                get_month_period_bounds,
                is_eom,
                is_eoy,
            )
            period_cfg = normalize_accounting_period_config(_get_system_config())
            month_bounds = get_month_period_bounds(_get_system_date(), period_cfg)
            close_flags = []
            if is_eom(_get_system_date(), period_cfg):
                close_flags.append("EOM")
            if is_eoy(_get_system_date(), period_cfg):
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
            sys_date = _get_system_date()
            tb_as_of = st.date_input("As of Date", value=sys_date, key="tb_as_of")
            
            if st.button("Generate Trial Balance"):
                tb = svc.get_trial_balance(tb_as_of)
                if tb:
                    df_tb = pd.DataFrame([{
                        "Code": r["code"], "Name": r["name"], "Category": r["category"],
                        "Debit": float(r["debit"]), "Credit": float(r["credit"])
                    } for r in tb])
                    st.dataframe(df_tb, use_container_width=True, hide_index=True)
                    st.write(f"**Total Debits:** {df_tb['Debit'].sum():.2f} | **Total Credits:** {df_tb['Credit'].sum():.2f}")
                else:
                    st.info("No data.")
                
        with rep_pl:
            st.markdown("### Profit and Loss")
            sys_date = _get_system_date()
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
                    
                pl = svc.get_profit_and_loss(pl_start, pl_as_of)
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
            sys_date = _get_system_date()
            bs_as_of = st.date_input("As of Date", value=sys_date, key="bs_as_of")
            if st.button("Generate Balance Sheet"):
                bs = svc.get_balance_sheet(bs_as_of)
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
            sys_date = _get_system_date()
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
                    
                eq = svc.get_statement_of_changes_in_equity(eq_start, eq_as_of)
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
            sys_date = _get_system_date()
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
                    
                cf = svc.get_cash_flow_statement(cf_start, cf_as_of)
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

            acc_cfg = _get_system_config().get("accounting_periods", {}) or {}
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

                snaps = svc.list_statement_snapshots(
                    statement_type=stmt_key,
                    period_type=period_key,
                    period_end_date_from=date_from if date_from else None,
                    period_end_date_to=date_to if date_to else None,
                    limit=int(limit),
                )
                if not snaps:
                    st.info("No snapshots found for the selected filters.")
                else:
                    import pandas as pd

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
                        snap = svc.get_statement_snapshot_with_lines(sel_id)
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

def notifications_ui():
    st.header("Notifications Module")
    
    tab_send, tab_templates, tab_history = st.tabs([
        "Send Notification",
        "Templates",
        "History"
    ])
    
    with tab_send:
        st.subheader("Send a Notification")
        with st.form("send_notification_form"):
            recipient_type = st.radio("Send to", ["Specific Customer", "All Active Customers", "Custom Phone/Email"], horizontal=True)
            
            customer_search = None
            if recipient_type == "Specific Customer":
                if _customers_available:
                    cust_list = list_customers()
                    if cust_list:
                        # Map customers to format for dropdown
                        cust_options = {
                            c["id"]: f"{get_display_name(int(c['id']))} (ID: {c['id']})"
                            for c in cust_list
                            if c.get("id") is not None
                        }
                        customer_id = st.selectbox("Select Customer", options=list(cust_options.keys()), format_func=lambda x: cust_options[x])
                    else:
                        st.warning("No customers found.")
                else:
                    st.error("Customers module is unavailable.")
            elif recipient_type == "Custom Phone/Email":
                custom_contact = st.text_input("Enter Email or Phone Number")
            
            st.divider()
            notification_type = st.selectbox("Notification Method", ["SMS", "Email", "In-App/Push"])
            template_used = st.selectbox("Use Template (Optional)", ["None", "Payment Reminder", "Payment Overdue", "Account Update", "Loan Approved"])
            
            subject = ""
            if notification_type == "Email":
                subject = st.text_input("Subject")
            
            message_body = st.text_area("Message Body", height=150)
            
            submitted = st.form_submit_button("Send Notification", type="primary")
            if submitted:
                if not message_body.strip():
                    st.error("Message body cannot be empty.")
                else:
                    st.success("Notification queued for delivery successfully!")
                    
                    # Store to history in session state for mock
                    if "notification_history" not in st.session_state:
                        st.session_state["notification_history"] = []
                    
                    target = ""
                    if recipient_type == "Specific Customer" and 'customer_id' in locals():
                        target = f"Customer ID: {customer_id}"
                    elif recipient_type == "Custom Phone/Email":
                        target = custom_contact
                    else:
                        target = "All Active Customers"
                        
                    st.session_state["notification_history"].insert(0, {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "type": notification_type,
                        "recipient": target,
                        "status": "Sent",
                        "message": message_body[:50] + "..." if len(message_body) > 50 else message_body
                    })

    with tab_templates:
        st.subheader("Manage Templates")
        st.info("Here you can define and edit standard templates to use for bulk or automated notifications.")
        
        with st.expander("Create New Template"):
            with st.form("new_template_form"):
                new_tpl_name = st.text_input("Template Name", placeholder="e.g. Loan Disbursement SMS")
                new_tpl_type = st.selectbox("Template Type", ["SMS", "Email", "In-App"])
                new_tpl_body = st.text_area("Template Content (use {variables} for dynamic fields)")
                if st.form_submit_button("Save Template"):
                    st.success(f"Template '{new_tpl_name}' saved.")
                    
        st.markdown("### Existing Templates")
        mock_templates = pd.DataFrame([
            {"Template Name": "Payment Reminder", "Type": "SMS", "Last Updated": "2024-01-15", "Content Preview": "Dear {name}, your payment of {amount} is due..."},
            {"Template Name": "Payment Overdue", "Type": "Email", "Last Updated": "2024-02-10", "Content Preview": "Notice: Your account is currently in arrears..."},
            {"Template Name": "Loan Approved", "Type": "SMS", "Last Updated": "2023-11-20", "Content Preview": "Congratulations {name}, your loan application..."},
        ])
        st.dataframe(mock_templates, hide_index=True, use_container_width=True)

    with tab_history:
        st.subheader("Notification History")
        
        col1, col2 = st.columns(2)
        with col1:
            filter_type = st.selectbox("Filter by Type", ["All", "SMS", "Email", "In-App"], key="hist_filter_type")
        with col2:
            filter_status = st.selectbox("Filter by Status", ["All", "Sent", "Failed", "Pending"], key="hist_filter_status")
            
        history = st.session_state.get("notification_history", [])
        
        if not history:
            st.info("No notifications have been sent during this session.")
            # Show some mock history if empty to make it look realistic
            mock_history = pd.DataFrame([
                {"timestamp": "2024-03-01 09:15:00", "type": "SMS", "recipient": "Customer ID: 12", "status": "Sent", "message": "Your loan has been disbursed...!"},
                {"timestamp": "2024-03-01 08:30:22", "type": "Email", "recipient": "Customer ID: 45", "status": "Failed", "message": "Statement for February 2024"},
                {"timestamp": "2024-02-28 14:05:10", "type": "SMS", "recipient": "All Active Customers", "status": "Sent", "message": "Notice: Our offices will be closed..."},
            ])
            st.dataframe(mock_history, hide_index=True, use_container_width=True)
        else:
            df_history = pd.DataFrame(history)
            
            if filter_type != "All":
                df_history = df_history[df_history["type"] == filter_type]
            if filter_status != "All":
                df_history = df_history[df_history["status"] == filter_status]
                
            st.dataframe(df_history, hide_index=True, use_container_width=True)


MJ_MANUAL_SUBACCOUNT_PLACEHOLDER = "— Select sub account —"


def _mj_widget_key_part(s) -> str:
    x = re.sub(r"[^a-zA-Z0-9]+", "_", str(s))
    x = x.strip("_") or "k"
    return x[:48]


def _mj_ordered_system_tags_for_direction(templates: list | None, direction: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    dir_u = (direction or "").strip().upper()
    for t in templates or []:
        if (t.get("direction") or "").strip().upper() != dir_u:
            continue
        tag = t.get("system_tag")
        if tag and tag not in seen:
            seen.add(tag)
            out.append(str(tag))
    return out


@st.cache_data(ttl=120, show_spinner=False)
def _cached_posting_leaf_accounts_for_balance_adjust() -> list:
    """
    Posting-leaf list for balance adjustment dropdowns.
    Cached ~2m to limit DB + tree walks on Streamlit reruns; TTL keeps COA edits visible quickly.
    """
    from accounting_service import AccountingService

    return AccountingService().list_posting_leaf_accounts()


def journals_ui():
    """
    Standalone Journals module for operational users.
    Focuses on posting manual journals using configured templates.
    """
    import psycopg2
    import psycopg2.extras

    from accounting_service import AccountingService
    from config import get_database_url
    from datetime import datetime
    from decimal import Decimal

    svc = AccountingService()

    st.markdown(
        "<div style='background-color: #0F766E; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Journals</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    try:
        bad_journals = svc.list_unbalanced_journal_entries()
        if bad_journals:
            st.error(
                f"**Data integrity:** {len(bad_journals)} journal header(s) are **materially** unbalanced "
                "(per-line 10dp, then totals compared at **2dp**; sub–2dp drift is ignored). "
                "**Avoid:** unbalanced journals cannot be saved. "
                "**Flag:** listed here + **Balanced** in Statements. "
                "Use **Repair LOAN_APPROVAL** when applicable."
            )
            with st.expander("Unbalanced journal headers (detail) & repair"):
                st.dataframe(pd.DataFrame([dict(r) for r in bad_journals]), width="stretch")
                st.caption(
                    "**imbalance_2dp** = rounded total debits minus rounded total credits (each total = sum of 10dp line amounts). "
                    "Classic LOAN_APPROVAL bug: principal debit too small vs cash + deferred fees — re-post fixes when supported."
                )
                repair_id = st.number_input(
                    "Loan ID (LOAN_APPROVAL repair)",
                    min_value=1,
                    step=1,
                    key="journals_repair_loan_id",
                )
                if st.button("Re-post LOAN_APPROVAL from loan record", key="journals_repair_loan_btn"):
                    try:
                        svc.repost_loan_approval_journal(int(repair_id), created_by="ui_user")
                        st.success(f"Re-posted LOAN_APPROVAL for loan {int(repair_id)}.")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
    except Exception as ex:
        st.caption(f"Could not check journal double-entry integrity: {ex}")

    tab_manual, tab_adjust = st.tabs(["Manual Journals", "Balance Adjustments"])

    with tab_manual:
        st.subheader("Post Manual Journal")
        with st.form("journals_manual_journal_form"):
            templates_all = svc.list_all_transaction_templates()
            event_types = sorted({t["event_type"] for t in templates_all})

            col_l, col_r = st.columns(2)
            with col_l:
                loan_id = st.text_input(
                    "Loan ID (optional)",
                    help="Required for templates that resolve accounts from the loan (e.g. PRODUCT / LOAN_CAPTURE).",
                )
                event_type = st.selectbox(
                    "Journal template (event type)",
                    event_types if event_types else ["(no templates configured)"],
                )
                amount = st.number_input("Amount", min_value=0.0, step=0.01, format="%.2f")
            with col_r:
                st.caption(
                    "Enter a **loan ID** when the template needs it to auto-pick accounts. "
                    "Otherwise use subaccount dropdowns where shown."
                )
                description = st.text_input("Narration (description)")
                is_reversal = st.checkbox("Reverse entry (swap debits and credits)", value=False)

            _et_ok = event_type and event_type != "(no templates configured)"
            loan_id_int = int(str(loan_id).strip()) if loan_id and str(loan_id).strip().isdigit() else None
            tmpl_rows = svc.get_transaction_templates(event_type) if _et_ok else []
            dr_tags = _mj_ordered_system_tags_for_direction(tmpl_rows, "DEBIT")
            cr_tags = _mj_ordered_system_tags_for_direction(tmpl_rows, "CREDIT")

            st.markdown("**Accounts from template**")
            st.caption(
                "Debit lines (left) and credit lines (right). When the tagged account has **subaccounts**, "
                "choose a leaf; **"
                + MJ_MANUAL_SUBACCOUNT_PLACEHOLDER
                + "** is not valid for posting."
            )
            c_dr, c_cr = st.columns(2)

            def _mj_render_one_side(side_label: str, side_k: str, tags: list[str]) -> None:
                st.markdown(f"**{side_label}**")
                if not tags:
                    st.caption("No lines on this side for this template.")
                    return
                for tag in tags:
                    st.caption(f"Tag `{tag}`")
                    row = svc.fetch_account_row_for_system_tag(tag)
                    if not row:
                        st.warning(f"No chart account carries system tag `{tag}`.")
                        continue
                    resolved, err = svc.try_resolve_posting_account_for_tag(tag, loan_id=loan_id_int)
                    kids = svc.list_active_direct_children_accounts(row["id"])
                    if resolved:
                        st.caption(
                            f"Posting account: **{resolved.get('code')}** — {resolved.get('name')} "
                            "*(auto-resolved)*"
                        )
                        continue
                    if kids:
                        labels = [MJ_MANUAL_SUBACCOUNT_PLACEHOLDER] + [
                            f"{c['code']} — {c['name']}" for c in kids
                        ]
                        sk = f"mj_pick_{side_k}_{_mj_widget_key_part(event_type)}_{_mj_widget_key_part(tag)}"
                        st.selectbox(
                            "Posting account",
                            range(len(labels)),
                            format_func=lambda i, lab=labels: lab[i],
                            index=0,
                            key=sk,
                            help="Pick the leaf account; the first option cannot be posted.",
                        )
                        continue
                    st.error(err or f"Cannot resolve `{tag}` and there are no subaccounts to choose.")

            with c_dr:
                _mj_render_one_side("Debit accounts", "dr", dr_tags)
            with c_cr:
                _mj_render_one_side("Credit accounts", "cr", cr_tags)

            journal_to_reverse = None
            if is_reversal:
                all_entries = svc.get_journal_entries()
                candidates = [
                    e
                    for e in all_entries
                    if e["event_tag"] == event_type
                    and (not loan_id or (e.get("event_id") == loan_id))
                ]
                if candidates:
                    labels = [
                        f"{e['entry_date']} | {e.get('reference') or ''} | {e['event_tag']} (ID: {e['id']})"
                        for e in candidates
                    ]
                    sel = st.selectbox(
                        "Journal to reverse",
                        labels,
                        help="Same template and accounts as the original; debits/credits are swapped on post.",
                    )
                    journal_to_reverse = candidates[labels.index(sel)]
                else:
                    st.info(
                        "No matching journals found to reverse for this template "
                        "and (if provided) loan ID."
                    )

            with st.expander("Extra account_overrides (JSON, optional)", expanded=False):
                overrides_json = st.text_area(
                    "Additional overrides",
                    height=56,
                    placeholder='{"some_system_tag": "uuid-of-leaf-account"}',
                    help="Merged with account picks above; explicit picks for a tag win over JSON for that tag.",
                    key="manual_journal_overrides_json",
                    label_visibility="collapsed",
                )
                st.caption("Optional. Per-tag dropdown selections above are applied automatically.")

            submitted3 = st.form_submit_button("Post journal")
            if submitted3:
                if not _et_ok or amount <= 0:
                    st.error("Choose a valid template and enter an amount greater than zero.")
                elif is_reversal and journal_to_reverse is None:
                    st.error("Select the original journal entry to reverse.")
                else:
                    ref = f"MANUAL-{int(datetime.now().timestamp())}"
                    if is_reversal and journal_to_reverse:
                        ref = f"REV-{journal_to_reverse.get('reference') or journal_to_reverse['id']}"
                        if not description:
                            description = f"Reversal of entry {journal_to_reverse['id']}"
                    _manual_loan_id = None
                    if loan_id and str(loan_id).strip().isdigit():
                        _manual_loan_id = int(str(loan_id).strip())

                    import json as _json

                    _merge_ov: dict = {}
                    _pick_errs: list[str] = []
                    _payload_ok = True
                    if overrides_json and str(overrides_json).strip():
                        try:
                            _pour = _json.loads(overrides_json)
                            if not isinstance(_pour, dict):
                                st.error("account_overrides JSON must be an object.")
                                _payload_ok = False
                            else:
                                _merge_ov.update(_pour)
                        except _json.JSONDecodeError as je:
                            st.error(f"Invalid JSON: {je}")
                            _payload_ok = False

                    tmpl_submit = svc.get_transaction_templates(event_type) if _payload_ok else []
                    if _payload_ok and not tmpl_submit:
                        st.error("This event type has no transaction template lines.")
                        _payload_ok = False

                    if _payload_ok:
                        dr_s = _mj_ordered_system_tags_for_direction(tmpl_submit, "DEBIT")
                        cr_s = _mj_ordered_system_tags_for_direction(tmpl_submit, "CREDIT")
                        for side_k, tags in (("dr", dr_s), ("cr", cr_s)):
                            for tag in tags:
                                row = svc.fetch_account_row_for_system_tag(tag)
                                if not row:
                                    _pick_errs.append(f"No chart account for system tag `{tag}`.")
                                    continue
                                kids = svc.list_active_direct_children_accounts(row["id"])
                                resolved, err = svc.try_resolve_posting_account_for_tag(
                                    tag, loan_id=_manual_loan_id
                                )
                                if resolved:
                                    continue
                                if not kids:
                                    _pick_errs.append(
                                        f"`{tag}`: {err or 'Cannot resolve posting account.'}"
                                    )
                                    continue
                                sk = f"mj_pick_{side_k}_{_mj_widget_key_part(event_type)}_{_mj_widget_key_part(tag)}"
                                idx = int(st.session_state.get(sk, 0))
                                uuid_list = [None] + [str(c["id"]) for c in kids]
                                if idx <= 0 or idx >= len(uuid_list) or not uuid_list[idx]:
                                    _pick_errs.append(
                                        f"Choose a sub account for `{tag}` on the "
                                        f"{'debit' if side_k == 'dr' else 'credit'} side "
                                        f"({MJ_MANUAL_SUBACCOUNT_PLACEHOLDER!r} is not valid for posting)."
                                    )
                                else:
                                    _merge_ov[tag] = uuid_list[idx]

                    if _pick_errs:
                        for msg in _pick_errs:
                            st.error(msg)
                        _payload_ok = False

                    _manual_payload = {"account_overrides": _merge_ov} if (_payload_ok and _merge_ov) else None

                    if _payload_ok:
                        try:
                            svc.post_event(
                                event_type=event_type,
                                reference=ref,
                                description=description,
                                event_id=loan_id
                                or (journal_to_reverse.get("event_id") if journal_to_reverse else "MANUAL"),
                                created_by="ui_user",
                                entry_date=datetime.today().date(),
                                amount=Decimal(str(amount)),
                                payload=_manual_payload,
                                is_reversal=is_reversal,
                                loan_id=_manual_loan_id,
                            )
                            st.success("Manual journal posted successfully.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error posting journal: {e}")

    with tab_adjust:
        st.subheader("Balance Adjustment Journal")
        st.info(
            "One-off GL corrections. Only **posting accounts** are listed: active accounts with **no** "
            "active subaccounts (the youngest nodes in each branch — e.g. A700000 alone if it has no children; "
            "otherwise A710000 / A720000 / … down to leaves). Labels show the code path from the root."
        )
        st.caption(
            "The leaf list is cached briefly (about two minutes) to avoid repeating the same chart walk on "
            "every Streamlit rerun; one `list_accounts` query plus an in-memory pass is already light."
        )

        posting_leaves = _cached_posting_leaf_accounts_for_balance_adjust()

        dr_i = 0
        cr_i = 0
        with st.form("balance_adjust_form"):
            col_dt, col_amt = st.columns([1, 1])
            with col_dt:
                value_date = st.date_input("Value Date", value=_get_system_date())
            with col_amt:
                amount = st.number_input("Amount", min_value=0.0, step=0.01)

            col_dr, col_cr = st.columns(2)
            if posting_leaves:
                with col_dr:
                    dr_i = st.selectbox(
                        "Debit account",
                        range(len(posting_leaves)),
                        format_func=lambda i, pl=posting_leaves: pl[i]["display_label"],
                        key="bal_adj_dr_leaf_idx",
                    )
                with col_cr:
                    cr_i = st.selectbox(
                        "Credit account",
                        range(len(posting_leaves)),
                        format_func=lambda i, pl=posting_leaves: pl[i]["display_label"],
                        key="bal_adj_cr_leaf_idx",
                    )
            else:
                with col_dr:
                    st.selectbox(
                        "Debit account",
                        ["(no posting accounts in chart)"],
                        disabled=True,
                        key="bal_adj_dr_leaf_idx_empty",
                    )
                with col_cr:
                    st.selectbox(
                        "Credit account",
                        ["(no posting accounts in chart)"],
                        disabled=True,
                        key="bal_adj_cr_leaf_idx_empty",
                    )

            narration = st.text_input("Narration / Description", key="bal_adj_narr")

            submitted_adj = st.form_submit_button(
                "Post Balance Adjustment",
                disabled=not posting_leaves,
            )

        if submitted_adj and posting_leaves:
            dr_row = posting_leaves[int(dr_i)] if 0 <= int(dr_i) < len(posting_leaves) else None
            cr_row = posting_leaves[int(cr_i)] if 0 <= int(cr_i) < len(posting_leaves) else None
            dr_code = dr_row["code"] if dr_row else None
            cr_code = cr_row["code"] if cr_row else None
            dr_id = dr_row["id"] if dr_row else None
            cr_id = cr_row["id"] if cr_row else None

            if not dr_code or not cr_code or not dr_id or not cr_id:
                st.error("Please select both debit and credit posting accounts.")
            elif dr_code == cr_code:
                st.error("Debit and credit accounts must be different.")
            elif amount <= 0:
                st.error("Amount must be greater than zero.")
            else:
                try:
                    conn = psycopg2.connect(
                        get_database_url(), cursor_factory=psycopg2.extras.RealDictCursor
                    )
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO journal_entries (entry_date, reference, description, event_id, event_tag, created_by)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                RETURNING id
                                """,
                                (
                                    value_date,
                                    "BAL_ADJ",
                                    narration or "Balance adjustment journal",
                                    None,
                                    "BALANCE_ADJUSTMENT",
                                    "ui_user",
                                ),
                            )
                            entry_id = cur.fetchone()["id"]

                            cur.execute(
                                """
                                INSERT INTO journal_items (entry_id, account_id, debit, credit, memo)
                                VALUES (%s, %s, %s, %s, %s)
                                """,
                                (entry_id, dr_id, Decimal(str(amount)), Decimal("0.0"), narration),
                            )
                            cur.execute(
                                """
                                INSERT INTO journal_items (entry_id, account_id, debit, credit, memo)
                                VALUES (%s, %s, %s, %s, %s)
                                """,
                                (entry_id, cr_id, Decimal("0.0"), Decimal(str(amount)), narration),
                            )
                        conn.commit()
                        st.success("Balance adjustment journal posted successfully.")
                        _cached_posting_leaf_accounts_for_balance_adjust.clear()
                        st.rerun()
                    finally:
                        conn.close()
                except Exception as e:
                    st.error(f"Error posting balance adjustment journal: {e}")


def document_management_ui():
    if not _documents_available:
        st.error(f"Documents module unavailable: {_documents_error}")
        return

    st.header("Document Management")
    
    tab_classes, tab_categories, tab_all_docs, tab_generated = st.tabs([
        "Document Classes",
        "Document Categories", 
        "All Documents",
        "Generated Documents"
    ])
    
    with tab_classes:
        st.subheader("Document Classes Configuration")
        st.write("Manage the high-level grouping of documents (e.g., 'Know Your Customer', 'Agreements').")
        
        with st.expander("Create New Class", expanded=False):
            with st.form("create_doc_class_form"):
                new_class_name = st.text_input("Class Name", placeholder="e.g. KYC Documents")
                new_class_desc = st.text_area("Description")
                if st.form_submit_button("Save Class"):
                    if new_class_name.strip():
                        try:
                            create_document_class(new_class_name.strip(), new_class_desc.strip())
                            st.success(f"Class '{new_class_name}' created.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error creating class: {e}")
                    else:
                        st.error("Class Name is required.")
        
        classes = list_document_classes(active_only=False)
        if classes:
            df_classes = pd.DataFrame(classes)
            st.dataframe(df_classes[["id", "name", "description", "is_active", "created_at"]], hide_index=True, use_container_width=True)
            
            st.subheader("Edit Class")
            edit_class_id = st.selectbox("Select Class to Edit", [c["id"] for c in classes], format_func=lambda x: next(c["name"] for c in classes if c["id"] == x))
            selected_class = next(c for c in classes if c["id"] == edit_class_id)
            
            with st.form("edit_doc_class_form"):
                edit_c_name = st.text_input("Class Name", value=selected_class["name"])
                edit_c_desc = st.text_area("Description", value=selected_class["description"] or "")
                edit_c_active = st.checkbox("Is Active?", value=selected_class["is_active"])
                
                if st.form_submit_button("Update Class"):
                    if edit_c_name.strip():
                        try:
                            update_document_class(edit_class_id, edit_c_name.strip(), edit_c_desc.strip(), edit_c_active)
                            st.success(f"Class '{edit_c_name}' updated.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error updating class: {e}")
                    else:
                        st.error("Class Name is required.")
        else:
            st.info("No document classes found. Create one above.")

    with tab_categories:
        st.subheader("Document Categories Configuration")
        st.write("Manage the specific types of documents within classes that can be uploaded.")
        
        active_classes = list_document_classes(active_only=True)
        class_options = {c["id"]: c["name"] for c in active_classes} if active_classes else {}
        
        with st.expander("Create New Category", expanded=False):
            with st.form("create_doc_cat_form"):
                if class_options:
                    new_cat_class_id = st.selectbox("Document Class", options=list(class_options.keys()), format_func=lambda x: class_options[x])
                else:
                    st.warning("Please create a Document Class first.")
                    new_cat_class_id = None
                    
                new_cat_name = st.text_input("Category Name", placeholder="e.g. Identity Document")
                new_cat_desc = st.text_area("Description")
                if st.form_submit_button("Save Category"):
                    if new_cat_name.strip() and new_cat_class_id:
                        try:
                            create_document_category(new_cat_name.strip(), new_cat_desc.strip(), new_cat_class_id)
                            st.success(f"Category '{new_cat_name}' created.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error creating category: {e}")
                    else:
                        st.error("Category Name and Class are required.")
        
        cats = list_document_categories(active_only=False)
        if cats:
            df_cats = pd.DataFrame(cats)
            # Reorder columns for display
            display_cols = ["id", "class_name", "name", "description", "is_active", "created_at"]
            # Ensure all columns exist
            display_cols = [c for c in display_cols if c in df_cats.columns]
            st.dataframe(df_cats[display_cols], hide_index=True, use_container_width=True)
            
            st.subheader("Edit Category")
            edit_cat_id = st.selectbox("Select Category to Edit", [c["id"] for c in cats], format_func=lambda x: next(c["name"] for c in cats if c["id"] == x))
            selected_cat = next(c for c in cats if c["id"] == edit_cat_id)
            
            with st.form("edit_doc_cat_form"):
                edit_cat_class_id = None
                if class_options:
                    default_idx = list(class_options.keys()).index(selected_cat["class_id"]) if selected_cat["class_id"] in class_options else 0
                    edit_cat_class_id = st.selectbox("Document Class", options=list(class_options.keys()), format_func=lambda x: class_options[x], index=default_idx)
                
                edit_name = st.text_input("Category Name", value=selected_cat["name"])
                edit_desc = st.text_area("Description", value=selected_cat["description"] or "")
                edit_active = st.checkbox("Is Active?", value=selected_cat["is_active"])
                
                if st.form_submit_button("Update Category"):
                    if edit_name.strip():
                        try:
                            update_document_category(edit_cat_id, edit_name.strip(), edit_desc.strip(), edit_active, edit_cat_class_id)
                            st.success(f"Category '{edit_name}' updated.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error updating category: {e}")
                    else:
                        st.error("Category Name is required.")
        else:
            st.info("No document categories found. Create one above.")

    with tab_all_docs:
        st.subheader("All Uploaded Documents")
        docs = list_documents()
        if docs:
            # We don't want to display the full bytea content in the dataframe
            display_docs = []
            for d in docs:
                display_docs.append({
                    "ID": d["id"],
                    "Entity": f"{d['entity_type'].capitalize()} #{d['entity_id']}",
                    "Category": d["category_name"] or "Uncategorized",
                    "File Name": d["file_name"],
                    "Size (KB)": round(d["file_size"] / 1024, 1),
                    "Uploaded At": d["uploaded_at"],
                    "Uploaded By": d["uploaded_by"]
                })
            st.dataframe(pd.DataFrame(display_docs), hide_index=True, use_container_width=True)
            
            st.subheader("Download Document")
            dl_doc_id = st.selectbox("Select Document to Download", [d["id"] for d in docs], format_func=lambda x: next(f"ID {d['id']} - {d['file_name']}" for d in docs if d["id"] == x))
            dl_doc = get_document(dl_doc_id)
            if dl_doc:
                # PostgreSQL `bytea` sometimes comes back as `memoryview` via the DB driver.
                # Streamlit's download_button expects real `bytes`.
                file_content = dl_doc.get("file_content")
                if file_content is None:
                    file_content = b""
                elif isinstance(file_content, memoryview):
                    file_content = file_content.tobytes()
                elif isinstance(file_content, bytearray):
                    file_content = bytes(file_content)
                elif isinstance(file_content, str):
                    file_content = file_content.encode("utf-8")
                st.download_button(
                    label=f"Download {dl_doc['file_name']}",
                    data=file_content,
                    file_name=dl_doc["file_name"],
                    mime=dl_doc["file_type"]
                )
        else:
            st.info("No documents found in the system.")

    with tab_generated:
        st.subheader("Autogenerated Documents")
        st.info("System-generated quotations, agreements, and offer letters will appear here once configured in the product rules.")
        # Future implementation for autogenerated documents.
        

def main():
    st.sidebar.header("Navigation")
    nav = st.sidebar.radio("Section", get_loan_app_sections())
    st.sidebar.divider()
    render_loan_app_section(nav)


LOAN_APP_SECTIONS = [
    "Customers",
    "Loan management",
    "Interest in Suspense",
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
]


def get_loan_app_sections() -> list[str]:
    return list(LOAN_APP_SECTIONS)


def render_loan_app_section(nav: str) -> None:
    _get_global_loan_settings()  # ensure defaults exist
    if nav == "Customers":
        customers_ui()
    elif nav == "Teller":
        teller_ui()
    elif nav == "Reamortisation":
        reamortisation_ui()
    elif nav == "Statements":
        statements_ui()
    elif nav == "Interest in Suspense":
        from interest_suspense_ui import render_suspense_ui

        render_suspense_ui()
    elif nav == "Portfolio reports":
        from portfolio_reports_ui import render_portfolio_reports_ui

        render_portfolio_reports_ui()
    elif nav == "Loan management":
        st.markdown(
            "<div style='color:#16A34A; font-weight:700; font-size:2rem; margin:0.25rem 0 0.75rem 0;'>Loan Management</div>",
            unsafe_allow_html=True,
        )
        pending_approval_count = 0
        try:
            pending_approval_count = len(
                list_loan_approval_drafts(status="PENDING", limit=10000) or []
            )
        except Exception:
            pending_approval_count = 0
        st.caption(
            f"**{pending_approval_count}** loan draft(s) awaiting approval. "
            "Use the section control below; your choice is kept after actions such as **Send back for rework**."
        )
        _lm_sections = ["Loan capture", "View schedule", "Loan calculators", "Approve loans"]
        st.session_state.setdefault("loan_mgmt_subnav", "Loan capture")
        if st.session_state["loan_mgmt_subnav"] not in _lm_sections:
            st.session_state["loan_mgmt_subnav"] = "Loan capture"
        _lm_idx = _lm_sections.index(st.session_state["loan_mgmt_subnav"])
        _lm_pick = st.radio(
            "Loan management section",
            _lm_sections,
            index=_lm_idx,
            horizontal=True,
            key="loan_mgmt_subnav",
            label_visibility="collapsed",
        )
        if _lm_pick == "Loan capture":
            capture_loan_ui()
        elif _lm_pick == "View schedule":
            view_schedule_ui()
        elif _lm_pick == "Loan calculators":
            st.caption("All calculators are available on one page. Open any section below.")
            col_left, col_right = st.columns(2)
            with col_left:
                with st.expander("Consumer Loan Calculator", expanded=True):
                    consumer_loan_ui()
                with st.expander("Bullet Loan Calculator", expanded=False):
                    bullet_loan_ui()
            with col_right:
                with st.expander("Term Loan Calculator", expanded=True):
                    term_loan_ui()
                with st.expander("Customised Repayments Calculator", expanded=False):
                    customised_repayments_ui()
        else:
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