from __future__ import annotations

import streamlit as st

from accrual_convention import normalize_accrual_start_convention

from ui.system_configurations.accounting_tab import render_accounting_config_tab
from ui.system_configurations.consumer_schemes_tab import render_consumer_schemes_tab
from ui.system_configurations.display_tab import render_display_tab
from ui.system_configurations.eod_tab import render_eod_config_tab
from ui.system_configurations.grade_scales_tab import render_grade_scales_tab
from ui.system_configurations.ifrs_provision_tab import render_ifrs_provision_tab
from ui.system_configurations.loan_purposes_tab import render_loan_purposes_tab
from ui.system_configurations.products_tab import render_products_tab
from ui.system_configurations.sectors_tab import render_sectors_tab
from ui.system_configurations.roles_access_tab import render_roles_access_tab
from ui.system_configurations.subscription_vendor_tab import render_subscription_vendor_tab


def render_system_configurations_ui(
    *,
    get_system_config,
    consumer_schemes_admin_editor_ui,
    parse_display_substrings_csv,
    customers_available: bool,
    list_sectors,
    list_subsectors,
    create_sector,
    create_subsector,
    loan_management_available: bool,
    loan_management_error: str,
    list_products,
    get_product_by_code,
    create_product,
    update_product,
    delete_product,
    get_product,
    get_product_config_from_db,
    save_product_config_to_db,
    list_loan_purposes,
    get_loan_purpose_by_id,
    create_loan_purpose,
    set_loan_purpose_active,
    update_loan_purpose,
    count_loan_purposes_rows,
    clear_all_loan_purposes,
) -> None:
    _get_system_config = get_system_config
    _consumer_schemes_admin_editor_ui = consumer_schemes_admin_editor_ui
    _parse_display_substrings_csv = parse_display_substrings_csv
    _customers_available = customers_available
    _loan_management_available = loan_management_available
    _loan_management_error = loan_management_error

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
        "incremental_loan_engine": False,
    }
    existing_tasks = eod_cfg.get("tasks") or {}
    eod_tasks: dict[str, bool] = {
        k: bool(existing_tasks.get(k, default))
        for k, default in eod_task_defaults.items()
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

    accrual_start_convention_selected = normalize_accrual_start_convention(
        cfg.get("accrual_start_convention")
    )

    (
        tab_sectors,
        tab_eod,
        tab_accounting,
        tab_consumer_schemes,
        tab_products,
        tab_loan_purposes,
        tab_grade_scales,
        tab_ifrs_prov,
        tab_display,
        tab_sub_vendor,
        tab_roles,
    ) = st.tabs(
        [
            "Sectors & subsectors",
            "EOD configurations",
            "Accounting configurations",
            "Consumer schemes",
            "Products",
            "Loan purposes",
            "Loan grade scales",
            "IFRS provision config",
            "Display & numbers",
            "Subscription (vendor)",
            "Roles & access",
        ],
    )

    with tab_sectors:
        render_sectors_tab(
            customers_available=_customers_available,
            list_sectors=list_sectors,
            list_subsectors=list_subsectors,
            create_sector=create_sector,
            create_subsector=create_subsector,
        )

    with tab_eod:
        _eod = render_eod_config_tab(
            eod_mode=eod_mode,
            eod_time=eod_time,
            eod_tasks=eod_tasks,
            policy_mode=policy_mode,
            blocking_stages=blocking_stages,
            advance_date_on_degraded=advance_date_on_degraded,
            accrual_start_convention_selected=accrual_start_convention_selected,
        )
        accrual_start_convention_selected = _eod.accrual_start_convention_selected
        eod_mode = _eod.eod_mode
        eod_time = _eod.eod_time
        eod_tasks = _eod.eod_tasks
        policy_mode = _eod.policy_mode
        blocking_stages = _eod.blocking_stages
        advance_date_on_degraded = _eod.advance_date_on_degraded

    with tab_accounting:
        _acc = render_accounting_config_tab(cfg=cfg)
        month_mode = _acc.month_mode
        month_day = _acc.month_day
        fiscal_year_end_month = _acc.fiscal_year_end_month
        snapshot_max_rows = _acc.snapshot_max_rows

    with tab_consumer_schemes:
        render_consumer_schemes_tab(
            consumer_schemes_admin_editor_ui=_consumer_schemes_admin_editor_ui,
        )

    with tab_products:
        render_products_tab(
            loan_management_available=_loan_management_available,
            loan_management_error=_loan_management_error,
            list_products=list_products,
            get_product_by_code=get_product_by_code,
            create_product=create_product,
            update_product=update_product,
            delete_product=delete_product,
            get_product=get_product,
            get_product_config_from_db=get_product_config_from_db,
            save_product_config_to_db=save_product_config_to_db,
            cfg=cfg,
        )

    with tab_loan_purposes:
        render_loan_purposes_tab(
            loan_management_available=_loan_management_available,
            loan_management_error=_loan_management_error,
            list_loan_purposes=list_loan_purposes,
            get_loan_purpose_by_id=get_loan_purpose_by_id,
            create_loan_purpose=create_loan_purpose,
            set_loan_purpose_active=set_loan_purpose_active,
            update_loan_purpose=update_loan_purpose,
            count_loan_purposes_rows=count_loan_purposes_rows,
            clear_all_loan_purposes=clear_all_loan_purposes,
        )

    with tab_grade_scales:
        render_grade_scales_tab()

    with tab_ifrs_prov:
        render_ifrs_provision_tab()

    with tab_sub_vendor:
        render_subscription_vendor_tab()

    with tab_roles:
        from middleware import get_current_user

        render_roles_access_tab(get_current_user=get_current_user)

    with tab_display:
        _disp = render_display_tab(cfg=cfg)
        disp_amount_decimals = _disp.disp_amount_decimals
        disp_thousands_separator = _disp.disp_thousands_separator
        disp_currency_symbol = _disp.disp_currency_symbol
        disp_streamlit_fmt = _disp.disp_streamlit_fmt
        disp_auto_all_float = _disp.disp_auto_all_float
        disp_money_subs = _disp.disp_money_subs
        disp_skip_subs = _disp.disp_skip_subs

    st.session_state["system_config"] = {
        **cfg,
        "accrual_start_convention": normalize_accrual_start_convention(
            accrual_start_convention_selected
        ),
        "display_format": {
            "amount_decimals": disp_amount_decimals,
            "thousands_separator": disp_thousands_separator,
            "currency_symbol": (disp_currency_symbol or "$").strip()[:8] or "$",
            "streamlit_money_format": disp_streamlit_fmt,
            "auto_format_all_float_columns": disp_auto_all_float,
            "money_column_name_substrings": _parse_display_substrings_csv(disp_money_subs),
            "skip_column_name_substrings": _parse_display_substrings_csv(disp_skip_subs),
        },
        "eod_settings": {
            "mode": eod_mode,
            "automatic_time": eod_time,
            "tasks": {
                "run_loan_engine": True,
                "post_accounting_events": eod_tasks.get("post_accounting_events", False),
                "generate_statements": eod_tasks.get("generate_statements", False),
                "snapshot_financial_statements": eod_tasks.get(
                    "snapshot_financial_statements", True
                ),
                "send_notifications": eod_tasks.get("send_notifications", False),
                "incremental_loan_engine": eod_tasks.get("incremental_loan_engine", False),
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
