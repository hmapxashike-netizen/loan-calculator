from __future__ import annotations

import streamlit as st

from accounting.core import MappingRegistry
from display_formatting import default_display_format, resolve_display_format


def get_mapping_registry(*, key: str = "accounting_mapping_registry") -> MappingRegistry:
    """Lazy-initialise an in-memory MappingRegistry stored in session state."""
    if key not in st.session_state:
        st.session_state[key] = MappingRegistry()
    return st.session_state[key]


def get_fx_rates(*, key: str = "accounting_fx_rates") -> list[dict]:
    """
    Simple FX rate store in session state.
    Each item: {"currency": str, "rate_to_base": float, "as_of": str}.
    """
    if key not in st.session_state:
        st.session_state[key] = []
    return st.session_state[key]


def get_global_loan_settings(*, key: str = "global_loan_settings") -> dict:
    """Global assumptions: interest_method, interest_type, rate_basis (no principal_input - per loan)."""
    if key not in st.session_state:
        st.session_state[key] = {
            "interest_method": "Reducing balance",
            "interest_type": "Simple",
            "rate_basis": "Per month",
        }
    return st.session_state[key]


def get_system_config(*, key: str = "system_config") -> dict:
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
            "term_loan": {
                "interest_pct": 7.0,
                "drawdown_pct": 2.5,
                "arrangement_pct": 2.5,
            },
            "bullet_loan": {
                "interest_pct": 7.0,
                "drawdown_pct": 2.5,
                "arrangement_pct": 2.5,
            },
            "customised_repayments": {
                "interest_pct": 7.0,
                "drawdown_pct": 2.5,
                "arrangement_pct": 2.5,
            },
        },
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
                "incremental_loan_engine": True,
                "loan_engine_commit_batch_size": 250,
                "loan_engine_log_timing": False,
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
        "display_format": default_display_format(),
        # Regular scheduled interest: NEXT_DAY = first accrual day after period start (legacy).
        "accrual_start_convention": "EFFECTIVE_DAY",
    }
    try:
        from provision_config import (
            PROVISION_RBZ_CONFIG_KEY,
            default_provision_rbz_config,
        )

        defaults[PROVISION_RBZ_CONFIG_KEY] = default_provision_rbz_config()
    except Exception:
        pass
    try:
        from loan_management.loan_pipeline_config import (
            default_business_facility_subtypes,
            default_loan_application_statuses,
        )

        defaults["loan_application_statuses"] = default_loan_application_statuses()
        defaults["business_facility_subtypes"] = default_business_facility_subtypes()
    except Exception:
        pass

    if key not in st.session_state:
        try:
            from loan_management import load_system_config_from_db

            db_cfg = load_system_config_from_db()
            if db_cfg:
                merged = defaults.copy()
                for k, v in db_cfg.items():
                    if (
                        k in merged
                        and isinstance(merged[k], dict)
                        and isinstance(v, dict)
                    ):
                        merged[k] = {**merged[k], **v}
                    else:
                        merged[k] = v
                st.session_state[key] = merged
            else:
                st.session_state[key] = defaults.copy()
        except Exception:
            st.session_state[key] = defaults.copy()

    cfg = st.session_state[key]
    for k, v in defaults.items():
        if k not in cfg:
            cfg[k] = v
    cfg["display_format"] = resolve_display_format(cfg.get("display_format"))
    return cfg


def ensure_core_session_state() -> None:
    """
    Ensure core session_state keys exist early in a rerun.
    Keep semantics identical: this only forces initialization that would otherwise happen lazily.
    """
    get_global_loan_settings()
    get_system_config()

