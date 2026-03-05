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
)

try:
    from customers import (
        create_individual,
        create_corporate,
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
    from loan_management import (
        save_loan as save_loan_to_db,
        record_repayment,
        record_repayments_batch,
        get_loans_by_customer,
        get_amount_due_summary,
        allocate_repayment_waterfall,
        NeedOverpaymentDecision,
        load_system_config_from_db,
        get_loan_daily_state_balances,
    )
    _loan_management_available = True
except Exception as e:
    _loan_management_available = False
    _loan_management_error = str(e)
    NeedOverpaymentDecision = None


# --- App state & global settings (UI) ---

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


def _get_global_loan_settings() -> dict:
    """Global assumptions: interest_method, interest_type, rate_basis (no principal_input - per loan)."""
    if "global_loan_settings" not in st.session_state:
        st.session_state["global_loan_settings"] = {
            "interest_method": "Reducing balance",
            "interest_type": "Simple",
            "rate_basis": "Per month",
        }
    return st.session_state["global_loan_settings"]


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
                "send_notifications": False,
            },
        },
    }
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
    """System configurations page: Loan configurations (used by all loan modules)."""
    st.markdown(
        "<div style='background-color: #16A34A; color: white; padding: 8px 12px; font-weight: bold; font-size: 1.1rem;'>System configurations</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)
    cfg = _get_system_config()
    glob = _get_global_loan_settings()

    # Current EOD settings (can be edited in this page)
    eod_cfg = cfg.get("eod_settings", {}) or {}
    eod_mode = eod_cfg.get("mode", "manual")
    eod_time = eod_cfg.get("automatic_time", "23:00")
    eod_task_defaults = {
        "run_loan_engine": True,
        "post_accounting_events": False,
        "generate_statements": False,
        "send_notifications": False,
    }
    existing_tasks = eod_cfg.get("tasks") or {}
    eod_tasks: dict[str, bool] = {
        k: bool(existing_tasks.get(k, default)) for k, default in eod_task_defaults.items()
    }

    tab_loan, tab_currency, tab_sectors, tab_waterfall, tab_suspension, tab_eod = st.tabs(
        [
            "Loan configurations",
            "Currency configurations",
            "Sectors & subsectors",
            "Payment waterfall",
            "Suspension & curing",
            "EOD configurations",
        ]
    )

    # ---------------- Loan configurations tab ----------------
    with tab_loan:
        st.subheader("Loan configurations")
        st.caption(
            "These settings apply to all loan types (Consumer, Term, Bullet, Customised). "
            "Loan calculators and capture use these as defaults."
        )
        im_options = ["Reducing balance", "Flat rate"]
        it_options = ["Simple", "Compound"]
        rb_options = ["Per annum", "Per month"]
        interest_method = st.radio(
            "Interest method",
            im_options,
            key="syscfg_interest_method",
            index=im_options.index(glob.get("interest_method", "Reducing balance")) if glob.get("interest_method") in im_options else 0,
        )
        interest_type = st.radio(
            "Interest type",
            it_options,
            key="syscfg_interest_type",
            index=it_options.index(glob.get("interest_type", "Simple")) if glob.get("interest_type") in it_options else 0,
        )
        rate_basis = st.radio(
            "Rate basis",
            rb_options,
            key="syscfg_rate_basis",
            index=rb_options.index(glob.get("rate_basis", "Per month")) if glob.get("rate_basis") in rb_options else 1,
        )
        st.session_state["global_loan_settings"] = {
            "interest_method": interest_method,
            "interest_type": interest_type,
            "rate_basis": rate_basis,
        }
        st.divider()
        st.subheader("Compounding")
        capitalization = st.radio(
            "Capitalization of unpaid interest",
            ["No", "Yes"],
            key="syscfg_capitalization",
            index=1 if cfg.get("capitalization_of_unpaid_interest") else 0,
        )

        st.divider()
        with st.expander("Penalty interest", expanded=False):
            st.subheader("Penalty interest")
            st.caption("How penalty interest is quoted and computed.")
            penalty_quotation = st.radio(
                "Quotation of penalty interest rate",
                ["Absolute Rate", "Margin"],
                key="syscfg_penalty_quotation",
                index=0 if cfg.get("penalty_interest_quotation") == "Absolute Rate" else 1,
                help="Absolute Rate: penalty as a fixed rate. Margin: penalty as a margin above the regular interest rate.",
            )
            penalty_balance = st.radio(
                "Balance for computation of penalty interest rate",
                ["Arrears", "Balance"],
                key="syscfg_penalty_balance",
                index=0 if cfg.get("penalty_balance_basis") == "Arrears" else 1,
                help="Arrears: penalty on outstanding arrears only. Balance: penalty on total balance outstanding.",
            )
            st.caption(
                "Default penalty rates per loan type (interpreted per Quotation above: "
                "Absolute = fixed rate %; Margin = margin % above regular rate)"
            )
            pr = cfg.get("penalty_rates", {})
            p1, p2, p3, p4 = st.columns(4)
            with p1:
                penalty_consumer = st.number_input(
                    "Consumer (%)",
                    0.0,
                    100.0,
                    float(pr.get("consumer_loan", 2.0)),
                    step=0.5,
                    key="syscfg_penalty_consumer",
                )
            with p2:
                penalty_term = st.number_input(
                    "Term (%)",
                    0.0,
                    100.0,
                    float(pr.get("term_loan", 2.0)),
                    step=0.5,
                    key="syscfg_penalty_term",
                )
            with p3:
                penalty_bullet = st.number_input(
                    "Bullet (%)",
                    0.0,
                    100.0,
                    float(pr.get("bullet_loan", 2.0)),
                    step=0.5,
                    key="syscfg_penalty_bullet",
                )
            with p4:
                penalty_customised = st.number_input(
                    "Customised (%)",
                    0.0,
                    100.0,
                    float(pr.get("customised_repayments", 2.0)),
                    step=0.5,
                    key="syscfg_penalty_customised",
                )

        st.divider()
        with st.expander("Default rates & fees per loan type", expanded=False):
            st.subheader("Default rates & fees per loan type")
            st.caption("Used as defaults in Loan capture; user can override.")
            dr = cfg.get("default_rates", {})
            st.markdown("**Consumer Loan** – manage schemes (interest rate & admin fee per scheme):")
            schemes = list(cfg.get("consumer_schemes", []))
            updated_schemes = []
            for idx, sch in enumerate(schemes):
                c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
                with c1:
                    st.caption(f"Scheme: **{sch.get('name', '')}**")
                with c2:
                    sch_rate = st.number_input(
                        "Rate %",
                        0.0,
                        100.0,
                        float(sch.get("interest_rate_pct", 7.0)),
                        step=0.1,
                        key=f"syscfg_sch_rate_{idx}",
                    )
                with c3:
                    sch_admin = st.number_input(
                        "Admin %",
                        0.0,
                        100.0,
                        float(sch.get("admin_fee_pct", 5.0)),
                        step=0.1,
                        key=f"syscfg_sch_admin_{idx}",
                    )
                with c4:
                    if st.button("Remove", key=f"syscfg_sch_remove_{idx}"):
                        schemes.pop(idx)
                        st.session_state["system_config"] = {**cfg, "consumer_schemes": schemes}
                        st.rerun()
                updated_schemes.append(
                    {
                        "name": sch.get("name", ""),
                        "interest_rate_pct": sch_rate,
                        "admin_fee_pct": sch_admin,
                    }
                )
            new_sch_name = st.text_input(
                "New scheme name", key="syscfg_new_scheme", placeholder="e.g. ABC"
            )
            nsr, nsa = st.columns(2)
            with nsr:
                new_sch_rate = st.number_input(
                    "New scheme rate %",
                    0.0,
                    100.0,
                    7.0,
                    step=0.1,
                    key="syscfg_new_sch_rate",
                )
            with nsa:
                new_sch_admin = st.number_input(
                    "New scheme admin %",
                    0.0,
                    100.0,
                    5.0,
                    step=0.1,
                    key="syscfg_new_sch_admin",
                )
            if st.button("Add scheme", key="syscfg_add_scheme") and new_sch_name and new_sch_name.strip():
                name = new_sch_name.strip().upper()
                if not any(s.get("name") == name for s in schemes):
                    schemes = [
                        *updated_schemes,
                        {
                            "name": name,
                            "interest_rate_pct": new_sch_rate,
                            "admin_fee_pct": new_sch_admin,
                        },
                    ]
                    st.session_state["system_config"] = {**cfg, "consumer_schemes": schemes}
                    st.rerun()
            consumer_addl = st.number_input(
                "Consumer: default additional rate (%) for future start dates",
                0.0,
                100.0,
                float(cfg.get("consumer_default_additional_rate_pct", 0)),
                step=0.1,
                key="syscfg_consumer_addl",
            )
            cr_def = dr.get("consumer_loan", {})
            co1, co2 = st.columns(2)
            with co1:
                consumer_other_rate = st.number_input(
                    "Consumer (Other): default interest %",
                    0.0,
                    100.0,
                    float(cr_def.get("interest_pct", 7.0)),
                    step=0.1,
                    key="syscfg_consumer_other_rate",
                )
            with co2:
                consumer_other_admin = st.number_input(
                    "Consumer (Other): default admin %",
                    0.0,
                    100.0,
                    float(cr_def.get("admin_fee_pct", 5.0)),
                    step=0.1,
                    key="syscfg_consumer_other_admin",
                )

            st.markdown("**Term Loan** – default interest & fees:")
            tr = dr.get("term_loan", {})
            t1, t2, t3 = st.columns(3)
            with t1:
                term_rate = st.number_input(
                    "Term interest %",
                    0.0,
                    100.0,
                    float(tr.get("interest_pct", 7.0)),
                    step=0.1,
                    key="syscfg_term_rate",
                )
            with t2:
                term_drawdown = st.number_input(
                    "Term drawdown %",
                    0.0,
                    100.0,
                    float(tr.get("drawdown_pct", 2.5)),
                    step=0.1,
                    key="syscfg_term_drawdown",
                )
            with t3:
                term_arr = st.number_input(
                    "Term arrangement %",
                    0.0,
                    100.0,
                    float(tr.get("arrangement_pct", 2.5)),
                    step=0.1,
                    key="syscfg_term_arr",
                )

            st.markdown("**Bullet Loan** – default interest & fees:")
            br = dr.get("bullet_loan", {})
            b1, b2, b3 = st.columns(3)
            with b1:
                bullet_rate = st.number_input(
                    "Bullet interest %",
                    0.0,
                    100.0,
                    float(br.get("interest_pct", 7.0)),
                    step=0.1,
                    key="syscfg_bullet_rate",
                )
            with b2:
                bullet_drawdown = st.number_input(
                    "Bullet drawdown %",
                    0.0,
                    100.0,
                    float(br.get("drawdown_pct", 2.5)),
                    step=0.1,
                    key="syscfg_bullet_drawdown",
                )
            with b3:
                bullet_arr = st.number_input(
                    "Bullet arrangement %",
                    0.0,
                    100.0,
                    float(br.get("arrangement_pct", 2.5)),
                    step=0.1,
                    key="syscfg_bullet_arr",
                )

            st.markdown("**Customised Repayments** – default interest & fees:")
            cr = dr.get("customised_repayments", {})
            c1, c2, c3 = st.columns(3)
            with c1:
                cust_rate = st.number_input(
                    "Customised interest %",
                    0.0,
                    100.0,
                    float(cr.get("interest_pct", 7.0)),
                    step=0.1,
                    key="syscfg_cust_rate",
                )
            with c2:
                cust_drawdown = st.number_input(
                    "Customised drawdown %",
                    0.0,
                    100.0,
                    float(cr.get("drawdown_pct", 2.5)),
                    step=0.1,
                    key="syscfg_cust_drawdown",
                )
            with c3:
                cust_arr = st.number_input(
                    "Customised arrangement %",
                    0.0,
                    100.0,
                    float(cr.get("arrangement_pct", 2.5)),
                    step=0.1,
                    key="syscfg_cust_arr",
                )

    # ---------------- Currency configurations tab ----------------
    with tab_currency:
        st.subheader("Currency configurations")
        st.caption(
            "Define the base currency for the system, accepted currencies, and default currencies per loan type."
        )
        base_currency = st.text_input(
            "Base currency (ISO code)",
            value=str(cfg.get("base_currency", "USD")).upper(),
            max_chars=8,
            key="syscfg_base_currency",
        ).strip().upper() or "USD"
        accepted_default = cfg.get("accepted_currencies", [base_currency])
        accepted_csv = st.text_input(
            "Accepted currencies (comma-separated)",
            value=",".join(accepted_default),
            help="Example: USD,ZWL,ZAR. The base currency should be included.",
            key="syscfg_accepted_currencies",
        )
        accepted_list = [
            c.strip().upper() for c in accepted_csv.split(",") if c.strip()
        ] or [base_currency]
        if base_currency not in accepted_list:
            accepted_list.insert(0, base_currency)

        loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
        consumer_default_ccy = st.selectbox(
            "Default currency – Consumer loans",
            accepted_list,
            index=accepted_list.index(
                loan_curr_cfg.get("consumer_loan", base_currency)
            )
            if loan_curr_cfg.get("consumer_loan", base_currency) in accepted_list
            else 0,
            key="syscfg_ccy_consumer",
        )
        term_default_ccy = st.selectbox(
            "Default currency – Term loans",
            accepted_list,
            index=accepted_list.index(
                loan_curr_cfg.get("term_loan", base_currency)
            )
            if loan_curr_cfg.get("term_loan", base_currency) in accepted_list
            else 0,
            key="syscfg_ccy_term",
        )
        bullet_default_ccy = st.selectbox(
            "Default currency – Bullet loans",
            accepted_list,
            index=accepted_list.index(
                loan_curr_cfg.get("bullet_loan", base_currency)
            )
            if loan_curr_cfg.get("bullet_loan", base_currency) in accepted_list
            else 0,
            key="syscfg_ccy_bullet",
        )
        cust_default_ccy = st.selectbox(
            "Default currency – Customised repayments",
            accepted_list,
            index=accepted_list.index(
                loan_curr_cfg.get("customised_repayments", base_currency)
            )
            if loan_curr_cfg.get("customised_repayments", base_currency)
            in accepted_list
            else 0,
            key="syscfg_ccy_cust",
        )
        # (No penalty/loan-type rate configs here; see Loan configurations tab.)

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
        eod_tasks["send_notifications"] = st.checkbox(
            "Send notifications (e.g. SMS/email) based on EOD results",
            value=eod_tasks.get("send_notifications", False),
            key="syscfg_eod_task_notify",
        )

    # ---------------- Payment waterfall tab ----------------
    with tab_waterfall:
        st.subheader("Payment waterfall")
        st.caption("Order in which payments are allocated.")
        waterfall_options = ["Standard", "Borrower-friendly"]
        waterfall_help = (
            "**Standard:** prioritises fees and interest, then principal.\n\n"
            "**Borrower-friendly:** prioritises principal first, then interest and fees.\n\n"
            "You can configure the detailed order of buckets below."
        )
        current_wf = cfg.get("payment_waterfall", "Standard")
        wf_index = 0 if current_wf.startswith("Standard") else 1
        payment_waterfall = st.radio(
            "Active waterfall profile",
            waterfall_options,
            key="syscfg_waterfall",
            index=wf_index,
            help=waterfall_help,
        )

        buckets = cfg.get(
            "waterfall_buckets",
            [
                "fees_charges_balance",
                "penalty_interest_balance",
                "default_interest_balance",
                "interest_arrears_balance",
                "interest_accrued_balance",
                "principal_arrears",
                "principal_not_due",
            ],
        )
        profiles = cfg.get("waterfall_profiles", {})
        std_order = profiles.get("standard", buckets)
        bf_order = profiles.get("borrower_friendly", list(reversed(buckets)))

        st.caption(
            "Configure bucket order for each waterfall profile. "
            "Buckets cannot be removed here; backend changes are required to change the set."
        )
        col_std, col_bf = st.columns(2)

        # Standard profile ordering (fixed buckets, editable numeric priority per row)
        with col_std:
            st.markdown("**Standard waterfall order**")
            order_index = {name: i for i, name in enumerate(std_order)}
            std_priorities: dict[str, int] = {}
            for i, b in enumerate(buckets):
                default_prio = order_index.get(b, i)
                std_priorities[b] = st.number_input(
                    label=b,
                    min_value=1,
                    max_value=len(buckets),
                    value=default_prio + 1,
                    step=1,
                    key=f"wf_std_{b}",
                    help="Lower number = higher priority (paid earlier).",
                )
            # Sort buckets by priority, then name for stability
            std_selected = [b for b, _ in sorted(std_priorities.items(), key=lambda x: (x[1], x[0]))]

        # Borrower-friendly profile ordering
        with col_bf:
            st.markdown("**Borrower-friendly waterfall order**")
            bf_order_index = {name: i for i, name in enumerate(bf_order)}
            bf_priorities: dict[str, int] = {}
            for i, b in enumerate(buckets):
                default_prio = bf_order_index.get(b, i)
                bf_priorities[b] = st.number_input(
                    label=b,
                    min_value=1,
                    max_value=len(buckets),
                    value=default_prio + 1,
                    step=1,
                    key=f"wf_bf_{b}",
                    help="Lower number = higher priority (paid earlier).",
                )
            bf_selected = [b for b, _ in sorted(bf_priorities.items(), key=lambda x: (x[1], x[0]))]

    # ---------------- Suspension & curing tab ----------------
    with tab_suspension:
        st.subheader("Suspension & curing")
        st.caption("How suspension and curing are triggered.")
        suspension_logic = st.radio(
            "Suspension logic",
            ["Manual", "Automatic"],
            key="syscfg_suspension",
            index=0 if cfg.get("suspension_logic") == "Manual" else 1,
        )
        curing_logic = st.radio(
            "Curing logic",
            ["Curing", "Yo-Yoing"],
            key="syscfg_curing",
            index=0 if cfg.get("curing_logic") == "Curing" else 1,
            help="Curing: 3–6 month curing period. Yo-Yoing: immediate curing action.",
        )

    st.session_state["system_config"] = {
        "waterfall_buckets": buckets,
        "waterfall_profiles": {
            "standard": std_selected,
            "borrower_friendly": bf_selected,
        },
        "base_currency": base_currency,
        "accepted_currencies": accepted_list,
        "loan_default_currencies": {
            "consumer_loan": consumer_default_ccy,
            "term_loan": term_default_ccy,
            "bullet_loan": bullet_default_ccy,
            "customised_repayments": cust_default_ccy,
        },
        "penalty_interest_quotation": penalty_quotation,
        "penalty_balance_basis": penalty_balance,
        "penalty_rates": {
            "consumer_loan": penalty_consumer,
            "term_loan": penalty_term,
            "bullet_loan": penalty_bullet,
            "customised_repayments": penalty_customised,
        },
        "consumer_schemes": updated_schemes,
        "consumer_default_additional_rate_pct": consumer_addl,
        "default_rates": {
            "consumer_loan": {"interest_pct": consumer_other_rate, "admin_fee_pct": consumer_other_admin},
            "term_loan": {"interest_pct": term_rate, "drawdown_pct": term_drawdown, "arrangement_pct": term_arr},
            "bullet_loan": {"interest_pct": bullet_rate, "drawdown_pct": bullet_drawdown, "arrangement_pct": bullet_arr},
            "customised_repayments": {"interest_pct": cust_rate, "drawdown_pct": cust_drawdown, "arrangement_pct": cust_arr},
        },
        "payment_waterfall": payment_waterfall,
        "suspension_logic": suspension_logic,
        "curing_logic": curing_logic,
        "capitalization_of_unpaid_interest": capitalization == "Yes",
        # EOD settings: configured here, referenced by the End of day page.
        "eod_settings": {
            "mode": eod_mode,
            "automatic_time": eod_time,
            "tasks": {
                "run_loan_engine": True,
                "post_accounting_events": eod_tasks.get("post_accounting_events", False),
                "generate_statements": eod_tasks.get("generate_statements", False),
                "send_notifications": eod_tasks.get("send_notifications", False),
            },
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

def _render_header():
    st.markdown(
        "<h1 style='text-align: center; font-size: 2.8rem; font-weight: 900; "
        "color: #1E3A8A; letter-spacing: 0.02em; margin-bottom: 0;'>"
        "LINCOLN CAPITAL (PRIVATE) LIMITED</h1>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p style='text-align: center; font-size: 1.1rem; color: #64748B; margin-top: 0.25rem;'>"
        "Loan Management System</p>",
        unsafe_allow_html=True,
    )
    st.divider()


def eod_ui():
    """End-of-day processing configuration and manual run."""
    from eod import run_eod_for_date

    st.markdown(
        "<div style='background-color: #16A34A; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>End of day</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    cfg = _get_system_config()
    eod_cfg = cfg.get("eod_settings", {}) or {}
    mode = eod_cfg.get("mode", "manual")
    automatic_time = eod_cfg.get("automatic_time", "23:00")

    st.caption(
        f"Current EOD mode: **{mode.upper()}**"
        + (f" (scheduled around {automatic_time})" if mode == "automatic" else "")
        + ". Configure this under **System configurations → Loan configurations → End of day (EOD) settings**."
    )

    st.divider()
    if mode == "manual":
        st.subheader("Manual EOD run")
        st.caption(
            "Runs EOD for the selected calendar date. This computes interest, updates loan buckets "
            "into `loan_daily_state`, and prepares data for accounting events that depend on EOD."
        )
        as_of = st.date_input("EOD as-of date", datetime.today().date())
        if st.button("Run EOD now", type="primary", key="eod_run_now"):
            try:
                result = run_eod_for_date(as_of)
                duration = result.finished_at - result.started_at
                st.success(
                    f"EOD completed for {result.as_of_date.isoformat()} – "
                    f"processed {result.loans_processed} loans in {duration.total_seconds():.2f} seconds."
                )
            except Exception as e:
                st.error(f"EOD run failed: {e}")
    else:
        st.subheader("Manual EOD run")
        st.info(
            "EOD is configured for **automatic** mode. Manual runs are disabled here to avoid "
            "conflicts with the external scheduler. Use your scheduling/ops tooling to trigger EOD."
        )


def consumer_loan_ui():
    schemes = _get_consumer_schemes()
    scheme_names = [s["name"] for s in schemes]
    cfg = _get_system_config()
    default_additional_rate_pct = cfg.get("consumer_default_additional_rate_pct", 0.0)

    st.sidebar.header("Consumer Loan Parameters")
    # Currency selection with system default + override
    accepted_currencies = cfg.get(
        "accepted_currencies", [cfg.get("base_currency", "USD")]
    )
    loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
    default_ccy = loan_curr_cfg.get("consumer_loan", cfg.get("base_currency", "USD"))
    if default_ccy not in accepted_currencies:
        accepted_currencies = [default_ccy, *accepted_currencies]
    currency = st.sidebar.selectbox(
        "Currency",
        accepted_currencies,
        index=accepted_currencies.index(default_ccy)
        if default_ccy in accepted_currencies
        else 0,
        key="cl_currency",
    )
    st.caption("Schemes and default rates are managed in **System configurations**.")
    scheme_options = scheme_names + ["Other"]
    scheme = st.sidebar.selectbox("Loan Scheme", scheme_options, key="cl_scheme")
    glob = _get_global_loan_settings()
    principal_input_choice = st.sidebar.radio(
        "What are you entering?",
        ["Net proceeds", "Principal (total loan amount)"],
        key="cl_principal_input",
    )
    input_total_facility = principal_input_choice == "Principal (total loan amount)"
    loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
    loan_required = st.sidebar.number_input(
        loan_input_label,
        min_value=0.0,
        value=140.0,
        step=10.0,
        format="%.2f",
        key="cl_principal",
    )
    loan_term = st.sidebar.number_input(
        "Term (Months)",
        min_value=1,
        max_value=60,
        value=6,
        step=1,
        key="cl_term",
    )
    start_date_input = st.sidebar.date_input("Start Date", datetime.today().date(), key="cl_start")
    start_date = datetime.combine(start_date_input, datetime.min.time())

    # Future start date: prompt for additional rate when start_date > next month
    today_normalized = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    next_month_limit = add_months(today_normalized, 1)
    additional_buffer_rate = 0.0

    if start_date > next_month_limit:
        st.sidebar.warning("Future date detected: additional interest rate applies per extra month.")
        additional_rate_pct = st.sidebar.number_input(
            "Additional Monthly Rate (%) per extra month",
            min_value=0.0,
            max_value=100.0,
            value=float(default_additional_rate_pct),
            step=0.1,
            help="Rate applied for each month the start date is beyond next month (0 is acceptable).",
            key="cl_add_rate",
        )
        months_excess = max(
            0,
            (start_date.year - next_month_limit.year) * 12
            + (start_date.month - next_month_limit.month),
        )
        additional_buffer_rate = (additional_rate_pct / 100.0) * months_excess

    # Base rates: from selected scheme or manual entry for Other
    if scheme != "Other":
        sch = next((s for s in schemes if s["name"] == scheme), None)
        base_rate = (sch["interest_rate_pct"] / 100.0) if sch else 0.07
        admin_fee = (sch["admin_fee_pct"] / 100.0) if sch else 0.07
    else:
        interest_rate_percent = st.sidebar.number_input(
            "Interest rate (%)",
            min_value=0.0,
            max_value=100.0,
            value=0.0,
            step=0.1,
            key="cl_other_rate",
        )
        admin_fee_percent = st.sidebar.number_input(
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
    details, df_schedule = compute_consumer_schedule(
        loan_required, loan_term, start_date, base_rate, admin_fee, input_total_facility,
        glob.get("rate_basis", "Per month"), flat_rate, scheme=scheme,
        additional_monthly_rate=additional_buffer_rate,
    )
    details["currency"] = currency
    total_facility = details["facility"]
    amount_required_display = details["principal"]
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
    st.markdown(f"**g. Disbursement Date:** {start_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**h. Loan Term (months):** {loan_term}")
    st.markdown(f"**j. First Repayment Date:** {first_repayment_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**k. No. of Repayments:** {loan_term} times")
    st.markdown(f"**i. End Date:** {end_date.strftime('%d-%b-%Y')}")

    # Notes section
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        "<div style='background-color: #F1F5F9; padding: 12px 16px; border-radius: 4px;'>"
        "<strong>Notes</strong><br>"
        "1. Select Scheme (a.). If the loan does not fall under a Scheme, select \"Other\"<br>"
        "2. Enter net proceeds in (b) or principal (total loan amount) via the sidebar<br>"
        "3. If you have selected \"Other\" under the Scheme, manually enter the interest rate (c.) and administration fees % (d.)<br>"
        "4. Enter the Loan Term in months (h.)<br>"
        "5. Monthly repayment (f.) assumes every month has 30 days<br>"
        "6. Default rates and schemes are in **System configurations**"
        "</div>",
        unsafe_allow_html=True,
    )

    # 5. Amortization Schedule
    st.divider()
    st.subheader("Repayment Schedule")
    st.dataframe(format_schedule_display(df_schedule), use_container_width=True, hide_index=True)

    # 6. Save button - DB-ready structure (from shared engine)
    loan_record = {**details, "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
    for k in ("start_date", "end_date", "first_repayment_date"):
        if k in loan_record and hasattr(loan_record[k], "isoformat"):
            loan_record[k] = loan_record[k].isoformat()

    st.divider()
    if st.button("Save Loan Record to System", type="primary", key="cl_save"):
        # TODO: Replace with db.insert(loan_record) when DB is ready
        st.success(f"Loan for ${loan_required:,.2f} has been prepared for database sync.")
        with st.expander("Preview record (for DB insertion)"):
            st.json(loan_record)


def term_loan_ui():
    glob = _get_global_loan_settings()
    cfg = _get_system_config()
    st.sidebar.header("Term Loan Parameters")
    # Currency selection with system default + override
    accepted_currencies = cfg.get(
        "accepted_currencies", [cfg.get("base_currency", "USD")]
    )
    loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
    default_ccy = loan_curr_cfg.get("term_loan", cfg.get("base_currency", "USD"))
    if default_ccy not in accepted_currencies:
        accepted_currencies = [default_ccy, *accepted_currencies]
    currency = st.sidebar.selectbox(
        "Currency",
        accepted_currencies,
        index=accepted_currencies.index(default_ccy)
        if default_ccy in accepted_currencies
        else 0,
        key="term_currency",
    )
    principal_input_choice = st.sidebar.radio(
        "What are you entering?",
        ["Net proceeds", "Principal (total loan amount)"],
        key="term_principal_input",
    )
    input_total_facility = principal_input_choice == "Principal (total loan amount)"
    loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
    loan_required = st.sidebar.number_input(
        loan_input_label,
        min_value=0.0,
        value=1000.0,
        step=100.0,
        format="%.2f",
        key="term_principal",
    )
    loan_term = st.sidebar.number_input(
        "Term (Months)",
        min_value=1,
        max_value=120,
        value=24,
        step=1,
        key="term_months",
    )
    disbursement_input = st.sidebar.date_input("Disbursement Date", datetime.today().date(), key="term_disb")
    disbursement_date = datetime.combine(disbursement_input, datetime.min.time())

    # Term loan: defaults from System configurations, user can override
    dr = cfg.get("default_rates", {}).get("term_loan", {})
    rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
    rate_pct = st.sidebar.number_input(rate_label, 0.0, 100.0, float(dr.get("interest_pct", 7.0)), step=0.1, key="term_rate")
    drawdown_fee_pct = st.sidebar.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="term_drawdown") / 100.0
    arrangement_fee_pct = st.sidebar.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct", 2.5)), step=0.1, key="term_arrangement") / 100.0
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
    annual_rate = (rate_pct / 100.0) * 12.0 if glob.get("rate_basis") == "Per month" else (rate_pct / 100.0)
    flat_rate = glob.get("interest_method") == "Flat rate"

    # Grace period
    st.sidebar.subheader("Grace Period")
    grace_type = st.sidebar.radio(
        "Grace period type",
        ["No grace period", "Principal moratorium", "Principal and interest moratorium"],
        key="term_grace",
    )
    moratorium_months = 0
    if "Principal moratorium" in grace_type:
        moratorium_months = st.sidebar.number_input("Moratorium length (months)", 1, 60, 3, key="term_moratorium_p")
    elif "Principal and interest" in grace_type:
        moratorium_months = st.sidebar.number_input("Moratorium length (months)", 1, 60, 3, key="term_moratorium_pi")

    # First repayment date (default: 1 month after disbursement)
    default_first_rep = add_months(disbursement_date, 1).date()
    first_rep_input = st.sidebar.date_input("First Repayment Date", default_first_rep, key="term_first_rep")
    first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())

    today_norm = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    next_month_limit = add_months(today_norm, 1)

    if grace_type == "No grace period" and first_repayment_date > next_month_limit:
        st.error("No grace period: First Repayment Date must not be greater than next month.")
        return

    if "Principal" in grace_type and moratorium_months >= loan_term:
        st.error("Moratorium length must be less than loan term.")
        return

    # Repayment timing
    st.sidebar.subheader("Repayment Timing")
    use_anniversary = st.sidebar.radio(
        "Repayments on",
        ["Anniversary date (same day each month)", "Last day of each month"],
        key="term_timing",
    ).startswith("Anniversary")

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
        glob.get("rate_basis", "Per month"), flat_rate,
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

    st.markdown(f"**a. Net proceeds:** {loan_required:,.2f} US Dollars")
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
    st.markdown(f"**d. Principal (total loan amount):** {details['facility']:,.2f} US Dollars")
    st.markdown(
        f"<span class='calc-value-red'><strong>e. Installment (from first P&I period):</strong> US${installment:,.2f}</span>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**f. Disbursement Date:** {disbursement_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**g. Loan Term (months):** {loan_term}")
    st.markdown(f"**h. First Repayment Date:** {first_repayment_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**i. Grace period:** {grace_type}")
    st.markdown(f"**j. End Date:** {end_date.strftime('%d-%b-%Y')}")

    st.divider()
    st.subheader("Repayment Schedule (Actual/360)")
    st.dataframe(format_schedule_display(df_schedule), use_container_width=True, hide_index=True)

    loan_record = {**details, "loan_type": "term_loan", "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
    for k in ("disbursement_date", "start_date", "end_date", "first_repayment_date"):
        if k in loan_record and hasattr(loan_record[k], "isoformat"):
            loan_record[k] = loan_record[k].isoformat()

    st.divider()
    if st.button("Save Term Loan Record to System", type="primary", key="term_save"):
        st.success(f"Term loan for ${loan_required:,.2f} has been prepared for database sync.")
        with st.expander("Preview record (for DB insertion)"):
            st.json(loan_record)


def bullet_loan_ui():
    glob = _get_global_loan_settings()
    cfg = _get_system_config()
    st.sidebar.header("Bullet Loan Parameters")
    # Currency selection with system default + override
    accepted_currencies = cfg.get(
        "accepted_currencies", [cfg.get("base_currency", "USD")]
    )
    loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
    default_ccy = loan_curr_cfg.get("bullet_loan", cfg.get("base_currency", "USD"))
    if default_ccy not in accepted_currencies:
        accepted_currencies = [default_ccy, *accepted_currencies]
    currency = st.sidebar.selectbox(
        "Currency",
        accepted_currencies,
        index=accepted_currencies.index(default_ccy)
        if default_ccy in accepted_currencies
        else 0,
        key="bullet_currency",
    )
    principal_input_choice = st.sidebar.radio(
        "What are you entering?",
        ["Net proceeds", "Principal (total loan amount)"],
        key="bullet_principal_input",
    )
    input_total_facility = principal_input_choice == "Principal (total loan amount)"
    bullet_type = st.sidebar.radio(
        "Bullet type",
        ["Straight bullet (no interim payments)", "Bullet with interest payments"],
        key="bullet_type",
    )
    loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
    loan_required = st.sidebar.number_input(
        loan_input_label,
        min_value=0.0,
        value=1000.0,
        step=100.0,
        format="%.2f",
        key="bullet_principal",
    )
    loan_term = st.sidebar.number_input(
        "Term (Months)",
        min_value=1,
        max_value=120,
        value=12,
        step=1,
        key="bullet_term",
    )
    disbursement_input = st.sidebar.date_input("Disbursement Date", datetime.today().date(), key="bullet_disb")
    disbursement_date = datetime.combine(disbursement_input, datetime.min.time())

    dr = cfg.get("default_rates", {}).get("bullet_loan", {})
    rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
    rate_pct = st.sidebar.number_input(rate_label, min_value=0.0, max_value=100.0, value=float(dr.get("interest_pct", 7.0)), step=0.1, key="bullet_rate")
    drawdown_fee_pct = st.sidebar.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="bullet_drawdown") / 100.0
    arrangement_fee_pct = st.sidebar.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct", 2.5)), step=0.1, key="bullet_arrangement") / 100.0
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
        default_first_rep = add_months(disbursement_date, 1).date()
        first_rep_input = st.sidebar.date_input("First Repayment Date", default_first_rep, key="bullet_first_rep")
        first_repayment_date = datetime.combine(first_rep_input, datetime.min.time())
        use_anniversary = st.sidebar.radio(
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

    st.markdown(f"**a. Net proceeds:** {loan_required:,.2f} US Dollars")
    st.markdown(f"**b. Interest Rate (annual, Actual/360):** {details['annual_rate'] * 100:.2f}%")
    st.markdown("<p class='calc-desc'>Interest on actual days / 360 basis</p>", unsafe_allow_html=True)
    st.markdown(f"**c. Drawdown fee (%):** {drawdown_fee_pct * 100:.2f}% | **Arrangement fee (%):** {arrangement_fee_pct * 100:.2f}%")
    st.markdown(f"<p class='calc-desc'>Total {total_fee * 100:.2f}% once-off on total facility</p>", unsafe_allow_html=True)
    st.markdown(f"**d. Principal (total loan amount):** {details['facility']:,.2f} US Dollars")
    st.markdown(
        f"<span class='calc-value-red'><strong>e. Total payment at maturity:</strong> US${details['total_payment']:,.2f}</span>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**f. Disbursement Date:** {disbursement_date.strftime('%d-%b-%Y')}")
    st.markdown(f"**g. Term (months):** {loan_term}")
    st.markdown(f"**h. Maturity Date:** {details['maturity_date'].strftime('%d-%b-%Y')}")
    if details.get("first_repayment_date") is not None:
        st.markdown(f"**i. First interest payment:** {details['first_repayment_date'].strftime('%d-%b-%Y')}")

    st.divider()
    st.subheader("Repayment Schedule (Actual/360)")
    st.dataframe(format_schedule_display(df_schedule), use_container_width=True, hide_index=True)

    loan_record = {**details, "loan_type": "bullet_loan", "timestamp": datetime.now().isoformat(), "amortization_schedule": df_schedule.to_dict(orient="records")}
    for k in ("disbursement_date", "maturity_date", "first_repayment_date"):
        if k in loan_record and loan_record[k] is not None and hasattr(loan_record[k], "isoformat"):
            loan_record[k] = loan_record[k].isoformat()

    st.divider()
    if st.button("Save Bullet Loan Record to System", type="primary", key="bullet_save"):
        st.success(f"Bullet loan for ${loan_required:,.2f} has been prepared for database sync.")
        with st.expander("Preview record (for DB insertion)"):
            st.json(loan_record)


def customised_repayments_ui():
    glob = _get_global_loan_settings()
    cfg = _get_system_config()
    flat_rate = glob.get("interest_method") == "Flat rate"

    st.sidebar.header("Customised Repayments Parameters")
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
    currency = st.sidebar.selectbox(
        "Currency",
        accepted_currencies,
        index=accepted_currencies.index(default_ccy)
        if default_ccy in accepted_currencies
        else 0,
        key="cust_currency",
    )
    principal_input_choice = st.sidebar.radio(
        "What are you entering?",
        ["Net proceeds", "Principal (total loan amount)"],
        key="cust_principal_input",
    )
    input_total_facility = principal_input_choice == "Principal (total loan amount)"
    loan_input_label = "Principal (total loan amount)" if input_total_facility else "Net proceeds"
    loan_required = st.sidebar.number_input(
        loan_input_label,
        min_value=0.0,
        value=1000.0,
        step=100.0,
        format="%.2f",
        key="cust_principal",
    )
    loan_term = st.sidebar.number_input(
        "Term (Months)",
        min_value=1,
        max_value=120,
        value=12,
        step=1,
        key="cust_term",
    )
    start_input = st.sidebar.date_input("Start Date", datetime.today().date(), key="cust_start")
    start_date = datetime.combine(start_input, datetime.min.time())
    irregular_calc = st.sidebar.checkbox("Irregular", value=False, key="cust_irregular", help="Allow editing dates and adding rows; schedule recomputes from table.")
    use_anniversary = st.sidebar.radio(
        "Repayments on",
        ["Anniversary date (same day each month)", "Last day of each month"],
        key="cust_timing",
    ).startswith("Anniversary")
    default_first_rep = add_months(start_date, 1).date()
    if not use_anniversary:
        default_first_rep = default_first_rep.replace(day=days_in_month(default_first_rep.year, default_first_rep.month))
    existing_cust = st.session_state.get("customised_repayments_df")
    first_rep_calc = _first_repayment_from_customised_table(existing_cust) if existing_cust is not None and len(existing_cust) > 1 else None
    first_rep_display_calc = (first_rep_calc.date() if first_rep_calc else default_first_rep)
    st.sidebar.date_input("First repayment date (from table)", first_rep_display_calc, key="cust_first_rep", disabled=True, help="From first row with non-zero payment.")
    first_repayment_date = datetime.combine(first_rep_display_calc, datetime.min.time())
    dr = cfg.get("default_rates", {}).get("customised_repayments", {})
    rate_label = "Interest rate (% per annum)" if glob.get("rate_basis") == "Per annum" else "Interest rate (% per month)"
    rate_pct = st.sidebar.number_input(rate_label, 0.0, 100.0, float(dr.get("interest_pct", 7.0)), step=0.1, key="cust_rate")
    drawdown_fee_pct = st.sidebar.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="cust_drawdown") / 100.0
    arrangement_fee_pct = st.sidebar.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct", 2.5)), step=0.1, key="cust_arrangement") / 100.0
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
    params_key = (round(total_facility, 2), loan_term, start_date.strftime("%Y-%m-%d"), irregular_calc)
    if session_key not in st.session_state or st.session_state.get("customised_params") != params_key:
        st.session_state["customised_params"] = params_key
        schedule_dates_init = repayment_dates(start_date, first_repayment_date, int(loan_term), use_anniversary)
        rows = [{"Period": 0, "Date": start_date.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": round(total_facility, 2), "Total Outstanding": round(total_facility, 2)}]
        for i, dt in enumerate(schedule_dates_init, 1):
            rows.append({"Period": i, "Date": dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0})
        st.session_state[session_key] = pd.DataFrame(rows)

    df = st.session_state[session_key].copy()
    schedule_dates = parse_schedule_dates_from_table(df, start_date=start_date)
    df = recompute_customised_from_payments(df, total_facility, schedule_dates, annual_rate, flat_rate, start_date)
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
                    last_dt = add_months(start_date, len(last_df))
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
        use_container_width=True,
        hide_index=True,
        key="cust_editor",
    )
    if not edited.equals(df):
        schedule_dates_edit = parse_schedule_dates_from_table(edited, start_date=start_date)
        df_updated = recompute_customised_from_payments(edited, total_facility, schedule_dates_edit, annual_rate, flat_rate, start_date)
        st.session_state[session_key] = df_updated
        st.rerun()

    final_total_outstanding = float(df.at[len(df) - 1, "Total Outstanding"]) if len(df) > 1 and "Total Outstanding" in df.columns else total_facility
    if abs(final_total_outstanding) < 0.01:
        st.success("Loan cleared. You may save this record.")
    else:
        st.warning(f"Total outstanding at end: **${final_total_outstanding:,.2f}**. Adjust payments so Total Outstanding is $0 to save.")

    can_save = abs(final_total_outstanding) < 0.01
    if st.button("Save Customised Repayments to System", type="primary", key="cust_save", disabled=not can_save):
        if can_save:
            st.success(f"Customised loan for ${loan_required:,.2f} has been prepared for database sync.")
            with st.expander("Preview record (for DB insertion)"):
                st.json({
                    "loan_type": "customised_repayments",
                    "timestamp": datetime.now().isoformat(),
                    "principal": float(loan_required),
                    "facility": float(total_facility),
                    "term": int(loan_term),
                    "annual_rate": float(annual_rate),
                    "drawdown_fee": float(drawdown_fee_pct),
                    "arrangement_fee": float(arrangement_fee_pct),
                    "start_date": start_date.isoformat(),
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
) -> tuple[dict, pd.DataFrame]:
    """Compute consumer loan schedule. Returns (details dict for DB, schedule DataFrame)."""
    if input_total_facility:
        total_facility = loan_required
        amount_display = total_facility * (1.0 - admin_fee)
    else:
        total_facility = loan_required / (1.0 - admin_fee)
        amount_display = loan_required
    base_monthly = (base_rate / 12.0) if rate_basis == "Per annum" else base_rate
    total_monthly_rate = base_monthly + additional_monthly_rate
    monthly_installment = float(npf.pmt(total_monthly_rate, loan_term, -total_facility))
    end_date = add_months(start_date, loan_term) - timedelta(days=1)
    first_rep = add_months(start_date, 1)
    df_schedule = get_amortization_schedule(
        total_facility, total_monthly_rate, int(loan_term), start_date, monthly_installment, flat_rate=flat_rate
    )
    details = {
        "facility": total_facility, "principal": amount_display, "term": loan_term,
        "monthly_rate": total_monthly_rate, "admin_fee": admin_fee, "scheme": scheme,
        "start_date": start_date, "end_date": end_date, "first_repayment_date": first_rep,
        "installment": monthly_installment, "payment_timing": "anniversary",
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
    if input_total_facility:
        total_facility = loan_required
    else:
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
        "facility": total_facility, "principal": loan_required, "term": loan_term,
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
    if input_total_facility:
        total_facility = loan_required
    else:
        total_facility = loan_required / (1.0 - total_fee)
    annual_rate = (rate_pct / 100.0) * 12.0 if rate_basis == "Per month" else (rate_pct / 100.0)
    maturity_date = add_months(disbursement_date, loan_term)
    schedule_dates = None
    if first_repayment_date is not None and "with interest" in bullet_type.lower():
        schedule_dates = repayment_dates(disbursement_date, first_repayment_date, int(loan_term), use_anniversary)
        maturity_date = schedule_dates[-1] if schedule_dates else maturity_date
    df_schedule = get_bullet_schedule(
        total_facility, annual_rate, disbursement_date, maturity_date,
        "straight" if "Straight" in bullet_type else "with_interest",
        schedule_dates, flat_rate=flat_rate,
    )
    total_payment = float(df_schedule["Payment"].sum())
    details = {
        "facility": total_facility, "principal": loan_required, "term": loan_term,
        "annual_rate": annual_rate, "drawdown_fee": drawdown_fee_pct, "arrangement_fee": arrangement_fee_pct,
        "disbursement_date": disbursement_date, "maturity_date": maturity_date,
        "total_payment": total_payment, "bullet_type": "straight" if "Straight" in bullet_type else "with_interest",
        "first_repayment_date": first_repayment_date, "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
    }
    return details, df_schedule


def capture_loan_ui():
    """Capture loan flow: select customer + type → compute schedule → save to DB."""
    if not _customers_available:
        st.error("Customer module is required for Capture Loan. Check database connection.")
        return
    if not _loan_management_available:
        st.error(f"Loan management module is not available. ({_loan_management_error})")
        return

    st.markdown(
        "<div style='background-color: #1E3A8A; color: white; padding: 8px 12px; font-weight: bold; font-size: 1.1rem;'>Capture Loan</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    t1, t2, t3 = st.tabs(["1. Customer & loan type", "2. Compute schedule", "3. Review & save"])

    with t1:
        st.subheader("Select customer and loan type")
        customers_list = list_customers(status="active") or []
        if not customers_list:
            st.warning("No active customers. Add a customer first under **Customers**.")
        else:
            options = [(c["id"], get_display_name(c["id"]) or f"Customer #{c['id']}") for c in customers_list]
            choice = st.selectbox(
                "Customer",
                range(len(options)),
                format_func=lambda i: options[i][1],
                key="cap_customer_sel",
            )
            if choice is not None:
                st.session_state["capture_customer_id"] = options[choice][0]
            loan_type = st.selectbox(
                "Loan type",
                ["Consumer Loan", "Term Loan", "Bullet Loan", "Customised Repayments"],
                key="cap_loan_type",
            )
            st.session_state["capture_loan_type"] = loan_type
            st.success("Proceed to **2. Compute schedule** to enter loan parameters and generate the schedule.")
        # Clear selection: reset entire capture flow
        if st.button("Clear selection", key="cap_clear_t1"):
            for k in list(st.session_state.keys()):
                if k.startswith("capture_"):
                    st.session_state.pop(k, None)
            st.rerun()

    with t2:
        st.subheader("Compute schedule")
        cid = st.session_state.get("capture_customer_id")
        ltype = st.session_state.get("capture_loan_type")
        if not cid or not ltype:
            st.info("Complete **1. Customer & loan type** first.")
        else:
            # Clear saved schedule only (keeps customer/loan type; user can recompute)
            if st.session_state.get("capture_loan_details") is not None or st.session_state.get("capture_loan_schedule_df") is not None:
                if st.button("Clear saved schedule", key="cap_clear_t2"):
                    st.session_state.pop("capture_loan_details", None)
                    st.session_state.pop("capture_loan_schedule_df", None)
                    st.rerun()
            if _agents_available:
                try:
                    agents_list_cap = list_agents(status="active") or []
                except Exception:
                    agents_list_cap = []
                agent_labels_cap = ["(None)"] + [a["name"] for a in agents_list_cap]
                agent_ids_cap = [None] + [a["id"] for a in agents_list_cap]
                sel_agent_label = st.selectbox("Agent", agent_labels_cap, key="capture_agent_sel")
                sel_agent_id = agent_ids_cap[agent_labels_cap.index(sel_agent_label)] if sel_agent_label else None
                st.session_state["capture_agent_id"] = sel_agent_id
            else:
                st.session_state["capture_agent_id"] = None
            glob = _get_global_loan_settings()
            flat_rate = glob.get("interest_method") == "Flat rate"
            payment_timing_anniversary = True  # will set from form

            if ltype == "Consumer Loan":
                cfg = _get_system_config()
                schemes = _get_consumer_schemes()
                scheme_names = [s["name"] for s in schemes]
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("consumer_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]

                cl_col1, cl_col2 = st.columns(2)
                with cl_col1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_cl_currency",
                    )
                    scheme = st.selectbox("Scheme", scheme_names + ["Other"], key="cap_cl_scheme")
                    principal_input = st.radio(
                        "What are you entering?",
                        ["Net proceeds", "Principal (total loan amount)"],
                        key="cap_cl_principal_input",
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
                    start_date = datetime.combine(
                        st.date_input("Start date", datetime.today().date(), key="cap_cl_start"),
                        datetime.min.time(),
                    )
                if scheme != "Other":
                    sch = next((s for s in schemes if s["name"] == scheme), None)
                    base_rate = (sch["interest_rate_pct"] / 100.0) if sch else 0.07
                    admin_fee = (sch["admin_fee_pct"] / 100.0) if sch else 0.07
                else:
                    def_rates = cfg.get("default_rates", {}).get("consumer_loan", {"interest_pct": 7.0, "admin_fee_pct": 5.0})
                    rate_col1, rate_col2 = st.columns(2)
                    with rate_col1:
                        base_rate = (
                            st.number_input(
                                "Interest rate (%)",
                                0.0,
                                100.0,
                                float(def_rates.get("interest_pct", 7.0)),
                                step=0.1,
                                key="cap_cl_rate",
                            )
                            / 100.0
                        )
                    with rate_col2:
                        admin_fee = (
                            st.number_input(
                                "Admin fee (%)",
                                0.0,
                                100.0,
                                float(def_rates.get("admin_fee_pct", 5.0)),
                                step=0.1,
                                key="cap_cl_admin",
                            )
                            / 100.0
                        )
                def_penalty = cfg.get("penalty_rates", {}).get("consumer_loan", 2.0)
                pen_col1, _ = st.columns([1, 1])
                with pen_col1:
                    penalty_pct = st.number_input(
                        "Penalty interest (%) – override default",
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_cl_penalty",
                        help="Interpreted per System config: Absolute = fixed rate; Margin = above regular rate",
                    )
                details, df_schedule = compute_consumer_schedule(
                    loan_required, loan_term, start_date, base_rate, admin_fee, input_tf,
                    glob.get("rate_basis", "Per month"), flat_rate, scheme=scheme,
                )
                details["currency"] = currency
                details["penalty_rate_pct"] = penalty_pct
                details["penalty_quotation"] = cfg.get("penalty_interest_quotation", "Absolute Rate")
                st.dataframe(format_schedule_display(df_schedule), use_container_width=True, hide_index=True)
                if st.button("Use this schedule", key="cap_cl_use"):
                    st.session_state["capture_loan_details"] = details
                    st.session_state["capture_loan_schedule_df"] = df_schedule
                    st.success("Schedule saved. Go to **3. Review & save**.")
                    st.rerun()

            elif ltype == "Term Loan":
                cfg = _get_system_config()
                dr = cfg.get("default_rates", {}).get("term_loan", {})
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("term_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]

                tcol1, tcol2 = st.columns(2)
                with tcol1:
                    currency = st.selectbox(
                        "Currency",
                        accepted_currencies,
                        index=accepted_currencies.index(default_ccy)
                        if default_ccy in accepted_currencies
                        else 0,
                        key="cap_term_currency",
                    )
                    principal_input = st.radio(
                        "What are you entering?",
                        ["Net proceeds", "Principal (total loan amount)"],
                        key="cap_term_principal_input",
                    )
                    input_tf = principal_input == "Principal (total loan amount)"
                    loan_required = st.number_input(
                        "Loan amount",
                        min_value=0.0,
                        value=1000.0,
                        step=100.0,
                        format="%.2f",
                        key="cap_term_principal",
                    )
                    loan_term = st.number_input("Term (months)", 1, 120, 24, key="cap_term_months")
                with tcol2:
                    disbursement_date = datetime.combine(
                        st.date_input("Disbursement date", datetime.today().date(), key="cap_term_disb"),
                        datetime.min.time(),
                    )
                    rate_pct = st.number_input(
                        "Interest rate (%)",
                        0.0,
                        100.0,
                        float(dr.get("interest_pct", 7.0)),
                        step=0.1,
                        key="cap_term_rate",
                    )
                    drawdown_pct = (
                        st.number_input(
                            "Drawdown fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("drawdown_pct", 2.5)),
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
                            float(dr.get("arrangement_pct", 2.5)),
                            step=0.1,
                            key="cap_term_arrangement",
                        )
                        / 100.0
                    )
                def_penalty = cfg.get("penalty_rates", {}).get("term_loan", 2.0)
                tpen1, tpen2 = st.columns(2)
                with tpen1:
                    penalty_pct = st.number_input(
                        "Penalty interest (%) – override default",
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_term_penalty",
                        help="Interpreted per System config",
                    )
                with tpen2:
                    grace_type = st.radio(
                        "Grace period",
                        ["No grace period", "Principal moratorium", "Principal and interest moratorium"],
                        key="cap_term_grace",
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
                df_col1, df_col2 = st.columns(2)
                default_first = add_months(disbursement_date, 1).date()
                with df_col1:
                    first_rep = datetime.combine(
                        st.date_input("First repayment date", default_first, key="cap_term_first_rep"),
                        datetime.min.time(),
                    )
                with df_col2:
                    use_anniversary = st.radio(
                        "Repayments on",
                        ["Anniversary date", "Last day of month"],
                        key="cap_term_timing",
                    ).startswith("Anniversary")
                if not use_anniversary and not is_last_day_of_month(first_rep):
                    st.error("When repayments are on last day of month, first repayment date must be the last day of that month.")
                else:
                    details, df_schedule = compute_term_schedule(
                        loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                        input_tf, grace_type, moratorium_months, first_rep, use_anniversary,
                        glob.get("rate_basis", "Per month"), flat_rate,
                    )
                    details["currency"] = currency
                    details["penalty_rate_pct"] = penalty_pct
                    details["penalty_quotation"] = cfg.get("penalty_interest_quotation", "Absolute Rate")
                    st.dataframe(format_schedule_display(df_schedule), use_container_width=True, hide_index=True)
                    if st.button("Use this schedule", key="cap_term_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_schedule
                        st.success("Schedule saved. Go to **3. Review & save**.")
                        st.rerun()

            elif ltype == "Bullet Loan":
                cfg = _get_system_config()
                dr = cfg.get("default_rates", {}).get("bullet_loan", {})
                accepted_currencies = cfg.get("accepted_currencies", [cfg.get("base_currency", "USD")])
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get("bullet_loan", cfg.get("base_currency", "USD"))
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]
                bcol1, bcol2 = st.columns(2)
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
                    )
                    principal_input = st.radio(
                        "What are you entering?",
                        ["Net proceeds", "Principal (total loan amount)"],
                        key="cap_bullet_principal_input",
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
                        st.date_input("Disbursement date", datetime.today().date(), key="cap_bullet_disb"),
                        datetime.min.time(),
                    )
                    rate_pct = st.number_input(
                        "Interest rate (%)",
                        0.0,
                        100.0,
                        float(dr.get("interest_pct", 7.0)),
                        step=0.1,
                        key="cap_bullet_rate",
                    )
                    drawdown_pct = (
                        st.number_input(
                            "Drawdown fee (%)",
                            0.0,
                            100.0,
                            float(dr.get("drawdown_pct", 2.5)),
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
                            float(dr.get("arrangement_pct", 2.5)),
                            step=0.1,
                            key="cap_bullet_arrangement",
                        )
                        / 100.0
                    )
                def_penalty = cfg.get("penalty_rates", {}).get("bullet_loan", 2.0)
                bpen1, _ = st.columns([1, 1])
                with bpen1:
                    penalty_pct = st.number_input(
                        "Penalty interest (%) – override default",
                        0.0,
                        100.0,
                        float(def_penalty),
                        step=0.5,
                        key="cap_bullet_penalty",
                        help="Interpreted per System config",
                    )
                first_rep = None
                use_anniversary = True
                if "with interest" in bullet_type:
                    default_first = add_months(disbursement_date, 1).date()
                    first_rep = datetime.combine(st.date_input("First repayment date", default_first, key="cap_bullet_first_rep"), datetime.min.time())
                    use_anniversary = st.radio("Interest payments on", ["Anniversary date", "Last day of month"], key="cap_bullet_timing").startswith("Anniversary")
                    if not use_anniversary and not is_last_day_of_month(first_rep):
                        st.error("First repayment date must be last day of month when using last day of month.")
                    else:
                        details, df_schedule = compute_bullet_schedule(
                            loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                            input_tf, bullet_type, first_rep, use_anniversary, glob.get("rate_basis", "Per month"), flat_rate,
                        )
                        details["currency"] = currency
                        details["penalty_rate_pct"] = penalty_pct
                        details["penalty_quotation"] = cfg.get("penalty_interest_quotation", "Absolute Rate")
                        st.dataframe(format_schedule_display(df_schedule), use_container_width=True, hide_index=True)
                        if st.button("Use this schedule", key="cap_bullet_use"):
                            st.session_state["capture_loan_details"] = details
                            st.session_state["capture_loan_schedule_df"] = df_schedule
                            st.success("Schedule saved. Go to **3. Review & save**.")
                            st.rerun()
                else:
                    details, df_schedule = compute_bullet_schedule(
                        loan_required, loan_term, disbursement_date, rate_pct, drawdown_pct, arrangement_pct,
                        input_tf, bullet_type, None, True, glob.get("rate_basis", "Per month"), flat_rate,
                    )
                    details["currency"] = currency
                    details["penalty_rate_pct"] = penalty_pct
                    details["penalty_quotation"] = cfg.get("penalty_interest_quotation", "Absolute Rate")
                    st.dataframe(format_schedule_display(df_schedule), use_container_width=True, hide_index=True)
                    if st.button("Use this schedule", key="cap_bullet_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_schedule
                        st.success("Schedule saved. Go to **3. Review & save**.")
                        st.rerun()

            else:
                # Customised Repayments
                cfg = _get_system_config()
                dr = cfg.get("default_rates", {}).get("customised_repayments", {})
                accepted_currencies = cfg.get(
                    "accepted_currencies", [cfg.get("base_currency", "USD")]
                )
                loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
                default_ccy = loan_curr_cfg.get(
                    "customised_repayments", cfg.get("base_currency", "USD")
                )
                if default_ccy not in accepted_currencies:
                    accepted_currencies = [default_ccy, *accepted_currencies]
                currency = st.selectbox(
                    "Currency",
                    accepted_currencies,
                    index=accepted_currencies.index(default_ccy)
                    if default_ccy in accepted_currencies
                    else 0,
                    key="cap_cust_currency",
                )
                principal_input = st.radio("What are you entering?", ["Net proceeds", "Principal (total loan amount)"], key="cap_cust_principal_input")
                input_tf = principal_input == "Principal (total loan amount)"
                loan_required = st.number_input("Loan amount", min_value=0.0, value=1000.0, step=100.0, format="%.2f", key="cap_cust_principal")
                loan_term = st.number_input("Term (months)", 1, 120, 12, key="cap_cust_term")
                start_date = datetime.combine(st.date_input("Start date", datetime.today().date(), key="cap_cust_start"), datetime.min.time())
                irregular = st.checkbox("Irregular", value=False, key="cap_cust_irregular", help="Allow editing dates and adding rows; schedule recomputes from table dates.")
                use_anniversary = st.radio("Repayments on", ["Anniversary date", "Last day of month"], key="cap_cust_timing").startswith("Anniversary")
                default_first = add_months(start_date, 1).date()
                if not use_anniversary:
                    default_first = default_first.replace(day=days_in_month(default_first.year, default_first.month))
                # first_rep for initial schedule build: use stored derived if set, else default
                first_rep_derived = st.session_state.get("cap_cust_first_rep_derived")
                first_rep_display = (first_rep_derived.date() if first_rep_derived is not None else default_first)
                first_rep = datetime.combine(first_rep_display, datetime.min.time())

                rate_pct = st.number_input("Interest rate (%)", 0.0, 100.0, float(dr.get("interest_pct", 7.0)), step=0.1, key="cap_cust_rate")
                drawdown_pct = st.number_input("Drawdown fee (%)", 0.0, 100.0, float(dr.get("drawdown_pct", 2.5)), step=0.1, key="cap_cust_drawdown") / 100.0
                arrangement_pct = st.number_input("Arrangement fee (%)", 0.0, 100.0, float(dr.get("arrangement_pct", 2.5)), step=0.1, key="cap_cust_arrangement") / 100.0
                def_penalty = cfg.get("penalty_rates", {}).get("customised_repayments", 2.0)
                penalty_pct = st.number_input("Penalty interest (%) – override default", 0.0, 100.0, float(def_penalty), step=0.5, key="cap_cust_penalty", help="Interpreted per System config")
                total_fee = drawdown_pct + arrangement_pct
                if input_tf:
                    total_facility = loan_required
                else:
                    total_facility = loan_required / (1.0 - total_fee)
                annual_rate = (rate_pct / 100.0) * 12.0 if glob.get("rate_basis") == "Per month" else (rate_pct / 100.0)

                cap_key = "cap_cust_df"
                cap_params = (round(total_facility, 2), loan_term, start_date.strftime("%Y-%m-%d"), irregular)
                if cap_key not in st.session_state or st.session_state.get("cap_cust_params") != cap_params:
                    st.session_state["cap_cust_params"] = cap_params
                    schedule_dates_init = repayment_dates(start_date, first_rep, int(loan_term), use_anniversary)
                    rows = [{"Period": 0, "Date": start_date.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": round(total_facility, 2), "Total Outstanding": round(total_facility, 2)}]
                    for i, dt in enumerate(schedule_dates_init, 1):
                        rows.append({"Period": i, "Date": dt.strftime("%d-%b-%Y"), "Payment": 0.0, "Interest": 0.0, "Principal": 0.0, "Principal Balance": 0.0, "Total Outstanding": 0.0})
                    st.session_state[cap_key] = pd.DataFrame(rows)
                    st.session_state.pop("cap_cust_first_rep_derived", None)

                df_cap = st.session_state[cap_key].copy()
                # Always derive schedule_dates from table so recompute matches displayed dates
                schedule_dates = parse_schedule_dates_from_table(df_cap, start_date=start_date)
                df_cap = recompute_customised_from_payments(df_cap, total_facility, schedule_dates, annual_rate, flat_rate, start_date)
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
                                last_dt = add_months(start_date, len(last_df))
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
                    use_container_width=True,
                    hide_index=True,
                    key="cap_cust_editor",
                )
                if not edited.equals(df_cap):
                    schedule_dates_edit = parse_schedule_dates_from_table(edited, start_date=start_date)
                    df_cap = recompute_customised_from_payments(edited, total_facility, schedule_dates_edit, annual_rate, flat_rate, start_date)
                    st.session_state[cap_key] = df_cap
                    st.session_state["cap_cust_first_rep_derived"] = _first_repayment_from_customised_table(df_cap)
                    st.rerun()

                # Show first repayment date from current table (first row with payment > 0)
                first_rep_from_current = _first_repayment_from_customised_table(df_cap)
                first_rep_label = first_rep_from_current.strftime("%d-%b-%Y") if first_rep_from_current else default_first.strftime("%d-%b-%Y") + " (no payment yet)"
                st.markdown(f"**First repayment date (from table):** {first_rep_label}")

                # For save: first repayment = first row with non-zero payment; end = last date in table
                first_rep_for_save = _first_repayment_from_customised_table(df_cap) or first_rep
                end_date_from_table = schedule_dates[-1] if schedule_dates else start_date

                final_to = float(df_cap.at[len(df_cap) - 1, "Total Outstanding"]) if len(df_cap) > 1 else total_facility
                if abs(final_to) < 0.01:
                    details = {
                        "facility": total_facility, "principal": loan_required, "term": loan_term,
                        "annual_rate": annual_rate, "drawdown_fee": drawdown_pct, "arrangement_fee": arrangement_pct,
                        "start_date": start_date, "end_date": end_date_from_table,
                        "first_repayment_date": first_rep_for_save, "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
                        "penalty_rate_pct": penalty_pct, "penalty_quotation": cfg.get("penalty_interest_quotation", "Absolute Rate"),
                        "currency": currency,
                    }
                    if st.button("Use this schedule", key="cap_cust_use"):
                        st.session_state["capture_loan_details"] = details
                        st.session_state["capture_loan_schedule_df"] = df_cap
                        st.success("Schedule saved. Go to **3. Review & save**.")
                        st.rerun()
                else:
                    st.warning("Clear the schedule (Total Outstanding = $0) before using it.")

    with t3:
        st.subheader("Review & save")
        # Show save result from previous run (success or failure)
        save_result = st.session_state.pop("capture_last_save_result", None)
        if save_result is not None:
            if save_result.get("success"):
                st.success(f"**Loan saved successfully to the database.** Loan ID: **{save_result.get('loan_id', '—')}**")
            else:
                st.error(f"**Save to database failed.** {save_result.get('error', 'Unknown error')}")

        details = st.session_state.get("capture_loan_details")
        df_schedule = st.session_state.get("capture_loan_schedule_df")
        cid = st.session_state.get("capture_customer_id")
        ltype = st.session_state.get("capture_loan_type")
        if not details or df_schedule is None or not cid or not ltype:
            if save_result is None:
                st.info("Complete **1. Customer & loan type** and **2. Compute schedule** first.")
            if st.button("Clear and start over", key="cap_clear_t3_empty"):
                for k in list(st.session_state.keys()):
                    if k.startswith("capture_"):
                        st.session_state.pop(k, None)
                st.rerun()
        else:
            st.markdown(f"**Customer:** {get_display_name(cid)} (ID {cid})")
            st.markdown(f"**Loan type:** {ltype}")
            st.markdown(f"**Principal:** {details.get('facility', 0):,.2f} | **Net proceeds:** {details.get('principal', 0):,.2f} | **Term:** {details.get('term', 0)} months")
            st.divider()
            st.subheader("Schedule")
            st.dataframe(format_schedule_display(df_schedule), use_container_width=True, hide_index=True)
            col_save, col_cancel = st.columns(2)
            with col_save:
                if st.button("Save loan to database", type="primary", key="cap_save_btn"):
                    try:
                        details_with_agent = {**details, "agent_id": st.session_state.get("capture_agent_id")}
                        loan_id = save_loan_to_db(cid, ltype, details_with_agent, df_schedule)
                        st.session_state["capture_last_save_result"] = {"success": True, "loan_id": loan_id}
                        for k in ["capture_loan_details", "capture_loan_schedule_df"]:
                            st.session_state.pop(k, None)
                        st.rerun()
                    except Exception as e:
                        st.session_state["capture_last_save_result"] = {"success": False, "error": str(e)}
                        st.rerun()
            with col_cancel:
                if st.button("Cancel / Clear and start over", key="cap_cancel_t3"):
                    for k in list(st.session_state.keys()):
                        if k.startswith("capture_"):
                            st.session_state.pop(k, None)
                    st.rerun()


def customers_ui():
    """Web UI to add and manage customers (individuals and corporates)."""
    if not _customers_available:
        st.error(f"Customer module is not available. Check database connection and install: psycopg2-binary. ({_customers_error})")
        return

    st.markdown(
        "<div style='background-color: #16A34A; color: white; padding: 8px 12px; font-weight: bold; font-size: 1.1rem;'>Customers</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs(["Add Individual", "Add Corporate", "View & manage", "Agents"])

    with tab1:
        st.subheader("New individual customer")
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
            employer_details = st.text_area("Employer details", placeholder="Optional", key="ind_employer_details")
            with st.expander("Addresses (optional)"):
                addr_type = st.text_input("Address type", placeholder="e.g. physical, postal", key="ind_addr_type")
                line1 = st.text_input("Address line 1", key="ind_addr_line1")
                line2 = st.text_input("Address line 2", key="ind_addr_line2")
                city = st.text_input("City", key="ind_addr_city")
                region = st.text_input("Region", key="ind_addr_region")
                postal_code = st.text_input("Postal code", key="ind_addr_postal_code")
                country = st.text_input("Country", key="ind_addr_country")
                use_addr = st.checkbox("Include this address", value=False, key="ind_use_addr")
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
                except Exception as e:
                    st.error(f"Could not create customer: {e}")
            elif submitted and not name.strip():
                st.warning("Please enter a name.")

    with tab2:
        st.subheader("New corporate customer")
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
            with st.expander("Directors (optional)"):
                dir_name = st.text_input("Director full name", key="corp_dir_name")
                dir_national_id = st.text_input("Director national ID", key="corp_dir_national_id")
                dir_designation = st.text_input("Director designation", key="corp_dir_designation")
                dir_phone1 = st.text_input("Director phone 1", key="corp_dir_phone1")
                dir_phone2 = st.text_input("Director phone 2", key="corp_dir_phone2")
                dir_email = st.text_input("Director email", key="corp_dir_email")
                use_dir = st.checkbox("Include this director", value=False, key="corp_use_dir")
            with st.expander("Shareholders (optional)"):
                sh_name = st.text_input("Shareholder full name", key="corp_sh_name")
                sh_national_id = st.text_input("Shareholder national ID", key="corp_sh_national_id")
                sh_designation = st.text_input("Shareholder designation", key="corp_sh_designation")
                sh_phone1 = st.text_input("Shareholder phone 1", key="corp_sh_phone1")
                sh_phone2 = st.text_input("Shareholder phone 2", key="corp_sh_phone2")
                sh_email = st.text_input("Shareholder email", key="corp_sh_email")
                sh_pct = st.number_input("Shareholding %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, key="corp_sh_pct")
                use_sh = st.checkbox("Include this shareholder", value=False, key="corp_use_sh")
            submitted = st.form_submit_button("Create corporate")
            if submitted and legal_name.strip():
                addresses = [{"address_type": addr_type or None, "line1": line1 or None, "line2": line2 or None, "city": city or None, "region": region or None, "postal_code": postal_code or None, "country": country or None}] if use_addr and line1.strip() else None
                contact_person = None
                if use_cp and cp_name.strip():
                    contact_person = {"full_name": cp_name.strip(), "national_id": cp_national_id.strip() or None, "designation": cp_designation.strip() or None, "phone1": cp_phone1.strip() or None, "phone2": cp_phone2.strip() or None, "email": cp_email.strip() or None, "address_line1": cp_addr1.strip() or None, "address_line2": cp_addr2.strip() or None, "city": cp_city.strip() or None, "country": cp_country.strip() or None}
                directors = [{"full_name": dir_name.strip(), "national_id": dir_national_id.strip() or None, "designation": dir_designation.strip() or None, "phone1": dir_phone1.strip() or None, "phone2": dir_phone2.strip() or None, "email": dir_email.strip() or None, "address_line1": None, "address_line2": None, "city": None, "country": None}] if use_dir and dir_name.strip() else None
                shareholders = [{"full_name": sh_name.strip(), "national_id": sh_national_id.strip() or None, "designation": sh_designation.strip() or None, "phone1": sh_phone1.strip() or None, "phone2": sh_phone2.strip() or None, "email": sh_email.strip() or None, "address_line1": None, "address_line2": None, "city": None, "country": None, "shareholding_pct": sh_pct}] if use_sh and sh_name.strip() else None
                try:
                    cid = create_corporate(
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
                    st.success(f"Corporate customer created. Customer ID: **{cid}**.")
                except Exception as e:
                    st.error(f"Could not create customer: {e}")
                    st.exception(e)
            elif submitted and not legal_name.strip():
                st.warning("Please enter a legal name.")

    with tab3:
        st.subheader("View & manage customers")
        status_filter = st.selectbox("Status", ["all", "active", "inactive"], key="cust_status_filter")
        type_filter = st.selectbox("Type", ["all", "individual", "corporate"], key="cust_type_filter")
        status = None if status_filter == "all" else status_filter
        customer_type = None if type_filter == "all" else type_filter
        try:
            customers_list = list_customers(status=status, customer_type=customer_type)
        except Exception as e:
            st.error(f"Could not load customers: {e}")
            customers_list = []
        if not customers_list:
            st.info("No customers found. Add one in the tabs above.")
        else:
            df = pd.DataFrame(customers_list)
            df["display_name"] = df["id"].apply(lambda i: get_display_name(i))
            st.dataframe(df[["id", "type", "status", "display_name", "created_at"]], use_container_width=True, hide_index=True)
            st.divider()
            view_id = st.number_input("View customer by ID", min_value=1, value=customers_list[0]["id"] if customers_list else 1, step=1, key="cust_view_id")
            if st.button("Load customer", key="cust_load"):
                st.session_state["cust_loaded_id"] = int(view_id)
            loaded_id = st.session_state.get("cust_loaded_id")
            if loaded_id is not None:
                rec = get_customer(loaded_id)
                if not rec:
                    st.warning("Customer not found.")
                    st.session_state.pop("cust_loaded_id", None)
                else:
                    st.subheader(f"Customer #{loaded_id}")
                    st.json(rec)
                    current_status = rec.get("status", "active")
                    new_active = st.radio("Set status", ["active", "inactive"], index=0 if current_status == "active" else 1, key="cust_set_status")
                    if st.button("Update status", key="cust_update_status"):
                        set_active(loaded_id, new_active == "active")
                        st.success(f"Status set to **{new_active}**.")
                        st.session_state["cust_loaded_id"] = loaded_id
                        st.rerun()

    with tab4:
        st.subheader("Agents")
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
                st.dataframe(df_agents[cols_show], use_container_width=True, hide_index=True)
            else:
                st.info("No agents found. Add one below.")
            st.divider()
            st.subheader("Add agent")
            with st.form("add_agent_form", clear_on_submit=True):
                col_a1, col_a2 = st.columns(2)
                with col_a1:
                    aname = st.text_input("Agent name *", key="agent_name")
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
                submitted_create_agent = st.form_submit_button("Create agent")
                if submitted_create_agent and aname.strip():
                    try:
                        aid = create_agent(
                            name=aname.strip(),
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
                        st.success(f"Agent created. Agent ID: **{aid}**.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not create agent: {e}")
                elif submitted_create_agent and not aname.strip():
                    st.warning("Please enter agent name.")
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
                                )
                                st.success("Agent updated.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Could not update agent: {e}")
                        elif submitted_update_agent and not ename.strip():
                            st.warning("Please enter agent name.")


def teller_ui():
    """Teller module: single repayment capture and batch payments."""
    if not _customers_available:
        st.error("Customer module is required for Teller. Check database connection.")
        return
    if not _loan_management_available:
        st.error(f"Loan management module is not available. ({_loan_management_error})")
        return

    st.markdown(
        "<div style='background-color: #7C3AED; color: white; padding: 8px 12px; font-weight: bold; font-size: 1.1rem;'>Teller</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    tab_single, tab_batch = st.tabs(["Single repayment", "Batch payments"])

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
            sel = st.selectbox("Select customer", labels, index=idx, key="teller_cust_select")
            cid = options[labels.index(sel)][0] if sel and labels else None
            st.session_state["teller_customer_id"] = cid

            if cid:
                loans_list = get_loans_by_customer(cid)
                loans_active = [l for l in loans_list if l.get("status") == "active"]
                if not loans_active:
                    st.info("No active loans for this customer.")
                else:
                    loan_options = [(l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('facility', 0):,.2f}") for l in loans_active]
                    loan_labels = [t[1] for t in loan_options]
                    loan_sel = st.selectbox("Select loan", loan_labels, key="teller_loan_select")
                    loan_id = loan_options[loan_labels.index(loan_sel)][0] if loan_sel and loan_labels else None

                    if loan_id:
                        # Amount due preview
                        try:
                            summary = get_amount_due_summary(loan_id)
                            amount_due = summary["amount_due"]
                            scheduled_total = summary["scheduled_total"]
                            repaid_total = summary["repaid_total"]
                        except Exception:
                            amount_due = None
                            scheduled_total = None
                            repaid_total = None

                        if amount_due is not None:
                            help_text = (
                                f"Scheduled payments up to today: {scheduled_total:,.2f}\n"
                                f"Total repayments up to today: {repaid_total:,.2f}\n"
                                f"Amount due today (scheduled - repaid): {amount_due:,.2f}"
                            )
                            st.metric(
                                label="Amount Due Today",
                                value=f"{amount_due:,.2f}",
                                help=help_text,
                            )

                        now = datetime.now()
                        with st.form("teller_single_form", clear_on_submit=True):
                            amount = st.number_input("Amount", min_value=0.01, value=100.0, step=100.0, format="%.2f", key="teller_amount")
                            customer_ref = st.text_input("Customer reference (appears on loan statement)", placeholder="e.g. Receipt #123", key="teller_cust_ref")
                            company_ref = st.text_input("Company reference (appears in general ledger)", placeholder="e.g. GL ref", key="teller_company_ref")
                            col1, col2 = st.columns(2)
                            with col1:
                                value_date = st.date_input("Value date", value=now.date(), key="teller_value_date")
                            with col2:
                                system_date = st.date_input("System date", value=now.date(), key="teller_system_date")
                            submitted = st.form_submit_button("Record repayment")
                            if submitted and amount > 0:
                                try:
                                    rid = record_repayment(
                                        loan_id=loan_id,
                                        amount=amount,
                                        payment_date=value_date,
                                        customer_reference=customer_ref.strip() or None,
                                        company_reference=company_ref.strip() or None,
                                        value_date=value_date,
                                        system_date=datetime.combine(system_date, now.time()),
                                    )
                                    cfg = load_system_config_from_db() if _loan_management_available else {}
                                    allocate_repayment_waterfall(rid, system_config=cfg)
                                    st.success(f"Repayment recorded. **Repayment ID: {rid}**")
                                except Exception as e:
                                    if NeedOverpaymentDecision and isinstance(e, NeedOverpaymentDecision):
                                        st.session_state["teller_overpayment"] = {
                                            "repayment_id": e.repayment_id,
                                            "loan_id": e.loan_id,
                                            "amount_remaining": e.amount_remaining,
                                            "effective_date": e.effective_date,
                                        }
                                        st.rerun()
                                    else:
                                        st.error(f"Could not record repayment: {e}")
                                        st.exception(e)
                            if st.session_state.get("teller_overpayment") and st.session_state["teller_overpayment"].get("loan_id") == loan_id:
                                od = st.session_state["teller_overpayment"]
                                st.warning(
                                    f"**Overpayment detected** (Standard waterfall reached step 6). "
                                    f"Amount **{od['amount_remaining']:,.2f}** remains. Choose how to apply:"
                                )
                                col_rec, col_unapp = st.columns(2)
                                with col_rec:
                                    if st.button("Loan Recast", key="teller_recast_btn"):
                                        try:
                                            from reamortisation import execute_loan_recast
                                            allocate_repayment_waterfall(
                                                od["repayment_id"],
                                                overpayment_action="recast",
                                                system_config=load_system_config_from_db(),
                                            )
                                            bal = get_loan_daily_state_balances(od["loan_id"], od["effective_date"])
                                            new_principal = (bal["principal_not_due"] + bal["principal_arrears"]) if bal else 0
                                            if new_principal > 0:
                                                execute_loan_recast(
                                                    od["loan_id"],
                                                    od["effective_date"],
                                                    new_principal,
                                                    trigger_repayment_id=od["repayment_id"],
                                                )
                                            del st.session_state["teller_overpayment"]
                                            st.success("Repayment allocated and loan recast completed. New instalment applied.")
                                            st.rerun()
                                        except Exception as ex:
                                            st.error(str(ex))
                                with col_unapp:
                                    if st.button("Unapplied Funds Account", key="teller_unapplied_btn"):
                                        try:
                                            allocate_repayment_waterfall(
                                                od["repayment_id"],
                                                overpayment_action="unapplied",
                                                system_config=load_system_config_from_db(),
                                            )
                                            del st.session_state["teller_overpayment"]
                                            st.success("Repayment allocated. Overpayment credited to Unapplied Funds.")
                                            st.rerun()
                                        except Exception as ex:
                                            st.error(str(ex))

    with tab_batch:
        st.subheader("Batch payments")
        st.caption("Upload an Excel file with repayment rows. Download the template below to see the required columns.")

        # Template download
        template_df = pd.DataFrame(columns=[
            "loan_id", "amount", "payment_date", "value_date", "customer_reference", "company_reference"
        ])
        today = datetime.now().date().isoformat()
        template_df.loc[0] = [1, 100.00, today, today, "Receipt-001", "GL-001"]
        buf = BytesIO()
        template_df.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        st.download_button(
            "Download template (Excel)",
            data=buf,
            file_name="teller_batch_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="teller_download_template",
        )

        uploaded = st.file_uploader("Upload Excel file", type=["xlsx", "xls"], key="teller_batch_upload")
        if uploaded:
            try:
                df = pd.read_excel(uploaded, engine="openpyxl")
                required = ["loan_id", "amount"]
                missing = [c for c in required if c not in df.columns]
                if missing:
                    st.error(f"Missing columns: {', '.join(missing)}. Use the template.")
                else:
                    st.dataframe(df.head(20), use_container_width=True, hide_index=True)
                    if len(df) > 20:
                        st.caption(f"Showing first 20 of {len(df)} rows.")
                    if st.button("Process batch", type="primary", key="teller_batch_process"):
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
                                    pdate = datetime.now().date().isoformat()
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
                                valid_rows.append({
                                    "loan_id": lid,
                                    "amount": amt,
                                    "payment_date": pdate,
                                    "value_date": vdate,
                                    "customer_reference": str(r.get("customer_reference", "")).strip() or None,
                                    "company_reference": str(r.get("company_reference", "")).strip() or None,
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
                loan_options = [(l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('facility', 0):,.2f}") for l in loans_active]
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
                        elif last_due and restructure_date < datetime.now().date() and restructure_date < last_due:
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
                                p["installment"] = float(npf.pmt(new_annual_rate / 1200, new_term, -float(loan.get("facility") or loan.get("principal") or 0)))
                            elif new_loan_type == "term_loan":
                                p["grace_type"] = loan.get("grace_type") or "none"
                                p["moratorium_months"] = loan.get("moratorium_months") or 0
                            elif new_loan_type == "bullet_loan":
                                from datetime import datetime as dt
                                p["maturity_date"] = dt.combine(restructure_date, dt.min.time())
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
                                use_container_width=True,
                                hide_index=True,
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
                    recast_date = st.date_input("Recast effective date", value=datetime.now().date(), key="recast_date")
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
                            use_container_width=True,
                            hide_index=True,
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
        st.caption("Overpayments credited to the Client Unapplied Funds Account. Apply on next due date or initiate a recast.")
        rows = list_unapplied_funds(status="pending")
        if not rows:
            st.info("No pending unapplied funds.")
        else:
            df_ua = pd.DataFrame(rows)
            cols = [c for c in ["id", "loan_id", "amount", "currency", "value_date", "status", "created_at"] if c in df_ua.columns]
            st.dataframe(df_ua[cols] if cols else df_ua, use_container_width=True, hide_index=True)


def statements_ui():
    """
    Generate statements on demand (no persistence).
    Customer loan statement: select customer/loan, date range; search by customer name or Loan ID.
    GL / ledger statements (later).
    """
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
        from statements import (
            generate_customer_loan_statement,
            CUSTOMER_LOAN_STATEMENT_HEADINGS,
        )
    except ImportError as e:
        st.error(f"Statements module not available: {e}")
        return

    tab_loan, tab_gl = st.tabs(["Customer loan statement", "General ledger (later)"])
    with tab_loan:
        st.subheader("Customer loan statement")
        st.caption(
            "Search by customer name or Loan ID. Select loan and date range. "
            "If no dates are specified, statement runs from start of loan to today. "
            "Generated on a non-due date: interest for current period to date is included so total exposure is correct."
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
                    preselect_cust_id = loan["customer_id"]
                    preselect_loan_id = lid
            except ValueError:
                pass
            if preselect_loan_id is None:
                search_lower = search.lower()
                customers = [c for c in customers if search_lower in (get_display_name(c["id"]) or "").lower()]

        if not customers and preselect_cust_id is None:
            st.info("No customers found. Create a customer or enter a valid Loan ID.")
        else:
            cust_options = [(c["id"], get_display_name(c["id"]) or f"Customer #{c['id']}") for c in customers]
            cust_labels = [t[1] for t in cust_options]
            default_idx = 0
            if preselect_cust_id is not None:
                try:
                    default_idx = next(i for i, t in enumerate(cust_options) if t[0] == preselect_cust_id)
                except StopIteration:
                    cust_options.insert(0, (preselect_cust_id, get_display_name(preselect_cust_id) or f"Customer #{preselect_cust_id}"))
                    cust_labels.insert(0, cust_options[0][1])
                    default_idx = 0
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
                loan_options = [(l["id"], f"Loan #{l['id']} | {l.get('loan_type', '')} | Principal: {l.get('facility', 0):,.2f}") for l in loans]
                loan_labels = [t[1] for t in loan_options]
                default_loan_idx = 0
                if preselect_loan_id is not None:
                    try:
                        default_loan_idx = next(i for i, t in enumerate(loan_options) if t[0] == preselect_loan_id)
                    except StopIteration:
                        default_loan_idx = 0
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
                start_default = disbursement or datetime.now().date()
                col_start, col_end = st.columns(2)
                with col_start:
                    start_date = st.date_input("Start date (optional)", value=start_default, key="stmt_start")
                with col_end:
                    end_date = st.date_input("End date (optional)", value=datetime.now().date(), key="stmt_end")
                st.caption("Leave defaults for start of loan to today.")

                if st.button("Generate statement", type="primary", key="stmt_gen"):
                    try:
                        rows, meta = generate_customer_loan_statement(
                            loan_id,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        if not rows:
                            st.info("No statement lines for this period.")
                        else:
                            df = pd.DataFrame(rows)
                            start = meta.get("start_date")
                            end = meta.get("end_date")
                            cust_id = meta.get("customer_id")
                            customer_name = get_display_name(cust_id) if cust_id is not None else "—"
                            start_fmt = start.strftime("%d%b%Y") if hasattr(start, "strftime") else str(start)
                            end_fmt = end.strftime("%d%b%Y") if hasattr(end, "strftime") else str(end)
                            gen = meta.get("generated_at")
                            generated_fmt = gen.strftime("%d %b %Y, %H:%M:%S") if gen and hasattr(gen, "strftime") else (str(gen) if gen else "")

                            st.markdown(
                                "<div style='margin-bottom: 1rem;'>"
                                f"<strong style='font-size: 1.25rem;'>Loan Statement</strong><br>"
                                f"<span style='color: #64748b;'>Customer: {customer_name} &nbsp;|&nbsp; Customer ID: {cust_id or '—'} &nbsp;|&nbsp; Loan ID: {loan_id}</span>"
                                "</div>",
                                unsafe_allow_html=True,
                            )
                            st.dataframe(df, use_container_width=True, hide_index=True)
                            st.markdown(
                                "<div style='margin-top: 1rem; padding-top: 0.75rem; border-top: 1px solid #e2e8f0; color: #64748b; font-size: 0.9rem;'>"
                                f"For the period from {start_fmt} to {end_fmt}<br>"
                                f"<strong>Generated:</strong> {generated_fmt}"
                                "</div>",
                                unsafe_allow_html=True,
                            )
                            buf = BytesIO()
                            df.to_csv(buf, index=False, date_format="%Y-%m-%d")
                            buf.seek(0)
                            st.download_button(
                                "Download as CSV",
                                data=buf,
                                file_name=f"loan_statement_{loan_id}_{start_date}_{end_date}.csv",
                                mime="text/csv",
                                key="stmt_download",
                            )
                    except Exception as ex:
                        st.error(str(ex))
                        st.exception(ex)

    with tab_gl:
        st.caption("General ledger and ledger account statements will be added here.")


def accounting_ui():
    """
    Accounting configurations: Chart of Accounts, Event mappings, FX rates.
    Backed by in-memory state (resets when app restarts).
    """
    registry = _get_mapping_registry()

    st.markdown(
        "<div style='background-color: #0F766E; color: white; padding: 8px 12px; "
        "font-weight: bold; font-size: 1.1rem;'>Accounting</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    tab_coa, tab_mappings, tab_fx = st.tabs(
        ["Chart of Accounts", "Event mappings", "FX rates"]
    )

    # Chart of Accounts
    with tab_coa:
        st.subheader("Chart of Accounts")
        st.caption(
            "Maintain accounting accounts used for postings. "
            "Coding rules are enforced by the engine."
        )

        accounts_list = [
            {
                "Code": acc.id,
                "Name": acc.name,
                "Category": acc.category.value,
                "Parent": acc.parent_id or "",
                "Branch": acc.branch or "",
                "Product line": acc.product_line or "",
                "Active": "Yes" if acc.is_active else "No",
            }
            for acc in registry.accounts.values()
        ]
        if accounts_list:
            st.dataframe(
                pd.DataFrame(accounts_list).sort_values("Code"),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No accounts defined yet. Add the first account below.")

        st.divider()
        st.subheader("Add / update account")

        coa_col1, coa_col2 = st.columns(2)
        with coa_col1:
            new_code = st.text_input(
                "Account code",
                help="7 characters, e.g. A100000 for a parent asset account.",
                key="acct_code",
            ).strip()
            new_name = st.text_input("Account name", key="acct_name").strip()
        with coa_col2:
            category_label_to_enum = {
                c.value.title().replace("_", " "): c for c in AccountCategory
            }
            selected_cat_label = st.selectbox(
                "Category",
                list(category_label_to_enum.keys()),
                key="acct_category",
            )
            selected_category = category_label_to_enum[selected_cat_label]

            parent_options = ["(None – parent account)"] + sorted(
                [acc.id for acc in registry.accounts.values() if acc.parent_id is None]
            )
            parent_choice = st.selectbox(
                "Parent account",
                parent_options,
                key="acct_parent",
            )
            parent_id = None if parent_choice.startswith("(") else parent_choice

        coa_col3, coa_col4 = st.columns(2)
        with coa_col3:
            branch = (
                st.text_input("Branch (optional)", key="acct_branch").strip() or None
            )
        with coa_col4:
            product_line = (
                st.text_input("Product line (optional)", key="acct_product").strip()
                or None
            )

        if st.button("Save account", type="primary", key="acct_save_btn"):
            if not new_code or not new_name:
                st.error("Please provide both account code and name.")
            else:
                try:
                    account = Account(
                        id=new_code,
                        name=new_name,
                        category=selected_category,
                        parent_id=parent_id,
                        branch=branch,
                        product_line=product_line,
                    )
                    registry.add_or_update_account(account)
                    st.success(f"Account {new_code} saved.")
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Could not save account: {e}")

    # Event mappings
    with tab_mappings:
        st.subheader("Event mappings")
        st.caption(
            "Map loan lifecycle events to debit/credit accounts. "
            "These mappings are used by the posting engine."
        )

        if not registry.accounts:
            st.warning("Define at least one account in the Chart of Accounts tab first.")
        else:
            mapping_rows = [
                {
                    "Event": m.event_tag.value,
                    "Side": m.side.value,
                    "Role": m.mapping_category.value,
                    "Account": m.account_id,
                }
                for m in registry.mappings
            ]
            if mapping_rows:
                st.dataframe(
                    pd.DataFrame(mapping_rows),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No event mappings configured yet. Add one below.")

            st.divider()
            st.subheader("Add / update mapping")

            col_ev1, col_ev2 = st.columns(2)
            with col_ev1:
                event_label_to_enum = {
                    t.value.replace("_", " ").title(): t for t in SystemEventTag
                }
                selected_event_label = st.selectbox(
                    "System event",
                    list(event_label_to_enum.keys()),
                    key="map_event",
                )
                selected_event = event_label_to_enum[selected_event_label]

                side_label_to_enum = {
                    "Debit": PostingSide.DEBIT,
                    "Credit": PostingSide.CREDIT,
                }
                selected_side_label = st.radio(
                    "Posting side",
                    list(side_label_to_enum.keys()),
                    key="map_side",
                )
                selected_side = side_label_to_enum[selected_side_label]

            with col_ev2:
                role_label_to_enum = {
                    r.value.replace("_", " ").title(): r for r in MappingCategory
                }
                selected_role_label = st.selectbox(
                    "Logical role",
                    list(role_label_to_enum.keys()),
                    key="map_role",
                )
                selected_role = role_label_to_enum[selected_role_label]

                account_ids = sorted(registry.accounts.keys())
                selected_account_id = st.selectbox(
                    "Account code",
                    account_ids,
                    key="map_account",
                )

            if st.button("Save mapping", type="primary", key="map_save_btn"):
                try:
                    mapping = EventAccountMapping(
                        event_tag=selected_event,
                        side=selected_side,
                        mapping_category=selected_role,
                        account_id=selected_account_id,
                    )
                    registry.add_or_update_mapping(mapping, user_id="ui_admin")
                    st.success("Mapping saved.")
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Could not save mapping: {e}")

    # FX rates
    with tab_fx:
        st.subheader("FX rates")
        st.caption(
            "Maintain simple FX rates to a base currency (e.g. USD). "
            "These are stored in memory only and reset when the app restarts."
        )

        fx_rates = _get_fx_rates()
        if fx_rates:
            st.dataframe(
                pd.DataFrame(fx_rates),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No FX rates configured yet.")

        st.divider()
        fx_c1, fx_c2, fx_c3 = st.columns(3)
        with fx_c1:
            cur = st.text_input(
                "Currency code", placeholder="e.g. ZWL", key="fx_currency"
            ).upper()
        with fx_c2:
            rate = st.number_input(
                "Rate to base",
                min_value=0.0,
                value=0.0,
                step=0.0001,
                format="%.4f",
                key="fx_rate",
            )
        with fx_c3:
            as_of = st.date_input(
                "As of date",
                value=datetime.today().date(),
                key="fx_as_of",
            )

        if st.button("Add / update FX rate", type="primary", key="fx_save_btn"):
            if not cur:
                st.error("Please enter a currency code.")
            elif rate <= 0:
                st.error("Rate must be greater than zero.")
            else:
                existing = next((r for r in fx_rates if r["currency"] == cur), None)
                if existing:
                    existing["rate_to_base"] = float(rate)
                    existing["as_of"] = as_of.isoformat()
                else:
                    fx_rates.append(
                        {
                            "currency": cur,
                            "rate_to_base": float(rate),
                            "as_of": as_of.isoformat(),
                        }
                    )
                st.session_state["accounting_fx_rates"] = fx_rates
                st.success(f"FX rate for {cur} saved.")


def main():
    _render_header()
    _get_global_loan_settings()  # ensure defaults exist

    st.sidebar.header("Navigation")
    # Display current system date and time for operator awareness.
    now = datetime.now()
    st.sidebar.caption(f"System date/time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    nav = st.sidebar.radio(
        "Section",
        [
            "Customers",
            "Loan management",
            "Teller",
            "Reamortisation",
            "Statements",
            "Accounting",
            "End of day",
            "System configurations",
        ],
    )
    st.sidebar.divider()

    if nav == "Customers":
        customers_ui()
    elif nav == "Teller":
        teller_ui()
    elif nav == "Reamortisation":
        reamortisation_ui()
    elif nav == "Statements":
        statements_ui()
    elif nav == "Loan management":
        lm_sub = st.sidebar.radio(
            "Loan management",
            ["Loan capture", "Loan calculators"],
            key="nav_loan_mgmt",
        )
        if lm_sub == "Loan capture":
            capture_loan_ui()
        else:
            loan_type = st.sidebar.radio(
                "Loan type",
                ["Consumer Loan", "Term Loan", "Bullet Loan", "Customised Repayments"],
                key="nav_loan_type",
            )
            if loan_type == "Consumer Loan":
                consumer_loan_ui()
            elif loan_type == "Term Loan":
                term_loan_ui()
            elif loan_type == "Bullet Loan":
                bullet_loan_ui()
            else:
                customised_repayments_ui()
    elif nav == "Accounting":
        accounting_ui()
    elif nav == "End of day":
        eod_ui()
    elif nav == "System configurations":
        system_configurations_ui()


if __name__ == "__main__":
    main()