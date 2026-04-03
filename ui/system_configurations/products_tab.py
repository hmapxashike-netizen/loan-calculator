"""Products tab."""

from __future__ import annotations

import streamlit as st



from style import render_main_header, render_sub_header, render_sub_sub_header

def render_products_tab(
    *,
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
    cfg: dict,
) -> None:
    render_sub_sub_header("Products")
    st.caption(
        "Products own loan config, currency, waterfall, suspension & curing. Loan type (on product) drives amortisation. System references products by code."
    )
    if not loan_management_available:
        st.error("Loan management module is required for Products.")
    else:
        products_list = list_products(active_only=False)
        if products_list:
            product_options = [(0, "(Select product to edit)")] + [
                (p["id"], f"{p['code']} – {p['name']} (ID: {p['id']})")
                for p in products_list
            ]
            option_labels = [t[1] for t in product_options]
            option_ids = [t[0] for t in product_options]

            st.markdown("**Products**")
            col_h1, col_h2, col_h3, col_h4, col_h5, col_h6 = st.columns(
                [1.5, 2, 1.2, 1, 0.8, 0.8]
            )
            with col_h1:
                st.caption("**Code**")
            with col_h2:
                st.caption("**Name**")
            with col_h3:
                st.caption("**Loan type**")
            with col_h4:
                st.caption("**Status**")
            with col_h5:
                st.caption("**Edit**")
            with col_h6:
                st.caption("**Delete**")
            for p in products_list:
                c1, c2, c3, c4, c5, c6 = st.columns([1.5, 2, 1.2, 1, 0.8, 0.8])
                with c1:
                    st.text(p.get("code", ""))
                with c2:
                    st.text(p.get("name", ""))
                with c3:
                    st.text(p.get("loan_type", ""))
                with c4:
                    st.text("Active" if p.get("is_active", True) else "Inactive")
                with c5:
                    if st.button("Edit", key=f"ptbl_edit_{p['id']}"):
                        idx = next(
                            (
                                i
                                for i, (oid, _) in enumerate(product_options)
                                if oid == p["id"]
                            ),
                            0,
                        )
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
                p_code = st.text_input(
                    "Code",
                    key="prod_add_code",
                    max_chars=32,
                    placeholder="e.g. TL-USD",
                )
                p_name = st.text_input(
                    "Name", key="prod_add_name", placeholder="Display name"
                )
                p_lt = st.selectbox(
                    "Loan type",
                    [
                        "term_loan",
                        "consumer_loan",
                        "bullet_loan",
                        "customised_repayments",
                    ],
                    key="prod_add_lt",
                )
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
                    lt = prod.get("loan_type", "term_loan")
                    with st.expander(
                        f"**{code_display}** – Rename & status", expanded=True
                    ):
                        col_rn, col_st = st.columns(2)
                        with col_rn:
                            new_name = st.text_input(
                                "Rename product",
                                value=prod.get("name") or "",
                                key="pedit_rename",
                            )
                            if (
                                st.button("Save name", key="pedit_save_name")
                                and new_name.strip()
                            ):
                                update_product(edit_id, name=new_name.strip())
                                st.success("Name updated.")
                                st.rerun()
                        with col_st:
                            current_active = bool(prod.get("is_active", True))
                            status_choice = st.radio(
                                "Status",
                                ["Active", "Inactive"],
                                index=0 if current_active else 1,
                                key="pedit_status",
                            )
                            if st.button("Update status", key="pedit_save_status"):
                                update_product(
                                    edit_id, is_active=(status_choice == "Active")
                                )
                                st.success("Status updated.")
                                st.rerun()
                    st.caption(
                        "Product config (overrides system config for loans using this product)."
                    )
                    st.markdown(
                        f"**Changes apply only to this product:** **{code_display}**"
                    )
                    p_reg_tab, p_pen_tab, p_ccy_tab, p_wf_tab, p_sus_tab = st.tabs(
                        [
                            "Regular interest",
                            "Penalty",
                            "Currency",
                            "Waterfall",
                            "Suspension & curing",
                        ]
                    )
                    with p_reg_tab:
                        glob_p = (p_cfg.get("global_loan_settings") or {}).copy()
                        for k, v in (cfg.get("global_loan_settings") or {}).items():
                            if k not in glob_p:
                                glob_p[k] = v
                        im_opts, it_opts, rb_opts = (
                            ["Reducing balance", "Flat rate"],
                            ["Simple", "Compound"],
                            ["Per annum", "Per month"],
                        )
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            im = st.radio(
                                "Interest method",
                                im_opts,
                                index=im_opts.index(
                                    glob_p.get("interest_method", "Reducing balance")
                                )
                                if glob_p.get("interest_method") in im_opts
                                else 0,
                                key=f"pedit_im_{pid}",
                            )
                        with c2:
                            it = st.radio(
                                "Interest type",
                                it_opts,
                                index=it_opts.index(
                                    glob_p.get("interest_type", "Simple")
                                )
                                if glob_p.get("interest_type") in it_opts
                                else 0,
                                key=f"pedit_it_{pid}",
                            )
                        with c3:
                            rb = st.radio(
                                "Rate basis",
                                rb_opts,
                                index=rb_opts.index(
                                    glob_p.get("rate_basis", "Per month")
                                )
                                if glob_p.get("rate_basis") in rb_opts
                                else 1,
                                key=f"pedit_rb_{pid}",
                            )
                        cap = st.radio(
                            "Capitalization",
                            ["No", "Yes"],
                            index=1
                            if p_cfg.get(
                                "capitalization_of_unpaid_interest",
                                cfg.get("capitalization_of_unpaid_interest"),
                            )
                            else 0,
                            key=f"pedit_cap_{pid}",
                        )
                        st.markdown("**Default rates (this product type)**")
                        dr = p_cfg.get("default_rates") or cfg.get("default_rates") or {}
                        row = dr.get(lt, {})
                        if lt == "consumer_loan":
                            cr_def = dr.get("consumer_loan", {})
                            co1, co2 = st.columns(2)
                            with co1:
                                dr_interest = st.number_input(
                                    "Interest %",
                                    0.0,
                                    100.0,
                                    float(cr_def.get("interest_pct", 7)),
                                    step=0.1,
                                    key=f"pedit_dr_int_{pid}",
                                )
                            with co2:
                                dr_admin = st.number_input(
                                    "Admin %",
                                    0.0,
                                    100.0,
                                    float(cr_def.get("admin_fee_pct", 5)),
                                    step=0.1,
                                    key=f"pedit_dr_adm_{pid}",
                                )
                        else:
                            d1, d2, d3 = st.columns(3)
                            with d1:
                                dr_interest = st.number_input(
                                    "Interest %",
                                    0.0,
                                    100.0,
                                    float(row.get("interest_pct", 7)),
                                    step=0.1,
                                    key=f"pedit_dr_int_{pid}",
                                )
                            with d2:
                                dr_drawdown = st.number_input(
                                    "Drawdown %",
                                    0.0,
                                    100.0,
                                    float(row.get("drawdown_pct", 2.5)),
                                    step=0.1,
                                    key=f"pedit_dr_dd_{pid}",
                                )
                            with d3:
                                dr_arr = st.number_input(
                                    "Arrangement %",
                                    0.0,
                                    100.0,
                                    float(row.get("arrangement_pct", 2.5)),
                                    step=0.1,
                                    key=f"pedit_dr_arr_{pid}",
                                )
                        if st.button("Save Regular interest", key=f"pedit_save_reg_{pid}"):
                            merge = dict(p_cfg)
                            merge["global_loan_settings"] = {
                                "interest_method": im,
                                "interest_type": it,
                                "rate_basis": rb,
                            }
                            merge["capitalization_of_unpaid_interest"] = cap == "Yes"
                            dr_merge = dict(merge.get("default_rates") or {})
                            if lt == "consumer_loan":
                                dr_merge["consumer_loan"] = {
                                    "interest_pct": dr_interest,
                                    "admin_fee_pct": dr_admin,
                                }
                            else:
                                dr_merge[lt] = {
                                    "interest_pct": dr_interest,
                                    "drawdown_pct": dr_drawdown,
                                    "arrangement_pct": dr_arr,
                                }
                            merge["default_rates"] = dr_merge
                            if save_product_config_to_db(prod["code"], merge):
                                st.success("Saved.")
                                st.rerun()
                    with p_pen_tab:
                        pr = p_cfg.get("penalty_rates") or cfg.get("penalty_rates") or {}
                        pq = (
                            p_cfg.get("penalty_interest_quotation")
                            or cfg.get("penalty_interest_quotation")
                            or "Absolute Rate"
                        )
                        pb = (
                            p_cfg.get("penalty_balance_basis")
                            or cfg.get("penalty_balance_basis")
                            or "Arrears"
                        )
                        col_q, col_b, col_p = st.columns(3)
                        with col_q:
                            penalty_quotation_p = st.radio(
                                "Quotation",
                                ["Absolute Rate", "Margin"],
                                index=0 if pq == "Absolute Rate" else 1,
                                key=f"pedit_pq_{pid}",
                            )
                        with col_b:
                            penalty_balance_p = st.radio(
                                "Balance for penalty interest",
                                ["Arrears", "Balance"],
                                index=0 if pb == "Arrears" else 1,
                                key=f"pedit_pb_{pid}",
                            )
                        with col_p:
                            pen_value = st.number_input(
                                "Default penalty %",
                                0.0,
                                100.0,
                                float(pr.get(lt, 2)),
                                step=0.5,
                                key=f"pedit_pen_{pid}",
                            )
                        if st.button("Save Penalty", key=f"pedit_save_pen_{pid}"):
                            merge = dict(p_cfg)
                            merge["penalty_interest_quotation"] = penalty_quotation_p
                            merge["penalty_balance_basis"] = penalty_balance_p
                            merge["penalty_rates"] = {
                                **(merge.get("penalty_rates") or {}),
                                lt: pen_value,
                            }
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
