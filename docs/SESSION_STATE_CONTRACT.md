# Session State Contract

This document lists the `st.session_state` keys used by the Streamlit app, grouped by responsibility.

**Rule for refactors**: do **not** rename these keys and do **not** change when they are created/cleared unless you are intentionally migrating state (and you have a migration plan).

## Core keys (cross-cutting)

- **`current_user`**: Authenticated user object/dict. Set by `middleware.py`; read by `middleware.py` and `auth_ui.py`.
- **`system_config`**: System configuration dict used across UI and engines. Created/merged in `app.py:_get_system_config()` and overwritten in System Config UI before saving.
- **`global_loan_settings`**: Default loan settings dict used by calculators/capture. Created in `app.py:_get_global_loan_settings()`.
- **`accounting_mapping_registry`**: In-memory `MappingRegistry` cache. Created lazily by `app.py:_get_mapping_registry()`.
- **`accounting_fx_rates`**: FX rates cache/list. Created lazily by `app.py:_get_fx_rates()`.

## Navigation / section memory

- **`loan_mgmt_subnav`**: Loan management sub-section (segmented bar; Title Case labels; kept across actions/reruns).

## System configurations UI (`syscfg_*`)

**Package:** `ui/system_configurations/` — `main.render_system_configurations_ui` orchestrates tabs; submodules are `sectors_tab`, `eod_tab`, `accounting_tab`, `consumer_schemes_tab`, `products_tab`, `loan_purposes_tab`, `grade_scales_tab`, `ifrs_provision_tab`, `display_tab`. **Shell:** `app.py:system_configurations_ui` builds safe DAL lambdas and delegates.

These keys back widgets and edit panels under **System configurations**.

- **Grade scale editor**
  - **`syscfg_gr_add_form_open`**: whether the “Add rule” form is open
  - **`syscfg_gr_edit_id`**: selected grade rule id for editing
- **EOD / accounting / display widgets**
  - Multiple widget keys exist (e.g. `syscfg_system_date`, `syscfg_eod_mode`, `syscfg_disp_*`, etc.). They are intentionally stable because Streamlit ties widget identity to the key.

## EOD UI (`eod_*`)

**Entry:** `ui/eod.py` (`render_eod_ui`); **shell:** `app.py:eod_ui` passes `get_system_config`, loan-management availability, and `load_system_config_from_db` via `globals().get(...)` when the loan_management import may be absent. **Service:** `services/eod_service.py` (imported inside the UI module).

### Session / primary run

- **`eod_last_result`**: persisted last EOD run result dict so status survives rerun.
- **`eod_confirm`**: “Run EOD now” confirmation checkbox state.
- **`eod_confirm_clear_requested`**: internal flag to clear `eod_confirm` on next rerun (Streamlit key mutation rules).
- **`eod_dismiss_last`**: dismiss button when the last result was a concurrent / lock failure.
- **`eod_run_now`**: primary run button.

### Fix EOD issues (maintenance; no system date advance)

- **`eod_fix_issues`**: master toggle for backfill, reallocate, and single-loan recompute sections.
- **Backfill (manual mode only):** `eod_backfill_date`, `eod_backfill_btn`.
- **Reallocate:** `eod_realloc_loan_id`, `eod_realloc_value_date`, `eod_realloc_ids_text`, `eod_realloc_by_loan_date`, `eod_realloc_by_ids`.
- **Single-loan recompute:** `eod_recompute_loan_id`, `eod_recompute_as_of`, `eod_run_single_loan_eod`.

## Loan capture / drafts (`capture_*`)

**Service layer:** `services/capture_service.py` holds staged-draft save and send-for-approval orchestration (no Streamlit). **UI:** `ui/capture_loan.py` (`render_capture_loan_ui`, invoked from `app.py:capture_loan_ui`) owns all `capture_*` keys below.

These keys represent the multi-step loan capture workflow state.

- **Step / flow**
  - **`capture_loan_step`**: current step index in the capture workflow
  - **`capture_flash_message`**: transient success/warn message shown after actions
  - **`capture_require_docs_prompt`**: whether to prompt for required docs
  - **`capture_stage1_draft_id`**: staged draft id
  - **`capture_rework_source_draft_id`**: original draft id when reworking
- **Selections**
  - **`capture_customer_id`**
  - **`capture_loan_type`**
  - **`capture_product_code`**
  - **`capture_agent_id`**
  - **`capture_relationship_manager_id`**
  - **`capture_cash_gl_account_id`**
  - **`capture_loan_purpose_id`**
  - **`capture_collateral_subtype_pick`**
- **Amounts / details**
  - **`capture_collateral_charge`**
  - **`capture_collateral_valuation`**
  - **`capture_loan_details`**: dict payload for draft/approval/service calls
  - **`capture_loan_schedule_df`**: pandas DataFrame representing the schedule preview

## Loan documents staging

- **`loan_docs_staged`**: staged list for loan capture docs prior to final submit.
- **`_fcapture_panel_css_v10`**: one-time CSS injection toggle for capture UI panel.
- **`capture_open_draft_panel`**: `None` | `"rework"` | `"staged"` — which loan draft helper panel is expanded (link toggles).

## Calculators / customised repayments

- **`customised_repayments_df`**: cached customised repayments DataFrame (calculator/capture).
- **`customised_params`**: fingerprint key to detect parameter changes and invalidate cached table.
- **`cap_cust_params`**: parameter fingerprint for customised capture schedule derivation.
- **`cap_cust_first_rep_derived`**: derived first repayment date/value cached for customised schedules.

## Update / approval flows

- **`update_loans_flash`**: flash message for update-loans actions.
- **`approve_selected_draft_id`**: selected approval draft id (used by approve-loans UI).

## Customers UI (`ui/customers.py`)

**Entry:** `render_customers_ui` (all five customer-area tabs). **Shell:** `app.py:customers_ui` resolves `list_document_categories` and `upload_document` once per run via `globals().get(...)` (same pattern as other delegates when the documents import may be absent).

**Tab renderers:** `render_add_individual_tab`, `render_add_corporate_tab`, `render_view_manage_customers_tab`, `render_agents_tab`, `customer_approvals_ui` (Approvals tab calls the latter with `is_tab=True`).

### Forms (Streamlit `st.form` ids)

Do not rename without checking nested widgets.

- **`individual_form`** — Add Individual (clear on submit).
- **`corporate_form`** — Add Corporate (clear on submit).
- **`add_agent_form`** — Add Agent.
- **`edit_agent_form`** — Edit Agent (Agents tab).
- **`edit_agent_manage_{id}`**, **`edit_customer_form_{id}`** — View & Manage (dynamic id in form id).

### Session state (non-widget)

- **`ind_docs_staged`**, **`corp_docs_staged`**, **`corp_contact_docs_staged`**, **`corp_director_docs_staged`**, **`agent_docs_staged`** — staged document rows before create/submit.
- **`cust_loaded_id`** — selected entity id for View & Manage action panel; removed when no action checkbox is enabled.
- **`agent_edit_loaded_id`** — agent id after “Load Agent” on the Agents tab.
- **`_cust_appr_panel_css`** — one-shot flag so Approvals injects panel CSS once.
- **`_farnda_cust_tbl_css`** — one-shot flag so View & Manage / Agents HTML entity tables inject shared table CSS once.

### Widget key prefixes (stable identities)

- **Add Individual:** `ind_*` (e.g. `ind_full_name`, `ind_sector`, `ind_doc_type`, `ind_doc_file`, …).
- **Add Corporate:** `corp_*` (company), `cp_*` / `dir_*` / `corp_sh_*` (contact person, director, shareholder fields and doc staging).
- **View & Manage:** `cust_status_filter`, `cust_type_filter`, `cust_show_status_tools_top`, `cust_show_contact_docs_tools_top`, `cust_show_edit_tools_top`, `cust_action_select`, `cust_set_status`, `cust_update_status`; embedded agent editor **`eam_*`**, **`agt_*`**; customer editor **`edit_ind_*`**, **`edit_corp_*`**, **`edit_sector`**, **`edit_subsector`**, **`edit_supp_doc`**; corporate sub-entity uploads use keys with a **dynamic suffix** `_{loaded_id}` (e.g. `cp_doc_pick_{loaded_id}`, `dir_doc_upload_{loaded_id}`).
- **Agents tab:** `agent_status_filter`, `agent_show_add_toggle`, `agent_show_edit_toggle`, `agent_*` on create form, `edit_agent_*` on edit flow, `agent_load_btn`.
- **Approvals:** `cust_appr_draft_id`, `cust_appr_action`, `cust_appr_note`, `cust_appr_submit`; read-only JSON previews **`cust_appr_old_{selected_id}`**, **`cust_appr_new_{selected_id}`**.

## Teller UI (`teller_*`)

**Defined in** `ui/teller.py` (`render_teller_ui`). Widget keys are stable Streamlit identities; three keys are also **written explicitly** to remember customer context across reruns/tabs.

### Explicit session assignments (not only widget-backed)

- **`teller_customer_id`**: selected customer for **Single repayment** (read to restore selectbox index; written after pick).
- **`teller_rev_customer_id`**: selected customer for **Reverse receipt**.
- **`teller_wr_customer_id`**: selected customer for **Receipt from fully written-off loan**.

### Single repayment tab

- **`teller_cust_select`**: customer selectbox  
- **`teller_loan_select`**: loan selectbox  
- **`teller_source_cash_gl`**: source cash / bank GL (cached A100000 list)  
- **`teller_amount`**, **`teller_cust_ref`**, **`teller_company_ref`**: form fields  
- **`teller_value_date`**, **`teller_system_date`**: form date inputs  
- Form id: `teller_single_form` (Streamlit form container; do not rename without checking widget nesting)

### Batch payments tab

- **`teller_download_template`**: template download button  
- **`teller_batch_upload`**: Excel uploader  
- **`teller_batch_process`**: process batch button  

### Reverse receipt tab

- **`teller_rev_cust_select`**, **`teller_rev_loan_select`**: customer / loan pickers  
- **`teller_rev_manual_id`**: optional receipt id text  
- **`teller_rev_receipt_select`**: recent receipts selectbox  
- **`teller_rev_button`**: reverse action button  

### Payment of borrowings tab

- **`teller_borrowing_value_date`**, **`teller_borrowing_amount`**, **`teller_borrowing_ref`**  
- **`teller_borrowing_system_date`**, **`teller_borrowing_desc`**  
- Form id: `teller_borrowing_payment_form`

### Write-off recovery tab

- **`teller_wr_cust_select`**, **`teller_wr_loan`**: customer / loan pickers  
- **`teller_wr_value_date`**, **`teller_wr_amount`**, **`teller_wr_cust_ref`**  
- **`teller_wr_system_date`**, **`teller_wr_company_ref`**  
- Form id: `teller_writeoff_recovery_form`

## Journals UI (`journals_*`, `mj_pick_*`, `bal_adj_*`)

**Entry:** `ui/journals/journals_ui.py` (`render_journals_ui`); **shell:** `app.py:journals_ui` passes `get_system_date`.

### Repair / integrity (outside forms)

- **`journals_repair_loan_id`**: loan id number input for LOAN_APPROVAL re-post.
- **`journals_repair_loan_btn`**: re-post action button.

### Manual journals tab

- **Form id:** `journals_manual_journal_form` (nested widgets depend on this; do not rename casually).
- **Subaccount picks (dynamic):** `mj_pick_{dr|cr}_{event_part}_{tag_part}` — one selectbox per unresolved template line that needs a leaf; `event_part` / `tag_part` come from `widget_key_part()` in `ui/journals/helpers.py` (sanitized, max 48 chars). **Read on submit** via `st.session_state[sk]` for the selected index.
- **`manual_journal_overrides_json`**: optional JSON text area inside the form expander.
- Other manual-journal fields (loan id, template, amount, narration, reversal checkbox, “journal to reverse” select) rely on **implicit** Streamlit keys inside the form unless extended with explicit keys later.

### Balance adjustments tab

- **Form id:** `balance_adjust_form`.
- **`bal_adj_dr_leaf_idx`**, **`bal_adj_cr_leaf_idx`**: debit/credit posting-leaf selectboxes when the chart has leaves.
- **`bal_adj_dr_leaf_idx_empty`**, **`bal_adj_cr_leaf_idx_empty`**: disabled placeholders when there are no posting accounts.
- **`bal_adj_narr`**: narration field.

## Reamortisation UI (`reamod_*`, `recast_*`, preview payloads)

**Entry:** `ui/reamortisation.py` (`render_reamortisation_ui`); **shell:** `app.py:reamortisation_ui` injects loan/customer helpers and schedule export/format callables.

### Session payloads (not widget keys)

- **`reamod_preview`**: dict set after **Preview schedule** (modification); cleared on commit/cancel or overwritten on the next preview. Must stay aligned with the code that reads `loan_id`, `restructure_date`, `new_loan_type`, `new_params`, `outstanding_interest`, `schedule_df`, etc.
- **`recast_preview`**: same idea for **Loan Recast** (keys include `loan_id`, `recast_date`, `new_principal_balance`, `schedule_df`, `new_installment`, …).

### Loan modification tab

- **`reamod_cust`**, **`reamod_loan`**, **`reamod_date`**, **`reamod_type`**, **`reamod_term`**, **`reamod_rate`**, **`reamod_interest`**
- **`reamod_preview_btn`**, **`reamod_commit`**, **`reamod_cancel_preview`**
- Schedule download widgets use prefix **`dl_reamod_sched_{loan_id}`** (passed to `schedule_export_downloads`).

### Loan recast tab

- **`recast_cust`**, **`recast_loan`**, **`recast_date`**, **`recast_principal`**
- **`recast_preview_btn`**, **`recast_commit`**, **`recast_cancel_preview`**
- Schedule downloads: **`dl_recast_sched_{loan_id}`**.

### Unapplied funds tab

- **`unapplied_recast_{uf_id}`** — one button per pending unapplied row (`uf_id` = entry id); dynamic suffix must remain stable per row across reruns for that entry.

## View schedule (loan management)

- **`view_schedule_loan_id`**: loan chosen on **View schedule** (not a `teller_*` key).

## UI modules (Streamlit; keys unchanged)

- **Loan calculators** (`cl_*`, `term_*`, `bullet_*`, `cust_*`, `customised_repayments_df`, `customised_params`): implemented in `ui/loan_calculators.py`; `app.py` exposes `consumer_loan_ui`, `term_loan_ui`, `bullet_loan_ui`, `customised_repayments_ui` as thin delegates.
- **Update / approve / view schedule** (`update_loan_*`, `approve_*`, `view_sched_*`): `ui/loan_management.py` via delegates `update_loans_ui`, `approve_loans_ui`, `view_schedule_ui`.
- **Statements** (`stmt_*`, `stmt_gl_*`): `ui/statements.py` via delegate `statements_ui`.

- **Customers** (`ind_*`, `corp_*`, `cust_*`, `agent_*`, `cust_appr_*`, …): `ui/customers.py` (`render_customers_ui` and tab helpers); `app.py:customers_ui` is a thin delegate.
- **Accounting** (`coa_*`, `tt_*`, `link_*`, `rgl_*`, `snap_*`, …): `ui/accounting/main.py` (`render_accounting_ui`); tab modules under `ui/accounting/`. **Service facades** (thin delegates over one `AccountingService`): `services/accounting_ui/` (`build_accounting_ui_bundle`, `CoaUi`, `TransactionTemplatesUi`, `ReceiptGlMappingUi`, `FinancialReportsUi`). Shell: `app.py:accounting_ui`.
- **Notifications** (`notification_history`, `hist_filter_type`, `hist_filter_status`, forms `send_notification_form`, `new_template_form`): `ui/notifications.py` (`render_notifications_ui`); `app.py:notifications_ui` delegates with `globals().get` for customer helpers when the customers import failed.
- **Document management** (forms `create_doc_class_form`, `edit_doc_class_form`, `create_doc_cat_form`, `edit_doc_cat_form`): `ui/document_management.py` (`render_document_management_ui`); `app.py:document_management_ui` delegates with `globals().get` for document DAL helpers.
- **Journals** (`journals_*`, `mj_pick_*`, `bal_adj_*`, forms `journals_manual_journal_form`, `balance_adjust_form`): `ui/journals/journals_ui.py` (`render_journals_ui`); `app.py:journals_ui` is a thin delegate.
- **Reamortisation** (`reamod_*`, `recast_*`, session `reamod_preview` / `recast_preview`, `unapplied_recast_*`): `ui/reamortisation.py` (`render_reamortisation_ui`); `app.py:reamortisation_ui` delegates with injected helpers.
- **End of day** (`eod_*`): `ui/eod.py` (`render_eod_ui`); `app.py:eod_ui` delegates with system config and loan-management helpers.
- **System configurations** (`syscfg_*`, `prod_*`, `pedit_*`, `new_sub_*`, `add_sub_*`, etc.): `ui/system_configurations/` package (`render_system_configurations_ui` in `main.py`, tab modules alongside); `app.py:system_configurations_ui` supplies DAL callables.

## Accounting UI (`coa_*`, `acco_*`, `tt_*`, …)

Keys under these prefixes support chart-of-accounts and accounting tabs (COA, templates, links, reports, etc.).
They are numerous and widget-driven; do not rename without a planned migration.

**Standalone Journals** (manual posting + balance adjustments) lives in `ui/journals/` and owns **`mj_pick_*`**, **`journals_*`**, **`bal_adj_*`** — documented under **Journals UI** above, not duplicated here.

## Portfolio reports

- **`portfolio_exports_visible`**: toggles export panel visibility in `portfolio_reports_ui.py`.

## Notes / gates for Stage 5 (centralizing state/config)

- **Do not change key names** (especially widget keys like `syscfg_*`, `eod_*`, `capture_*`).
- **Do not change initialization timing**: any key that is expected to exist before a widget is created must still exist at the same point in the rerun.
- **Smoke tests after Stage 5**
  - Full browser refresh
  - Navigate: **System configurations → Display & numbers** (save), **Loan management → Loan capture**, **End of day**.

