"""
Stable ``loan_management`` package API: re-exports from domain modules.

Callers should keep using ``from loan_management import record_repayment``, etc.
Implementation lives in the sibling modules imported below, not in this barrel.
"""

from __future__ import annotations

from .allocation_audit import _log_allocation_audit
from .allocation_queries import (
    get_allocation_totals_for_loan_date,
    get_credits_for_loan_date,
    get_net_allocation_for_loan_date,
    get_repayment_opening_delinquency_total,
    get_repayments_with_allocations,
    get_unallocated_for_loan_date,
)
from .amount_due import get_amount_due_summary
from .approval_drafts import (
    approve_loan_approval_draft,
    dismiss_loan_approval_draft,
    get_loan_approval_draft,
    list_loan_approval_drafts,
    resubmit_loan_approval_draft,
    save_loan_approval_draft,
    send_back_loan_approval_draft,
    update_loan_approval_draft_staged,
)
from .approval_journal import build_loan_approval_journal_payload
from .cash_gl import _merge_cash_gl_into_payload, get_cached_source_cash_account_entries
from .daily_state import (
    get_loan_daily_state_balances,
    get_loan_daily_state_balances_for_recast_preview,
    get_loan_daily_state_range,
    save_loan_daily_state,
)
from .db import _connection
from .delinquency_views import get_teller_amount_due_today
from .loan_purposes import (
    clear_all_loan_purposes,
    count_loan_purposes_rows,
    create_loan_purpose,
    ensure_loan_purpose_rows,
    get_loan_purpose_by_id,
    list_loan_purposes,
    set_loan_purpose_active,
    update_loan_purpose,
)
from .loan_records import (
    get_loan,
    get_loans_by_customer,
    update_loan_details,
    update_loan_restructure_flags,
    update_loan_safe_details,
)
from .product_catalog import (
    create_product,
    delete_product,
    get_product,
    get_product_by_code,
    get_product_config_from_db,
    list_products,
    load_system_config_from_db,
    save_product_config_to_db,
    save_system_config_to_db,
    update_product,
)
from .reallocation import reallocate_repayment
from .repayment_queries import (
    get_batch_loan_ids_with_reversed_receipts_in_range,
    get_liquidation_repayment_ids_for_value_date,
    get_loan_ids_with_reversed_receipts_on_date,
    get_repayment_ids_for_loan_and_date,
    get_repayment_ids_for_value_date,
)
from .repayment_record import record_repayment, record_repayments_batch
from .repayment_waterfall import allocate_repayment_waterfall
from .repost_gl_range import repost_gl_for_loan_date_range
from .reverse_repayment import reverse_repayment
from .save_loan import save_loan
from .schedules import (
    apply_schedule_version_bumps,
    batch_list_schedule_bumping_events,
    collect_due_dates_in_range_all_schedule_versions,
    format_schedule_date_for_storage,
    schedule_date_to_iso_for_exchange,
    get_latest_schedule_version,
    get_max_schedule_due_date_on_or_before,
    get_original_facility_for_statements,
    get_schedule_line_on_version_for_date,
    get_schedule_lines,
    list_schedule_bumping_events,
    parse_schedule_line_date,
    save_new_schedule_version,
    schedule_version_effective_on,
)
from .serialization import _date_conv
from .unapplied_eod import apply_unapplied_funds_to_arrears_eod
from .unapplied_queries import (
    get_loans_with_unapplied_balance,
    get_unapplied_ledger_entries_for_statement,
)
from .unapplied_recast import apply_unapplied_funds_recast
from .recast_orchestration import (
    execute_unapplied_liquidation_for_restructure,
    execute_recast_from_unapplied,
    get_unapplied_balance_for_restructure,
    list_unapplied_credit_rows_for_recast,
    preview_recast_from_unapplied,
)
from .waterfall_core import BUCKET_TO_ALLOC, _get_waterfall_config, compute_waterfall_allocation
