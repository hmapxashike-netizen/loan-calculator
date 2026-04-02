"""
Loan management: persist loan details, schedules, and repayments to the database.
Uses loans.py for computation only; this module handles DB writes.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import pandas as pd

from decimal_utils import as_10dp

from .allocation_audit import _log_allocation_audit
from .approval_journal import build_loan_approval_journal_payload
from .cash_gl import (
    SOURCE_CASH_ACCOUNT_CACHE_KEY,
    SOURCE_CASH_TREE_ROOT_CODE,
    _merge_cash_gl_into_payload,
    _parse_optional_uuid_str,
    _post_event_for_loan,
    get_cached_source_cash_account_entries,
    validate_source_cash_gl_account_id_for_new_posting,
)
from .allocation_queries import (
    _get_opening_balances_for_repayment,
    _sum_net_allocations_earlier_same_day,
    get_allocation_totals_for_loan_date,
    get_credits_for_loan_date,
    get_net_allocation_for_loan_date,
    get_repayment_opening_delinquency_total,
    get_repayments_with_allocations,
    get_unallocated_for_loan_date,
)
from .amount_due import get_amount_due_summary
from .apply_allocations_loan_date import apply_allocations_for_loan_date
from .approval_drafts import (
    approve_loan_approval_draft,
    dismiss_loan_approval_draft,
    get_loan_approval_draft,
    list_loan_approval_drafts,
    resubmit_loan_approval_draft,
    save_loan_approval_draft,
    send_back_loan_approval_draft,
    terminate_loan,
    update_loan_approval_draft_staged,
)
from .daily_state import (
    get_loan_daily_state_balances,
    get_loan_daily_state_range,
    save_loan_daily_state,
)
from .db import _connection
from .delinquency_views import get_teller_amount_due_today, get_total_delinquency_arrears_summary
from .exceptions import NeedOverpaymentDecision
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
from .loan_records import get_loan, get_loans_by_customer, update_loan_details, update_loan_safe_details
from .product_catalog import (
    CONFIG_KEY_PRODUCT_PREFIX,
    CONFIG_KEY_SYSTEM,
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
    get_loan_ids_with_reversed_receipts_on_date,
    get_repayment_ids_for_loan_and_date,
    get_repayment_ids_for_value_date,
)
from .repayment_record import record_repayment, record_repayments_batch
from .repayment_waterfall import allocate_repayment_waterfall
from .repost_gl_range import repost_gl_for_loan_date_range
from .reverse_repayment import reverse_repayment
from .save_loan import save_loan
from .schema_ddl import (
    _ensure_loan_approval_drafts_table,
    _ensure_loan_purposes_schema,
    _ensure_loans_schema_for_save_loan,
)
from .schedules import (
    get_latest_schedule_version,
    get_schedule_lines,
    replace_schedule_lines,
    save_new_schedule_version,
)
from .serialization import _date_conv, _json_safe
from .unapplied_eod import apply_unapplied_funds_to_arrears_eod
from .unapplied_queries import (
    get_loans_with_unapplied_balance,
    get_unapplied_balance,
    get_unapplied_entries,
    get_unapplied_ledger_balance,
    get_unapplied_ledger_entries_for_statement,
    get_unapplied_repayment_ids,
)
from .waterfall_core import (
    BUCKET_TO_ALLOC,
    STANDARD_SKIP_BUCKETS,
    _get_waterfall_config,
    compute_waterfall_allocation,
)
