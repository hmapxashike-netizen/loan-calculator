"""
Statements module: generate statements on demand (no persistence).
- Customer loan statements (this module)
- Other loan statement types (later)
- General ledger / ledger account statements (later)
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from loan_management import (
    get_loan,
    get_loan_daily_state_range,
    get_repayments_with_allocations,
    get_loan_daily_state_balances,
    get_unapplied_balance,
    get_unapplied_entries,
    get_schedule_lines,
)


# Column headings per statement spec (allocation 1-5 = fees, penalty, default interest, interest arrears, principal arrears)
# These five columns are shown in a collapsible section in the UI to reduce clutter
ALLOC_BUCKET_COLUMNS = [
    "Alloc (1) Fees",
    "Alloc (2) Penalty Interest",
    "Alloc (3) Default Interest",
    "Alloc (4) Interest Arrears",
    "Alloc (5) Principal Arrears",
]
CUSTOMER_LOAN_STATEMENT_HEADINGS = [
    "Transaction Date",
    "Value Date",
    "Narration",
    "Debits",
    "Interest",
    "Fees",
    "Credits",
    "Portion of Credit Allocated to Interest",
    "Credit Allocated to Fees",
    "Credit Allocated to Capital",
    *ALLOC_BUCKET_COLUMNS,
    "Total Outstanding Balance",
    "Unapplied funds",
]


# Periodic (monthly/schedule) statement: one row per due date + receipts in period; interest/penalty/default summed per period
# Arrears after Total Outstanding Balance for clarity
PERIODIC_STATEMENT_HEADINGS = [
    "Due Date",
    "Narration",
    "Interest",
    "Penalty",
    "Default",
    "Principal",
    "Fees",
    "Credits",
    "Portion of Credit Allocated to Interest",
    "Credit Allocated to Fees",
    "Credit Allocated to Capital",
    "Total Outstanding Balance",
    "Arrears",
    "Unapplied funds",
]
# Numeric columns: use 0 instead of None so display shows 0 not "None"
PERIODIC_NUMERIC_HEADINGS = [
    "Interest", "Penalty", "Default", "Principal", "Fees", "Credits",
    "Portion of Credit Allocated to Interest", "Credit Allocated to Fees", "Credit Allocated to Capital",
    "Total Outstanding Balance", "Arrears", "Unapplied funds",
]

# Customer-facing statement: simplified view from internal periodic. One line per periodic entry.
# Shows interest (→ interest arrears on due date), penalty, default, disbursement, fees + credits.
# Arrears = principal arrears. Receipts = one line per receipt; narration from source (e.g. customer reference or "Receipt").
CUSTOMER_FACING_STATEMENT_HEADINGS = [
    "Due Date",
    "Narration",
    "Debits",
    "Credits",
    "Balance",
    "Arrears",
    "Unapplied funds",
]


def _parse_schedule_date(s: Any) -> date | None:
    """Parse schedule line Date (e.g. '31-Mar-2026') to date."""
    if s is None:
        return None
    if isinstance(s, date):
        return s
    if hasattr(s, "date"):
        return s.date() if callable(getattr(s, "date")) else s
    if isinstance(s, str):
        try:
            return datetime.strptime(s[:32].strip(), "%d-%b-%Y").date()
        except ValueError:
            pass
    return None


def _date_conv(d: Any) -> date | None:
    if d is None:
        return None
    if hasattr(d, "date"):
        return d.date() if callable(getattr(d, "date")) else d
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        return datetime.fromisoformat(d.replace("Z", "+00:00")).date()
    return None


def generate_customer_loan_statement(
    loan_id: int,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    as_of_date: date | None = None,
    allowed_customer_ids: list[int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Generate a customer loan statement for the given loan and date range.
    Statements are not saved; always generated from current data.

    allowed_customer_ids: If set, the loan's customer_id must be in this list (e.g. for
    borrower self-service so they only see their own loans). If None, no check (staff use).

    Returns (rows, meta) where:
      - rows: list of dicts with keys = CUSTOMER_LOAN_STATEMENT_HEADINGS
      - meta: loan_id, customer_id, start_date, end_date, loan_type, principal, currency

    If start_date/end_date are None: start = loan disbursement/start, end = today (or as_of_date).
    "Total Outstanding Balance" shows the total loan exposure (principal + all interest buckets + fees)
    on each line, excluding unapplied funds. When statement end date is not a due date, an interest
    line for "current period to date" is added so total exposure includes interest accrued to date.

    Allocation runs at EOD. The statement is a daily statement: balances and allocation breakdown
    (buckets 1–5) are correct as of the last EOD run for each date. Same-day receipts show credits
    and allocation only after EOD has been run for that value date.
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")

    if allowed_customer_ids is not None:
        cust_id = loan.get("customer_id")
        if cust_id is None or cust_id not in allowed_customer_ids:
            raise ValueError("You are not authorized to view this loan statement.")

    disbursement = _date_conv(loan.get("disbursement_date") or loan.get("start_date"))
    today = as_of_date or date.today()
    start = start_date or disbursement or today
    end = end_date or today
    if start > end:
        start, end = end, start

    meta = {
        "loan_id": loan_id,
        "customer_id": loan.get("customer_id"),
        "start_date": start,
        "end_date": end,
        "loan_type": loan.get("loan_type"),
        "principal": float(loan.get("principal") or 0),
        "currency": (loan.get("metadata") or {}).get("currency") or "USD",
        "generated_at": datetime.now(),
    }

    daily_states = get_loan_daily_state_range(loan_id, start, end)
    repayments = get_repayments_with_allocations(loan_id, start, end, include_reversed=True)

    # Single query for unapplied: entries (value_date, amount) then aggregate in memory
    try:
        unapplied_entries = get_unapplied_entries(loan_id, end)
    except Exception:
        unapplied_entries = []
    unapplied_at_end = sum(amt for _vd, amt in unapplied_entries)

    def _unapplied_at(d: date) -> float:
        return round(sum(amt for vd, amt in unapplied_entries if vd <= d), 2)

    # Balance at date: from last daily_state on or before that date (avoid N get_loan_daily_state_balances)
    def _total_outstanding_from_ds(ds: dict) -> float:
        pnd = float(ds.get("principal_not_due") or 0)
        pa = float(ds.get("principal_arrears") or 0)
        ia = float(ds.get("interest_accrued_balance") or 0)
        iar = float(ds.get("interest_arrears_balance") or 0)
        di = float(ds.get("default_interest_balance") or 0)
        pi = float(ds.get("penalty_interest_balance") or 0)
        fees = float(ds.get("fees_charges_balance") or 0)
        return pnd + pa + ia + iar + di + pi + fees

    daily_states_sorted = sorted(
        (ds for ds in daily_states if _date_conv(ds.get("as_of_date"))),
        key=lambda ds: _date_conv(ds["as_of_date"]) or date(9999, 12, 31),
    )

    def _balance_at_date(d: date) -> float | None:
        cand = None
        for ds in daily_states_sorted:
            as_of = _date_conv(ds.get("as_of_date"))
            if as_of and as_of <= d:
                cand = _total_outstanding_from_ds(ds)
            else:
                break
        return cand

    # Index repayments by value_date for grouping
    from collections import defaultdict
    repayments_by_date: dict[date, list[dict]] = defaultdict(list)
    for r in repayments:
        vd = _date_conv(r.get("value_date") or r.get("payment_date"))
        if vd:
            repayments_by_date[vd].append(r)

    # Previous state for delta (fees increase, principal increase)
    prev_state: dict[str, float] | None = None
    if disbursement and start <= disbursement <= end:
        prev_state = {
            "principal_not_due": 0.0,
            "fees_charges_balance": 0.0,
        }

    rows: list[dict[str, Any]] = []
    processed_repayment_ids: set[int] = set()
    sum_interest_shown = 0.0

    # Disbursement line (any increase in Principal Not Due on transaction date)
    if disbursement and start <= disbursement <= end:
        fac = float(loan.get("principal") or loan.get("disbursed_amount") or 0)
        if fac > 0:
            row = _blank_row()
            row["Transaction Date"] = disbursement
            row["Value Date"] = disbursement
            row["Narration"] = "Disbursement"
            row["Debits"] = round(fac, 2)
            rows.append(row)
            prev_state = {"principal_not_due": fac, "fees_charges_balance": 0.0}

    # Interest and Fees from daily state
    for i, ds in enumerate(daily_states):
        as_of = _date_conv(ds["as_of_date"])
        if not as_of:
            continue

        principal_not_due = float(ds.get("principal_not_due") or 0)
        principal_arrears = float(ds.get("principal_arrears") or 0)
        int_accrued = float(ds.get("interest_accrued_balance") or 0)
        int_arrears = float(ds.get("interest_arrears_balance") or 0)
        default_int = float(ds.get("default_interest_balance") or 0)
        penalty_int = float(ds.get("penalty_interest_balance") or 0)
        fees_bal = float(ds.get("fees_charges_balance") or 0)

        total_int_bal = int_accrued + int_arrears + default_int + penalty_int
        # Prefer total_exposure from DB; fall back to recomputing from buckets.
        total_outstanding = float(ds.get("total_exposure") or 0) or (
            principal_not_due
            + principal_arrears
            + total_int_bal
            + fees_bal
        )

        # Interest line: Regular + Default + Penalty daily for this day
        reg = float(ds.get("regular_interest_daily") or 0)
        def_d = float(ds.get("default_interest_daily") or 0)
        pen = float(ds.get("penalty_interest_daily") or 0)
        interest_total = reg + def_d + pen
        if interest_total != 0:
            row = _blank_row()
            row["Transaction Date"] = as_of
            row["Value Date"] = as_of
            row["Narration"] = "Interest"
            row["Interest"] = round(interest_total, 2)
            row["Total Outstanding Balance"] = round(total_outstanding, 2)
            row["Unapplied funds"] = _unapplied_at(as_of)
            rows.append(row)
            sum_interest_shown += interest_total

        # Fees: each increase in Fees & Charges
        prev_fees = prev_state.get("fees_charges_balance", 0) if prev_state is not None else 0
        if fees_bal > prev_fees:
            fee_inc = fees_bal - prev_fees
            if fee_inc > 0:
                row = _blank_row()
                row["Transaction Date"] = as_of
                row["Value Date"] = as_of
                row["Narration"] = "Fees & Charges"
                row["Fees"] = round(fee_inc, 2)
                row["Total Outstanding Balance"] = round(total_outstanding, 2)
                row["Unapplied funds"] = _unapplied_at(as_of)
                rows.append(row)
        if prev_state is None:
            prev_state = {}
        prev_state["principal_not_due"] = float(ds.get("principal_not_due") or 0)
        prev_state["fees_charges_balance"] = fees_bal

    # Credits (and reversals): each receipt on its value date (one row per receipt for reconciliation)
    for vd in sorted(repayments_by_date.keys()):
        for r in repayments_by_date[vd]:
            amount = float(r.get("amount") or 0)
            row = _blank_row()
            row["Transaction Date"] = _date_conv(r.get("payment_date")) or vd
            row["Value Date"] = vd
            ref = (r.get("customer_reference") or "").strip() or "Receipt"
            row["Narration"] = f"Reversal of {ref}" if amount < 0 else ref
            row["Credits"] = round(amount, 2)  # positive = payment, negative = reversal
            alloc_int = float(r.get("alloc_interest_total") or 0)
            alloc_fees = float(r.get("alloc_fees_total") or 0)
            alloc_cap = float(r.get("alloc_principal_total") or 0)
            row["Portion of Credit Allocated to Interest"] = round(alloc_int, 2)
            row["Credit Allocated to Fees"] = round(alloc_fees, 2)
            row["Credit Allocated to Capital"] = round(alloc_cap, 2)
            # Allocation buckets 1-5 (waterfall)
            row["Alloc (1) Fees"] = round(float(r.get("alloc_fees_charges") or 0), 2)
            row["Alloc (2) Penalty Interest"] = round(float(r.get("alloc_penalty_interest") or 0), 2)
            row["Alloc (3) Default Interest"] = round(float(r.get("alloc_default_interest") or 0), 2)
            row["Alloc (4) Interest Arrears"] = round(float(r.get("alloc_interest_arrears") or 0), 2)
            row["Alloc (5) Principal Arrears"] = round(float(r.get("alloc_principal_arrears") or 0), 2)
            tot = _balance_at_date(vd)
            if tot is None:
                bal = get_loan_daily_state_balances(loan_id, vd)
                if bal:
                    total_int = (
                        bal["interest_accrued_balance"]
                        + bal["interest_arrears_balance"]
                        + bal["default_interest_balance"]
                        + bal["penalty_interest_balance"]
                    )
                    tot = bal["principal_not_due"] + bal["principal_arrears"] + total_int + bal["fees_charges_balance"]
            if tot is not None:
                row["Total Outstanding Balance"] = round(tot, 2)
            row["Unapplied funds"] = round(_unapplied_at(vd), 2)
            rows.append(row)

    # Sort rows by value date then transaction date then narration
    rows.sort(key=lambda r: (r.get("Value Date") or date(9999, 12, 31), r.get("Transaction Date") or date(9999, 12, 31), r.get("Narration") or ""))

    end_balances = get_loan_daily_state_balances(loan_id, end)
    # Current period to date from stored period-to-date columns (no calc, one read)
    if end_balances:
        reg_p = end_balances.get("regular_interest_period_to_date") or 0
        pen_p = end_balances.get("penalty_interest_period_to_date") or 0
        def_p = end_balances.get("default_interest_period_to_date") or 0
        current_period_interest = reg_p + pen_p + def_p
        if abs(current_period_interest) > 0.01:
            total_int_end = (
                end_balances["interest_accrued_balance"] + end_balances["interest_arrears_balance"]
                + end_balances["default_interest_balance"] + end_balances["penalty_interest_balance"]
            )
            principal_total_end = end_balances["principal_not_due"] + end_balances["principal_arrears"]
            fees_end = end_balances["fees_charges_balance"]
            total_outstanding_end = principal_total_end + total_int_end + fees_end
            row = _blank_row()
            row["Transaction Date"] = end
            row["Value Date"] = end
            row["Narration"] = "Interest (current period to date)"
            row["Interest"] = round(current_period_interest, 2)
            row["Total Outstanding Balance"] = round(total_outstanding_end, 2)
            row["Unapplied funds"] = round(unapplied_at_end, 2)
            rows.append(row)

    # Closing line: as at end date
    row = _blank_row()
    row["Transaction Date"] = end
    row["Value Date"] = end
    row["Narration"] = f"Total outstanding balance as at {end.isoformat()}"
    if end_balances:
        total_interest_balance_end = (
            end_balances["interest_accrued_balance"] + end_balances["interest_arrears_balance"]
            + end_balances["default_interest_balance"] + end_balances["penalty_interest_balance"]
        )
        principal_total_end = (
            end_balances["principal_not_due"] + end_balances["principal_arrears"]
        )
        fees_end = end_balances["fees_charges_balance"]
        total_outstanding_end = principal_total_end + total_interest_balance_end + fees_end
        row["Total Outstanding Balance"] = round(total_outstanding_end, 2)
    row["Unapplied funds"] = round(unapplied_at_end, 2)
    rows.append(row)

    # Sort: by value date, then transaction date, then closing line last
    def _sort_key(r: dict) -> tuple:
        v = r.get("Value Date") or date(9999, 12, 31)
        t = r.get("Transaction Date") or date(9999, 12, 31)
        n = (r.get("Narration") or "")
        last = 1 if n.startswith("Total outstanding balance as at") else 0
        return (v, t, last)
    rows.sort(key=_sort_key)

    return rows, meta


def generate_customer_loan_statement_periodic(
    loan_id: int,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    as_of_date: date | None = None,
    allowed_customer_ids: list[int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Generate a periodic (monthly/schedule) statement: one row per due date with interest
    (scheduled amount that moved to interest arrears), penalty and default as sums of daily
    accruals for that schedule period, principal/arrears/fees/credits. Current period interest
    (accrual + penalty + default since last due) added at end when statement end is not a due date.
    """
    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    if allowed_customer_ids is not None:
        cust_id = loan.get("customer_id")
        if cust_id is None or cust_id not in allowed_customer_ids:
            raise ValueError("You are not authorized to view this loan statement.")

    disbursement = _date_conv(loan.get("disbursement_date") or loan.get("start_date"))
    today = as_of_date or date.today()
    start = start_date or disbursement or today
    end = end_date or today
    if start > end:
        start, end = end, start

    meta = {
        "loan_id": loan_id,
        "customer_id": loan.get("customer_id"),
        "start_date": start,
        "end_date": end,
        "loan_type": loan.get("loan_type"),
        "principal": float(loan.get("principal") or 0),
        "currency": (loan.get("metadata") or {}).get("currency") or "USD",
        "generated_at": datetime.now(),
        "statement_type": "periodic",
    }

    schedule_lines = get_schedule_lines(loan_id)
    if not schedule_lines:
        return [], meta

    # Build (due_date, period_start, interest_component, principal_component) for each due in range
    prev_due: date | None = None
    period_start = disbursement or start
    due_entries: list[tuple[date, date, float, float]] = []  # (due_date, period_start, interest, principal)
    for sl in schedule_lines:
        due_d = _parse_schedule_date(sl.get("Date"))
        if not due_d:
            continue
        if prev_due is not None:
            period_start = prev_due
        interest_c = float(sl.get("interest") or sl.get("Interest") or 0)
        principal_c = float(sl.get("principal") or sl.get("Principal") or 0)
        if start <= due_d <= end:
            due_entries.append((due_d, period_start, interest_c, principal_c))
        prev_due = due_d

    daily_states = get_loan_daily_state_range(loan_id, start, end)
    repayments = get_repayments_with_allocations(loan_id, start, end, include_reversed=True)
    try:
        unapplied_entries = get_unapplied_entries(loan_id, end)
    except Exception:
        unapplied_entries = []
    unapplied_at_end = sum(amt for _, amt in unapplied_entries)

    def _unapplied_at(d: date) -> float:
        return round(sum(amt for vd, amt in unapplied_entries if vd <= d), 2)

    ds_by_date: dict[date, dict] = {}
    for ds in daily_states:
        ad = _date_conv(ds.get("as_of_date"))
        if ad:
            ds_by_date[ad] = ds

    def _state_at(d: date) -> dict | None:
        for d0 in sorted(ds_by_date.keys(), reverse=True):
            if d0 <= d:
                return ds_by_date[d0]
        return get_loan_daily_state_balances(loan_id, d)

    def _total_outstanding(ds: dict) -> float:
        return (
            float(ds.get("principal_not_due") or 0)
            + float(ds.get("principal_arrears") or 0)
            + float(ds.get("interest_accrued_balance") or 0)
            + float(ds.get("interest_arrears_balance") or 0)
            + float(ds.get("default_interest_balance") or 0)
            + float(ds.get("penalty_interest_balance") or 0)
            + float(ds.get("fees_charges_balance") or 0)
        )

    rows: list[dict[str, Any]] = []
    processed_repayment_ids: set[int] = set()
    fac = float(loan.get("principal") or loan.get("disbursed_amount") or 0)

    if disbursement and start <= disbursement <= end and fac > 0:
        row = _blank_row_periodic()
        row["Due Date"] = disbursement
        row["Narration"] = "Disbursement"
        row["Principal"] = round(fac, 2)
        rows.append(row)

    prev_fees = 0.0
    for due_d, period_start_d, interest_c, principal_c in due_entries:
        # Period totals from stored columns (state at last day of period = due_d - 1)
        state_last_of_period = _state_at(due_d - timedelta(days=1)) if due_d > period_start_d else None
        if state_last_of_period and "penalty_interest_period_to_date" in state_last_of_period:
            sum_penalty = float(state_last_of_period.get("penalty_interest_period_to_date") or 0)
            sum_default = float(state_last_of_period.get("default_interest_period_to_date") or 0)
        else:
            sum_penalty = sum_default = 0.0
            for ds in daily_states:
                ad = _date_conv(ds.get("as_of_date"))
                if not ad or ad < period_start_d or ad >= due_d:
                    continue
                sum_penalty += float(ds.get("penalty_interest_daily") or 0)
                sum_default += float(ds.get("default_interest_daily") or 0)

        state_at_due = _state_at(due_d)
        principal_arrears = float(state_at_due.get("principal_arrears") or 0) if state_at_due else 0.0
        fees_now = float(state_at_due.get("fees_charges_balance") or 0) if state_at_due else 0.0
        fees_in_period = max(0.0, fees_now - prev_fees)
        prev_fees = fees_now
        tot_out = _total_outstanding(state_at_due) if state_at_due else None

        # Narration from row content (Interest & Principal & Fees etc.) instead of redundant "Due date"
        parts = []
        if interest_c and interest_c > 0:
            parts.append("Interest")
        if principal_c and principal_c > 0:
            parts.append("Principal")
        if fees_in_period and fees_in_period > 0:
            parts.append("Fees")
        if sum_penalty and sum_penalty > 0:
            parts.append("Penalty")
        if sum_default and sum_default > 0:
            parts.append("Default")
        due_narration = " & ".join(parts) if parts else "Due date"

        row = _blank_row_periodic()
        row["Due Date"] = due_d
        row["Narration"] = due_narration
        row["Interest"] = round(interest_c, 2)
        row["Penalty"] = round(sum_penalty, 2)
        row["Default"] = round(sum_default, 2)
        row["Principal"] = round(principal_c, 2)
        row["Fees"] = round(fees_in_period, 2) if fees_in_period else 0
        row["Total Outstanding Balance"] = round(tot_out, 2) if tot_out is not None else 0
        row["Arrears"] = round(principal_arrears, 2)
        row["Unapplied funds"] = _unapplied_at(due_d)
        rows.append(row)

        # Receipts in period: value_date in (period_start_d, due_d] (due date is first day of next period)
        for r in repayments:
            vd = _date_conv(r.get("value_date") or r.get("payment_date"))
            if not vd or vd <= period_start_d or vd > due_d:
                continue
            amount = float(r.get("amount") or 0)
            rec_row = _blank_row_periodic()
            rec_row["Due Date"] = vd
            ref = (r.get("customer_reference") or "").strip() or "Receipt"
            rec_row["Narration"] = f"Reversal of {ref}" if amount < 0 else ref
            rec_row["Credits"] = round(amount, 2)  # positive = payment, negative = reversal
            rec_row["Portion of Credit Allocated to Interest"] = round(float(r.get("alloc_interest_total") or 0), 2)
            rec_row["Credit Allocated to Fees"] = round(float(r.get("alloc_fees_total") or 0), 2)
            rec_row["Credit Allocated to Capital"] = round(float(r.get("alloc_principal_total") or 0), 2)
            bal = _state_at(vd)
            if bal:
                rec_row["Total Outstanding Balance"] = round(_total_outstanding(bal), 2)
                rec_row["Arrears"] = round(float(bal.get("principal_arrears") or 0), 2)
            else:
                rec_row["Total Outstanding Balance"] = 0
                rec_row["Arrears"] = 0
            rec_row["Unapplied funds"] = _unapplied_at(vd)
            rows.append(rec_row)
            if r.get("id") is not None:
                try:
                    processed_repayment_ids.add(int(r["id"]))
                except (TypeError, ValueError):
                    pass

    # Any receipts not falling into a schedule period but within [start, end] should still appear as credits
    for r in repayments:
        rid = r.get("id")
        try:
            rid_int = int(rid) if rid is not None else None
        except (TypeError, ValueError):
            rid_int = None
        if rid_int is not None and rid_int in processed_repayment_ids:
            continue
        vd = _date_conv(r.get("value_date") or r.get("payment_date"))
        if not vd or vd < start or vd > end:
            continue
        amount = float(r.get("amount") or 0)
        rec_row = _blank_row_periodic()
        rec_row["Due Date"] = vd
        ref = (r.get("customer_reference") or "").strip() or "Receipt"
        rec_row["Narration"] = f"Reversal of {ref}" if amount < 0 else ref
        rec_row["Credits"] = round(amount, 2)
        rec_row["Portion of Credit Allocated to Interest"] = round(float(r.get("alloc_interest_total") or 0), 2)
        rec_row["Credit Allocated to Fees"] = round(float(r.get("alloc_fees_total") or 0), 2)
        rec_row["Credit Allocated to Capital"] = round(float(r.get("alloc_principal_total") or 0), 2)
        bal = _state_at(vd)
        if bal:
            rec_row["Total Outstanding Balance"] = round(_total_outstanding(bal), 2)
            rec_row["Arrears"] = round(float(bal.get("principal_arrears") or 0), 2)
        else:
            rec_row["Total Outstanding Balance"] = 0
            rec_row["Arrears"] = 0
        rec_row["Unapplied funds"] = _unapplied_at(vd)
        rows.append(rec_row)

    # Current period interest: from stored period-to-date at end (one read, no summing)
    last_due_in_range = due_entries[-1][0] if due_entries else None
    if last_due_in_range and end > last_due_in_range:
        end_bal = _state_at(end)
        if end_bal:
            current_period = (
                float(end_bal.get("regular_interest_period_to_date") or 0)
                + float(end_bal.get("penalty_interest_period_to_date") or 0)
                + float(end_bal.get("default_interest_period_to_date") or 0)
            )
        else:
            current_period = 0.0
            for ds in daily_states:
                ad = _date_conv(ds.get("as_of_date"))
                if not ad or ad <= last_due_in_range or ad > end:
                    continue
                current_period += float(ds.get("regular_interest_daily") or 0) + float(ds.get("default_interest_daily") or 0) + float(ds.get("penalty_interest_daily") or 0)
        if abs(current_period) > 0.01:
            row = _blank_row_periodic()
            row["Due Date"] = end
            row["Narration"] = "Current period interest (since last due date)"
            row["Interest"] = round(current_period, 2)
            end_bal = _state_at(end)
            if end_bal:
                row["Total Outstanding Balance"] = round(_total_outstanding(end_bal), 2)
                row["Arrears"] = round(float(end_bal.get("principal_arrears") or 0), 2)
            row["Unapplied funds"] = round(unapplied_at_end, 2)
            rows.append(row)

    row = _blank_row_periodic()
    row["Due Date"] = end
    row["Narration"] = f"Total outstanding balance as at {end.isoformat()}"
    end_bal = _state_at(end)
    if end_bal:
        row["Total Outstanding Balance"] = round(_total_outstanding(end_bal), 2)
        row["Arrears"] = round(float(end_bal.get("principal_arrears") or 0), 2)
    row["Unapplied funds"] = round(unapplied_at_end, 2)
    rows.append(row)

    rows.sort(key=lambda r: (r.get("Due Date") or date(9999, 12, 31), (0 if (r.get("Narration") or "").startswith("Total outstanding") else 1)))
    return rows, meta


def generate_customer_facing_statement(
    loan_id: int,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    as_of_date: date | None = None,
    allowed_customer_ids: list[int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Generate a customer-facing loan statement from the internal periodic statement.
    Debits: interest, penalty, default, disbursement (principal only on disbursement row), fees.
    Credits, Balance, Arrears (principal arrears), Unapplied funds.
    """
    rows_periodic, meta = generate_customer_loan_statement_periodic(
        loan_id,
        start_date=start_date,
        end_date=end_date,
        as_of_date=as_of_date,
        allowed_customer_ids=allowed_customer_ids,
    )
    meta = {**meta, "statement_type": "customer_facing"}
    out: list[dict[str, Any]] = []
    for r in rows_periodic:
        narration = (r.get("Narration") or "").strip()
        credits = float(r.get("Credits") or 0)
        balance = r.get("Total Outstanding Balance")
        balance = round(float(balance), 2) if balance is not None else 0.0
        arrears = r.get("Arrears")
        arrears = round(float(arrears), 2) if arrears is not None else 0.0
        unapplied = r.get("Unapplied funds")
        unapplied = round(float(unapplied), 2) if unapplied is not None else 0.0

        if narration == "Disbursement":
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": "Disbursement",
                "Debits": round(float(r.get("Principal") or 0), 2),
                "Credits": 0.0,
                "Balance": balance,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
            })
        elif narration.startswith("Total outstanding balance"):
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration or "",
                "Debits": 0.0,
                "Credits": round(credits, 2),
                "Balance": balance,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
            })
        elif credits != 0:
            # Receipt or reversal
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration or "Receipt",
                "Debits": 0.0,
                "Credits": round(credits, 2),
                "Balance": balance,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
            })
        else:
            # Interest/penalty/default/fees row: emit separate entries for each non-zero component
            interest = float(r.get("Interest") or 0)
            penalty = float(r.get("Penalty") or 0)
            default = float(r.get("Default") or 0)
            fees = float(r.get("Fees") or 0)
            total_debits = interest + penalty + default + fees
            if abs(total_debits) > 0.01:
                out.append({
                    "Due Date": r.get("Due Date"),
                    "Narration": "Interest & Charges",
                    "Debits": round(total_debits, 2),
                    "Credits": 0.0,
                    "Balance": balance,
                    "Arrears": arrears,
                    "Unapplied funds": unapplied,
                })

    # Ensure "Total outstanding balance as at ..." line is always last in customer-facing view
    closing_index = None
    for idx, row in enumerate(out):
        narr = (row.get("Narration") or "")
        if narr.startswith("Total outstanding balance as at"):
            closing_index = idx
            break
    if closing_index is not None and closing_index != len(out) - 1:
        closing_row = out.pop(closing_index)
        out.append(closing_row)

    return out, meta


def _blank_row() -> dict[str, Any]:
    return {h: None for h in CUSTOMER_LOAN_STATEMENT_HEADINGS}


def _blank_row_periodic() -> dict[str, Any]:
    row: dict[str, Any] = {}
    for h in PERIODIC_STATEMENT_HEADINGS:
        row[h] = 0 if h in PERIODIC_NUMERIC_HEADINGS else None
    return row
