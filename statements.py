"""
Statements module: generate statements on demand (no persistence).
- Customer loan statements (this module)
- Other loan statement types (later)
- General ledger / ledger account statements (later)
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from loan_management import (
    get_loan,
    get_loan_daily_state_range,
    get_repayments_with_allocations,
    get_loan_daily_state_balances,
    get_unapplied_balance,
    get_unapplied_entries,
    get_unapplied_ledger_entries_for_statement,
    get_schedule_lines,
)


def _get_effective_date() -> date:
    """System business date for statement/amount-due logic."""
    try:
        from system_business_date import get_effective_date
        return get_effective_date()
    except Exception:
        return date.today()


def _debug_log_stmt(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    try:
        import time
        payload = {
            "sessionId": "eae17f",
            "id": f"log_{int(time.time() * 1000)}",
            "timestamp": int(time.time() * 1000),
            "location": location,
            "message": message,
            "data": data,
            "runId": "pre-fix",
            "hypothesisId": hypothesis_id,
        }
        log_path = os.path.join(os.path.dirname(__file__), "debug-eae17f.log")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass


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


def _get_drawdown_breakdown(loan: dict[str, Any]) -> list[tuple[str, float]]:
    """
    Return drawdown breakdown as [(narration, amount), ...] for statement display.
    Parts always sum to principal (facility amount) so the statement reconciles.

    Reads absolute fee amount columns (admin_fee_amount, drawdown_fee_amount,
    arrangement_fee_amount).  For older rows where these are 0/NULL, derives
    amounts from rate * principal as a fallback.

    Identity: Disbursed Amount + Administration Fees + Drawdown Fees + Arrangement Fees = Principal
    """
    principal = float(loan.get("principal") or 0)

    # Prefer stored absolute amounts; derive from rate * principal if absent
    def _fee_amount(amount_key: str, rate_key: str) -> float:
        stored = float(loan.get(amount_key) or 0)
        if stored > 0:
            return stored
        rate = float(loan.get(rate_key) or 0)
        return round(principal * rate, 2) if rate > 0 else 0.0

    admin_fee_amt = _fee_amount("admin_fee_amount", "admin_fee")
    drawdown_fee_amt = _fee_amount("drawdown_fee_amount", "drawdown_fee")
    arrangement_fee_amt = _fee_amount("arrangement_fee_amount", "arrangement_fee")

    total_fees = round(admin_fee_amt + drawdown_fee_amt + arrangement_fee_amt, 2)
    net_proceeds = round(principal - total_fees, 2)

    parts: list[tuple[str, float]] = [("Disbursed Amount", net_proceeds)]
    if admin_fee_amt > 0:
        parts.append(("Administration Fees", round(admin_fee_amt, 2)))
    if drawdown_fee_amt > 0:
        parts.append(("Drawdown Fees", round(drawdown_fee_amt, 2)))
    if arrangement_fee_amt > 0:
        parts.append(("Arrangement Fees", round(arrangement_fee_amt, 2)))
    return parts


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


CENT = Decimal("0.01")
MILLI = Decimal("0.001")


def _to_dec(v: Any) -> Decimal:
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def _q2(v: Any) -> Decimal:
    return _to_dec(v).quantize(CENT, rounding=ROUND_HALF_UP)


def _f2(v: Any) -> float:
    return float(_q2(v))


def _q3(v: Any) -> Decimal:
    return _to_dec(v).quantize(MILLI, rounding=ROUND_HALF_UP)


def _f3(v: Any) -> float:
    return float(_q3(v))


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
    today = as_of_date or _get_effective_date()
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
    unapplied_lines = get_unapplied_ledger_entries_for_statement(loan_id, start, end)
    unapplied_lines_sorted = sorted(
        unapplied_lines,
        key=lambda u: (
            _date_conv(u.get("value_date")) or date(9999, 12, 31),
            int(u.get("repayment_id") or 0),
            str(u.get("entry_kind") or ""),
        ),
    )
    # #region agent log
    _debug_log_stmt(
        "H1",
        "statements.generate_customer_loan_statement:inputs",
        "Daily statement inputs loaded",
        {
            "loan_id": loan_id,
            "start": str(start),
            "end": str(end),
            "repayments_count": len(repayments),
            "unapplied_lines_count": len(unapplied_lines),
            "unapplied_reversal_count": sum(1 for u in unapplied_lines if (u.get("entry_kind") or "").lower() == "reversal"),
            "unapplied_liquidation_count": sum(1 for u in unapplied_lines if (u.get("entry_kind") or "").lower() == "liquidation"),
        },
    )
    # #endregion

    try:
        unapplied_entries = get_unapplied_entries(loan_id, end)
    except Exception:
        unapplied_entries = []
    # #region agent log
    _debug_log_stmt(
        "H3",
        "statements.generate_customer_loan_statement_periodic:inputs",
        "Periodic statement inputs loaded",
        {
            "loan_id": loan_id,
            "start": str(start),
            "end": str(end),
            "repayments_count": len(repayments),
            "unapplied_entry_count": 0,
        },
    )
    # #endregion
    def _unapplied_at(d: date) -> float:
        return round(sum(amt for vd, amt in unapplied_entries if vd <= d), 2)
    unapplied_at_end = _unapplied_at(end)

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

    # Disbursement lines (Option 2: breakdown by loan type)
    if disbursement and start <= disbursement <= end:
        fac = float(loan.get("principal") or 0)
        if fac > 0:
            breakdown = _get_drawdown_breakdown(loan)
            for narration, amt in breakdown:
                if amt <= 0:
                    continue
                row = _blank_row()
                row["Transaction Date"] = disbursement
                row["Value Date"] = disbursement
                row["Narration"] = narration
                row["Debits"] = round(amt, 2)
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

    # Unapplied ledger movements as explicit statement lines.
    # Reversals are intentionally not emitted as separate rows because
    # the running unapplied balance already reflects them.
    for u in unapplied_lines:
        vd = _date_conv(u.get("value_date"))
        if not vd:
            continue
        kind = (u.get("entry_kind") or "").strip().lower()
        delta = float(u.get("unapplied_delta") or 0)
        if abs(delta) < 1e-9:
            continue
        if kind == "reversal":
            continue
        row = _blank_row()
        row["Transaction Date"] = vd
        row["Value Date"] = vd
        rk = u.get("repayment_key") or ""
        if kind == "liquidation":
            row["Narration"] = f"Liquidation of unapplied receipt no {rk}"
            # Internal movement only: no cash debit/credit impact on statement totals.
            row["Credits"] = 0.0
            row["Alloc (1) Fees"] = round(float(u.get("alloc_fees_charges") or 0), 2)
            row["Alloc (2) Penalty Interest"] = round(float(u.get("alloc_penalty_int") or 0), 2)
            row["Alloc (3) Default Interest"] = round(float(u.get("alloc_default_int") or 0), 2)
            row["Alloc (4) Interest Arrears"] = round(float(u.get("alloc_int_arrears") or 0), 2)
            row["Alloc (5) Principal Arrears"] = round(float(u.get("alloc_prin_arrears") or 0), 2)
        else:
            row["Narration"] = f"Unapplied from receipt no {rk}"
            row["Credits"] = 0.0
        tot = _balance_at_date(vd)
        if tot is not None:
            row["Total Outstanding Balance"] = round(tot, 2)
        row["Unapplied funds"] = round(_unapplied_at(vd), 2)
        rows.append(row)

    # #region agent log
    _debug_log_stmt(
        "H2",
        "statements.generate_customer_loan_statement:rows",
        "Daily statement rows prepared",
        {
            "loan_id": loan_id,
            "rows_total": len(rows),
            "rows_with_liquidation_narration": sum(
                1 for r in rows if "Liquidation of unapplied receipt no" in str(r.get("Narration") or "")
            ),
        },
    )
    # #endregion

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
    today = as_of_date or _get_effective_date()
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

    # Build (due_date, period_start, interest_component, principal_component, include_period_start_day)
    # for each due in range.
    prev_due: date | None = None
    period_start = disbursement or start
    due_entries: list[tuple[date, date, float, float, bool]] = []  # (due_date, period_start, interest, principal, include_start_day)
    for sl in schedule_lines:
        due_d = _parse_schedule_date(sl.get("Date"))
        if not due_d:
            continue
        is_first_period = prev_due is None
        if prev_due is not None:
            period_start = prev_due
        interest_c = float(sl.get("interest") or sl.get("Interest") or 0)
        principal_c = float(sl.get("principal") or sl.get("Principal") or 0)
        # Some schedules include a same-day stub row at disbursement/start with only
        # pro-rated interest. Exclude it from periodic statement rows to avoid
        # double-counting against the first real period movement.
        if is_first_period and due_d == (disbursement or start) and abs(principal_c) <= 1e-9:
            prev_due = due_d
            continue
        if start <= due_d <= end:
            due_entries.append((due_d, period_start, interest_c, principal_c, is_first_period))
        prev_due = due_d
    if disbursement and due_entries and due_entries[0][0] == disbursement:
        due_entries = due_entries[1:]

    daily_states = get_loan_daily_state_range(loan_id, start, end)
    repayments = get_repayments_with_allocations(loan_id, start, end, include_reversed=True)
    unapplied_lines = get_unapplied_ledger_entries_for_statement(loan_id, start, end)
    unapplied_lines_sorted = sorted(
        unapplied_lines,
        key=lambda u: (
            _date_conv(u.get("value_date")) or date(9999, 12, 31),
            int(u.get("repayment_id") or 0),
            str(u.get("entry_kind") or ""),
        ),
    )
    try:
        unapplied_entries = get_unapplied_entries(loan_id, end)
    except Exception:
        unapplied_entries = []
    def _unapplied_at(d: date) -> float:
        return _f3(sum(amt for vd, amt in unapplied_entries if vd <= d))
    unapplied_at_end = _unapplied_at(end)

    # #region agent log
    _debug_log_stmt(
        "H5",
        "statements.generate_customer_loan_statement_periodic:unapplied_lines",
        "Periodic statement unapplied ledger lines loaded",
        {
            "loan_id": loan_id,
            "unapplied_lines_count": len(unapplied_lines),
            "unapplied_reversal_count": sum(1 for u in unapplied_lines if (u.get("entry_kind") or "").lower() == "reversal"),
            "unapplied_liquidation_count": sum(1 for u in unapplied_lines if (u.get("entry_kind") or "").lower() == "liquidation"),
        },
    )
    # #endregion

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
    fac = float(loan.get("principal") or 0)

    if disbursement and start <= disbursement <= end and fac > 0:
        breakdown = _get_drawdown_breakdown(loan)
        for narration, amt in breakdown:
            if amt <= 0:
                continue
            row = _blank_row_periodic()
            row["Due Date"] = disbursement
            row["Narration"] = narration
            row["Principal"] = _f3(amt)
            rows.append(row)

    prev_fees = 0.0
    for due_d, period_start_d, interest_c, principal_c, include_start_day in due_entries:
        # Reconciliation rule: periodic lines must equal the sum of daily accruals for that period.
        # Period window uses (previous_due_date, current_due_date] for deterministic period totals.
        sum_regular = 0.0
        sum_penalty = 0.0
        sum_default = 0.0
        for ds in daily_states:
            ad = _date_conv(ds.get("as_of_date"))
            if not ad or ad > due_d:
                continue
            # Period accrual window matches engine rule: (period_start, due_date].
            if ad <= period_start_d:
                continue
            sum_regular += float(ds.get("regular_interest_daily") or 0)
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
        if sum_regular and sum_regular > 0:
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
        row["Interest"] = _f3(sum_regular)
        row["Penalty"] = _f3(sum_penalty)
        row["Default"] = _f3(sum_default)
        row["Principal"] = _f3(principal_c)
        row["Fees"] = _f3(fees_in_period) if fees_in_period else 0.0
        row["Total Outstanding Balance"] = _f3(tot_out) if tot_out is not None else 0.0
        row["Arrears"] = _f3(principal_arrears)
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
            rec_row["Credits"] = _f3(amount)  # positive = payment, negative = reversal
            rec_row["Portion of Credit Allocated to Interest"] = _f3(float(r.get("alloc_interest_total") or 0))
            rec_row["Credit Allocated to Fees"] = _f3(float(r.get("alloc_fees_total") or 0))
            rec_row["Credit Allocated to Capital"] = _f3(float(r.get("alloc_principal_total") or 0))
            bal = _state_at(vd)
            if bal:
                rec_row["Total Outstanding Balance"] = _f3(_total_outstanding(bal))
                rec_row["Arrears"] = _f3(float(bal.get("principal_arrears") or 0))
            else:
                rec_row["Total Outstanding Balance"] = 0.0
                rec_row["Arrears"] = 0.0
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
        rec_row["Credits"] = _f3(amount)
        rec_row["Portion of Credit Allocated to Interest"] = _f3(float(r.get("alloc_interest_total") or 0))
        rec_row["Credit Allocated to Fees"] = _f3(float(r.get("alloc_fees_total") or 0))
        rec_row["Credit Allocated to Capital"] = _f3(float(r.get("alloc_principal_total") or 0))
        bal = _state_at(vd)
        if bal:
            rec_row["Total Outstanding Balance"] = _f3(_total_outstanding(bal))
            rec_row["Arrears"] = _f3(float(bal.get("principal_arrears") or 0))
        else:
            rec_row["Total Outstanding Balance"] = 0.0
            rec_row["Arrears"] = 0.0
        rec_row["Unapplied funds"] = _unapplied_at(vd)
        rows.append(rec_row)

    # Unapplied ledger movements as explicit periodic rows.
    # Reversals are intentionally not emitted as separate rows because
    # the running unapplied balance already reflects them.
    for u in unapplied_lines:
        vd = _date_conv(u.get("value_date"))
        if not vd or vd < start or vd > end:
            continue
        kind = (u.get("entry_kind") or "").strip().lower()
        delta = float(u.get("unapplied_delta") or 0)
        if abs(delta) < 1e-9:
            continue
        if kind == "reversal":
            continue
        row = _blank_row_periodic()
        row["Due Date"] = vd
        rk = u.get("repayment_key") or ""
        if kind == "liquidation":
            row["Narration"] = f"Liquidation of unapplied receipt no {rk}"
            # Internal movement only: no cash debit/credit impact on statement totals.
            row["Credits"] = 0.0
            row["Portion of Credit Allocated to Interest"] = _f3(
                float(u.get("alloc_int_arrears") or 0)
                + float(u.get("alloc_penalty_int") or 0)
                + float(u.get("alloc_default_int") or 0),
            )
            row["Credit Allocated to Fees"] = _f3(float(u.get("alloc_fees_charges") or 0))
            row["Credit Allocated to Capital"] = _f3(float(u.get("alloc_prin_arrears") or 0))
        else:
            row["Narration"] = f"Unapplied from receipt no {rk}"
            row["Credits"] = 0.0
        bal = _state_at(vd)
        if bal:
            row["Total Outstanding Balance"] = _f3(_total_outstanding(bal))
            row["Arrears"] = _f3(float(bal.get("principal_arrears") or 0))
        row["Unapplied funds"] = _unapplied_at(vd)
        rows.append(row)

    # Table-derived non-cash movement residual (explicit, never hidden):
    # opening + charges + reversals - receipts + closing_unapplied = closing_balance
    # => required_charges = closing_balance - closing_unapplied - opening - reversals + receipts
    closing_state = _state_at(end)
    closing_balance = _total_outstanding(closing_state) if closing_state else 0.0
    positive_receipts = sum(max(0.0, float(r.get("amount") or 0)) for r in repayments)
    reversal_debits = sum(abs(min(0.0, float(r.get("amount") or 0))) for r in repayments)
    required_charge_total = (
        closing_balance
        - unapplied_at_end
        - fac
        - reversal_debits
        + positive_receipts
    )
    current_charge_total = 0.0
    for r in rows:
        narr = str(r.get("Narration") or "")
        if (
            narr == "Disbursement"
            or narr in ("Disbursed Amount", "Administration Fees", "Drawdown Fees", "Arrangement Fees", "Loan Drawdown")
            or narr.startswith("Total outstanding balance")
            or narr.startswith("Unapplied from receipt no ")
            or narr.startswith("Liquidation of unapplied receipt no ")
        ):
            continue
        if abs(float(r.get("Credits") or 0)) > 1e-9:
            continue
        current_charge_total += (
            float(r.get("Interest") or 0)
            + float(r.get("Penalty") or 0)
            + float(r.get("Default") or 0)
            + float(r.get("Fees") or 0)
        )
    non_cash_residual = _f3(required_charge_total - current_charge_total)
    if abs(non_cash_residual) > 0.0005:
        row = _blank_row_periodic()
        row["Due Date"] = end
        row["Narration"] = "Table-derived non-cash movement"
        row["Interest"] = _f3(non_cash_residual)
        if closing_state:
            row["Total Outstanding Balance"] = _f3(_total_outstanding(closing_state))
            row["Arrears"] = _f3(float(closing_state.get("principal_arrears") or 0))
        row["Unapplied funds"] = _f3(unapplied_at_end)
        rows.append(row)

    # Current (incomplete) period interest: period-to-date per type stored separately
    # so the customer-facing statement can emit one line per type.
    last_due_in_range = due_entries[-1][0] if due_entries else None
    if last_due_in_range and end > last_due_in_range:
        end_bal = _state_at(end)
        if end_bal:
            cur_regular = float(end_bal.get("regular_interest_period_to_date") or 0)
            cur_penalty = float(end_bal.get("penalty_interest_period_to_date") or 0)
            cur_default = float(end_bal.get("default_interest_period_to_date") or 0)
        else:
            cur_regular = cur_penalty = cur_default = 0.0
            for ds in daily_states:
                ad = _date_conv(ds.get("as_of_date"))
                if not ad or ad <= last_due_in_range or ad > end:
                    continue
                cur_regular += float(ds.get("regular_interest_daily") or 0)
                cur_penalty += float(ds.get("penalty_interest_daily") or 0)
                cur_default += float(ds.get("default_interest_daily") or 0)
        current_period_total = cur_regular + cur_penalty + cur_default
        if abs(current_period_total) > 0.01:
            row = _blank_row_periodic()
            row["Due Date"] = end
            row["Narration"] = "Current period interest (since last due date)"
            row["Interest"] = _f3(cur_regular)
            row["Penalty"] = _f3(cur_penalty)
            row["Default"] = _f3(cur_default)
            end_bal = _state_at(end)
            if end_bal:
                row["Total Outstanding Balance"] = _f3(_total_outstanding(end_bal))
                row["Arrears"] = _f3(float(end_bal.get("principal_arrears") or 0))
            row["Unapplied funds"] = _f3(unapplied_at_end)
            rows.append(row)

    row = _blank_row_periodic()
    row["Due Date"] = end
    row["Narration"] = f"Total outstanding balance as at {end.isoformat()}"
    end_bal = _state_at(end)
    if end_bal:
        row["Total Outstanding Balance"] = _f3(_total_outstanding(end_bal))
        row["Arrears"] = _f3(float(end_bal.get("principal_arrears") or 0))
    row["Unapplied funds"] = _f3(unapplied_at_end)
    rows.append(row)

    rows.sort(key=lambda r: (r.get("Due Date") or date(9999, 12, 31), (0 if (r.get("Narration") or "").startswith("Total outstanding") else 1)))
    # #region agent log
    period_charge_rows = [
        {
            "due_date": str(r.get("Due Date")),
            "narration": str(r.get("Narration") or ""),
            "interest": round(float(r.get("Interest") or 0), 3),
            "penalty": round(float(r.get("Penalty") or 0), 3),
            "default": round(float(r.get("Default") or 0), 3),
            "principal": round(float(r.get("Principal") or 0), 3),
            "credits": round(float(r.get("Credits") or 0), 3),
            "unapplied": round(float(r.get("Unapplied funds") or 0), 3),
        }
        for r in rows
        if not str(r.get("Narration") or "").startswith("Total outstanding")
    ]
    _debug_log_stmt(
        "H_PERIOD_WINDOWS",
        "statements.generate_customer_loan_statement_periodic:windows",
        "Periodic rows detailed windows",
        {
            "loan_id": loan_id,
            "rows_preview": period_charge_rows[:12],
            "rows_tail": period_charge_rows[-6:],
        },
    )
    # #endregion
    # #region agent log
    _debug_log_stmt(
        "H_PERIODIC_SUMS",
        "statements.generate_customer_loan_statement_periodic:totals",
        "Periodic rows totals for reconciliation tracing",
        {
            "loan_id": loan_id,
            "start": str(start),
            "end": str(end),
            "rows": len(rows),
            "sum_interest": round(sum(float(r.get("Interest") or 0) for r in rows), 3),
            "sum_penalty": round(sum(float(r.get("Penalty") or 0) for r in rows), 3),
            "sum_default": round(sum(float(r.get("Default") or 0) for r in rows), 3),
            "sum_principal": round(sum(float(r.get("Principal") or 0) for r in rows), 3),
            "sum_fees": round(sum(float(r.get("Fees") or 0) for r in rows), 3),
            "sum_credits": round(sum(float(r.get("Credits") or 0) for r in rows), 3),
            "closing_balance": round(float((rows[-1].get("Total Outstanding Balance") if rows else 0) or 0), 3),
            "closing_unapplied": round(float((rows[-1].get("Unapplied funds") if rows else 0) or 0), 3),
        },
    )
    # #endregion
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
    # #region agent log
    _debug_log_stmt(
        "H4",
        "statements.generate_customer_facing_statement:source_rows",
        "Customer-facing statement built from periodic rows",
        {
            "loan_id": loan_id,
            "periodic_rows_count": len(rows_periodic),
        },
    )
    # #endregion
    meta = {**meta, "statement_type": "customer_facing"}
    out: list[dict[str, Any]] = []
    for r in rows_periodic:
        narration = (r.get("Narration") or "").strip()
        credits = _to_dec(r.get("Credits") or 0)
        # Balance is recomputed as running balance after the loop; placeholder 0.0 for now.
        arrears = _f3(r.get("Arrears") or 0)
        unapplied = _f3(r.get("Unapplied funds") or 0)

        if narration in ("Disbursement", "Disbursed Amount", "Administration Fees", "Drawdown Fees", "Arrangement Fees", "Loan Drawdown"):
            debits = _f3(r.get("Principal") or 0)
            if debits > 0:
                out.append({
                    "Due Date": r.get("Due Date"),
                    "Narration": narration,
                    "Debits": debits,
                    "Credits": 0.0,
                    "Balance": 0.0,
                    "Arrears": arrears,
                    "Unapplied funds": unapplied,
                })
        elif narration.startswith("Total outstanding balance"):
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration or "",
                "Debits": 0.0,
                "Credits": _f3(credits),
                "Balance": 0.0,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
            })
        elif narration == "Table-derived non-cash movement":
            internal_amt = (
                _to_dec(r.get("Interest") or 0)
                + _to_dec(r.get("Penalty") or 0)
                + _to_dec(r.get("Default") or 0)
                + _to_dec(r.get("Fees") or 0)
            )
            if internal_amt >= Decimal("0"):
                out.append({
                    "Due Date": r.get("Due Date"),
                    "Narration": narration,
                    "Debits": _f3(internal_amt),
                    "Credits": 0.0,
                    "Balance": 0.0,
                    "Arrears": arrears,
                    "Unapplied funds": unapplied,
                })
            else:
                out.append({
                    "Due Date": r.get("Due Date"),
                    "Narration": narration,
                    "Debits": 0.0,
                    "Credits": _f3(abs(internal_amt)),
                    "Balance": 0.0,
                    "Arrears": arrears,
                    "Unapplied funds": unapplied,
                })
        elif credits != Decimal("0"):
            # Customer cash movement only:
            # positive receipt -> Credits, negative reversal -> Debits.
            if credits > Decimal("0"):
                out.append({
                    "Due Date": r.get("Due Date"),
                    "Narration": narration or "Receipt",
                    "Debits": 0.0,
                    "Credits": _f3(credits),
                    "Balance": 0.0,
                    "Arrears": arrears,
                    "Unapplied funds": unapplied,
                })
            else:
                out.append({
                    "Due Date": r.get("Due Date"),
                    "Narration": narration or "Reversal",
                    "Debits": _f3(abs(credits)),
                    "Credits": 0.0,
                    "Balance": 0.0,
                    "Arrears": arrears,
                    "Unapplied funds": unapplied,
                })
        else:
            # Scheduled-period charges: emit separate rows per component.
            # Quantize to 3dp and distribute residual to largest component so
            # component rows sum exactly to the quantized period total.
            interest = _to_dec(r.get("Interest") or 0)
            penalty = _to_dec(r.get("Penalty") or 0)
            default = _to_dec(r.get("Default") or 0)
            fees = _to_dec(r.get("Fees") or 0)
            # Incomplete current period: suffix each component with "(period to date)"
            is_current_period = narration == "Current period interest (since last due date)"
            sfx = " (period to date)" if is_current_period else ""
            raw_components = [
                (f"Accrued interest{sfx}", interest),
                (f"Penalty interest{sfx}", penalty),
                (f"Default interest{sfx}", default),
                ("Fees & Charges", fees),
            ]
            raw_components = [(n, v) for n, v in raw_components if abs(v) > Decimal("0")]
            if raw_components:
                rounded_total = _q3(sum((v for _, v in raw_components), Decimal("0")))
                rounded_components = [(n, _q3(v), v) for n, v in raw_components]
                rounded_sum = sum((v for _, v, _ in rounded_components), Decimal("0"))
                residual = rounded_total - rounded_sum
                if residual != Decimal("0"):
                    idx = max(range(len(rounded_components)), key=lambda i: abs(rounded_components[i][2]))
                    n, rv, raw_v = rounded_components[idx]
                    rounded_components[idx] = (n, rv + residual, raw_v)
                for n, rv, _raw_v in rounded_components:
                    if rv == Decimal("0"):
                        continue
                    out.append({
                        "Due Date": r.get("Due Date"),
                        "Narration": n,
                        "Debits": _f3(rv),
                        "Credits": 0.0,
                        "Balance": 0.0,
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

    # Compute running balance: Balance[i] = Balance[i-1] + Debits[i] - Credits[i]
    running_bal = Decimal("0")
    for _rb_row in out:
        running_bal += _to_dec(_rb_row.get("Debits") or 0) - _to_dec(_rb_row.get("Credits") or 0)
        _rb_row["Balance"] = _f3(running_bal)

    # Deterministic reconciliation payload for audits/debugging.
    closing_row = out[-1] if out else {}
    closing_unapplied_d = _to_dec(closing_row.get("Unapplied funds") or 0)
    # Use engine closing balance for reconciliation (running balance is display-only).
    closing_balance_d = _to_dec(
        (rows_periodic[-1].get("Total Outstanding Balance") if rows_periodic else 0) or 0
    )
    total_debits_d = sum((_to_dec(r.get("Debits") or 0) for r in out), Decimal("0"))
    total_credits_d = sum((_to_dec(r.get("Credits") or 0) for r in out), Decimal("0"))
    formula_lhs_d = total_debits_d - total_credits_d + closing_unapplied_d
    formula_diff_d = formula_lhs_d - closing_balance_d

    # Customer cash inflow/outflow (external) split.
    reversal_debits_d = sum(
        (
            _to_dec(r.get("Debits") or 0)
            for r in out
            if str(r.get("Narration") or "").startswith("Reversal of ")
        ),
        Decimal("0"),
    )
    external_cash_credits_d = sum(
        (
            _to_dec(r.get("Credits") or 0)
            for r in out
            if _to_dec(r.get("Credits") or 0) > Decimal("0")
            and not str(r.get("Narration") or "").startswith("Liquidation of unapplied receipt no ")
            and str(r.get("Narration") or "") != "Table-derived non-cash movement"
        ),
        Decimal("0"),
    )

    # Internal movement sourced from periodic rows (liquidation allocation sum).
    internal_liquidation_applied_d = Decimal("0")
    for r in rows_periodic:
        narr = str(r.get("Narration") or "")
        if narr.startswith("Liquidation of unapplied receipt no "):
            internal_liquidation_applied_d += (
                _to_dec(r.get("Portion of Credit Allocated to Interest") or 0)
                + _to_dec(r.get("Credit Allocated to Fees") or 0)
                + _to_dec(r.get("Credit Allocated to Capital") or 0)
            )

    meta["reconciliation"] = {
        "total_debits": _f3(total_debits_d),
        "total_credits": _f3(total_credits_d),
        "external_cash_credits": _f3(external_cash_credits_d),
        "reversal_debits": _f3(reversal_debits_d),
        "internal_liquidation_applied": _f3(internal_liquidation_applied_d),
        "closing_unapplied": _f3(closing_unapplied_d),
        "closing_balance": _f3(closing_balance_d),
        "formula_lhs": _f3(formula_lhs_d),
        "formula_diff": _f3(formula_diff_d),
        "rounding_adjustment_applied": 0.0,
    }
    # #region agent log
    _debug_log_stmt(
        "H_FACING_BUCKETS",
        "statements.generate_customer_facing_statement:bucket_totals",
        "Customer-facing bucket totals for residual tracing",
        {
            "loan_id": loan_id,
            "rows": len(out),
            "sum_debits_disbursement": round(sum(float(r.get("Debits") or 0) for r in out if str(r.get("Narration") or "") == "Disbursement"), 3),
            "sum_debits_reversals": round(sum(float(r.get("Debits") or 0) for r in out if str(r.get("Narration") or "").startswith("Reversal of ")), 3),
            "sum_debits_charges": round(sum(float(r.get("Debits") or 0) for r in out if str(r.get("Narration") or "") in {
                    "Accrued interest", "Penalty interest", "Default interest", "Fees & Charges",
                    "Accrued interest (period to date)", "Penalty interest (period to date)", "Default interest (period to date)",
                }), 3),
            "sum_credits_receipts": round(sum(float(r.get("Credits") or 0) for r in out if float(r.get("Credits") or 0) > 0), 3),
            "required_charges_for_identity": round(
                (
                    float(closing_balance_d)
                    + sum(float(r.get("Credits") or 0) for r in out)
                    - float(closing_unapplied_d)
                    - sum(float(r.get("Debits") or 0) for r in out if str(r.get("Narration") or "") == "Disbursement")
                    - sum(float(r.get("Debits") or 0) for r in out if str(r.get("Narration") or "").startswith("Reversal of "))
                ),
                3,
            ),
            "closing_balance": _f3(closing_balance_d),
            "closing_unapplied": _f3(closing_unapplied_d),
            "formula_lhs": _f3(formula_lhs_d),
            "formula_diff": _f3(formula_diff_d),
            "charge_rows": [
                {
                    "due_date": str(r.get("Due Date")),
                    "narration": str(r.get("Narration") or ""),
                    "debits": round(float(r.get("Debits") or 0), 3),
                    "credits": round(float(r.get("Credits") or 0), 3),
                }
                for r in out
                if str(r.get("Narration") or "") in {
                    "Accrued interest", "Penalty interest", "Default interest", "Fees & Charges",
                    "Accrued interest (period to date)", "Penalty interest (period to date)", "Default interest (period to date)",
                }
            ],
            "unapplied_last5": [
                {
                    "due_date": str(r.get("Due Date")),
                    "narration": str(r.get("Narration") or ""),
                    "unapplied": float(r.get("Unapplied funds") or 0),
                }
                for r in out[-5:]
            ],
        },
    )
    # #endregion

    # Hard correctness gate: non-zero residual means statement construction bug.
    if _q3(formula_diff_d) != Decimal("0"):
        raise ValueError(
            "Statement reconciliation bug: Debits - Credits + Unapplied != Balance "
            f"(diff={_f3(formula_diff_d)})"
        )

    return out, meta


def _blank_row() -> dict[str, Any]:
    return {h: None for h in CUSTOMER_LOAN_STATEMENT_HEADINGS}


def _blank_row_periodic() -> dict[str, Any]:
    row: dict[str, Any] = {}
    for h in PERIODIC_STATEMENT_HEADINGS:
        row[h] = 0 if h in PERIODIC_NUMERIC_HEADINGS else None
    return row
