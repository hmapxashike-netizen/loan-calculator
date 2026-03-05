"""
Statements module: generate statements on demand (no persistence).
- Customer loan statements (this module)
- Other loan statement types (later)
- General ledger / ledger account statements (later)
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from loan_management import (
    get_loan,
    get_loan_daily_state_range,
    get_repayments_with_allocations,
    get_loan_daily_state_balances,
    get_unapplied_balance,
)


# Column headings per statement spec
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
    "Total Outstanding Balance",
    "Unapplied funds",
]


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
      - meta: loan_id, customer_id, start_date, end_date, loan_type, facility, currency

    If start_date/end_date are None: start = loan disbursement/start, end = today (or as_of_date).
    "Total Outstanding Balance" shows the total loan exposure (principal + all interest buckets + fees)
    on each line, excluding unapplied funds. When statement end date is not a due date, an interest
    line for "current period to date" is added so total exposure includes interest accrued to date.
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
        "facility": float(loan.get("facility") or 0),
        "currency": "USD",
        "generated_at": datetime.now(),
    }

    daily_states = get_loan_daily_state_range(loan_id, start, end)
    repayments = get_repayments_with_allocations(loan_id, start, end)

    try:
        unapplied_at_end = get_unapplied_balance(loan_id, end)
    except Exception:
        unapplied_at_end = 0.0

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
    sum_interest_shown = 0.0

    # Disbursement line (any increase in Principal Not Due on transaction date)
    if disbursement and start <= disbursement <= end:
        fac = float(loan.get("facility") or loan.get("principal") or 0)
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
            row["Unapplied funds"] = round(get_unapplied_balance(loan_id, as_of), 2)
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
                row["Unapplied funds"] = round(get_unapplied_balance(loan_id, as_of), 2)
                rows.append(row)
        if prev_state is None:
            prev_state = {}
        prev_state["principal_not_due"] = float(ds.get("principal_not_due") or 0)
        prev_state["fees_charges_balance"] = fees_bal

    # Credits: each receipt on its value date (one row per receipt for reconciliation)
    for vd in sorted(repayments_by_date.keys()):
        for r in repayments_by_date[vd]:
            amount = float(r.get("amount") or 0)
            if amount <= 0:
                continue
            row = _blank_row()
            row["Transaction Date"] = _date_conv(r.get("payment_date")) or vd
            row["Value Date"] = vd
            row["Narration"] = (r.get("customer_reference") or "Receipt").strip() or "Receipt"
            row["Credits"] = round(amount, 2)
            alloc_int = float(r.get("alloc_interest_total") or 0)
            alloc_fees = float(r.get("alloc_fees_total") or 0)
            alloc_cap = float(r.get("alloc_principal_total") or 0)
            row["Portion of Credit Allocated to Interest"] = round(alloc_int, 2)
            row["Credit Allocated to Fees"] = round(alloc_fees, 2)
            row["Credit Allocated to Capital"] = round(alloc_cap, 2)
            bal = get_loan_daily_state_balances(loan_id, vd)
            if bal:
                total_int = (
                    bal["interest_accrued_balance"]
                    + bal["interest_arrears_balance"]
                    + bal["default_interest_balance"]
                    + bal["penalty_interest_balance"]
                )
                principal_total = bal["principal_not_due"] + bal["principal_arrears"]
                fees_bal = bal["fees_charges_balance"]
                total_outstanding = principal_total + total_int + fees_bal
                row["Total Outstanding Balance"] = round(total_outstanding, 2)
            # Show this receipt's unapplied amount on its own row for reconciliation (no running total).
            if (alloc_int + alloc_fees + alloc_cap) < 0.01 and amount > 0:
                row["Unapplied funds"] = round(amount, 2)
            else:
                row["Unapplied funds"] = round(get_unapplied_balance(loan_id, vd), 2)
            rows.append(row)

    # Sort rows by value date then transaction date then narration
    rows.sort(key=lambda r: (r.get("Value Date") or date(9999, 12, 31), r.get("Transaction Date") or date(9999, 12, 31), r.get("Narration") or ""))

    # Interest for current period to date (when statement end is not a due date)
    # So total exposure includes interest accrued to date
    end_balances = get_loan_daily_state_balances(loan_id, end)
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
        interest_stub = total_interest_balance_end - sum_interest_shown
        if abs(interest_stub) > 0.01:
            row = _blank_row()
            row["Transaction Date"] = end
            row["Value Date"] = end
            row["Narration"] = "Interest (current period to date)"
            row["Interest"] = round(interest_stub, 2)
            row["Total Outstanding Balance"] = round(total_outstanding_end, 2)
            row["Unapplied funds"] = round(unapplied_at_end, 2)
            rows.append(row)

    # Closing line: as at end date (total interest balance, unapplied funds)
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


def _blank_row() -> dict[str, Any]:
    return {h: None for h in CUSTOMER_LOAN_STATEMENT_HEADINGS}
