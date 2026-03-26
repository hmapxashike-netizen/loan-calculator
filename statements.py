"""
Statements module: generate statements on demand (no persistence).
- Customer loan statements (this module)
- Other loan statement types (later)
- General ledger / ledger account statements (later)
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from loan_management import (
    get_loan,
    get_loan_daily_state_range,
    get_repayments_with_allocations,
    get_loan_daily_state_balances,
    get_repayment_opening_delinquency_total,
    get_unapplied_balance,
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


# Column headings per statement spec (allocation 1-5 = fees, penalty, default interest, interest arrears, principal arrears)
# These five columns are shown in a collapsible section in the UI to reduce clutter
ALLOC_BUCKET_COLUMNS = [
    "Alloc (1) Fees",
    "Alloc (2) Penalty Interest",
    "Alloc (3) Default Interest",
    "Alloc (4) Interest Arrears",
    "Alloc (5) Principal Arrears",
]
# Periodic (monthly/schedule) statement: one row per due date + receipts in period; interest/penalty/default summed per period
# Arrears = total delinquency (principal + interest arrears + default + penalty + fees balances)
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
# Arrears = total delinquency (principal + interest arrears + default + penalty + fees). Receipts = one line per receipt.
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


def _total_delinquency_arrears(ds: dict | None) -> float:
    """
    Total delinquency from loan_daily_state: amounts past due / in arrears buckets
    (excludes principal not yet due and unbilled accrued interest).
    principal_arrears + interest_arrears_balance + default_interest_balance
    + penalty_interest_balance + fees_charges_balance
    """
    if not ds:
        return 0.0
    return (
        float(ds.get("principal_arrears") or 0)
        + float(ds.get("interest_arrears_balance") or 0)
        + float(ds.get("default_interest_balance") or 0)
        + float(ds.get("penalty_interest_balance") or 0)
        + float(ds.get("fees_charges_balance") or 0)
    )


def _repayment_statement_narration(
    *,
    amount: float,
    repayment_id: int,
    teller_ref: str,
    original_repayment_id: Any = None,
) -> str:
    """
    One customer-facing receipt line per loan_repayments row.
    Reversals reference the original receipt id (REV n), not the reversing row's PK.
    """
    ref = (teller_ref or "").strip() or "Receipt"
    if amount < 0:
        if original_repayment_id is not None:
            try:
                oid = int(original_repayment_id)
            except (TypeError, ValueError):
                oid = 0
            if oid > 0:
                return f"REV {oid}"
        base = f"Repayment id {repayment_id}: {ref}" if repayment_id else ref
        return f"Reversal of {base}"
    return f"Repayment id {repayment_id}: {ref}" if repayment_id else ref


def _is_statement_reversal_narration(narration: str) -> bool:
    n = str(narration or "")
    if "Reversal of unapplied" in n:
        return False
    return n.startswith("REV ") or n.startswith("Reversal of")


def _is_external_cash_receipt_row(row: dict[str, Any]) -> bool:
    """
    Customer-facing rows that represent external cash in (credit, no debit).
    These are sorted after same-day accruals/charges for presentation.
    Excludes liquidation-of-unapplied (internal allocation) and closing lines.
    """
    # Bucket-component rows (e.g., "Default interest" rendered as credits for a
    # reversal) can have Credits>0. We only want actual cash receipt/reversal
    # rows, which always carry a `_repayment_id`.
    if row.get("_repayment_id") is None:
        return False
    narr = str(row.get("Narration") or "")
    credits = _to_dec(row.get("Credits") or 0)
    debits = _to_dec(row.get("Debits") or 0)
    if credits <= Decimal("0"):
        return False
    if debits != Decimal("0"):
        return False
    if narr.startswith("Liquidation of unapplied"):
        return False
    if narr.startswith("Total outstanding balance"):
        return False
    return True


def _reorder_customer_facing_rows_receipts_last(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Within each Due Date, place external cash receipts after other lines when both exist.
    Keeps the closing 'Total outstanding balance as at ...' line last overall.
    """
    closing: list[dict[str, Any]] = []
    rest: list[dict[str, Any]] = []
    for row in rows:
        narr = str(row.get("Narration") or "")
        if narr.startswith("Total outstanding balance as at"):
            closing.append(row)
        else:
            rest.append(row)

    by_date: dict[Any, list[dict[str, Any]]] = {}
    date_order: list[Any] = []
    for row in rest:
        d = row.get("Due Date")
        if d not in by_date:
            by_date[d] = []
            date_order.append(d)
        by_date[d].append(row)

    reordered: list[dict[str, Any]] = []
    for d in date_order:
        chunk = by_date[d]
        # Same-day ordering rule:
        # - Original external cash receipts (credits) should appear first (after charges)
        # - Reversal rows (debit side, REV n / legacy "Reversal of") after original receipts same day.
        rcpt = [r for r in chunk if _is_external_cash_receipt_row(r)]
        reversal = [r for r in chunk if _is_statement_reversal_narration(str(r.get("Narration") or ""))]
        # For "period to date" interest components, we intentionally display
        # them *after* receipts and reversals on the same day. This allows
        # the running Balance to drift (vs daily-state) until the
        # current-period correction lines are applied.
        def _is_period_to_date_interest_after(row: dict[str, Any]) -> bool:
            narr = str(row.get("Narration") or "")
            if not narr.endswith("(period to date)"):
                return False
            return (
                narr.startswith("Accrued interest")
                or narr.startswith("Penalty interest")
                or narr.startswith("Default interest")
            )
            
        def _is_unapplied_ledger_line(row: dict[str, Any]) -> bool:
            narr = str(row.get("Narration") or "")
            return (
                "Unapplied funds credit" in narr
                or narr.startswith("Liquidation of unapplied")
                or "Reversal of unapplied" in narr
            )

        other_before = [
            r for r in chunk 
            if r not in rcpt and r not in reversal and not _is_period_to_date_interest_after(r) and not _is_unapplied_ledger_line(r)
        ]
        other_after = [
            r for r in chunk 
            if r not in rcpt and r not in reversal and _is_period_to_date_interest_after(r)
        ]
        unapplied_lines_after = [
            r for r in chunk 
            if _is_unapplied_ledger_line(r)
        ]
        reordered.extend(other_before + rcpt + reversal + other_after + unapplied_lines_after)

    reordered.extend(closing)
    return reordered


def _apply_customer_facing_arrears_before_first_receipt(loan_id: int, out: list[dict[str, Any]]) -> None:
    """
    On dates that mix charge lines and external cash receipts, every line that appears
    before the first receipt should show delinquency at waterfall opening for that receipt
    (amounts owed immediately before that receipt is applied). Receipt rows keep the
    post-allocation arrears already supplied from periodic data (end-of-day / post-receipt).
    """
    from collections import defaultdict

    by_date: dict[Any, list[int]] = defaultdict(list)
    for i, row in enumerate(out):
        by_date[row.get("Due Date")].append(i)

    for d, indices in by_date.items():
        d_conv = _date_conv(d) if d is not None else None
        if d_conv is None:
            continue
        rows_for_d = [out[i] for i in indices]
        has_external_receipt = any(
            _is_external_cash_receipt_row(r) and r.get("_repayment_id") is not None for r in rows_for_d
        )
        has_other = any(
            not _is_external_cash_receipt_row(r)
            and not str(r.get("Narration") or "").startswith("Total outstanding balance as at")
            for r in rows_for_d
        )
        if not has_external_receipt or not has_other:
            continue

        first_rid: int | None = None
        for i in indices:
            r = out[i]
            if _is_external_cash_receipt_row(r) and r.get("_repayment_id") is not None:
                try:
                    first_rid = int(r["_repayment_id"])
                except (TypeError, ValueError):
                    first_rid = None
                break
        if first_rid is None:
            continue

        pre = get_repayment_opening_delinquency_total(first_rid)
        if pre is None:
            ds = get_loan_daily_state_balances(loan_id, d_conv - timedelta(days=1))
            pre_f = _f3(_total_delinquency_arrears(ds)) if ds else 0.0
        else:
            pre_f = _f3(pre)

        for i in indices:
            r = out[i]
            narr = str(r.get("Narration") or "")
            if narr.startswith("Total outstanding balance as at"):
                continue
            if narr.startswith("Liquidation of unapplied") or narr.startswith("Reversal of unapplied"):
                # Keep unapplied ledger movement rows (incl reversals) at their computed
                # post-movement arrears. These are internal allocation movements, not cash receipts.
                continue
            if _is_external_cash_receipt_row(r):
                continue
            r["Arrears"] = pre_f


def _generate_periodic_statement(
    loan_id: int,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    as_of_date: date | None = None,
    allowed_customer_ids: list[int] | None = None,
    include_principal_arrears_billing: bool = True,
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
    # Never render statement schedule rows beyond system "today"/as_of_date,
    # even if the caller requests a future end date.
    if end > today:
        end = today
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

    # True payment-due dates from the amortisation schedule (in statement range).
    # Used by customer-facing Balance snap — must not include ad-hoc dates that only
    # carry receipt allocation component rows (e.g. "Default interest paid").
    meta["schedule_due_dates"] = sorted({due_d for due_d, *_ in due_entries})

    # Repayments that allocate to default/penalty/fees for a given schedule due are only
    # those with value_date in (period_start_d, due_d], where period_start_d is the prior
    # schedule due (or disbursement for the first period). That window must not be
    # polluted by statement start/end: loads from before `start` when needed so
    # in-period receipts are not dropped from allocation / receipt rows.
    repay_start = start
    if due_entries:
        earliest_schedule_anchor = min(period_start for _, period_start, *_ in due_entries)
        repay_start = min(start, earliest_schedule_anchor)

    daily_states = get_loan_daily_state_range(loan_id, start, end)
    repayments = get_repayments_with_allocations(loan_id, repay_start, end, include_reversed=True)
    unapplied_lines = get_unapplied_ledger_entries_for_statement(loan_id, start, end)
    unapplied_lines_sorted = sorted(
        unapplied_lines,
        key=lambda u: (
            _date_conv(u.get("value_date")) or date(9999, 12, 31),
            int(u.get("repayment_id") or 0),
            str(u.get("entry_kind") or ""),
            int(u.get("id") or 0),
        ),
    )
    def _unapplied_at(
        d: date,
        *,
        repayment_id: int | None = None,
        entry_kind: str | None = None,
        include_same_day_liquidations: bool = True,
    ) -> float:
        latest_running = 0.0
        for u in unapplied_lines_sorted:
            vd = _date_conv(u.get("value_date"))
            if not vd:
                continue
            rid = int(u.get("repayment_id") or 0)
            kind = str(u.get("entry_kind") or "")
            liq_repayment_id = u.get("liquidation_repayment_id")
            if vd > d:
                break
            if vd < d:
                latest_running = float(u.get("unapplied_running_balance") or 0)
                continue
            # For non-liquidation statement rows on a given date, do not consume
            # same-day liquidation movements yet. This preserves the pre-liquidation
            # unapplied balance until the explicit liquidation row is rendered.
            if not include_same_day_liquidations and liq_repayment_id is not None:
                continue
            if repayment_id is None:
                latest_running = float(u.get("unapplied_running_balance") or 0)
                continue
            if rid < repayment_id:
                latest_running = float(u.get("unapplied_running_balance") or 0)
                continue
            if rid == repayment_id and (entry_kind is None or kind <= entry_kind):
                latest_running = float(u.get("unapplied_running_balance") or 0)
                continue
            break
        return _f3(latest_running)
    unapplied_at_end = _unapplied_at(end)

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
        state_at_due = _state_at(due_d)
        # Accrued interest, default, penalty: from loan daily state period_to_date at due date
        sum_regular = float(state_at_due.get("regular_interest_period_to_date") or 0) if state_at_due else 0.0
        sum_penalty = float(state_at_due.get("penalty_interest_period_to_date") or 0) if state_at_due else 0.0
        sum_default = float(state_at_due.get("default_interest_period_to_date") or 0) if state_at_due else 0.0
        fees_now = float(state_at_due.get("fees_charges_balance") or 0) if state_at_due else 0.0
        fees_in_period = max(0.0, fees_now - prev_fees)
        prev_fees = fees_now
        # Gross schedule-period accrual at the due date from loan_daily_state
        # (same as sum of daily accruals in that period for default/penalty; fees delta
        # from balance). Do not net receipts into these columns: repayments and bucket
        # lines are shown separately so "accrued to date" on the due line stays aligned
        # with default_interest_period_to_date / penalty_interest_period_to_date (and
        # fees movement), not a misleading residual after earlier-in-period allocations.
        due_penalty_before_settlement = sum_penalty
        due_default_before_settlement = sum_default
        due_fees_before_settlement = fees_in_period
        # Receipt attribution for this due: only value_date strictly inside the current
        # amortisation period (after prior due, up to and including this due). Payments
        # on or before period_start_d belong to earlier schedule periods and must not
        # affect this period's default/penalty/fees paid lines or remaining_paid_* caps.
        period_pos_penalty = 0.0
        period_pos_default = 0.0
        period_pos_fees = 0.0
        for rr in repayments:
            vd_rr = _date_conv(rr.get("value_date") or rr.get("payment_date"))
            if not vd_rr or vd_rr <= period_start_d or vd_rr > due_d:
                continue
            ap = float(rr.get("alloc_penalty_interest") or 0)
            ad = float(rr.get("alloc_default_interest") or 0)
            af = float(rr.get("alloc_fees_charges") or 0)
            period_pos_penalty += max(0.0, ap)
            period_pos_default += max(0.0, ad)
            period_pos_fees += max(0.0, af)
        period_pos_penalty = min(period_pos_penalty, due_penalty_before_settlement)
        period_pos_default = min(period_pos_default, due_default_before_settlement)
        period_pos_fees = min(period_pos_fees, due_fees_before_settlement)
        sum_penalty = max(0.0, due_penalty_before_settlement)
        sum_default = max(0.0, due_default_before_settlement)
        fees_in_period = max(0.0, due_fees_before_settlement)
        remaining_paid_penalty = period_pos_penalty
        remaining_paid_default = period_pos_default
        remaining_paid_fees = period_pos_fees
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
        row["Arrears"] = _f3(_total_delinquency_arrears(state_at_due))
        row["Unapplied funds"] = _unapplied_at(due_d, include_same_day_liquidations=False)
        rows.append(row)

        # Optional informational billing line (non-cash): principal due becoming
        # principal arrears on due date, even when same-day liquidation settles it.
        if include_principal_arrears_billing and principal_c > 1e-9:
            bill_row = _blank_row_periodic()
            bill_row["Due Date"] = due_d
            bill_row["Narration"] = f"Principal arrears billing ({_f3(principal_c):,.2f})"
            if state_at_due:
                bill_row["Total Outstanding Balance"] = _f3(_total_outstanding(state_at_due))
                bill_row["Arrears"] = _f3(_total_delinquency_arrears(state_at_due))
            bill_row["Unapplied funds"] = _unapplied_at(due_d, include_same_day_liquidations=False)
            bill_row["_arrears_debit_impact"] = _f3(principal_c)
            rows.append(bill_row)

        # Receipts in this schedule period only: (period_start_d, due_d]
        for r in repayments:
            vd = _date_conv(r.get("value_date") or r.get("payment_date"))
            if not vd or vd <= period_start_d or vd > due_d:
                continue
            amount = float(r.get("amount") or 0)
            alloc_total = float(r.get("alloc_total") or 0)
            rec_row = _blank_row_periodic()
            rec_row["Due Date"] = vd
            rid_label = int(r.get("id") or 0)
            teller_ref = (
                (r.get("customer_reference") or "").strip()
                or (r.get("reference") or "").strip()
                or "Receipt"
            )
            rec_row["Narration"] = _repayment_statement_narration(
                amount=amount,
                repayment_id=rid_label,
                teller_ref=teller_ref,
                original_repayment_id=r.get("original_repayment_id"),
            )
            rec_row["Credits"] = _f3(alloc_total)
            rec_row["Portion of Credit Allocated to Interest"] = _f3(float(r.get("alloc_interest_total") or 0))
            rec_row["Credit Allocated to Fees"] = _f3(float(r.get("alloc_fees_total") or 0))
            rec_row["Credit Allocated to Capital"] = _f3(float(r.get("alloc_principal_total") or 0))
            bal = _state_at(vd)
            if bal:
                rec_row["Total Outstanding Balance"] = _f3(_total_outstanding(bal))
                rec_row["Arrears"] = _f3(_total_delinquency_arrears(bal))
            else:
                rec_row["Total Outstanding Balance"] = 0.0
                rec_row["Arrears"] = 0.0
            rec_row["Unapplied funds"] = _unapplied_at(
                vd,
                repayment_id=int(r.get("id") or 0),
                include_same_day_liquidations=False,
            )
            rid_for_paid = int(r.get("id") or 0)
            if rid_for_paid:
                alloc_penalty = float(r.get("alloc_penalty_interest") or 0)
                alloc_default = float(r.get("alloc_default_interest") or 0)
                alloc_fees = float(r.get("alloc_fees_charges") or 0)
                paid_components = [
                    (
                        "Penalty interest paid",
                        (min(alloc_penalty, remaining_paid_penalty) if alloc_penalty >= 0 else alloc_penalty),
                        "Penalty",
                    ),
                    (
                        "Default interest paid",
                        (min(alloc_default, remaining_paid_default) if alloc_default >= 0 else alloc_default),
                        "Default",
                    ),
                    (
                        "Fees & Charges paid",
                        (min(alloc_fees, remaining_paid_fees) if alloc_fees >= 0 else alloc_fees),
                        "Fees",
                    ),
                ]
                for pname, pamt, bucket in paid_components:
                    # Reversal receipts carry negative allocation amounts.
                    # We still want to emit the inverse movement for these buckets.
                    if abs(pamt) <= 1e-9:
                        continue
                    prow = _blank_row_periodic()
                    prow["Due Date"] = vd
                    prow["Narration"] = f"{pname} ({teller_ref})"
                    if bucket == "Penalty":
                        prow["Penalty"] = _f3(pamt)
                    elif bucket == "Default":
                        prow["Default"] = _f3(pamt)
                    else:
                        prow["Fees"] = _f3(pamt)
                    bal_paid = _state_at(vd)
                    if bal_paid:
                        prow["Total Outstanding Balance"] = _f3(_total_outstanding(bal_paid))
                        prow["Arrears"] = _f3(_total_delinquency_arrears(bal_paid))
                    prow["Unapplied funds"] = _unapplied_at(
                        vd,
                        repayment_id=rid_for_paid,
                        include_same_day_liquidations=False,
                    )
                    rows.append(prow)
                # Consume remaining_paid_* only from positive allocations; reversals
                # do not reduce the positive cap used for sequential min() display.
                remaining_paid_penalty = max(
                    0.0,
                    remaining_paid_penalty - max(0.0, min(alloc_penalty, remaining_paid_penalty)),
                )
                remaining_paid_default = max(
                    0.0,
                    remaining_paid_default - max(0.0, min(alloc_default, remaining_paid_default)),
                )
                remaining_paid_fees = max(
                    0.0,
                    remaining_paid_fees - max(0.0, min(alloc_fees, remaining_paid_fees)),
                )
            rec_row["_arrears_credit_impact"] = _f3(
                float(r.get("alloc_interest_arrears") or 0)
                + float(r.get("alloc_penalty_interest") or 0)
                + float(r.get("alloc_default_interest") or 0)
                + float(r.get("alloc_principal_arrears") or 0)
                + float(r.get("alloc_fees_charges") or 0)
            )
            if r.get("id") is not None:
                try:
                    rec_row["_repayment_id"] = int(r["id"])
                except (TypeError, ValueError):
                    pass
            rows.append(rec_row)
            if r.get("id") is not None:
                try:
                    processed_repayment_ids.add(int(r["id"]))
                except (TypeError, ValueError):
                    pass

    # Receipts not processed under any due row (e.g. value_date outside every schedule period
    # we rendered), but still in the repayment fetch window overlapping the statement end.
    for r in repayments:
        rid = r.get("id")
        try:
            rid_int = int(rid) if rid is not None else None
        except (TypeError, ValueError):
            rid_int = None
        if rid_int is not None and rid_int in processed_repayment_ids:
            continue
        vd = _date_conv(r.get("value_date") or r.get("payment_date"))
        if not vd or vd < repay_start or vd > end:
            continue
        amount = float(r.get("amount") or 0)
        alloc_total = float(r.get("alloc_total") or 0)
        rec_row = _blank_row_periodic()
        rec_row["Due Date"] = vd
        rid_label = int(r.get("id") or 0)
        teller_ref = (
            (r.get("customer_reference") or "").strip()
            or (r.get("reference") or "").strip()
            or "Receipt"
        )
        rec_row["Narration"] = _repayment_statement_narration(
            amount=amount,
            repayment_id=rid_label,
            teller_ref=teller_ref,
            original_repayment_id=r.get("original_repayment_id"),
        )
        rec_row["Credits"] = _f3(alloc_total)
        rec_row["Portion of Credit Allocated to Interest"] = _f3(float(r.get("alloc_interest_total") or 0))
        rec_row["Credit Allocated to Fees"] = _f3(float(r.get("alloc_fees_total") or 0))
        rec_row["Credit Allocated to Capital"] = _f3(float(r.get("alloc_principal_total") or 0))
        bal = _state_at(vd)
        if bal:
            rec_row["Total Outstanding Balance"] = _f3(_total_outstanding(bal))
            rec_row["Arrears"] = _f3(_total_delinquency_arrears(bal))
        else:
            rec_row["Total Outstanding Balance"] = 0.0
            rec_row["Arrears"] = 0.0
        rec_row["Unapplied funds"] = _unapplied_at(
            vd,
            repayment_id=int(r.get("id") or 0),
            include_same_day_liquidations=False,
        )
        rec_row["_arrears_credit_impact"] = _f3(
            float(r.get("alloc_interest_arrears") or 0)
            + float(r.get("alloc_penalty_interest") or 0)
            + float(r.get("alloc_default_interest") or 0)
            + float(r.get("alloc_principal_arrears") or 0)
            + float(r.get("alloc_fees_charges") or 0)
        )
        if r.get("id") is not None:
            try:
                rec_row["_repayment_id"] = int(r["id"])
            except (TypeError, ValueError):
                pass
        rows.append(rec_row)

        # Always emit bucket movement rows for penalty/default/fees allocations on the receipt date.
        # This includes reversal receipts (negative allocations), which will be rendered as credits
        # in the customer-facing statement to undo the earlier bucket debits.
        rid_for_paid = int(r.get("id") or 0) if r.get("id") is not None else 0
        if rid_for_paid:
            alloc_penalty = float(r.get("alloc_penalty_interest") or 0)
            alloc_default = float(r.get("alloc_default_interest") or 0)
            alloc_fees = float(r.get("alloc_fees_charges") or 0)
            paid_components_any = [
                ("Penalty interest paid", alloc_penalty, "Penalty"),
                ("Default interest paid", alloc_default, "Default"),
                ("Fees & Charges paid", alloc_fees, "Fees"),
            ]
            for pname, pamt, bucket in paid_components_any:
                if abs(pamt) <= 1e-9:
                    continue
                prow = _blank_row_periodic()
                prow["Due Date"] = vd
                prow["Narration"] = f"{pname} ({teller_ref})"
                if bucket == "Penalty":
                    prow["Penalty"] = _f3(pamt)
                elif bucket == "Default":
                    prow["Default"] = _f3(pamt)
                else:
                    prow["Fees"] = _f3(pamt)
                bal_paid = _state_at(vd)
                if bal_paid:
                    prow["Total Outstanding Balance"] = _f3(_total_outstanding(bal_paid))
                    prow["Arrears"] = _f3(_total_delinquency_arrears(bal_paid))
                prow["Unapplied funds"] = _unapplied_at(
                    vd,
                    repayment_id=rid_for_paid,
                    include_same_day_liquidations=False,
                )
                # Customer-facing uses Debits/Credits on component rows; paying a bucket
                # reduces delinquency (credit impact), reversing that payment increases it.
                if pamt >= 0:
                    prow["_arrears_credit_impact"] = _f3(pamt)
                else:
                    prow["_arrears_debit_impact"] = _f3(abs(pamt))
                rows.append(prow)

    # Unapplied ledger movements as explicit periodic rows.
    for u in unapplied_lines:
        vd = _date_conv(u.get("value_date"))
        if not vd or vd < start or vd > end:
            continue
        kind = (u.get("entry_kind") or "").strip().lower()
        delta = float(u.get("unapplied_delta") or 0)
        if abs(delta) < 1e-9:
            continue
            
        row = _blank_row_periodic()
        row["Due Date"] = vd
        rk = u.get("repayment_key") or ""
        
        if kind == "liquidation":
            row["Narration"] = f"Liquidation of unapplied receipt no {rk}"
        elif kind == "credit":
            row["Narration"] = f"Unapplied funds credit from receipt no {rk}"
        elif kind == "reversal":
            if delta > 0:
                row["Narration"] = f"Reversal of unapplied liquidation from receipt no {rk}"
            else:
                row["Narration"] = f"Reversal of unapplied funds credit from receipt no {rk}"
        else:
            continue

        # Internal movement only: no cash debit/credit impact on statement totals.
        row["Credits"] = 0.0
        row["Portion of Credit Allocated to Interest"] = _f3(
            float(u.get("alloc_int_arrears") or 0)
            + float(u.get("alloc_penalty_int") or 0)
            + float(u.get("alloc_default_int") or 0),
        )
        row["Credit Allocated to Fees"] = _f3(float(u.get("alloc_fees_charges") or 0))
        row["Credit Allocated to Capital"] = _f3(float(u.get("alloc_prin_arrears") or 0))
        bal = _state_at(vd)
        if bal:
            row["Total Outstanding Balance"] = _f3(_total_outstanding(bal))
            row["Arrears"] = _f3(_total_delinquency_arrears(bal))
        row["Unapplied funds"] = _f3(float(u.get("unapplied_running_balance") or 0))
        row["_arrears_credit_impact"] = _f3(
            float(u.get("alloc_prin_arrears") or 0)
            + float(u.get("alloc_int_arrears") or 0)
            + float(u.get("alloc_penalty_int") or 0)
            + float(u.get("alloc_default_int") or 0)
            + float(u.get("alloc_fees_charges") or 0)
        )
        rows.append(row)

    # Current (incomplete) period interest -- emitted BEFORE the non-cash residual
    # so it is counted in current_charge_total and the residual is zero for clean loans.
    # Fires when:
    #   (a) there are due dates in range and end is beyond the last one, OR
    #   (b) no due dates have fallen yet (statement is entirely in the first period).
    last_due_in_range = due_entries[-1][0] if due_entries else None
    period_boundary = last_due_in_range or disbursement
    end_bal_exact = ds_by_date.get(end)
    last_persisted_ds_date = max(ds_by_date.keys()) if ds_by_date else end
    if period_boundary is not None and end > period_boundary:
        # Use period_to_date columns only when we have an exact daily-state row
        # for statement end date. If end-day EOD has not run yet, the fallback
        # from _state_at(end) may return prior-day period totals (e.g. due date),
        # which would incorrectly duplicate prior period accrual in the statement.
        # If EOD for `end` has not run yet, daily-state only exists up to the
        # last persisted date. Any "period to date" lines should be labelled
        # with that last persisted daily-state date (not the requested `end`).
        as_of_for_state = end if end_bal_exact else last_persisted_ds_date
        if end_bal_exact:
            cur_regular = float(end_bal_exact.get("regular_interest_period_to_date") or 0)
            cur_penalty = float(end_bal_exact.get("penalty_interest_period_to_date") or 0)
            cur_default = float(end_bal_exact.get("default_interest_period_to_date") or 0)
        else:
            cur_regular = cur_penalty = cur_default = 0.0
            for ds in daily_states:
                ad = _date_conv(ds.get("as_of_date"))
                if not ad or ad > end:
                    continue
                if last_due_in_range and ad <= last_due_in_range:
                    continue
                if not last_due_in_range and disbursement and ad <= disbursement:
                    continue
                cur_regular += float(ds.get("regular_interest_daily") or 0)
                cur_penalty += float(ds.get("penalty_interest_daily") or 0)
                cur_default += float(ds.get("default_interest_daily") or 0)
        # Fees: no period_to_date column — use balance delta since last period boundary (daily state).
        st_as_of = _state_at(as_of_for_state)
        cur_fees = 0.0
        if st_as_of:
            fe_now = float(st_as_of.get("fees_charges_balance") or 0)
            if last_due_in_range:
                st_ld = _state_at(last_due_in_range)
                fe_ld = float(st_ld.get("fees_charges_balance") or 0) if st_ld else 0.0
            elif disbursement:
                st0 = _state_at(disbursement)
                fe_ld = float(st0.get("fees_charges_balance") or 0) if st0 else 0.0
            else:
                fe_ld = 0.0
            cur_fees = max(0.0, fe_now - fe_ld)
        current_period_total = cur_regular + cur_penalty + cur_default + cur_fees
        if abs(current_period_total) > 0.005:
            row = _blank_row_periodic()
            row["Due Date"] = as_of_for_state
            _narr_sfx = "since last due date" if last_due_in_range else "since disbursement"
            row["Narration"] = f"Current period interest ({_narr_sfx})"
            row["Interest"] = _f3(cur_regular)
            row["Penalty"] = _f3(cur_penalty)
            row["Default"] = _f3(cur_default)
            row["Fees"] = _f3(cur_fees) if cur_fees else 0.0
            end_bal = _state_at(as_of_for_state)
            if end_bal:
                row["Total Outstanding Balance"] = _f3(_total_outstanding(end_bal))
                row["Arrears"] = _f3(_total_delinquency_arrears(end_bal))
            row["Unapplied funds"] = _f3(_unapplied_at(as_of_for_state))
            rows.append(row)

    row = _blank_row_periodic()
    # Same as above: if EOD for `end` is not persisted yet, label totals
    # using the last persisted daily-state date.
    closing_as_of = end if end_bal_exact else last_persisted_ds_date
    row["Due Date"] = closing_as_of
    row["Narration"] = f"Total outstanding balance as at {closing_as_of.isoformat()}"
    end_bal = _state_at(closing_as_of)
    if end_bal:
        row["Total Outstanding Balance"] = _f3(_total_outstanding(end_bal))
        row["Arrears"] = _f3(_total_delinquency_arrears(end_bal))
    row["Unapplied funds"] = _f3(_unapplied_at(closing_as_of))
    rows.append(row)

    def _sort_key(r: dict) -> tuple:
        due_date = r.get("Due Date") or date(9999, 12, 31)
        narr = r.get("Narration") or ""
        
        if narr.startswith("Total outstanding"):
            order = 99
        elif "Unapplied funds credit" in narr:
            order = 5
        elif narr.startswith("Liquidation of unapplied"):
            order = 6
        elif "Reversal of unapplied" in narr:
            order = 7
        else:
            order = 1
            
        return (due_date, order, narr)

    rows.sort(key=_sort_key)
    return rows, meta


def generate_customer_facing_statement(
    loan_id: int,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    as_of_date: date | None = None,
    allowed_customer_ids: list[int] | None = None,
    include_principal_arrears_billing: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Generate a customer-facing loan statement from the internal periodic statement.
    Debits: interest, penalty, default, disbursement (principal only on disbursement row), fees.
    Credits, Balance, Arrears (total delinquency incl. fees), Unapplied funds.
    """
    rows_periodic, meta = _generate_periodic_statement(
        loan_id,
        start_date=start_date,
        end_date=end_date,
        as_of_date=as_of_date,
        allowed_customer_ids=allowed_customer_ids,
        include_principal_arrears_billing=include_principal_arrears_billing,
    )
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
        elif narration.startswith("Liquidation of unapplied"):
            credits_alloc = _to_dec(r.get("Portion of Credit Allocated to Interest") or 0) + \
                            _to_dec(r.get("Credit Allocated to Fees") or 0) + \
                            _to_dec(r.get("Credit Allocated to Capital") or 0)
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration,
                "Debits": 0.0,
                "Credits": _f3(credits_alloc),
                "Balance": 0.0,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
                "_arrears_credit_impact": _f3(
                    r.get("_arrears_credit_impact") if r.get("_arrears_credit_impact") is not None else credits_alloc
                ),
            })
        elif narration.startswith("Reversal of unapplied liquidation"):
            # Undo of a liquidation: show it as a Debit of the same magnitude,
            # so the statement reads like "Liquidation" (credit) then "REV" (debit).
            credits_alloc = _to_dec(r.get("Portion of Credit Allocated to Interest") or 0) + \
                            _to_dec(r.get("Credit Allocated to Fees") or 0) + \
                            _to_dec(r.get("Credit Allocated to Capital") or 0)
            debits_alloc = abs(credits_alloc)
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration,
                "Debits": _f3(debits_alloc),
                "Credits": 0.0,
                "Balance": 0.0,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
            })
        elif narration.startswith("Unapplied funds credit") or narration.startswith("Reversal of unapplied"):
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration,
                "Debits": 0.0,
                "Credits": 0.0,
                "Balance": 0.0,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
            })
        elif narration.startswith("Principal arrears billing"):
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration,
                "Debits": 0.0,
                "Credits": 0.0,
                "Balance": 0.0,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
                "_arrears_debit_impact": _f3(r.get("_arrears_debit_impact") or 0),
            })
        elif credits != Decimal("0"):
            # Customer cash movement only:
            # positive receipt -> Credits, negative reversal -> Debits.
            if credits > Decimal("0"):
                rec_out = {
                    "Due Date": r.get("Due Date"),
                    "Narration": narration or "Receipt",
                    "Debits": 0.0,
                    "Credits": _f3(credits),
                    "Balance": 0.0,
                    "Arrears": arrears,
                    "Unapplied funds": unapplied,
                }
                rid = r.get("_repayment_id")
                if rid is not None:
                    try:
                        rec_out["_repayment_id"] = int(rid)
                    except (TypeError, ValueError):
                        pass
                rec_out["_arrears_credit_impact"] = _f3(r.get("_arrears_credit_impact") or 0)
                out.append(rec_out)
            else:
                out.append({
                    "Due Date": r.get("Due Date"),
                    "Narration": narration or "Reversal",
                    "Debits": _f3(abs(credits)),
                    "Credits": 0.0,
                    "Balance": 0.0,
                    "Arrears": arrears,
                    "Unapplied funds": unapplied,
                    "_arrears_debit_impact": _f3(abs(_to_dec(r.get("_arrears_credit_impact") or 0))),
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
            is_current_period = narration.startswith("Current period interest (")
            sfx = " (period to date)" if is_current_period else ""
            raw_components = [
                (f"Accrued interest{sfx}", interest),
                (f"Penalty interest{sfx}", penalty),
                (f"Default interest{sfx}", default),
                ("Fees & Charges", fees),
            ]
            raw_components = [(n, v) for n, v in raw_components if abs(v) > Decimal("0")]
            narr_l = narration.lower()
            # Receipt allocation split rows (periodic narration e.g. "Default interest paid (…)").
            # They must post as Credits when paying down a bucket (positive alloc), not as Debits
            # like schedule charges; labels should say "… paid" so they are not mistaken for reversals.
            is_bucket_payment_line = (
                "penalty interest paid" in narr_l
                or "default interest paid" in narr_l
                or "fees & charges paid" in narr_l
            )
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
                    if is_bucket_payment_line and " paid" not in n:
                        if n.startswith("Accrued interest"):
                            n = n.replace("Accrued interest", "Accrued interest paid", 1)
                        elif n.startswith("Penalty interest"):
                            n = n.replace("Penalty interest", "Penalty interest paid", 1)
                        elif n.startswith("Default interest"):
                            n = n.replace("Default interest", "Default interest paid", 1)
                        elif n.startswith("Fees & Charges"):
                            n = "Fees & Charges paid" if n == "Fees & Charges" else n.replace(
                                "Fees & Charges", "Fees & Charges paid", 1
                            )
                    # Charges: positive -> Debit; negative (reversal of accrual) -> Credit.
                    # Bucket payment lines: positive alloc -> Credit; negative -> Debit.
                    if is_bucket_payment_line:
                        component_debits = _f3(abs(rv)) if rv < 0 else 0.0
                        component_credits = _f3(rv) if rv > 0 else 0.0
                    else:
                        component_debits = _f3(rv) if rv > 0 else 0.0
                        component_credits = _f3(abs(rv)) if rv < 0 else 0.0
                    out.append({
                        "Due Date": r.get("Due Date"),
                        "Narration": n,
                        "Debits": component_debits,
                        "Credits": component_credits,
                        "Balance": 0.0,
                        "Arrears": arrears,
                        "Unapplied funds": unapplied,
                    })

    # Same-day presentation: accruals/charges first, external cash receipts last; closing line last.
    out = _reorder_customer_facing_rows_receipts_last(out)
    _apply_customer_facing_arrears_before_first_receipt(loan_id, out)

    # Compute running arrears in display order so movements align with transactions.
    # Opening point is delinquency as at day-start (closing of prior date).
    opening_arrears_d = Decimal("0")
    try:
        start_d = meta.get("start_date")
        start_d = _date_conv(start_d) if start_d is not None else None
        if start_d is not None:
            ds_open = get_loan_daily_state_balances(loan_id, start_d - timedelta(days=1))
            opening_arrears_d = _to_dec(_f3(_total_delinquency_arrears(ds_open)))
    except Exception:
        opening_arrears_d = Decimal("0")

    indices_by_due_date: dict[date, list[int]] = {}
    last_idx_by_due_date: dict[Any, int] = {}
    # Last movement row per date (exclude closing "Total outstanding" line). Arrears snap to
    # loan_daily_state must run here — otherwise the true last row is closing, snap never runs,
    # and prior same-day rows show running arrears that disagree with persisted EOD delinquency.
    last_non_closing_idx_by_due_date: dict[Any, int] = {}
    for idx, row in enumerate(out):
        d = row.get("Due Date")
        if d is None:
            continue
        indices_by_due_date.setdefault(d, []).append(idx)
        last_idx_by_due_date[d] = idx
        narr_lc = str(row.get("Narration") or "")
        if not narr_lc.startswith("Total outstanding balance as at"):
            last_non_closing_idx_by_due_date[d] = idx

    # Balance snap dates = schedule payment due dates only (from meta).
    # Do not infer from arbitrary periodic rows with charge columns — that would treat
    # same-day receipt allocation lines (e.g. 17 Jun default paid) as a "due date"
    # and wrongly overwrite the running Balance with EOD daily_state on the reversal row.
    due_dates: set[date] = set()
    for d0 in meta.get("schedule_due_dates") or []:
        dd = _date_conv(d0)
        if dd is not None:
            due_dates.add(dd)

    running_arrears_d = opening_arrears_d
    for i, _ar_row in enumerate(out):
        narr = str(_ar_row.get("Narration") or "")
        if narr.startswith("Total outstanding balance as at"):
            # Align with persisted EOD delinquency (same as loan_daily_state for this date).
            d_close = _date_conv(_ar_row.get("Due Date"))
            if d_close is not None:
                ds_close = get_loan_daily_state_balances(loan_id, d_close)
                if ds_close:
                    running_arrears_d = _to_dec(_f3(_total_delinquency_arrears(ds_close)))
            _ar_row["Arrears"] = _f3(running_arrears_d)
            continue

        debit_imp = _to_dec(_ar_row.get("_arrears_debit_impact") or 0)
        credit_imp = _to_dec(_ar_row.get("_arrears_credit_impact") or 0)

        if debit_imp == Decimal("0") and credit_imp == Decimal("0"):
            if narr.startswith("Accrued interest") or narr.startswith("Penalty interest") or narr.startswith("Default interest") or narr.startswith("Fees & Charges"):
                deb = _to_dec(_ar_row.get("Debits") or 0)
                cred = _to_dec(_ar_row.get("Credits") or 0)
                if deb != Decimal("0"):
                    debit_imp = deb
                elif cred != Decimal("0"):
                    credit_imp = cred

        running_arrears_d = running_arrears_d + debit_imp - credit_imp
        if running_arrears_d < Decimal("0"):
            running_arrears_d = Decimal("0")

        # Snap at end-of-day delinquency on the last *movement* row for this calendar date
        # (not the closing total line — see last_non_closing_idx_by_due_date).
        d_snap = _ar_row.get("Due Date")
        snap_idx = last_non_closing_idx_by_due_date.get(d_snap)
        if d_snap is not None and snap_idx is not None and snap_idx == i:
            exact_ds = get_loan_daily_state_balances(loan_id, _date_conv(d_snap))
            if exact_ds:
                running_arrears_d = _to_dec(_f3(_total_delinquency_arrears(exact_ds)))

        _ar_row["Arrears"] = _f3(running_arrears_d)

    def _total_outstanding_from_ds(ds: dict | None) -> float:
        if not ds:
            return 0.0
        return (
            float(ds.get("principal_not_due") or 0)
            + float(ds.get("principal_arrears") or 0)
            + float(ds.get("interest_accrued_balance") or 0)
            + float(ds.get("interest_arrears_balance") or 0)
            + float(ds.get("default_interest_balance") or 0)
            + float(ds.get("penalty_interest_balance") or 0)
            + float(ds.get("fees_charges_balance") or 0)
        )

    # Compute running Balance as:
    # - start from prior-day daily_state total outstanding (day-start before statement start)
    # - then Balance += Debits - Credits for each row in display order
    # - at each schedule due date (end-of-date), snap Balance to persisted daily_state total outstanding.
    running_bal = Decimal("0")
    stmt_start = meta.get("start_date")
    try:
        if stmt_start is not None:
            opening_d = _date_conv(stmt_start) - timedelta(days=1)
            ds_open = get_loan_daily_state_balances(loan_id, opening_d)
            if ds_open:
                running_bal = _to_dec(_f3(_total_outstanding_from_ds(ds_open)))
    except Exception:
        running_bal = Decimal("0")

    for i, _rb_row in enumerate(out):
        running_bal += _to_dec(_rb_row.get("Debits") or 0) - _to_dec(_rb_row.get("Credits") or 0)
        _rb_row["Balance"] = _f3(running_bal)

        d = _rb_row.get("Due Date")
        if d is None or last_idx_by_due_date.get(d) != i:
            continue

        # Due-date reconciliation: due date and closing line must equal daily_state.
        narr = str(_rb_row.get("Narration") or "")
        if d in due_dates or narr.startswith("Total outstanding balance as at"):
            try:
                exact_ds = get_loan_daily_state_balances(loan_id, d)
                if exact_ds:
                    running_bal = _to_dec(_f3(_total_outstanding_from_ds(exact_ds)))
                    _rb_row["Balance"] = _f3(running_bal)
            except Exception:
                pass

    # (Drift notification logic removed.)

    for _row in out:
        _row.pop("_repayment_id", None)
        _row.pop("_arrears_credit_impact", None)
        _row.pop("_arrears_debit_impact", None)

    return out, meta


def _blank_row_periodic() -> dict[str, Any]:
    row: dict[str, Any] = {}
    for h in PERIODIC_STATEMENT_HEADINGS:
        row[h] = 0 if h in PERIODIC_NUMERIC_HEADINGS else None
    return row
