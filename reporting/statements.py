"""
Statements module: generate statements on demand (no persistence).
- Customer loan statements (this module)
- Other loan statement types (later)
- General ledger / ledger account statements (later)
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from loan_management import (
    apply_schedule_version_bumps,
    collect_due_dates_in_range_all_schedule_versions,
    get_loan,
    get_loan_daily_state_range,
    get_repayments_with_allocations,
    get_loan_daily_state_balances,
    get_repayment_opening_delinquency_total,
    get_unapplied_ledger_entries_for_statement,
    get_max_schedule_due_date_on_or_before,
    get_original_facility_for_statements,
    get_schedule_line_on_version_for_date,
    get_schedule_lines,
    list_schedule_bumping_events,
)


def _get_effective_date() -> date:
    """System business date for statement/amount-due logic."""
    try:
        from eod.system_business_date import get_effective_date
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


def _get_drawdown_breakdown(
    loan: dict[str, Any],
    *,
    facility_principal: float | None = None,
) -> list[tuple[str, float]]:
    """
    Return drawdown breakdown as [(narration, amount), ...] for statement display.
    Parts always sum to principal (facility amount) so the statement reconciles.

    Reads absolute fee amount columns (admin_fee_amount, drawdown_fee_amount,
    arrangement_fee_amount).  For older rows where these are 0/NULL, derives
    amounts from rate * principal as a fallback.

    Identity: Disbursed Amount + Administration Fees + Drawdown Fees + Arrangement Fees = Principal

    ``facility_principal``: original facility at booking. When omitted, uses
    ``loan['principal']`` (after recast this is the **new** balance — use the explicit
    argument from :func:`get_original_facility_for_statements` for correct drawdown).
    """
    principal_dec = as_10dp(
        Decimal(str(facility_principal if facility_principal is not None else (loan.get("principal") or 0)))
    )

    # Prefer stored absolute amounts; derive from rate * principal if absent
    def _fee_amount(amount_key: str, rate_key: str) -> Decimal:
        stored = as_10dp(Decimal(str(loan.get(amount_key) or 0)))
        if stored > 0:
            return stored
        rate = Decimal(str(loan.get(rate_key) or 0))
        return as_10dp(principal_dec * rate) if rate > 0 else Decimal("0")

    admin_fee_amt = _fee_amount("admin_fee_amount", "admin_fee")
    drawdown_fee_amt = _fee_amount("drawdown_fee_amount", "drawdown_fee")
    arrangement_fee_amt = _fee_amount("arrangement_fee_amount", "arrangement_fee")

    total_fees = as_10dp(admin_fee_amt + drawdown_fee_amt + arrangement_fee_amt)
    net_proceeds = as_10dp(principal_dec - total_fees)

    parts: list[tuple[str, float]] = [("Disbursed Amount", net_proceeds)]
    if admin_fee_amt > 0:
        parts.append(("Administration Fees", as_10dp(admin_fee_amt)))
    if drawdown_fee_amt > 0:
        parts.append(("Drawdown Fees", as_10dp(drawdown_fee_amt)))
    if arrangement_fee_amt > 0:
        parts.append(("Arrangement Fees", as_10dp(arrangement_fee_amt)))
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


def _to_dec(v: Any) -> Decimal:
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def _f10(v: Any) -> float:
    """Money on statements: align with ledger / COA 10dp policy."""
    try:
        return float(as_10dp(Decimal(str(v if v is not None else 0))))
    except Exception:
        return float(Decimal(str(v or 0)))


def _f3(v: Any) -> float:
    """Statement numeric fields use 10dp (same as ``_f10``); name retained for call-site churn."""

    return _f10(v)


def _quantize_statement_decimal(v: Decimal) -> Decimal:
    """Component split / residual logic at 10dp (matches ``_f10``)."""
    return Decimal(str(as_10dp(v)))


def _liq_rev_interleave_sort(narr: str) -> tuple[int, int, str]:
    """
    Order LIQ-<id> immediately before REV-LIQ-<id> on the same date.
    Putting all LIQ rows before all REV-LIQ rows breaks running Balance / Unapplied columns
    when a liquidation is reversed after later liquidations were posted.
    """
    import re

    s = str(narr or "")
    m = re.match(r"^LIQ-(\d+)", s)
    if m:
        return (int(m.group(1)), 0, s)
    m = re.match(r"^REV-LIQ-(\d+)", s)
    if m:
        return (int(m.group(1)), 1, s)
    if s.startswith("Liquidation of unapplied"):
        return (10_000_000, 0, s)
    if "Reversal of unapplied liquidation" in s:
        return (10_000_000, 1, s)
    return (10_000_001, 0, s)


def _total_delinquency_arrears(ds: dict | None) -> float:
    """
    Total delinquency from loan_daily_state: amounts past due / in arrears buckets
    (excludes principal not yet due and unbilled accrued interest).
    principal_arrears + interest_arrears_balance + default_interest_balance
    + penalty_interest_balance + fees_charges_balance
    """
    if not ds:
        return 0.0
    # Prefer the persisted daily-state derived column when present.
    # This keeps statement "snap to EOD" aligned with the portfolio engine,
    # while preserving intra-day manipulations elsewhere in the statement pipeline.
    if ds.get("total_delinquency_arrears") is not None:
        try:
            return float(ds.get("total_delinquency_arrears") or 0)
        except (TypeError, ValueError):
            pass
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
    try:
        amt_txt = f"{abs(float(amount)):,.2f}"
    except (TypeError, ValueError):
        amt_txt = "0.00"
    if amount < 0:
        if original_repayment_id is not None:
            try:
                oid = int(original_repayment_id)
            except (TypeError, ValueError):
                oid = 0
            if oid > 0:
                return f"REV {oid} (Receipt {amt_txt})"
        base = f"Repayment id {repayment_id}: {ref}" if repayment_id else ref
        return f"Reversal of {base} (Receipt {amt_txt})"
    base = f"Repayment id {repayment_id}: {ref}" if repayment_id else ref
    return f"{base} (Receipt {amt_txt})"


def _is_internal_unapplied_liquidation_repayment_for_statement(r: dict[str, Any]) -> bool:
    """
    Synthetic repayment rows for applying unapplied (EOD, recast, cascade unwind).
    They must not appear as duplicate cash-style receipt lines: the unapplied_funds_ledger
    already emits LIQ-*/REV-LIQ-* rows for the same economics.
    """
    ref = f"{r.get('reference') or ''} {r.get('customer_reference') or ''} {r.get('company_reference') or ''}"
    rl = ref.lower()
    if "unapplied funds allocation" in rl:
        return True
    if "reversal of unapplied funds" in rl:
        return True
    if "loan recast" in rl and "unapplied" in rl:
        return True
    return False


def _is_statement_reversal_narration(narration: str) -> bool:
    n = str(narration or "")
    if "Reversal of unapplied" in n or n.startswith("REV-LIQ-") or n.startswith("REV-RCPT-"):
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
    if narr.startswith("Liquidation of unapplied") or narr.startswith("LIQ-"):
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
                or narr.startswith("OP-")
                or narr.startswith("LIQ-")
                or narr.startswith("REV-LIQ-")
                or narr.startswith("REV-RCPT-")
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


def _sort_unapplied_ledger_for_statement(lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        lines,
        key=lambda u: (
            _date_conv(u.get("value_date")) or date(9999, 12, 31),
            int(u.get("repayment_id") or 0),
            str(u.get("entry_kind") or ""),
        ),
    )


def _ledger_unapplied_balance_before_day(lines_sorted: list[dict[str, Any]], day: date) -> Decimal:
    """Unapplied balance after all ledger entries strictly before ``day`` (view cumulative)."""
    latest = Decimal("0")
    for u in lines_sorted:
        vd = _date_conv(u.get("value_date"))
        if not vd or vd >= day:
            continue
        latest = _to_dec(u.get("unapplied_running_balance") or 0)
    return _to_dec(_f3(latest))


def _ledger_unapplied_balance_through_date(lines_sorted: list[dict[str, Any]], through: date) -> Decimal:
    """Unapplied balance after all ledger entries with value_date <= ``through``."""
    latest = Decimal("0")
    for u in lines_sorted:
        vd = _date_conv(u.get("value_date"))
        if not vd or vd > through:
            continue
        latest = _to_dec(u.get("unapplied_running_balance") or 0)
    return _to_dec(_f3(latest))


def _match_ledger_row_for_unapplied_narration(
    narr: str,
    vd: date,
    lines_sorted: list[dict[str, Any]],
) -> dict[str, Any] | None:
    import re

    rk_m = re.search(r"receipt no\s+(\S+)", narr, re.I)
    if not rk_m:
        rk_m = re.search(r"From Repayment id\s+(\S+)", narr, re.I)
    if not rk_m:
        rk_m = re.search(r"(?:OP|LIQ|RCPT)-(\d+)", narr)
    rk = (rk_m.group(1).rstrip(")") if rk_m else "").strip()
    if not rk:
        return None
    want_amt_m = re.search(r"\(Receipt ([0-9.]+)\)", narr)
    want_amt = float(want_amt_m.group(1)) if want_amt_m else None

    want_kind: str | None = None
    if narr.startswith("Liquidation of unapplied") or narr.startswith("LIQ-"):
        want_kind = "liquidation"
    elif "Unapplied funds credit" in narr or narr.startswith("OP-"):
        want_kind = "credit"
    elif "Reversal of unapplied" in narr or narr.startswith("REV-LIQ-") or narr.startswith("REV-RCPT-"):
        want_kind = "reversal"
    else:
        return None

    candidates: list[dict[str, Any]] = []
    for u in lines_sorted:
        if _date_conv(u.get("value_date")) != vd:
            continue
        if str(u.get("repayment_key") or "").strip() != rk:
            continue
        candidates.append(u)

    filtered = [u for u in candidates if str(u.get("entry_kind") or "").lower() == want_kind]
    pool = filtered if filtered else candidates
    if not pool:
        return None
    if want_amt is not None:
        for u in pool:
            du = float(u.get("unapplied_delta") or 0)
            if abs(abs(du) - want_amt) < 0.02:
                return u
    return pool[0]


def _apply_customer_facing_unapplied_from_ledger(loan_id: int, meta: dict[str, Any], out: list[dict[str, Any]]) -> None:
    """
    Set the customer-facing Unapplied funds column from unapplied_funds_ledger only.

    - Opening for each calendar day = cumulative balance before that day (sum of deltas / view running).
    - Explicit unapplied movement rows use the matched ledger line's unapplied_running_balance.
    - Receipt lines that have a separate same-day 'credit' ledger line keep the running balance
      unchanged on the receipt (surplus is shown on the unapplied line); otherwise the receipt
      row picks up the post-receipt ledger running balance for that repayment.
    - Closing line uses cumulative balance through meta end_date (not loan_daily_state), so it
      stays aligned with the ledger even when daily_state.unallocated is missing or stale.
    """
    import re

    start = _date_conv(meta.get("start_date"))
    end = _date_conv(meta.get("end_date"))
    if not start or not end:
        return

    raw = get_unapplied_ledger_entries_for_statement(loan_id, start, end)
    lines_sorted = _sort_unapplied_ledger_for_statement(raw)

    overpay_narr_re = re.compile(r"Overpayment from Repayment id\s+(\d+)", re.I)
    op_key_re = re.compile(r"^OP-(\d+)(?:\s|$)", re.I)

    def _stmt_has_dedicated_overpayment_line_for_receipt(d0: date, rid0: int) -> bool:
        """True when customer output already has a same-day line that shows the unapplied credit for this receipt."""
        for pr in out:
            if _date_conv(pr.get("Due Date")) != d0:
                continue
            n = str(pr.get("Narration") or "")
            m = overpay_narr_re.search(n)
            if m:
                try:
                    if int(m.group(1)) == rid0:
                        return True
                except (TypeError, ValueError):
                    pass
            op_m = op_key_re.match(n.strip())
            if op_m:
                try:
                    if int(op_m.group(1)) == rid0:
                        return True
                except (TypeError, ValueError):
                    pass
        return False

    rep_re = re.compile(r"^Repayment id\s+(\d+)", re.I)
    running = Decimal("0")
    current_day: date | None = None

    for row in out:
        narr = str(row.get("Narration") or "")
        d = _date_conv(row.get("Due Date"))
        if d is None:
            continue

        if current_day != d:
            current_day = d
            running = _ledger_unapplied_balance_before_day(lines_sorted, d)

        if narr.startswith("Total outstanding balance as at"):
            closing_d = _date_conv(meta.get("end_date")) or d
            running = _ledger_unapplied_balance_through_date(lines_sorted, closing_d)
            row["Unapplied funds"] = _f3(running)
            continue

        if (
            "Unapplied funds credit" in narr
            or narr.startswith("Liquidation of unapplied")
            or "Reversal of unapplied" in narr
            or narr.startswith("OP-")
            or narr.startswith("LIQ-")
            or narr.startswith("REV-LIQ-")
            or narr.startswith("REV-RCPT-")
        ):
            matched = _match_ledger_row_for_unapplied_narration(narr, d, lines_sorted)
            if matched:
                running = _to_dec(_f3(float(matched.get("unapplied_running_balance") or 0)))
            else:
                delta = Decimal("0")
                m = re.search(r"\(Receipt ([0-9.]+)\)", narr)
                if m:
                    amt = _to_dec(m.group(1))
                    if narr.startswith("Liquidation of unapplied") or narr.startswith("LIQ-"):
                        delta = -amt
                    elif "Reversal of unapplied liquidation" in narr or narr.startswith("REV-LIQ-"):
                        delta = amt
                    elif "Unapplied funds credit" in narr or narr.startswith("OP-"):
                        delta = amt
                    elif "Reversal of unapplied funds credit" in narr or narr.startswith("REV-RCPT-"):
                        delta = -amt
                running = running + delta
            row["Unapplied funds"] = _f3(running)
            continue

        mrep = rep_re.match(narr)
        if mrep:
            rid = int(mrep.group(1))
            # Ledger always has a 'credit' row for overpayment, but the periodic statement often
            # does not emit a separate "Overpayment from Repayment id …" line. In that case the
            # increase belongs on this receipt row (post-receipt running balance from ledger).
            if not _stmt_has_dedicated_overpayment_line_for_receipt(d, rid):
                best: Decimal | None = None
                for u in lines_sorted:
                    if _date_conv(u.get("value_date")) != d:
                        continue
                    if int(u.get("repayment_id") or 0) != rid:
                        continue
                    b = _to_dec(_f3(float(u.get("unapplied_running_balance") or 0)))
                    best = b if best is None else max(best, b)
                if best is not None:
                    running = best
            row["Unapplied funds"] = _f3(running)
            continue

        row["Unapplied funds"] = _f3(running)

    for row in out:
        row.pop("_unapplied_delta", None)


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
            if narr.startswith("Liquidation of unapplied") or narr.startswith("Reversal of unapplied") or narr.startswith("LIQ-") or narr.startswith("REV-LIQ-") or narr.startswith("REV-RCPT-") or narr.startswith("OP-"):
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

    orig_facility = get_original_facility_for_statements(loan_id, loan)
    booked_principal = float(loan.get("principal") or 0)
    meta = {
        "loan_id": loan_id,
        "customer_id": loan.get("customer_id"),
        "start_date": start,
        "end_date": end,
        "loan_type": loan.get("loan_type"),
        "principal": booked_principal,
        "original_facility": float(orig_facility) if orig_facility is not None else booked_principal,
        "currency": (loan.get("metadata") or {}).get("currency") or "USD",
        "generated_at": datetime.now(),
        "statement_type": "periodic",
    }

    schedule_lines = get_schedule_lines(loan_id)
    if not schedule_lines:
        return [], meta

    # Instalment dates in range can exist only on older schedule versions (pre-recast).
    # For each due date D, use lines from the version in force on D (recast/modification audit).
    bump_events = list_schedule_bumping_events(loan_id)
    due_dates_win = collect_due_dates_in_range_all_schedule_versions(loan_id, start, end)
    due_entries: list[tuple[date, date, float, float, bool]] = []
    for due_d in due_dates_win:
        ver_eff = apply_schedule_version_bumps(due_d, bump_events)
        sl = get_schedule_line_on_version_for_date(loan_id, ver_eff, due_d)
        if sl is None:
            continue
        interest_c = float(sl.get("interest") or sl.get("Interest") or 0)
        principal_c = float(sl.get("principal") or sl.get("Principal") or 0)
        bound = due_d - timedelta(days=1)
        anchor0 = get_max_schedule_due_date_on_or_before(loan_id, bound)
        if anchor0 is not None and (disbursement is None or anchor0 >= disbursement):
            period_start = anchor0
        else:
            period_start = disbursement or start
        # Same-day stub at disbursement: pro-rated interest only (no principal movement).
        if disbursement and due_d == disbursement and abs(principal_c) <= 1e-9:
            continue
        due_entries.append((due_d, period_start, interest_c, principal_c, False))
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

    def _has_completed_eod_for_date(d: date) -> bool:
        """
        True if there is a completed EOD run recorded for date d.

        Why we need this:
        `loan_daily_state` rows can exist for a date even when EOD has not run for that date
        (e.g. created/updated by intraday receipt allocation). In that case, period-to-date
        columns (regular/default/penalty *_period_to_date) are often copied from prior day
        and are NOT trustworthy for statement "current period" accrual lines.
        """
        try:
            from loan_management import _connection
            from psycopg2.extras import RealDictCursor

            with _connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM eod_runs
                        WHERE as_of_date = %s
                          AND finished_at IS NOT NULL
                          AND run_status IN ('SUCCESS', 'DEGRADED')
                        ORDER BY started_at DESC
                        LIMIT 1
                        """,
                        (d,),
                    )
                    return bool(cur.fetchone())
        except Exception:
            # If audit table isn't present, fall back to trusting daily_state (legacy behavior).
            return True

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
    drawdown_facility = float(orig_facility) if orig_facility is not None else booked_principal

    if disbursement and start <= disbursement <= end and drawdown_facility > 0:
        breakdown = _get_drawdown_breakdown(loan, facility_principal=drawdown_facility)
        for narration, amt in breakdown:
            if amt <= 0:
                continue
            row = _blank_row_periodic()
            row["Due Date"] = disbursement
            row["Narration"] = narration
            row["Principal"] = _f3(amt)
            rows.append(row)

    from eod.core import load_system_config_from_db
    sys_cfg = load_system_config_from_db() or {}
    try:
        from eod.core import get_product_config_from_db
        p_cfg = get_product_config_from_db(loan.get("product_code"))
        if p_cfg:
            sys_cfg = {**sys_cfg, **p_cfg}
    except Exception:
        pass

    prev_fees = 0.0
    for due_d, period_start_d, interest_c, principal_c, include_start_day in due_entries:
        state_at_due = _state_at(due_d)
        
        # Accrued interest, default, penalty: period_to_date for the instalment period ends the
        # calendar day before the due date (due date starts the next period).
        state_for_accruals = _state_at(due_d - timedelta(days=1))

        sum_regular_from_state = (
            float(state_for_accruals.get("regular_interest_period_to_date") or 0) if state_for_accruals else 0.0
        )
        # Customer policy: contractual schedule interest for the period (10dp) is authoritative
        # when present; avoids statement/EOD drift (e.g. 101.75 vs 105.2631578947).
        if interest_c and float(interest_c) > 1e-12:
            sum_regular = float(as_10dp(Decimal(str(interest_c))))
        else:
            sum_regular = float(as_10dp(Decimal(str(sum_regular_from_state))))
        sum_penalty = float(state_for_accruals.get("penalty_interest_period_to_date") or 0) if state_for_accruals else 0.0
        sum_default = float(state_for_accruals.get("default_interest_period_to_date") or 0) if state_for_accruals else 0.0
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

        # Date range: accrual from period start through the day before instalment due.
        end_d_for_label = due_d - timedelta(days=1)
        if end_d_for_label < period_start_d:
            end_d_for_label = period_start_d
        start_str = period_start_d.strftime('%d/%m/%y')
        end_str = end_d_for_label.strftime('%d/%m/%y')
        period_range_sfx = f" ({start_str} to {end_str})"

        row = _blank_row_periodic()
        row["Due Date"] = due_d
        row["Narration"] = due_narration
        row["_period_range_sfx"] = period_range_sfx
        row["Interest"] = _f10(sum_regular)
        row["Penalty"] = _f10(sum_penalty)
        row["Default"] = _f10(sum_default)
        row["Principal"] = _f3(principal_c)
        row["Fees"] = _f10(fees_in_period) if fees_in_period else 0.0
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
            try:
                _rpid_inner = int(r.get("id") or 0)
            except (TypeError, ValueError):
                _rpid_inner = 0
            if _rpid_inner and _rpid_inner in processed_repayment_ids:
                continue
            if _is_internal_unapplied_liquidation_repayment_for_statement(r):
                if _rpid_inner:
                    processed_repayment_ids.add(_rpid_inner)
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
                    # Hide bucket split "... paid" rows in customer-facing statements.
                    # The cash movement is already represented by "Repayment id ..." rows.
                    _ = (pname, pamt, bucket, teller_ref)
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
        if _is_internal_unapplied_liquidation_repayment_for_statement(r):
            if rid_int is not None:
                processed_repayment_ids.add(rid_int)
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
                # Hide bucket split "... paid" rows in customer-facing statements.
                _ = (pname, pamt, bucket, teller_ref)

        # Unapplied ledger movements as explicit periodic rows.
    for u in unapplied_lines:
        vd = _date_conv(u.get("value_date"))
        if not vd or vd < start or vd > end:
            continue
        kind = (u.get("entry_kind") or "").strip().lower()
        delta = _f10(float(u.get("unapplied_delta") or 0))
        if delta == 0.0:
            continue
            
        row = _blank_row_periodic()
        row["Due Date"] = vd
        rk = u.get("repayment_key") or ""
        rep_id = u.get("repayment_id")
        parent_id = u.get("parent_repayment_id")
        reversal_of_id = u.get("reversal_of_id")
        
        if kind == "liquidation":
            if parent_id:
                row["Narration"] = f"LIQ-{rep_id} from OP-{parent_id}"
            else:
                row["Narration"] = f"LIQ-{rep_id}"
        elif kind == "credit":
            rid_disp = rep_id if rep_id is not None else (rk or "").strip() or "?"
            src_amt = u.get("source_receipt_amount")
            if src_amt is not None:
                row["Narration"] = (
                    f"Overpayment from Repayment id {rid_disp} (Receipt {_f10(float(src_amt))})"
                )
            else:
                row["Narration"] = f"Overpayment from Repayment id {rid_disp}"
        elif kind == "reversal":
            if delta > 0:
                # Reversal of liquidation
                if parent_id:
                    row["Narration"] = f"REV-LIQ-{reversal_of_id} (Orig: OP-{parent_id})"
                else:
                    row["Narration"] = f"REV-LIQ-{reversal_of_id}"
            else:
                # Reversal of credit (receipt)
                row["Narration"] = f"REV-Overpayment-{reversal_of_id} (Voiding OP-{reversal_of_id})"
        else:
            continue

        # Internal movement only: no cash debit/credit impact on periodic "Credits" column;
        # customer-facing statement derives **Credits** from allocation buckets (and falls back
        # to |unapplied_delta| when the view leaves splits at zero so Balance reduces correctly).
        row["Credits"] = 0.0
        _liq_alloc_int = (
            float(u.get("alloc_int_arrears") or 0)
            + float(u.get("alloc_penalty_int") or 0)
            + float(u.get("alloc_default_int") or 0)
        )
        _liq_alloc_fees = float(u.get("alloc_fees_charges") or 0)
        _liq_alloc_cap = float(u.get("alloc_prin_arrears") or 0)
        _liq_alloc_sum = _liq_alloc_int + _liq_alloc_fees + _liq_alloc_cap
        if kind == "liquidation" and delta < 0 and _liq_alloc_sum < 1e-9:
            # unapplied_delta is negative when funds leave unapplied into loan buckets.
            row["Portion of Credit Allocated to Interest"] = _f10(abs(delta))
            row["Credit Allocated to Fees"] = 0.0
            row["Credit Allocated to Capital"] = 0.0
        else:
            row["Portion of Credit Allocated to Interest"] = _f10(_liq_alloc_int)
            row["Credit Allocated to Fees"] = _f10(_liq_alloc_fees)
            row["Credit Allocated to Capital"] = _f10(_liq_alloc_cap)
        bal = _state_at(vd)
        if bal:
            row["Total Outstanding Balance"] = _f3(_total_outstanding(bal))
            row["Arrears"] = _f3(_total_delinquency_arrears(bal))
            
        # The unapplied funds running balance on the ledger reflects ONLY that specific transaction.
        # However, for customer-facing display in the statement column, if there are multiple
        # unapplied transactions on the same day or between due dates, we want the running total
        # relative to the date's real closing balance.
        # We start with the daily end-of-day balance...
        unapplied_display = _unapplied_at(vd)
        # But we must apply the specific transaction delta for rows that occur sequentially, so
        # the statement will waterfall backwards or forwards through the intraday transactions.
        # (This is handled accurately in the customer_facing_statement post-processing waterfall).
        # We will set a marker to help the post-processing engine sequence it properly.
        row["Unapplied funds"] = _f3(float(u.get("unapplied_running_balance") or 0))
        row["_unapplied_delta"] = delta
        _acr_imp = _liq_alloc_sum
        if kind == "liquidation" and delta < 0 and _acr_imp < 1e-9:
            _acr_imp = abs(delta)
        row["_arrears_credit_impact"] = _f10(_acr_imp)
        rows.append(row)

    # Current (incomplete) period interest -- emitted BEFORE the non-cash residual
    # so it is counted in current_charge_total and the residual is zero for clean loans.
    # Fires when:
    #   (a) there are due dates in range and end is strictly after the last one (tail of a period), OR
    #   (b) no due dates have fallen yet (statement is entirely in the first period).
    # When end equals a schedule due date, accrual through the prior day is already on that due
    # row — do not emit a duplicate stub for the same window.
    last_due_in_range = due_entries[-1][0] if due_entries else None
    period_boundary = last_due_in_range or disbursement
    # Only treat end-day daily_state as "exact" for current-period accrual lines if EOD ran for that date.
    end_bal_exact = ds_by_date.get(end) if _has_completed_eod_for_date(end) else None
    last_persisted_ds_date = max(ds_by_date.keys()) if ds_by_date else end
    
    # Precompute unapplied balances strictly from ledger for specific statement generation points
    end_unapplied_ledger = float(_unapplied_at(end))

    has_current_period = False
    if period_boundary is not None:
        if due_entries and last_due_in_range is not None:
            has_current_period = end > last_due_in_range
        else:
            has_current_period = end >= period_boundary

    if has_current_period:
        # Period-to-date stub: row date = **statement end** (`end`, capped to system date).
        # Label window = **most recent due (inclusive)** through **end − 1 day (inclusive)**.
        stub_row_date = end
        stub_period_end = end - timedelta(days=1)
        # Accrual period start for the stub must follow schedule boundaries across all saved
        # versions (post-recast latest schedule often omits historical dues such as 29-Mar).
        stub_anchor_due = get_max_schedule_due_date_on_or_before(loan_id, stub_period_end)
        skip_accrual = False
        if disbursement is None:
            if stub_period_end < stub_row_date:
                skip_accrual = True
        elif stub_period_end < disbursement:
            skip_accrual = True
        elif last_due_in_range and stub_period_end < last_due_in_range:
            if end != last_due_in_range:
                skip_accrual = True

        stub_ds_exact = ds_by_date.get(stub_period_end) if _has_completed_eod_for_date(stub_period_end) else None

        if skip_accrual:
            cur_regular = cur_penalty = cur_default = 0.0
            cur_fees = 0.0
            current_period_total = 0.0
        else:
            if stub_ds_exact:
                cur_regular = float(stub_ds_exact.get("regular_interest_period_to_date") or 0)
                cur_penalty = float(stub_ds_exact.get("penalty_interest_period_to_date") or 0)
                cur_default = float(stub_ds_exact.get("default_interest_period_to_date") or 0)
            else:
                cur_regular = cur_penalty = cur_default = 0.0
                for ds in daily_states:
                    ad = _date_conv(ds.get("as_of_date"))
                    if not ad or ad > stub_period_end:
                        continue
                    if stub_anchor_due and ad < stub_anchor_due:
                        continue
                    if (
                        not stub_anchor_due
                        and last_due_in_range
                        and ad < last_due_in_range
                    ):
                        continue
                    if (
                        not stub_anchor_due
                        and not last_due_in_range
                        and disbursement
                        and ad < disbursement
                    ):
                        continue

                    cur_regular += float(ds.get("regular_interest_daily") or 0)
                    cur_penalty += float(ds.get("penalty_interest_daily") or 0)
                    cur_default += float(ds.get("default_interest_daily") or 0)
            st_as_of = _state_at(stub_period_end)
            cur_fees = 0.0
            if st_as_of:
                fe_now = float(st_as_of.get("fees_charges_balance") or 0)
                if stub_anchor_due:
                    st_ld = _state_at(stub_anchor_due)
                    fe_ld = float(st_ld.get("fees_charges_balance") or 0) if st_ld else 0.0
                elif last_due_in_range:
                    st_ld = _state_at(last_due_in_range)
                    fe_ld = float(st_ld.get("fees_charges_balance") or 0) if st_ld else 0.0
                elif disbursement:
                    st0 = _state_at(disbursement)
                    fe_ld = float(st0.get("fees_charges_balance") or 0) if st0 else 0.0
                else:
                    fe_ld = 0.0
                cur_fees = max(0.0, fe_now - fe_ld)
            cur_regular = float(as_10dp(Decimal(str(cur_regular))))
            cur_penalty = float(as_10dp(Decimal(str(cur_penalty))))
            cur_default = float(as_10dp(Decimal(str(cur_default))))
            cur_fees = float(as_10dp(Decimal(str(cur_fees))))
            current_period_total = cur_regular + cur_penalty + cur_default + cur_fees
        if abs(current_period_total) > 0.005:
            row = _blank_row_periodic()
            row["Due Date"] = stub_row_date
            curr_start = stub_anchor_due or last_due_in_range or disbursement or stub_period_end
            if disbursement and curr_start < disbursement:
                curr_start = disbursement
            if curr_start > stub_period_end:
                curr_start = stub_period_end
            curr_start_str = curr_start.strftime("%d/%m/%y")
            curr_end_str = stub_period_end.strftime("%d/%m/%y")
            period_range_sfx = f" ({curr_start_str} to {curr_end_str})"

            row["Narration"] = f"Accrued interest{period_range_sfx}"
            row["_period_range_sfx"] = period_range_sfx
            row["Interest"] = _f10(cur_regular)
            row["Penalty"] = _f10(cur_penalty)
            row["Default"] = _f10(cur_default)
            row["Fees"] = _f10(cur_fees) if cur_fees else 0.0
            end_bal = _state_at(stub_period_end)
            if end_bal:
                row["Total Outstanding Balance"] = _f3(_total_outstanding(end_bal))
                row["Arrears"] = _f3(_total_delinquency_arrears(end_bal))
            row["Unapplied funds"] = _f3(float(_unapplied_at(stub_row_date)))
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
    row["Unapplied funds"] = _f3(end_unapplied_ledger)
    rows.append(row)

    def _sort_key(r: dict) -> tuple:
        due_date = r.get("Due Date") or date(9999, 12, 31)
        narr = r.get("Narration") or ""
        
        if narr.startswith("Total outstanding"):
            return (due_date, 99, (0, 0, ""), narr)
        if "Unapplied funds credit" in narr or narr.startswith("OP-"):
            return (due_date, 5, (0, 0, ""), narr)
        if (
            narr.startswith("Liquidation of unapplied")
            or narr.startswith("LIQ-")
            or narr.startswith("REV-LIQ-")
            or "Reversal of unapplied liquidation" in narr
        ):
            return (due_date, 6, _liq_rev_interleave_sort(narr), narr)
        if narr.startswith("REV-RCPT-") or (
            "Reversal of unapplied" in narr and "liquidation" not in narr.lower()
        ):
            return (due_date, 7, (0, 0, ""), narr)
        return (due_date, 1, (0, 0, ""), narr)

    rows.sort(key=_sort_key)
    return rows, meta


def recalculate_flow_statement_running_balances(
    rows: list[dict[str, Any]],
    *,
    opening_loan: Any,
    opening_unapplied: Any,
) -> None:
    """
    Recompute ``Balance`` and ``Unapplied funds`` from openings in **strict row order**
    (print / CSV / PDF). Call after roll-up or any reorder that moves rows without updating
    those columns. Matches :func:`reporting.statement_events.apply_dual_running_customer_events`
    when each row carries optional ``_unapplied_delta`` from unapplied ledger meta.
    """
    lb = _to_dec(opening_loan)
    ub = _to_dec(opening_unapplied)
    for r in rows:
        if r.get("_debit_dec") is not None:
            deb = _to_dec(r.get("_debit_dec"))
        else:
            deb = _to_dec(r.get("Debits") or 0)
        if r.get("_credit_dec") is not None:
            cred = _to_dec(r.get("_credit_dec"))
        else:
            cred = _to_dec(r.get("Credits") or 0)
        ud_raw = r.get("_unapplied_delta")
        if ud_raw is not None:
            ub = Decimal(str(as_10dp(ub + _to_dec(ud_raw))))
        lb = Decimal(str(as_10dp(lb + deb - cred)))
        r["Balance"] = _f3(lb)
        r["Unapplied funds"] = _f3(ub)


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
        elif narration.startswith("Liquidation of unapplied") or narration.startswith("LIQ-"):
            credits_alloc = _to_dec(r.get("Portion of Credit Allocated to Interest") or 0) + \
                            _to_dec(r.get("Credit Allocated to Fees") or 0) + \
                            _to_dec(r.get("Credit Allocated to Capital") or 0)
            
            delta_val = r.get("_unapplied_delta", 0.0)
            import re
            m = re.search(r"\(Receipt ([0-9.]+)\)", narration)
            if m and float(delta_val) == 0.0:
                delta_val = float(m.group(1))
            delta_val = _f10(delta_val)

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
                "_unapplied_delta": delta_val,
            })
        elif narration.startswith("Reversal of unapplied liquidation") or narration.startswith("REV-LIQ-"):
            # Undo of a liquidation: show it as a Debit of the same magnitude,
            # so the statement reads like "Liquidation" (credit) then "REV" (debit).
            credits_alloc = _to_dec(r.get("Portion of Credit Allocated to Interest") or 0) + \
                            _to_dec(r.get("Credit Allocated to Fees") or 0) + \
                            _to_dec(r.get("Credit Allocated to Capital") or 0)
            debits_alloc = abs(credits_alloc)
            
            delta_val = r.get("_unapplied_delta", 0.0)
            import re
            m = re.search(r"\(Receipt ([0-9.]+)\)", narration)
            if m and float(delta_val) == 0.0:
                delta_val = -float(m.group(1))
            delta_val = _f10(delta_val)

            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration,
                "Debits": _f3(debits_alloc),
                "Credits": 0.0,
                "Balance": 0.0,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
                "_unapplied_delta": delta_val,
            })
        elif narration.startswith("Unapplied funds credit") or narration.startswith("Reversal of unapplied") or narration.startswith("OP-") or narration.startswith("REV-RCPT-"):
            # Use ledger _unapplied_delta only. Do not substitute (Receipt X) = full receipt amount;
            # that is not the same as unapplied_delta and breaks running Unapplied after reversals.
            delta_val = r.get("_unapplied_delta", 0.0)
            import re
            m = re.search(r"\(Receipt ([0-9.]+)\)", narration)
            if m and float(delta_val) == 0.0 and not narration.startswith("OP-") and not narration.startswith("REV-RCPT-"):
                val = float(m.group(1))
                if "Unapplied funds credit" in narration or "Liquidation of unapplied" in narration or narration.startswith("LIQ-"):
                    delta_val = val
                else:
                    delta_val = -val
            delta_val = _f10(delta_val)

            # Use delta_val to show the credit properly
            # if it's an unapplied funds credit, make sure delta_val represents the credit
            out.append({
                "Due Date": r.get("Due Date"),
                "Narration": narration,
                "Debits": 0.0,
                "Credits": 0.0,
                "Balance": 0.0,
                "Arrears": arrears,
                "Unapplied funds": unapplied,
                "_unapplied_delta": delta_val,
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
            # Quantize to 10dp and distribute residual to largest component so
            # component rows sum exactly to the quantized period total.
            interest = _to_dec(r.get("Interest") or 0)
            penalty = _to_dec(r.get("Penalty") or 0)
            default = _to_dec(r.get("Default") or 0)
            fees = _to_dec(r.get("Fees") or 0)
            # Use explicit date range if available, otherwise fallback
            sfx = r.get("_period_range_sfx") or ""
            is_current_period = narration.startswith("Current period interest (")
            if is_current_period and not sfx:
                sfx = " (period to date)"
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
                rounded_total = _quantize_statement_decimal(
                    sum((v for _, v in raw_components), Decimal("0"))
                )
                rounded_components = [
                    (n, _quantize_statement_decimal(v), v) for n, v in raw_components
                ]
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
                    # Customer-facing simplification:
                    # hide bucket-allocation split rows (e.g. "... paid").
                    # Cash movement is already shown by "Repayment id ..." rows.
                    if is_bucket_payment_line:
                        continue
                    # Charges: positive -> Debit; negative (reversal of accrual) -> Credit.
                    # Bucket payment lines are informational allocation breakdown.
                    # Cash movement is already represented by "Repayment id ..." rows.
                    # Guard: never let these rows contribute additional statement credits/debits.
                    component_debits = _f10(rv) if rv > 0 else 0.0
                    component_credits = _f10(abs(rv)) if rv < 0 else 0.0
                    comp_row = {
                        "Due Date": r.get("Due Date"),
                        "Narration": n,
                        "Debits": component_debits,
                        "Credits": component_credits,
                        "Balance": 0.0,
                        "Arrears": arrears,
                        "Unapplied funds": unapplied,
                    }
                    rid_comp = r.get("_repayment_id")
                    if rid_comp is not None:
                        try:
                            comp_row["_repayment_id"] = int(rid_comp)
                        except (TypeError, ValueError):
                            pass
                    out.append(comp_row)

    # Preserve periodic/engine row order so printed Balance is a true running total (no same-day
    # reshuffle that reordered rows without recomputing Balance).
    _apply_customer_facing_arrears_before_first_receipt(loan_id, out)

    # Guardrail: for any repayment_id, total statement Credits attributed to that repayment
    # must never exceed the repayment credit itself (alloc_total shown on "Repayment id ...").
    # If future rendering changes reintroduce extra credit rows for the same repayment,
    # trim non-repayment rows first.
    repayment_credit_cap: dict[int, Decimal] = {}
    repayment_credit_used: dict[int, Decimal] = {}
    for row in out:
        rid = row.get("_repayment_id")
        if rid is None:
            continue
        try:
            rid_i = int(rid)
        except (TypeError, ValueError):
            continue
        narr = str(row.get("Narration") or "")
        c = _to_dec(row.get("Credits") or 0)
        if narr.startswith("Repayment id ") and c > Decimal("0"):
            repayment_credit_cap[rid_i] = c

    for row in out:
        rid = row.get("_repayment_id")
        if rid is None:
            continue
        try:
            rid_i = int(rid)
        except (TypeError, ValueError):
            continue
        cap = repayment_credit_cap.get(rid_i)
        if cap is None:
            continue
        c = _to_dec(row.get("Credits") or 0)
        if c <= Decimal("0"):
            continue
        used = repayment_credit_used.get(rid_i, Decimal("0"))
        remaining = cap - used
        if remaining <= Decimal("0"):
            row["Credits"] = 0.0
            continue
        if c > remaining:
            row["Credits"] = _f3(remaining)
            repayment_credit_used[rid_i] = cap
        else:
            repayment_credit_used[rid_i] = used + c

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

    # Last movement row per date (exclude closing "Total outstanding" line). Arrears snap to
    # loan_daily_state on that row so same-day lines before it stay on the running delinquency path.
    last_non_closing_idx_by_due_date: dict[Any, int] = {}
    for idx, row in enumerate(out):
        d = row.get("Due Date")
        if d is None:
            continue
        narr_lc = str(row.get("Narration") or "")
        if not narr_lc.startswith("Total outstanding balance as at"):
            last_non_closing_idx_by_due_date[d] = idx

    # Unapplied column: align to unapplied_funds_ledger (running_balance / matched rows).
    # The old _unapplied_delta accumulator drifted for LIQ/RECAST flows and never matched DB.
    _apply_customer_facing_unapplied_from_ledger(loan_id, meta, out)

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

    # Running Balance: prior-day opening + sum(Debits − Credits) in strict print order (no EOD snaps).
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

        # No mid-statement snaps to loan_daily_state: they matched EOD on one row but left prior
        # same-day lines with a Balance that did not read as a continuous running total when printed.

    # (Drift notification logic removed.)

    for _row in out:
        _row.pop("_repayment_id", None)
        _row.pop("_arrears_credit_impact", None)
        _row.pop("_arrears_debit_impact", None)

    return out, meta


def generate_customer_facing_flow_statement(
    loan_id: int,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    as_of_date: date | None = None,
    allowed_customer_ids: list[int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Customer-facing statement from discrete subledger events: drawdown, daily accruals,
    repayment bucket allocations, and unapplied ledger rows. **No mid-statement balance
    snap** — ``Balance`` is a running total; ``Unapplied funds`` runs from signed deltas.

    Arrears shown as total delinquency **as at** ``end_date`` on every row (static for this v1).

    Does **not** change GL. See ``docs/STATEMENT_FLOW_AND_RECONCILIATION_PLAN.md``.
    """
    from decimal import Decimal

    from reporting.statement_events import (
        apply_dual_running_customer_events,
        build_merged_customer_flow_events,
        reconcile_running_to_loan_daily_state,
        total_outstanding_decimal,
    )

    loan = get_loan(loan_id)
    if not loan:
        raise ValueError(f"Loan {loan_id} not found.")
    if allowed_customer_ids is not None:
        cust_id = loan.get("customer_id")
        if cust_id is None or cust_id not in allowed_customer_ids:
            raise ValueError("You are not authorized to view this loan statement.")

    today = as_of_date or _get_effective_date()
    disbursement = _date_conv(loan.get("disbursement_date") or loan.get("start_date"))
    start = start_date or disbursement or today
    end = end_date or today
    if end > today:
        end = today
    if start > end:
        start, end = end, start

    prior_ds = get_loan_daily_state_balances(loan_id, start - timedelta(days=1))
    opening_loan = total_outstanding_decimal(prior_ds) if prior_ds else Decimal("0")

    merged, opening_unapplied = build_merged_customer_flow_events(loan_id, start, end)
    dual = apply_dual_running_customer_events(merged, opening_loan, opening_unapplied)

    ds_end = get_loan_daily_state_balances(loan_id, end)
    arrears_row = _f3(_total_delinquency_arrears(ds_end))

    out: list[dict[str, Any]] = []
    for ev, loan_b, u_b in dual:
        deb_d = as_10dp(ev.debit)
        cred_d = as_10dp(ev.credit)
        row_ev: dict[str, Any] = {
            "Due Date": ev.event_date,
            "Narration": ev.narration,
            "Debits": float(deb_d),
            "Credits": float(cred_d),
            "Balance": _f3(loan_b),
            "Arrears": arrears_row,
            "Unapplied funds": _f3(u_b),
            "_event_type": ev.event_type,
            # Exact 10dp for roll-up + balance replay (float Debits alone drifts vs Decimal events).
            "_debit_dec": deb_d,
            "_credit_dec": cred_d,
        }
        if ev.meta.get("unapplied_delta") is not None:
            row_ev["_unapplied_delta"] = _f10(ev.meta["unapplied_delta"])
        out.append(row_ev)

    loan_closing = dual[-1][1] if dual else opening_loan
    unapplied_closing = dual[-1][2] if dual else opening_unapplied
    out.append(
        {
            "Due Date": end,
            "Narration": f"Total outstanding (flow) as at {end.isoformat()}",
            "Debits": 0.0,
            "Credits": 0.0,
            "Balance": _f3(loan_closing),
            "Arrears": arrears_row,
            "Unapplied funds": _f3(unapplied_closing),
        }
    )

    recon = reconcile_running_to_loan_daily_state(loan_closing, loan_id, end)
    meta = {
        "loan_id": loan_id,
        "customer_id": loan.get("customer_id"),
        "start_date": start,
        "end_date": end,
        "statement_type": "customer_facing_flow",
        "opening_loan": _f3(opening_loan),
        "opening_unapplied": _f3(opening_unapplied),
        "reconcile_loan_total": recon,
        "currency": (loan.get("metadata") or {}).get("currency") or "USD",
        "generated_at": datetime.now(),
    }
    notifications: list[str] = []
    if not recon.get("ok"):
        notifications.append(
            f"Flow statement loan total vs loan_daily_state: diff {recon.get('diff')} "
            f"(subledger {recon.get('subledger_closing')}). Mid-life fee posts or other gaps may apply."
        )
    meta["notifications"] = notifications
    return out, meta


def preview_statement_eod_flow_events(
    loan_id: int,
    start_date: date,
    end_date: date,
    *,
    eod_only: bool = False,
) -> dict[str, Any]:
    """
    Build discrete flow events, running loan outstanding (opening = end of day before
    ``start_date``), and reconcile to ``loan_daily_state`` at ``end_date``.

    ``eod_only=False`` (default): drawdown + daily accruals + repayment bucket allocations
    (see ``reporting/statement_events.build_complete_loan_flow_events``).

    ``eod_only=True``: daily accruals + fee-balance deltas only (diagnostic; do not mix with
    full payment fee allocations in one narrative).

    See ``docs/STATEMENT_FLOW_AND_RECONCILIATION_PLAN.md``.
    """
    from reporting.statement_events import build_flow_preview_for_loan

    return build_flow_preview_for_loan(loan_id, start_date, end_date, eod_only=eod_only)


def _blank_row_periodic() -> dict[str, Any]:
    row: dict[str, Any] = {}
    for h in PERIODIC_STATEMENT_HEADINGS:
        row[h] = 0 if h in PERIODIC_NUMERIC_HEADINGS else None
    return row
