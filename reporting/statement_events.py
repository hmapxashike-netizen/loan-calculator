"""
Discrete statement events for flow-based loan outstanding (no GL changes).

- **EOD slice:** daily regular/penalty/default accruals + optional fee-balance deltas
  (fee deltas double-count with bucket-level payments — use ``eod_only=True`` preview or
  omit fee deltas when analysing payment-inclusive flows).
- **Full slice:** drawdown debits + daily accruals + one line per receipt for allocations
  (net ``alloc_*`` from ``get_repayments_with_allocations``); optional ``meta['unapplied_delta']``
  when ``unallocated`` is non-zero, with matching unapplied-ledger **credit** / **reversal**
  rows omitted when they duplicate that delta.
  No separate fee-balance delta lines (fee movement is inside ``alloc_fees_charges``).

Unapplied-ledger-only movements stay out of the **loan outstanding** running total until
liquidation allocates to buckets (slice 3 / separate unapplied column).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable, Sequence

from decimal_utils import as_10dp

# Intra-day sort order (see docs/STATEMENT_EVENT_ORDER.md)
_SORT_DISBURSEMENT = 15
_SORT_ACCRUAL_REGULAR = 20
_SORT_ACCRUAL_PENALTY = 21
_SORT_ACCRUAL_DEFAULT = 22
_SORT_FEE_BALANCE_DELTA = 30
_SORT_REPAYMENT_BUCKET_BASE = 50
_SORT_UNAPPLIED_BASE = 60

_UNAPPLIED_KIND_ORDER: dict[str, int] = {"credit": 0, "reversal": 1, "liquidation": 2}


def _d(v: Any) -> Decimal:
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def _row_date(row: dict[str, Any]) -> date | None:
    ad = row.get("as_of_date")
    if ad is None:
        return None
    if isinstance(ad, date):
        return ad
    if isinstance(ad, datetime):
        return ad.date()
    if isinstance(ad, str):
        try:
            return datetime.fromisoformat(ad.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


# Keys summed for flow reconcile vs ``loan_daily_state`` (matches EOD ``total_exposure`` save).
_TOTAL_OUTSTANDING_BUCKET_KEYS: tuple[str, ...] = (
    "principal_not_due",
    "principal_arrears",
    "interest_accrued_balance",
    "interest_arrears_balance",
    "default_interest_balance",
    "penalty_interest_balance",
    "fees_charges_balance",
)


def total_outstanding_decimal(ds: dict[str, Any] | None) -> Decimal:
    """Same components as customer ``Balance`` / ``_total_outstanding`` in statements."""
    if not ds:
        return Decimal("0")
    return sum((_d(ds.get(k)) for k in _TOTAL_OUTSTANDING_BUCKET_KEYS), Decimal("0"))


@dataclass(frozen=True)
class StatementEvent:
    """Single statement line for flow-based reconstruction."""

    event_date: date
    event_type: str
    narration: str
    debit: Decimal
    credit: Decimal
    repayment_id: int | None = None
    sort_ordinal: int = 0
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.debit < 0 or self.credit < 0:
            raise ValueError("debit and credit must be non-negative")


def _q(v: Decimal) -> Decimal:
    return Decimal(str(as_10dp(v)))


def build_daily_interest_accrual_events(
    rows: Iterable[dict[str, Any]],
) -> list[StatementEvent]:
    """
    One debit event per calendar day per bucket when EOD persisted a non-zero daily.
    """
    out: list[StatementEvent] = []
    for row in rows:
        d = _row_date(row)
        if d is None:
            continue
        reg = _d(row.get("regular_interest_daily"))
        if reg > 0:
            # Some EOD paths may expose a non-zero ``regular_interest_daily`` even when
            # regular-interest stock is fully zeroed by day-close operations.
            # Do not emit a flow debit in that case, otherwise closing reconcile drifts.
            reg_stock_close = _d(row.get("interest_accrued_balance")) + _d(row.get("interest_arrears_balance"))
            if reg_stock_close <= 0:
                reg = Decimal("0")
        if reg > 0:
            out.append(
                StatementEvent(
                    event_date=d,
                    event_type="REGULAR_INTEREST_ACCRUAL",
                    narration=f"Regular interest accrual (EOD {d.isoformat()})",
                    debit=_q(reg),
                    credit=Decimal("0"),
                    sort_ordinal=_SORT_ACCRUAL_REGULAR,
                )
            )
        pen = _d(row.get("penalty_interest_daily"))
        if pen > 0:
            out.append(
                StatementEvent(
                    event_date=d,
                    event_type="PENALTY_INTEREST_ACCRUAL",
                    narration=f"Penalty interest accrual (EOD {d.isoformat()})",
                    debit=_q(pen),
                    credit=Decimal("0"),
                    sort_ordinal=_SORT_ACCRUAL_PENALTY,
                )
            )
        default = _d(row.get("default_interest_daily"))
        if default > 0:
            out.append(
                StatementEvent(
                    event_date=d,
                    event_type="DEFAULT_INTEREST_ACCRUAL",
                    narration=f"Default interest accrual (EOD {d.isoformat()})",
                    debit=_q(default),
                    credit=Decimal("0"),
                    sort_ordinal=_SORT_ACCRUAL_DEFAULT,
                )
            )
    return out


def build_fee_balance_delta_events(
    rows: list[dict[str, Any]],
    *,
    prior_fees_balance: Decimal,
) -> list[StatementEvent]:
    """
    Discrete fee *flow* from day-to-day change in ``fees_charges_balance``.

    Positive delta → debit (new charges). Negative → credit (reduction without
    splitting cause—allocations may also move fees; this is a subledger summary).
    """
    if not rows:
        return []
    out: list[StatementEvent] = []
    prev_fee = _q(prior_fees_balance)
    for row in sorted(rows, key=lambda r: (_row_date(r) or date.min,)):
        d = _row_date(row)
        if d is None:
            continue
        cur_fee = _q(_d(row.get("fees_charges_balance")))
        delta = _q(cur_fee - prev_fee)
        if delta != 0:
            if delta > 0:
                out.append(
                    StatementEvent(
                        event_date=d,
                        event_type="FEES_BALANCE_DELTA",
                        narration=f"Fees & charges movement (EOD {d.isoformat()})",
                        debit=_q(delta),
                        credit=Decimal("0"),
                        sort_ordinal=_SORT_FEE_BALANCE_DELTA,
                    )
                )
            else:
                out.append(
                    StatementEvent(
                        event_date=d,
                        event_type="FEES_BALANCE_DELTA",
                        narration=f"Fees & charges movement (EOD {d.isoformat()})",
                        debit=Decimal("0"),
                        credit=_q(-delta),
                        sort_ordinal=_SORT_FEE_BALANCE_DELTA,
                    )
                )
        prev_fee = cur_fee
    return out


def _parse_loan_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


# Atomic buckets only. ``alloc_principal_total`` / ``alloc_interest_total`` are roll-ups
# (e.g. principal_total = not_due + arrears); emitting them alongside components double-counts.
_ALLOC_BUCKET_SPECS: tuple[tuple[str, str], ...] = (
    ("alloc_principal_not_due", "Principal (not yet due)"),
    ("alloc_principal_arrears", "Principal arrears"),
    ("alloc_interest_accrued", "Interest accrued"),
    ("alloc_interest_arrears", "Interest arrears"),
    ("alloc_default_interest", "Default interest"),
    ("alloc_penalty_interest", "Penalty interest"),
    ("alloc_fees_charges", "Fees & charges"),
)


def build_disbursement_events_for_loan(
    loan: dict[str, Any],
    start_date: date,
    end_date: date,
) -> list[StatementEvent]:
    """
    Principal drawdown lines (debits) on disbursement date when it falls in the window.
    Uses the same fee split as the customer statement (``_get_drawdown_breakdown``).
    """
    from loan_management import get_original_facility_for_statements
    from reporting.statements import _get_drawdown_breakdown

    disb = _parse_loan_date(loan.get("disbursement_date") or loan.get("start_date"))
    lid = loan.get("id")
    orig_f = None
    if lid is not None:
        try:
            orig_f = get_original_facility_for_statements(int(lid), loan)
        except (TypeError, ValueError):
            orig_f = None
    fac_amt = float(orig_f) if orig_f is not None else float(loan.get("principal") or 0)
    fac = _d(fac_amt)
    if disb is None or fac <= 0:
        return []
    if not (start_date <= disb <= end_date):
        return []
    out: list[StatementEvent] = []
    for narr, amt in _get_drawdown_breakdown(loan, facility_principal=fac_amt):
        a = _d(amt)
        if a <= 0:
            continue
        out.append(
            StatementEvent(
                event_date=disb,
                event_type="DISBURSEMENT",
                narration=f"{narr} (drawdown)",
                debit=_q(a),
                credit=Decimal("0"),
                sort_ordinal=_SORT_DISBURSEMENT,
                meta={"drawdown_line": narr},
            )
        )
    return out


def build_repayment_allocation_events(repayments: Iterable[dict[str, Any]]) -> list[StatementEvent]:
    """
    One event per repayment: net allocated amount as debit/credit (reduces loan outstanding
    on credit). When ``unallocated`` is non-zero, sets ``meta['unapplied_delta']`` so the
    running unapplied column moves with the receipt; :func:`build_merged_customer_flow_events`
    drops redundant unapplied-ledger **credit** lines for the same receipt/date/delta.
    """
    out: list[StatementEvent] = []
    for r in repayments:
        rid = int(r.get("id") or 0)
        vd = r.get("value_date") or r.get("payment_date")
        event_d = _parse_loan_date(vd)
        if event_d is None:
            continue
        amt = _d(r.get("amount"))
        ref = (r.get("customer_reference") or r.get("reference") or "").strip()
        head = f"Repayment id {rid}"
        if ref:
            head = f"{head}: {ref}"
        head = f"{head} (Receipt {_q(amt)})"

        total_credit = Decimal("0")
        total_debit = Decimal("0")
        for col, _label in _ALLOC_BUCKET_SPECS:
            v = _d(r.get(col))
            if v == 0:
                continue
            if v > 0:
                total_credit = _q(total_credit + v)
            else:
                total_debit = _q(total_debit + (-v))

        if total_credit == 0 and total_debit == 0:
            continue

        rev = amt < 0
        narr = f"Reversal - {head}" if rev else head
        meta: dict[str, Any] = {}
        unallocated = _q(_d(r.get("unallocated")))
        if unallocated != 0:
            meta["unapplied_delta"] = str(as_10dp(unallocated))

        out.append(
            StatementEvent(
                event_date=event_d,
                event_type="PAYMENT_RECEIPT",
                narration=narr,
                debit=total_debit,
                credit=total_credit,
                repayment_id=rid if rid else None,
                sort_ordinal=_SORT_REPAYMENT_BUCKET_BASE,
                meta=meta,
            )
        )
    return out


def _dedupe_unapplied_ledger_credits_bundled_on_receipts(
    loan_events: Sequence[StatementEvent],
    unapplied_events: list[StatementEvent],
) -> list[StatementEvent]:
    """
    Remove unapplied-ledger rows that only repeat the same ``unapplied_delta`` already carried
    on a ``PAYMENT_RECEIPT`` for the same repayment and date.

    Covers **credit** (overpayment bundled on the receipt line) and **reversal** (negative
    ``unallocated`` on the reversal receipt). Without dropping the ledger reversal, dual
    running would zero unapplied on the receipt reversal row then subtract again on the
    ledger line (incorrect negative balance on statements).
    """
    bundled: dict[tuple[int, date], Decimal] = {}
    for e in loan_events:
        if e.event_type != "PAYMENT_RECEIPT":
            continue
        raw = e.meta.get("unapplied_delta")
        if raw is None:
            continue
        try:
            delta_b = _q(Decimal(str(raw)))
        except Exception:
            continue
        if delta_b == 0:
            continue
        rid = e.repayment_id or 0
        if rid <= 0:
            continue
        bundled[(rid, e.event_date)] = delta_b

    if not bundled:
        return unapplied_events

    kept: list[StatementEvent] = []
    for u in unapplied_events:
        if u.event_type != "UNAPPLIED_LEDGER":
            kept.append(u)
            continue
        kind = str(u.meta.get("entry_kind") or "").lower()
        if kind not in ("credit", "reversal"):
            kept.append(u)
            continue
        rid_u = u.repayment_id or 0
        key = (rid_u, u.event_date)
        if key not in bundled:
            kept.append(u)
            continue
        try:
            ud_u = _q(Decimal(str(u.meta.get("unapplied_delta", "0"))))
        except Exception:
            kept.append(u)
            continue
        if ud_u == bundled[key]:
            continue
        kept.append(u)
    return kept


def _ledger_value_date(row: dict[str, Any]) -> date | None:
    vd = row.get("value_date")
    return _parse_loan_date(vd)


def unapplied_opening_balance_from_ledger(loan_id: int, day_before_start: date) -> Decimal:
    """Running unapplied at end of ``day_before_start`` (inclusive ledger rows)."""
    from loan_management import get_unapplied_ledger_entries_for_statement

    rows = get_unapplied_ledger_entries_for_statement(loan_id, day_before_start, day_before_start)
    if not rows:
        return Decimal("0")
    return _q(_d(rows[-1].get("unapplied_running_balance")))


def _unapplied_ledger_loan_debit_credit(row: dict[str, Any]) -> tuple[Decimal, Decimal]:
    """
    Loan outstanding movement for suspense ↔ buckets (excludes pure overpayment credits).

    Liquidation (funds leave unapplied into loan buckets): **credit** reduces loan total.
    Reversal of that allocation (lineage via parent receipt): **debit** increases loan total.

    Uses ``abs(unapplied_delta)`` so principal-not-due / accrued splits not omitted in the
    ledger view still reconcile to the full amount applied (synthetic repayment rows are
    excluded from flow PAYMENT_* events).
    """
    kind = str(row.get("entry_kind") or "").strip().lower()
    delta = _d(row.get("unapplied_delta"))
    parent = row.get("parent_repayment_id")
    if kind == "liquidation" and delta < 0:
        return Decimal("0"), _q(abs(delta))
    if kind == "reversal" and delta > 0 and parent is not None:
        return _q(abs(delta)), Decimal("0")
    return Decimal("0"), Decimal("0")


def build_unapplied_ledger_statement_events(
    ledger_rows: Iterable[dict[str, Any]],
) -> list[StatementEvent]:
    """
    One event per ledger row. ``meta['unapplied_delta']`` moves **Unapplied funds**;
    liquidations also set ``debit``/``credit`` so **loan Balance** reflects suspense applied
    to buckets (synthetic allocation receipts are not duplicated as PAYMENT_* flow lines).
    """
    out: list[StatementEvent] = []
    for row in ledger_rows:
        vd = _ledger_value_date(row)
        if vd is None:
            continue
        delta = _d(row.get("unapplied_delta"))
        kind = str(row.get("entry_kind") or "").lower()
        rid = row.get("repayment_id")
        try:
            rid_i = int(rid) if rid is not None else 0
        except (TypeError, ValueError):
            rid_i = 0
        liq = row.get("liquidation_repayment_id")
        parts = [f"Unapplied {kind}"]
        if rid_i:
            parts.append(f"repayment {rid_i}")
        if liq:
            parts.append(f"liquidation {liq}")
        narr = " - ".join(parts)
        sub = _UNAPPLIED_KIND_ORDER.get(kind, 9)
        db, cr = _unapplied_ledger_loan_debit_credit(dict(row))
        out.append(
            StatementEvent(
                event_date=vd,
                event_type="UNAPPLIED_LEDGER",
                narration=narr,
                debit=_q(db),
                credit=_q(cr),
                repayment_id=rid_i if rid_i else None,
                sort_ordinal=_SORT_UNAPPLIED_BASE + sub,
                meta={
                    "unapplied_delta": str(as_10dp(delta)),
                    "entry_kind": kind,
                    "liquidation_repayment_id": liq,
                },
            )
        )
    return out


def build_merged_customer_flow_events(
    loan_id: int,
    start_date: date,
    end_date: date,
) -> tuple[list[StatementEvent], Decimal]:
    """
    Loan flow events + unapplied ledger rows with ``start_date <= value_date <= end_date``.
    Returns (merged_sorted_events, opening_unapplied_balance).
    """
    from loan_management import get_unapplied_ledger_entries_for_statement

    loan_ev = build_complete_loan_flow_events(loan_id, start_date, end_date)
    all_u = get_unapplied_ledger_entries_for_statement(loan_id, start_date, end_date)
    prior = [r for r in all_u if (d0 := _ledger_value_date(r)) is not None and d0 < start_date]
    opening_u = _q(_d(prior[-1].get("unapplied_running_balance"))) if prior else Decimal("0")
    window = [r for r in all_u if (d1 := _ledger_value_date(r)) is not None and start_date <= d1 <= end_date]
    u_ev = build_unapplied_ledger_statement_events(window)
    u_ev = _dedupe_unapplied_ledger_credits_bundled_on_receipts(loan_ev, u_ev)
    return merge_sort_statement_events([*loan_ev, *u_ev]), opening_u


def merge_sort_statement_events(events: Iterable[StatementEvent]) -> list[StatementEvent]:
    """Stable ordering: date, ordinal, repayment_id, type, narration."""
    return sorted(
        events,
        key=lambda e: (
            e.event_date,
            e.sort_ordinal,
            e.repayment_id or 0,
            e.event_type,
            e.narration,
        ),
    )


def merge_sort_eod_flow_events(events: Iterable[StatementEvent]) -> list[StatementEvent]:
    """Backward-compatible alias for :func:`merge_sort_statement_events`."""
    return merge_sort_statement_events(events)


def apply_running_loan_outstanding(
    events: Sequence[StatementEvent],
    opening_balance: Decimal,
) -> list[tuple[StatementEvent, Decimal]]:
    """
    Balance[i] = Balance[i-1] + debit[i] - credit[i]. No snap to stock mid-stream.
    Returns (event, balance_after) pairs.
    """
    bal = _q(opening_balance)
    out: list[tuple[StatementEvent, Decimal]] = []
    for ev in events:
        bal = _q(bal + ev.debit - ev.credit)
        out.append((ev, bal))
    return out


_ACCRUAL_ROLLUP_TYPES: frozenset[str] = frozenset(
    {
        "REGULAR_INTEREST_ACCRUAL",
        "PENALTY_INTEREST_ACCRUAL",
        "DEFAULT_INTEREST_ACCRUAL",
    }
)

_ACCRUAL_ROLLUP_LABELS: dict[str, str] = {
    "REGULAR_INTEREST_ACCRUAL": "Regular interest",
    "PENALTY_INTEREST_ACCRUAL": "Penalty interest",
    "DEFAULT_INTEREST_ACCRUAL": "Default interest",
}


def _strip_flow_row_for_rollup_display(r: dict[str, Any]) -> dict[str, Any]:
    """
    Remove engine-only keys for display, but keep ``_unapplied_delta`` so callers can
    recompute ``Balance`` / ``Unapplied funds`` after reordering rolled accrual rows.
    Keep ``_debit_dec`` / ``_credit_dec`` (10dp) so roll-up sums and balance replay match
    :func:`apply_dual_running_customer_events` (float ``Debits`` alone drifts).
    """
    _keep = frozenset({"_unapplied_delta", "_debit_dec", "_credit_dec"})
    return {
        k: v
        for k, v in r.items()
        if (not str(k).startswith("_")) or k in _keep
    }


def _flow_row_money_debit(r: dict[str, Any]) -> Decimal:
    raw = r.get("_debit_dec")
    if raw is not None:
        return _q(raw if isinstance(raw, Decimal) else Decimal(str(raw)))
    return as_10dp(Decimal(str(r.get("Debits") or 0)))


def _flow_row_money_credit(r: dict[str, Any]) -> Decimal:
    raw = r.get("_credit_dec")
    if raw is not None:
        return _q(raw if isinstance(raw, Decimal) else Decimal(str(raw)))
    return as_10dp(Decimal(str(r.get("Credits") or 0)))


def _fmt_roll_day(d: Any) -> str:
    if d is None:
        return ""
    if isinstance(d, datetime):
        return d.date().strftime("%d/%m/%y")
    if isinstance(d, date):
        return d.strftime("%d/%m/%y")
    if isinstance(d, str) and len(d) >= 10:
        try:
            return datetime.fromisoformat(d.replace("Z", "+00:00")).date().strftime("%d/%m/%y")
        except ValueError:
            return d[:10]
    return str(d)[:16]


def _parse_flow_statement_row_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, str) and len(raw) >= 10:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
        except ValueError:
            pass
    return None


def _is_flow_closing_total_row(r: dict[str, Any]) -> bool:
    return "Total outstanding (flow)" in str(r.get("Narration") or "")


def _rollup_effective_system_date() -> date:
    try:
        from eod.system_business_date import get_effective_date

        return get_effective_date()
    except Exception:
        return date.today()


def _flow_row_has_receipt_or_fee_on_date(rows: Iterable[dict[str, Any]], on_date: date) -> bool:
    """Receipt allocation or drawdown on ``on_date`` extends accrual roll-up through that day."""
    for r in rows:
        d = _parse_flow_statement_row_date(r.get("Due Date"))
        if d != on_date:
            continue
        et = str(r.get("_event_type") or "")
        if et == "DISBURSEMENT" or et.startswith("PAYMENT_"):
            return True
    return False


_FLOW_ACCRUAL_TYPES: frozenset[str] = frozenset(
    {
        "REGULAR_INTEREST_ACCRUAL",
        "PENALTY_INTEREST_ACCRUAL",
        "DEFAULT_INTEREST_ACCRUAL",
    }
)


def _day_has_cash_or_loan_movement(events: Sequence[StatementEvent], on_date: date) -> bool:
    """
    True when ``on_date`` has drawdown, a receipt, or unapplied ledger movement that affects
    loan outstanding (liquidation / reversal). Used to decide whether EOD accruals on that
    calendar day belong in the customer flow.
    """
    d0 = on_date
    if hasattr(d0, "date") and callable(getattr(d0, "date", None)):
        d0 = d0.date()
    for e in events:
        if e.event_date != d0:
            continue
        if e.event_type == "DISBURSEMENT":
            return True
        if e.event_type.startswith("PAYMENT_"):
            return True
        if e.event_type == "UNAPPLIED_LEDGER" and (e.debit > 0 or e.credit > 0):
            return True
    return False


def exclude_statement_end_accruals_without_activity(
    events: list[StatementEvent],
    *,
    statement_end: date,
    window_start: date,
) -> tuple[list[StatementEvent], bool]:
    """
    Drop interest accrual events dated **statement_end** (the inclusive end of this statement)
    unless there is drawdown / receipt / loan-moving unapplied activity on that day.

    Uses the statement's **end date**, not the live system clock, so historical statements
    (e.g. period ending 2024-04-02) behave correctly even when the machine date or DB
    ``current_system_date`` is years later.

    Returns (filtered_events, applied) where ``applied`` is True if any event was removed.
    """
    if not events:
        return events, False
    end_d = statement_end
    if hasattr(end_d, "date") and callable(getattr(end_d, "date", None)):
        end_d = end_d.date()
    if end_d < window_start:
        return events, False
    if _day_has_cash_or_loan_movement(events, end_d):
        return events, False
    out: list[StatementEvent] = []
    removed = False
    for e in events:
        if e.event_type in _FLOW_ACCRUAL_TYPES and e.event_date == end_d:
            removed = True
            continue
        out.append(e)
    return out, removed


def _accrual_inclusion_cap(rows: list[dict[str, Any]], system_business_date: date) -> date:
    cap = system_business_date - timedelta(days=1)
    if _flow_row_has_receipt_or_fee_on_date(rows, system_business_date):
        cap = system_business_date
    return cap


def _sorted_dues_from_disbursement(dues: list[date], disbursement: date) -> list[date]:
    return sorted({d for d in dues if d >= disbursement})


def _accrual_period_index(accrual_day: date, disbursement: date, dues_f: list[date]) -> int:
    """
    [disbursement, due_1), [due_1, due_2), … [due_{n-1}, due_n), [due_n, ∞).
    """
    if not dues_f:
        return 0
    if accrual_day < dues_f[0]:
        return 0
    for i in range(len(dues_f) - 1):
        if dues_f[i] <= accrual_day < dues_f[i + 1]:
            return i + 1
    return len(dues_f)


def _static_flow_row_sort_tier(r: dict[str, Any]) -> int:
    et = str(r.get("_event_type") or "")
    if et == "DISBURSEMENT":
        return 0
    if et.startswith("PAYMENT_"):
        return 1
    if et == "UNAPPLIED_LEDGER":
        return 2
    if et in _ACCRUAL_ROLLUP_TYPES:
        return 3
    return 5


def _rollup_adjacent_accruals_only(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge **adjacent** same-type accrual rows when no schedule context is available."""
    if not rows:
        return []

    out: list[dict[str, Any]] = []
    i = 0
    n = len(rows)
    while i < n:
        r = rows[i]
        et = r.get("_event_type")
        if et in _ACCRUAL_ROLLUP_TYPES:
            total_debit = Decimal("0")
            total_credit = Decimal("0")
            j = i
            d0 = r.get("Due Date")
            d1 = d0
            while j < n and rows[j].get("_event_type") == et:
                total_debit += _flow_row_money_debit(rows[j])
                total_credit += _flow_row_money_credit(rows[j])
                d1 = rows[j].get("Due Date")
                j += 1
            last = rows[j - 1]
            label = _ACCRUAL_ROLLUP_LABELS.get(str(et), str(et))
            narr = f"{label} accrual ({_fmt_roll_day(d0)} to {_fmt_roll_day(d1)}, roll-up)"
            base = _strip_flow_row_for_rollup_display(r)
            base["Due Date"] = d1
            base["Narration"] = narr
            td = as_10dp(total_debit)
            tc = as_10dp(total_credit)
            base["Debits"] = float(td)
            base["Credits"] = float(tc)
            base["_debit_dec"] = td
            base["_credit_dec"] = tc
            for k in ("Balance", "Arrears", "Unapplied funds"):
                if k in last:
                    base[k] = last[k]
            out.append(base)
            i = j
        else:
            out.append(_strip_flow_row_for_rollup_display(r))
            i += 1
    return out


def _rollup_accruals_by_schedule_periods(
    body_rows: list[dict[str, Any]],
    *,
    disbursement: date,
    dues_f: list[date],
    cap: date,
) -> list[dict[str, Any]]:
    """One line per (accrual type, schedule period) for accrual days ``<= cap``."""
    if not body_rows:
        return []

    buckets: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    static: list[tuple[date, int, dict[str, Any]]] = []

    for r in body_rows:
        et = r.get("_event_type")
        d = _parse_flow_statement_row_date(r.get("Due Date"))
        if et in _ACCRUAL_ROLLUP_TYPES and d is not None:
            if d > cap:
                static.append((d, _static_flow_row_sort_tier(r), _strip_flow_row_for_rollup_display(r)))
                continue
            pix = _accrual_period_index(d, disbursement, dues_f)
            buckets[(str(et), pix)].append(r)
            continue
        if d is None:
            static.append((date.min, 99, _strip_flow_row_for_rollup_display(r)))
        else:
            static.append((d, _static_flow_row_sort_tier(r), _strip_flow_row_for_rollup_display(r)))

    def _et_order(et_s: str) -> int:
        if et_s == "REGULAR_INTEREST_ACCRUAL":
            return 0
        if et_s == "PENALTY_INTEREST_ACCRUAL":
            return 1
        if et_s == "DEFAULT_INTEREST_ACCRUAL":
            return 2
        return 9

    rolled_sortable: list[tuple[date, int, str, dict[str, Any]]] = []
    for (et_s, pix), lst in buckets.items():
        if not lst:
            continue
        lst.sort(key=lambda x: _parse_flow_statement_row_date(x.get("Due Date")) or date.min)
        total_debit = sum((_flow_row_money_debit(x) for x in lst), Decimal("0"))
        total_credit = sum((_flow_row_money_credit(x) for x in lst), Decimal("0"))
        last = lst[-1]
        d_first = _parse_flow_statement_row_date(lst[0].get("Due Date"))
        d_last = _parse_flow_statement_row_date(last.get("Due Date"))
        if d_first is None or d_last is None:
            continue
        label = _ACCRUAL_ROLLUP_LABELS.get(et_s, et_s)
        narr = f"{label} accrual ({_fmt_roll_day(d_first)} to {_fmt_roll_day(d_last)}, schedule period)"
        base = _strip_flow_row_for_rollup_display(lst[0])
        base["Due Date"] = d_last
        base["Narration"] = narr
        td = as_10dp(total_debit)
        tc = as_10dp(total_credit)
        base["Debits"] = float(td)
        base["Credits"] = float(tc)
        base["_debit_dec"] = td
        base["_credit_dec"] = tc
        for k in ("Balance", "Arrears", "Unapplied funds"):
            if k in last:
                base[k] = last[k]
        rolled_sortable.append((d_last, 4, et_s, base))

    rolled_sortable.sort(key=lambda x: (x[0], _et_order(x[2]), x[2]))
    rolled_lines = [(t[0], t[1], t[3]) for t in rolled_sortable]

    all_rows = static + rolled_lines
    all_rows.sort(key=lambda x: (x[0], x[1], str(x[2].get("Narration", ""))))
    return [t[2] for t in all_rows]


def rollup_flow_statement_rows_for_display(
    rows: list[dict[str, Any]],
    *,
    loan_id: int | None = None,
    disbursement_date: date | None = None,
    system_business_date: date | None = None,
    schedule_due_dates: list[date] | None = None,
) -> list[dict[str, Any]]:
    """
    Roll up REGULAR / PENALTY / DEFAULT accruals for flow display.

    With ``loan_id`` or explicit ``schedule_due_dates``, groups by contractual schedule period
    (disbursement to first due exclusive, then between dues). Accrual days after
    ``system_business_date - 1`` are excluded from roll-up unless a **receipt or drawdown**
    is dated on the system business date.

    Without schedule context, merges only **adjacent** same-type accruals.
    """
    if not rows:
        return []

    closing = rows[-1] if rows and _is_flow_closing_total_row(rows[-1]) else None
    body = rows[:-1] if closing else rows[:]

    use_period = loan_id is not None or schedule_due_dates is not None
    if not use_period:
        out_body = _rollup_adjacent_accruals_only(body)
        if closing:
            return out_body + [_strip_flow_row_for_rollup_display(closing)]
        return out_body

    sys_d = system_business_date or _rollup_effective_system_date()
    if hasattr(sys_d, "date") and callable(getattr(sys_d, "date", None)):
        sys_d = sys_d.date()
    cap = _accrual_inclusion_cap(body, sys_d)

    disb = disbursement_date
    if disb is None and loan_id is not None:
        from loan_management import get_loan

        loan_row = get_loan(loan_id)
        if loan_row:
            disb = _parse_loan_date(loan_row.get("disbursement_date") or loan_row.get("start_date"))

    dues = schedule_due_dates
    if dues is None and loan_id is not None and disb is not None:
        from loan_management import collect_due_dates_in_range_all_schedule_versions

        max_d = cap
        for r in body:
            dd = _parse_flow_statement_row_date(r.get("Due Date"))
            if dd and dd > max_d:
                max_d = dd
        end_collect = max(max_d, sys_d) + timedelta(days=365 * 15)
        dues = collect_due_dates_in_range_all_schedule_versions(loan_id, disb, end_collect)

    if disb is None or dues is None:
        out_body = _rollup_adjacent_accruals_only(body)
    else:
        dues_f = _sorted_dues_from_disbursement(list(dues), disb)
        out_body = _rollup_accruals_by_schedule_periods(
            body,
            disbursement=disb,
            dues_f=dues_f,
            cap=cap,
        )

    if closing:
        return out_body + [_strip_flow_row_for_rollup_display(closing)]
    return out_body


def apply_dual_running_customer_events(
    events: Sequence[StatementEvent],
    opening_loan: Decimal,
    opening_unapplied: Decimal,
) -> list[tuple[StatementEvent, Decimal, Decimal]]:
    """
    Loan running total from debits/credits; unapplied from ``meta['unapplied_delta']``.
    Unapplied ledger rows may also carry loan ``debit``/``credit`` (e.g. liquidation from
    suspense into buckets) without double-counting excluded synthetic allocation receipts.
    """
    loan_b = _q(opening_loan)
    u_b = _q(opening_unapplied)
    out: list[tuple[StatementEvent, Decimal, Decimal]] = []
    for ev in events:
        ud_meta = ev.meta.get("unapplied_delta")
        if ud_meta is not None:
            u_b = _q(u_b + Decimal(str(ud_meta)))
            loan_b = _q(loan_b + ev.debit - ev.credit)
        else:
            loan_b = _q(loan_b + ev.debit - ev.credit)
        out.append((ev, loan_b, u_b))
    return out


def reconcile_running_to_loan_daily_state(
    computed_closing: Decimal,
    loan_id: int,
    as_of_date: date,
) -> dict[str, Any]:
    """
    Post-hoc check: |computed - total_outstanding(as_of_date)| should be 0 at 10dp
    once *all* event types for the loan-total definition are included. Residual gaps
    (e.g. mid-life fee posts not via receipts) show as non-zero ``diff``.

    For manual checks: compare ``subledger_closing`` (sum of the seven buckets) to the
    flow **Balance** closing, **not** to a single column such as ``principal_not_due`` alone.
    ``total_exposure_saved`` is the persisted EOD total (should match the seven-bucket sum).
    ``regular_interest_in_suspense_balance`` is **not** part of that sum — do not add it
    unless you have a separate reporting definition.

    Common false "off by 1.28" (or liquidation amount): comparing ``loan_daily_state`` for
    **date A** after accrual to **date B** where B includes **extra** accrual on B before
    other events — use the same ``as_of_date`` row and the seven-bucket total.
    """
    from loan_management import get_loan_daily_state_balances

    ds = get_loan_daily_state_balances(loan_id, as_of_date)
    if not ds:
        return {
            "ok": False,
            "full_reconcile_ok": False,
            "reason": "no_daily_state",
            "diff": None,
            "subledger_closing": None,
        }
    sub = total_outstanding_decimal(ds)
    diff = _q(computed_closing - sub)
    tol = Decimal("0.0000000001")
    ok = abs(diff) <= tol
    te_saved = _d(ds.get("total_exposure"))
    diff_vs_saved_te = _q(computed_closing - te_saved)
    per_bucket = {k: float(_d(ds.get(k))) for k in _TOTAL_OUTSTANDING_BUCKET_KEYS}
    reg_susp = float(_d(ds.get("regular_interest_in_suspense_balance")))
    return {
        "ok": ok,
        "full_reconcile_ok": ok,
        "diff": float(diff),
        "subledger_closing": float(sub),
        "computed_closing": float(computed_closing),
        "total_exposure_saved": float(te_saved),
        "diff_vs_total_exposure_saved": float(diff_vs_saved_te),
        "per_bucket_closing": per_bucket,
        "regular_interest_in_suspense_balance": reg_susp,
    }


# --- DB-backed entry (slice 1) ---


def build_eod_flow_events_for_loan(loan_id: int, start_date: date, end_date: date) -> list[StatementEvent]:
    """
    Load ``loan_daily_state`` for [start_date, end_date] and build sorted EOD flow events
    (interest dailies + fee balance deltas). Requires prior-day snapshot for fee delta baseline.
    """
    from loan_management import get_loan_daily_state_balances, get_loan_daily_state_range

    rows = get_loan_daily_state_range(loan_id, start_date, end_date)
    prior = get_loan_daily_state_balances(loan_id, start_date - timedelta(days=1))
    prior_fees = _d(prior.get("fees_charges_balance")) if prior else Decimal("0")

    accr = build_daily_interest_accrual_events(rows)
    fees = build_fee_balance_delta_events(rows, prior_fees_balance=prior_fees)
    return merge_sort_eod_flow_events([*accr, *fees])


def build_complete_loan_flow_events(loan_id: int, start_date: date, end_date: date) -> list[StatementEvent]:
    """
    Drawdown (if in range) + daily accruals + one ``PAYMENT_RECEIPT`` line per repayment
    (net allocation across buckets). Omits fee-balance delta rows to avoid double-counting
    with ``alloc_fees_charges`` on receipts.
    """
    from loan_management import get_loan, get_loan_daily_state_range, get_repayments_with_allocations

    loan = get_loan(loan_id)
    if not loan:
        return []
    disb = build_disbursement_events_for_loan(loan, start_date, end_date)
    rows = get_loan_daily_state_range(loan_id, start_date, end_date)
    accr = build_daily_interest_accrual_events(rows)
    reps = get_repayments_with_allocations(loan_id, start_date, end_date, include_reversed=True)
    pay = build_repayment_allocation_events(reps)
    return merge_sort_statement_events([*disb, *accr, *pay])


def build_flow_preview_for_loan(
    loan_id: int,
    start_date: date,
    end_date: date,
    *,
    eod_only: bool = False,
) -> dict[str, Any]:
    """
    Opening (end of start_date-1) + flow events through end_date, running balance,
    and reconcile to ``loan_daily_state`` total outstanding at end_date.

    ``eod_only=True``: slice-1 style (accruals + fee-balance deltas only).
    Default ``False``: full flow (drawdown + accruals + repayment buckets; no fee deltas).
    """
    from loan_management import get_loan_daily_state_balances

    if eod_only:
        events = build_eod_flow_events_for_loan(loan_id, start_date, end_date)
        note = (
            "eod_only: daily accruals + fee-balance deltas. Do not merge with payment fee "
            "allocations in the same run (double-count risk)."
        )
    else:
        events = build_complete_loan_flow_events(loan_id, start_date, end_date)
        note = (
            "full: drawdown + daily accruals + repayment bucket allocations. Fee-balance deltas "
            "omitted (fees move via alloc_fees_charges). Mid-life fee posts not from receipts "
            "may require separate events for a tight reconcile."
        )

    prior = get_loan_daily_state_balances(loan_id, start_date - timedelta(days=1))
    opening = total_outstanding_decimal(prior) if prior else Decimal("0")
    run = apply_running_loan_outstanding(events, opening)
    computed = run[-1][1] if run else opening
    recon = reconcile_running_to_loan_daily_state(computed, loan_id, end_date)
    return {
        "loan_id": loan_id,
        "start_date": start_date,
        "end_date": end_date,
        "eod_only": eod_only,
        "opening_balance": float(opening),
        "events": [
            {
                "event_date": ev.event_date.isoformat(),
                "event_type": ev.event_type,
                "narration": ev.narration,
                "debit": float(ev.debit),
                "credit": float(ev.credit),
                "balance_after": float(bal),
                "repayment_id": ev.repayment_id,
            }
            for ev, bal in run
        ],
        "computed_closing_from_flows_only": float(computed),
        "reconcile": recon,
        "note": note,
    }

