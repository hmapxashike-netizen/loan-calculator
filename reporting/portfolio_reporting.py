"""
Read-only portfolio analytics: debtor arrears ageing buckets and principal maturity profile;
**creditor arrears ageing** reuses the same schedule + daily-state vintage logic on creditor drawdowns;
**creditor maturity profile** buckets **scheduled** future principal (or P+I) from `creditor_schedule_lines`
only (no daily-state weighting).

Uses latest `loan_schedules` version + `schedule_lines` and latest `loan_daily_state` on/before as-of
(debtor); latest `creditor_loan_schedules` + `creditor_schedule_lines` per drawdown (creditor schedule maturity).
Does not modify allocations, EOD, or engine persistence.

Arrears ageing methodology (credit-style vintage by obligation):
- Only instalments **strictly past due** are used: **due date < as-of** (due on as-of is not in arrears for
  this report; **0 days past due** is not bucketed into 1–30).
- **Principal arrears**: allocated to past-due instalments **newest due date first** (most recent missed
  instalment before as-of), each line taking at most its scheduled **principal**. Surplus after all caps stacks
  on that **newest** past-due line (same index-0 rule as the allocator).
- **Interest arrears**: same order against scheduled **interest**, surplus on **newest** past-due line.
- **Penalty & default interest**: daily ``loan_daily_state`` rows supply ``penalty_interest_balance``,
  ``penalty_interest_daily``, ``default_interest_balance``, and ``default_interest_daily``. **Primary**
  vintage uses **positive daily accrual** on each row’s ``as_of_date``, bucketed by ``max(1, (as_of - d).days)``,
  then **scaled** to the reporting **balance**. If no row has positive daily accrual for that component, **fallback**
  is **positive day-over-day balance deltas** (same scaling). **Fees** use balance deltas only (no ``*_daily`` in LDS).
  If there is no history, that component falls back to **pro-rata** by allocated P+I per past-due line (else newest).
- **DPD buckets** (inclusive): **1–30**, **31–60**, **61–90**, **91–180**, **181+** days past due (from each line’s
  **due date < as-of**). There is **no** arrears “unallocated” bucket: if arrears > 0 but no past-due schedule
  lines are available, the report raises.

Debtor maturity: ``principal_not_due`` from ``loan_daily_state`` spread across **future** instalments by
scheduled **principal** weights; buckets by **days to due**. If there is no future principal (or cash-flow basis
has no principal weights) to spread against, the full amount is reported in the **360+ days** bucket.

Creditor maturity (schedule-only): each **future** line’s scheduled amount goes to the bucket for its due date;
``scheduled_future_total`` equals the sum of bucket columns.

Regulatory maturity profile: same allocation as maturity, re-bucketed into finer bands (0–7, 8–14, 15–30, …, 360+)
via ``bucket_regulatory_maturity_for_loan`` / ``build_regulatory_maturity_summary_table``.

**Gap analysis** (``build_debtor_creditor_maturity_gap_summary``): sums debtor and creditor **standard** maturity
bucket columns at the same as-of and view type, then net and cumulative net by tenor.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from psycopg2.extras import RealDictCursor

from decimal_utils import as_10dp
from loan_management import _connection
from reporting.statements import _parse_schedule_date

# Loan row flags (see loan_management.schema_ddl — cheap OR filters in SQL).
RESTRUCTURE_SCOPE_REMODIFIED = "remodified_in_place"
RESTRUCTURE_SCOPE_SPLIT = "originated_from_split"
RESTRUCTURE_SCOPE_TOPUP = "modification_topup_applied"


def restructure_scope_sql(scope: frozenset[str] | None, *, table_alias: str = "l") -> str:
    """
    When ``scope`` is non-empty, require the loan row to match **at least one** selected tag (OR).
    Empty / None scope → no extra predicate.
    """
    if not scope:
        return ""
    ta = table_alias
    parts: list[str] = []
    if RESTRUCTURE_SCOPE_REMODIFIED in scope:
        parts.append(f"COALESCE({ta}.remodified_in_place, FALSE) = TRUE")
    if RESTRUCTURE_SCOPE_SPLIT in scope:
        parts.append(f"COALESCE({ta}.originated_from_split, FALSE) = TRUE")
    if RESTRUCTURE_SCOPE_TOPUP in scope:
        parts.append(f"COALESCE({ta}.modification_topup_applied, FALSE) = TRUE")
    if not parts:
        return ""
    return " AND (" + " OR ".join(parts) + ")"


def _parse_schedule_line_date(raw: Any) -> date | None:
    """Parse schedule line Date; same as statements, plus common ISO / slash formats."""
    d = _parse_schedule_date(raw)
    if d is not None:
        return d
    if isinstance(raw, str):
        s = raw.strip()[:32]
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


def _row_schedule_date(row: dict[str, Any]) -> date | None:
    """Resolve Date column regardless of PG/psycopg2 key casing."""
    return _parse_schedule_line_date(
        row.get("Date") or row.get("date") or row.get("DATE")
    )


def _row_period(row: dict[str, Any]) -> int:
    try:
        return int(row.get("Period") or row.get("period") or 0)
    except (TypeError, ValueError):
        return 0

# Arrears: dpd = (as_of - due_date).days for lines with due < as-of; only buckets 1–30 … 181+.
ARREARS_BUCKET_KEYS = ("bkt_1_30", "bkt_31_60", "bkt_61_90", "bkt_91_180", "bkt_181p")
ARREARS_BUCKET_LABELS = ("1–30 dpd", "31–60 dpd", "61–90 dpd", "91–180 dpd", "181+ dpd")

# Maturity: days to due = (due_date - as_of).days
MATURITY_BUCKET_KEYS = ("bkt_1_7", "bkt_8_30", "bkt_31_60", "bkt_61_90", "bkt_91_360", "bkt_360p")
MATURITY_BUCKET_LABELS = ("0–7 days", "8–30 days", "31–60 days", "61–90 days", "91–360 days", "360+ days")

# Regulatory / AL-style maturity (finer buckets than MATURITY_BUCKET_KEYS). Same cash allocation logic as
# bucket_maturity_for_loan; creditor (outflow) side is aggregated separately elsewhere (not in this engine).
REGULATORY_MATURITY_BUCKET_KEYS = (
    "rb_0_7",
    "rb_8_14",
    "rb_15_30",
    "rb_31_60",
    "rb_61_90",
    "rb_91_120",
    "rb_121_180",
    "rb_181_360",
    "rb_360p",
)
REGULATORY_MATURITY_BUCKET_LABELS = (
    "i) 0-7 days",
    "ii) 8-14 days",
    "iii) 15-30 days",
    "iv) 31-60 days",
    "v) 61-90 days",
    "vi) 91-120 days",
    "vii) 121-180 days",
    "viii) 181-360 days",
    "ix) 360+ days",
)


def _zero_arrears_buckets() -> dict[str, Decimal]:
    return {k: Decimal("0") for k in ARREARS_BUCKET_KEYS}


def _zero_maturity_buckets() -> dict[str, Decimal]:
    return {k: Decimal("0") for k in MATURITY_BUCKET_KEYS}


def _zero_regulatory_maturity_buckets() -> dict[str, Decimal]:
    return {k: Decimal("0") for k in REGULATORY_MATURITY_BUCKET_KEYS}


def _arrears_bucket_index(days_past_due: int) -> int:
    """Map integer DPD to bucket index (0..4). Requires DPD >= 1 (due date strictly before as-of)."""
    if days_past_due < 1:
        raise ValueError(
            f"Arrears ageing requires days past due >= 1 for schedule-based lines; got {days_past_due}"
        )
    if days_past_due <= 30:
        return 0
    if days_past_due <= 60:
        return 1
    if days_past_due <= 90:
        return 2
    if days_past_due <= 180:
        return 3
    return 4


def _scale_arrears_buckets_to_closing(
    raw: dict[str, Decimal], closing_balance: Decimal
) -> dict[str, Decimal]:
    """Scale non-negative bucket weights so they sum to `closing_balance` (10 dp); residue on 181+ bucket."""
    closing = as_10dp(closing_balance)
    total_raw = as_10dp(sum(raw.values(), Decimal("0")))
    if total_raw <= 0 or closing <= 0:
        return _zero_arrears_buckets()
    scale = as_10dp(closing / total_raw)
    scaled = {k: as_10dp(raw[k] * scale) for k in ARREARS_BUCKET_KEYS}
    ssum = as_10dp(sum(scaled.values(), Decimal("0")))
    diff = as_10dp(closing - ssum)
    if diff != 0:
        k_last = ARREARS_BUCKET_KEYS[-1]
        scaled[k_last] = as_10dp(scaled[k_last] + diff)
    return scaled


def buckets_from_daily_balance_series(
    as_of: date,
    dated_balances: list[tuple[date, Decimal]],
    closing_balance: Decimal,
) -> dict[str, Decimal]:
    """
    Map a component balance into arrears DPD buckets using **positive day-over-day balance increases** only.
    Assumes `dated_balances` is sorted ascending by date. Scaled to `closing_balance`.
    """
    closing = as_10dp(closing_balance)
    raw = _zero_arrears_buckets()
    if closing <= 0:
        return raw
    if not dated_balances:
        return raw

    prev = Decimal("0")
    for d, bal in dated_balances:
        b = as_10dp(bal)
        chg = as_10dp(b - prev)
        prev = b
        if chg > 0:
            age_days = max(1, (as_of - d).days)
            idx = _arrears_bucket_index(age_days)
            k = ARREARS_BUCKET_KEYS[idx]
            raw[k] = as_10dp(raw[k] + chg)

    total_raw = as_10dp(sum(raw.values(), Decimal("0")))
    if total_raw <= 0:
        d_last = dated_balances[-1][0]
        age_days = max(1, (as_of - d_last).days)
        idx = _arrears_bucket_index(age_days)
        out = _zero_arrears_buckets()
        out[ARREARS_BUCKET_KEYS[idx]] = closing
        return out

    return _scale_arrears_buckets_to_closing(raw, closing)


def buckets_from_daily_flow_or_balance(
    as_of: date,
    dated_rows: list[tuple[date, Decimal, Decimal]],
    closing_balance: Decimal,
) -> dict[str, Decimal]:
    """
    Each row is ``(as_of_date, end_of_day_balance, daily_accrual)``. If any row has ``daily_accrual > 0`` in the
    series, **primary** attribution uses those positive flows (by row date). Otherwise uses **balance deltas**
    between consecutive balances (see ``buckets_from_daily_balance_series``). Result scaled to ``closing_balance``.
    """
    closing = as_10dp(closing_balance)
    if closing <= 0 or not dated_rows:
        return _zero_arrears_buckets()

    raw_flow = _zero_arrears_buckets()
    total_flow = Decimal("0")
    for d, _bal, daily in dated_rows:
        fl = as_10dp(max(Decimal("0"), daily))
        if fl > 0:
            age_days = max(1, (as_of - d).days)
            k = ARREARS_BUCKET_KEYS[_arrears_bucket_index(age_days)]
            raw_flow[k] = as_10dp(raw_flow[k] + fl)
            total_flow = as_10dp(total_flow + fl)

    if total_flow > 0:
        return _scale_arrears_buckets_to_closing(raw_flow, closing)

    return buckets_from_daily_balance_series(
        as_of, [(d, b) for d, b, _ in dated_rows], closing
    )


def _normalize_ancillary_daily_rows(
    series: list[tuple[date, Decimal] | tuple[date, Decimal, Decimal]],
) -> list[tuple[date, Decimal, Decimal]]:
    """Allow legacy ``(date, balance)`` rows; third element defaults to zero daily flow."""
    out: list[tuple[date, Decimal, Decimal]] = []
    for row in series:
        if len(row) == 3:
            d, bal, daily = row
            out.append((d, as_10dp(bal), as_10dp(daily)))
        else:
            d, bal = row
            out.append((d, as_10dp(bal), Decimal("0")))
    return out


def _ancillary_to_buckets_by_pi_weights(
    as_of: date,
    anc: Decimal,
    entries: list[tuple[date, int, Decimal, Decimal]],
    alloc_p: list[Decimal],
    alloc_i: list[Decimal],
) -> dict[str, Decimal]:
    """Spread one ancillary total across past-due lines by P+I weights, then bucket by line due date."""
    buckets = _zero_arrears_buckets()
    anc = as_10dp(anc)
    if anc <= 0 or not entries:
        return buckets
    weights = [as_10dp(alloc_p[j] + alloc_i[j]) for j in range(len(entries))]
    if as_10dp(sum(weights)) <= 0:
        due0, _per0, _, _ = entries[0]
        bk = ARREARS_BUCKET_KEYS[_arrears_bucket_index(max(1, (as_of - due0).days))]
        buckets[bk] = anc
        return buckets
    parts = _allocate_proportional(anc, weights)
    for j, (due, _per, _, _) in enumerate(entries):
        part = as_10dp(parts[j])
        if part <= 0:
            continue
        bk = ARREARS_BUCKET_KEYS[_arrears_bucket_index(max(1, (as_of - due).days))]
        buckets[bk] = as_10dp(buckets[bk] + part)
    return buckets


def _maturity_bucket_index(days_to_due: int) -> int:
    if days_to_due < 0:
        return 5
    if days_to_due <= 7:
        return 0
    if days_to_due <= 30:
        return 1
    if days_to_due <= 60:
        return 2
    if days_to_due <= 90:
        return 3
    if days_to_due <= 360:
        return 4
    return 5


def _regulatory_maturity_bucket_index(days_to_due: int) -> int:
    """Map days from as-of to instalment due date into regulatory maturity bucket index (0..8)."""
    last = len(REGULATORY_MATURITY_BUCKET_KEYS) - 1
    if days_to_due < 0:
        return last
    if days_to_due <= 7:
        return 0
    if days_to_due <= 14:
        return 1
    if days_to_due <= 30:
        return 2
    if days_to_due <= 60:
        return 3
    if days_to_due <= 90:
        return 4
    if days_to_due <= 120:
        return 5
    if days_to_due <= 180:
        return 6
    if days_to_due <= 360:
        return 7
    return last


def _line_principal_interest(row: dict[str, Any]) -> tuple[Decimal, Decimal]:
    p = as_10dp(row.get("principal") or row.get("Principal") or 0)
    i = as_10dp(row.get("interest") or row.get("Interest") or 0)
    return p, i


def _allocate_proportional(total: Decimal, weights: list[Decimal]) -> list[Decimal]:
    """Split total across weights at 10dp; last line absorbs rounding residual."""
    total = as_10dp(total)
    if total <= 0:
        return [Decimal("0")] * len(weights)
    s = as_10dp(sum(weights))
    if s <= 0 or not weights:
        return [Decimal("0")] * len(weights)
    out: list[Decimal] = []
    acc = Decimal("0")
    for k, w in enumerate(weights):
        if k == len(weights) - 1:
            out.append(as_10dp(total - acc))
        else:
            part = as_10dp(total * as_10dp(w) / s)
            out.append(part)
            acc = as_10dp(acc + part)
    return out


def _fifo_allocate_to_lines(total: Decimal, caps: list[Decimal]) -> list[Decimal]:
    """
    Allocate `total` across lines in list order, each line taking at most its cap.
    Surplus after all caps is added to the first line (for arrears: newest past-due instalment).
    """
    total = as_10dp(total)
    n = len(caps)
    if n == 0:
        return []
    out = [Decimal("0")] * n
    rem = total
    for idx in range(n):
        if rem <= 0:
            break
        cap = as_10dp(max(Decimal("0"), caps[idx]))
        take = as_10dp(min(rem, cap))
        out[idx] = take
        rem = as_10dp(rem - take)
    if rem > 0:
        out[0] = as_10dp(out[0] + rem)
    return out


def bucket_arrears_for_loan(
    as_of: date,
    *,
    principal_arrears: Decimal,
    interest_arrears: Decimal,
    fees_charges: Decimal,
    penalty: Decimal,
    default_int: Decimal,
    schedule_lines: list[dict[str, Any]],
    daily_series: dict[str, list[tuple[date, Decimal] | tuple[date, Decimal, Decimal]]] | None = None,
) -> dict[str, Decimal]:
    """Return bucket key -> amount (10dp): P/I newest past-due first; penalty/default/fees from daily LDS when given."""
    buckets = _zero_arrears_buckets()

    entries: list[tuple[date, int, Decimal, Decimal]] = []
    for row in sorted(schedule_lines, key=_row_period):
        due = _row_schedule_date(row)
        if due is None or due >= as_of:
            continue
        p, i = _line_principal_interest(row)
        entries.append((due, _row_period(row), p, i))

    # Newest due date first (closest to as-of), then higher period — matches “most recent past-due instalment” first.
    entries.sort(key=lambda x: (x[0], x[1]), reverse=True)

    total_arrears = as_10dp(
        principal_arrears + interest_arrears + fees_charges + penalty + default_int
    )
    if not entries:
        if total_arrears > 0:
            raise ValueError(
                "No past-due schedule instalments (due date < as-of with parseable dates); "
                "cannot bucket arrears without at least one such line."
            )
        return buckets

    caps_p = [as_10dp(e[2]) for e in entries]
    caps_i = [as_10dp(e[3]) for e in entries]
    alloc_p = _fifo_allocate_to_lines(principal_arrears, caps_p)
    alloc_i = _fifo_allocate_to_lines(interest_arrears, caps_i)

    for j, (due, _per, _, _) in enumerate(entries):
        part = as_10dp(alloc_p[j] + alloc_i[j])
        if part <= 0:
            continue
        bk = ARREARS_BUCKET_KEYS[_arrears_bucket_index((as_of - due).days)]
        buckets[bk] = as_10dp(buckets[bk] + part)

    ds = daily_series or {}

    def _component_buckets(key: str, balance: Decimal) -> dict[str, Decimal]:
        bal = as_10dp(balance)
        if bal <= 0:
            return _zero_arrears_buckets()
        series = ds.get(key) or []
        if series:
            rows = _normalize_ancillary_daily_rows(series)
            return buckets_from_daily_flow_or_balance(as_of, rows, bal)
        return _ancillary_to_buckets_by_pi_weights(as_of, bal, entries, alloc_p, alloc_i)

    for k, part in _component_buckets("penalty", penalty).items():
        buckets[k] = as_10dp(buckets[k] + part)
    for k, part in _component_buckets("default", default_int).items():
        buckets[k] = as_10dp(buckets[k] + part)
    for k, part in _component_buckets("fees", fees_charges).items():
        buckets[k] = as_10dp(buckets[k] + part)

    return buckets


def bucket_maturity_for_loan(
    as_of: date,
    *,
    principal_not_due: Decimal,
    schedule_lines: list[dict[str, Any]],
    view_type: str = "principal",
) -> dict[str, Decimal]:
    buckets = _zero_maturity_buckets()
    pnd = as_10dp(principal_not_due)
    if pnd <= 0 and view_type == "principal":
        return buckets

    sorted_lines = sorted(schedule_lines, key=_row_period)
    future: list[tuple[date, Decimal, Decimal]] = []
    for row in sorted_lines:
        due = _row_schedule_date(row)
        if due is None:
            continue
        if due > as_of:
            pr, int_amt = _line_principal_interest(row)
            if pr > 0 or int_amt > 0:
                future.append((due, pr, int_amt))

    if view_type == "principal":
        wf = as_10dp(sum(x[1] for x in future))
        if wf <= 0:
            buckets["bkt_360p"] = pnd
            return buckets

        weights = [x[1] for x in future]
        parts = _allocate_proportional(pnd, weights)
        for (due, _, _), part in zip(future, parts, strict=True):
            days_to = (due - as_of).days
            idx = _maturity_bucket_index(days_to)
            key = MATURITY_BUCKET_KEYS[idx]
            buckets[key] = as_10dp(buckets[key] + part)
    else:
        wf = as_10dp(sum(x[1] for x in future))
        weights = [x[1] for x in future]
        parts_pr = _allocate_proportional(pnd, weights) if wf > 0 else [Decimal("0")] * len(future)
        if wf <= 0 and pnd > 0:
            buckets["bkt_360p"] = as_10dp(buckets["bkt_360p"] + pnd)

        for (due, _, int_amt), part_pr in zip(future, parts_pr, strict=True):
            days_to = (due - as_of).days
            idx = _maturity_bucket_index(days_to)
            key = MATURITY_BUCKET_KEYS[idx]
            buckets[key] = as_10dp(buckets[key] + part_pr + int_amt)

    return buckets


def bucket_maturity_from_future_scheduled_cashflows(
    as_of: date,
    *,
    schedule_lines: list[dict[str, Any]],
    view_type: str = "principal",
) -> dict[str, Decimal]:
    """
    Place **scheduled** future instalments into maturity tenor buckets by days from ``as_of`` to line due date.

    **Principal** view: each future line contributes its scheduled **principal** to the bucket for that due date.
    **Cash flow** view: each line contributes **principal + interest**. No weighting to ``principal_not_due`` or
    other balances — pure schedule timing (creditor maturity profile).
    """
    buckets = _zero_maturity_buckets()
    for row in sorted(schedule_lines, key=_row_period):
        due = _row_schedule_date(row)
        if due is None or due <= as_of:
            continue
        pr, int_amt = _line_principal_interest(row)
        if view_type == "principal":
            amt = as_10dp(pr)
        else:
            amt = as_10dp(pr + int_amt)
        if amt <= 0:
            continue
        days_to = (due - as_of).days
        idx = _maturity_bucket_index(days_to)
        key = MATURITY_BUCKET_KEYS[idx]
        buckets[key] = as_10dp(buckets[key] + amt)
    return buckets


def bucket_regulatory_maturity_for_loan(
    as_of: date,
    *,
    principal_not_due: Decimal,
    schedule_lines: list[dict[str, Any]],
    view_type: str = "principal",
) -> dict[str, Decimal]:
    """
    Same allocation methodology as ``bucket_maturity_for_loan`` (principal-not-due weighted by future
    scheduled principal; cash-flow view adds interest to each line's bucket), but using
    ``REGULATORY_MATURITY_BUCKET_KEYS`` band boundaries.
    """
    buckets = _zero_regulatory_maturity_buckets()
    pnd = as_10dp(principal_not_due)
    if pnd <= 0 and view_type == "principal":
        return buckets

    sorted_lines = sorted(schedule_lines, key=_row_period)
    future: list[tuple[date, Decimal, Decimal]] = []
    for row in sorted_lines:
        due = _row_schedule_date(row)
        if due is None:
            continue
        if due > as_of:
            pr, int_amt = _line_principal_interest(row)
            if pr > 0 or int_amt > 0:
                future.append((due, pr, int_amt))

    if view_type == "principal":
        wf = as_10dp(sum(x[1] for x in future))
        if wf <= 0:
            buckets["rb_360p"] = pnd
            return buckets

        weights = [x[1] for x in future]
        parts = _allocate_proportional(pnd, weights)
        for (due, _, _), part in zip(future, parts, strict=True):
            days_to = (due - as_of).days
            idx = _regulatory_maturity_bucket_index(days_to)
            key = REGULATORY_MATURITY_BUCKET_KEYS[idx]
            buckets[key] = as_10dp(buckets[key] + part)
    else:
        wf = as_10dp(sum(x[1] for x in future))
        weights = [x[1] for x in future]
        parts_pr = _allocate_proportional(pnd, weights) if wf > 0 else [Decimal("0")] * len(future)
        if wf <= 0 and pnd > 0:
            buckets["rb_360p"] = as_10dp(buckets["rb_360p"] + pnd)

        for (due, _, int_amt), part_pr in zip(future, parts_pr, strict=True):
            days_to = (due - as_of).days
            idx = _regulatory_maturity_bucket_index(days_to)
            key = REGULATORY_MATURITY_BUCKET_KEYS[idx]
            buckets[key] = as_10dp(buckets[key] + part_pr + int_amt)

    return buckets


def fetch_latest_schedule_lines_batch(loan_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """Latest schedule version per loan; lines ordered by Period."""
    out: dict[int, list[dict[str, Any]]] = {lid: [] for lid in loan_ids}
    if not loan_ids:
        return out
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (loan_id)
                        id AS schedule_id,
                        loan_id
                    FROM loan_schedules
                    WHERE loan_id = ANY(%s)
                    ORDER BY loan_id, version DESC
                )
                SELECT l.loan_id, sl.*
                FROM latest l
                JOIN schedule_lines sl ON sl.loan_schedule_id = l.schedule_id
                ORDER BY l.loan_id, sl."Period"
                """,
                (loan_ids,),
            )
            for row in cur.fetchall() or []:
                lid = int(row["loan_id"])
                if lid in out:
                    line = dict(row)
                    line.pop("loan_id", None)
                    out[lid].append(line)
    return out


def fetch_latest_creditor_schedule_lines_batch(
    creditor_drawdown_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    """Latest schedule version per creditor drawdown; lines ordered by Period (same shape as debtor lines)."""
    out: dict[int, list[dict[str, Any]]] = {did: [] for did in creditor_drawdown_ids}
    if not creditor_drawdown_ids:
        return out
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (creditor_drawdown_id)
                        id AS schedule_id,
                        creditor_drawdown_id
                    FROM creditor_loan_schedules
                    WHERE creditor_drawdown_id = ANY(%s)
                    ORDER BY creditor_drawdown_id, version DESC
                )
                SELECT l.creditor_drawdown_id, csl.*
                FROM latest l
                JOIN creditor_schedule_lines csl ON csl.creditor_loan_schedule_id = l.schedule_id
                ORDER BY l.creditor_drawdown_id, csl."Period"
                """,
                (creditor_drawdown_ids,),
            )
            for row in cur.fetchall() or []:
                did = int(row["creditor_drawdown_id"])
                if did not in out:
                    continue
                line = dict(row)
                line.pop("creditor_drawdown_id", None)
                out[did].append(line)
    return out


def fetch_creditor_schedule_maturity_drawdown_rows(
    *,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    """
    Creditor drawdowns in scope for **schedule-based** maturity (no daily-state balance filter).
    """
    status_clause = "AND cl.status = 'active'" if active_only else ""
    sql = f"""
        SELECT
            cl.id AS creditor_drawdown_id,
            cp.name AS lender_name,
            cl.creditor_facility_id,
            COALESCE(cl.creditor_loan_type_code, '') AS loan_type
        FROM creditor_drawdowns cl
        JOIN creditor_facilities cf ON cf.id = cl.creditor_facility_id
        JOIN creditor_counterparties cp ON cp.id = cf.creditor_counterparty_id
        WHERE 1=1 {status_clause}
        ORDER BY cl.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall() or []]


def explain_creditor_maturity_profile_empty(as_of: date, *, active_only: bool) -> str:
    """
    Human-readable reason why :func:`build_creditor_maturity_profile_report` returned no rows
    (read-only counts; schedule-based maturity).
    """
    status_clause = "AND cl.status = 'active'" if active_only else ""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*)::int FROM creditor_drawdowns cl WHERE 1=1 {status_clause}",
            )
            n_dd = int(cur.fetchone()[0] or 0)
            if n_dd == 0:
                return (
                    "No creditor drawdowns match the current scope. If you expected rows here, try turning off "
                    "**Active loans only**, or add facilities and drawdowns under **Creditor loans** first."
                )
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT cl.id)::int
                FROM creditor_drawdowns cl
                WHERE EXISTS (
                    SELECT 1 FROM creditor_loan_schedules s
                    WHERE s.creditor_drawdown_id = cl.id
                )
                {status_clause}
                """,
            )
            n_sch = int(cur.fetchone()[0] or 0)
            if n_sch == 0:
                return (
                    f"There are **{n_dd}** drawdown(s) in scope, but none have a **creditor loan schedule** stored. "
                    "Capture or rebuild a schedule under **Creditor loans** (Capture drawdown)."
                )
            return (
                f"There are **{n_dd}** drawdown(s) with schedules, but no **future** instalments after **{as_of}** "
                "have positive scheduled **principal** (principal-only view) or **principal + interest** "
                "(cash-flow view). Try an earlier as-of or check schedule dates and amounts."
            )


def fetch_loan_daily_ancillary_series_batch(
    loan_ids: list[int], as_of: date
) -> dict[int, dict[str, list[tuple[date, Decimal, Decimal]]]]:
    """
    Daily `loan_daily_state` for penalty, default, and fees (on or before `as_of`), ascending by date.
    Each list entry is ``(as_of_date, balance, daily_accrual)``; fees use ``daily_accrual = 0`` (no LDS daily column).
    Penalty/default use ``buckets_from_daily_flow_or_balance`` (flow first, else balance deltas).
    """
    if not loan_ids:
        return {}
    out: dict[int, dict[str, list[tuple[date, Decimal, Decimal]]]] = {
        lid: {"penalty": [], "default": [], "fees": []} for lid in loan_ids
    }
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT loan_id, as_of_date,
                       penalty_interest_balance,
                       COALESCE(penalty_interest_daily, 0) AS penalty_interest_daily,
                       default_interest_balance,
                       COALESCE(default_interest_daily, 0) AS default_interest_daily,
                       fees_charges_balance
                FROM loan_daily_state
                WHERE loan_id = ANY(%s) AND as_of_date <= %s
                ORDER BY loan_id, as_of_date
                """,
                (loan_ids, as_of),
            )
            for row in cur.fetchall() or []:
                lid = int(row["loan_id"])
                if lid not in out:
                    continue
                d = row["as_of_date"]
                if isinstance(d, datetime):
                    d = d.date()
                p = as_10dp(row.get("penalty_interest_balance") or 0)
                p_d = as_10dp(row.get("penalty_interest_daily") or 0)
                di = as_10dp(row.get("default_interest_balance") or 0)
                di_d = as_10dp(row.get("default_interest_daily") or 0)
                f = as_10dp(row.get("fees_charges_balance") or 0)
                out[lid]["penalty"].append((d, p, p_d))
                out[lid]["default"].append((d, di, di_d))
                out[lid]["fees"].append((d, f, Decimal("0")))
    return out


def fetch_creditor_daily_ancillary_series_batch(
    creditor_drawdown_ids: list[int], as_of: date
) -> dict[int, dict[str, list[tuple[date, Decimal, Decimal]]]]:
    """
    Daily ``creditor_loan_daily_state`` for penalty, default, and fees (on or before ``as_of``),
    ascending by date — same shape as :func:`fetch_loan_daily_ancillary_series_batch`.
    """
    if not creditor_drawdown_ids:
        return {}
    out: dict[int, dict[str, list[tuple[date, Decimal, Decimal]]]] = {
        did: {"penalty": [], "default": [], "fees": []} for did in creditor_drawdown_ids
    }
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT creditor_drawdown_id, as_of_date,
                       penalty_interest_balance,
                       COALESCE(penalty_interest_daily, 0) AS penalty_interest_daily,
                       default_interest_balance,
                       COALESCE(default_interest_daily, 0) AS default_interest_daily,
                       fees_charges_balance
                FROM creditor_loan_daily_state
                WHERE creditor_drawdown_id = ANY(%s) AND as_of_date <= %s
                ORDER BY creditor_drawdown_id, as_of_date
                """,
                (creditor_drawdown_ids, as_of),
            )
            for row in cur.fetchall() or []:
                did = int(row["creditor_drawdown_id"])
                if did not in out:
                    continue
                d = row["as_of_date"]
                if isinstance(d, datetime):
                    d = d.date()
                p = as_10dp(row.get("penalty_interest_balance") or 0)
                p_d = as_10dp(row.get("penalty_interest_daily") or 0)
                di = as_10dp(row.get("default_interest_balance") or 0)
                di_d = as_10dp(row.get("default_interest_daily") or 0)
                f = as_10dp(row.get("fees_charges_balance") or 0)
                out[did]["penalty"].append((d, p, p_d))
                out[did]["default"].append((d, di, di_d))
                out[did]["fees"].append((d, f, Decimal("0")))
    return out


def fetch_loans_arrears_base_rows(
    as_of: date,
    *,
    active_only: bool,
    restructure_scope: frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    status_clause = "AND l.status = 'active'" if active_only else ""
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        SELECT
            l.id AS loan_id,
            l.product_code,
            l.scheme,
            COALESCE(ind.name, corp.trading_name, corp.legal_name, '') AS customer_name,
            lds.as_of_date AS state_as_of,
            lds.days_overdue,
            lds.principal_arrears,
            lds.interest_arrears_balance,
            lds.fees_charges_balance,
            lds.penalty_interest_balance,
            lds.default_interest_balance,
            COALESCE(lds.total_delinquency_arrears,
                lds.principal_arrears + lds.interest_arrears_balance
                + lds.default_interest_balance + lds.penalty_interest_balance + lds.fees_charges_balance
            ) AS total_delinquency_arrears,
            COALESCE(lds.total_exposure, 0) AS total_exposure
        FROM loans l
        LEFT JOIN customers c ON c.id = l.customer_id
        LEFT JOIN individuals ind ON ind.customer_id = c.id
        LEFT JOIN corporates corp ON corp.customer_id = c.id
        INNER JOIN LATERAL (
            SELECT *
            FROM loan_daily_state x
            WHERE x.loan_id = l.id AND x.as_of_date <= %s
            ORDER BY x.as_of_date DESC
            LIMIT 1
        ) lds ON TRUE
        WHERE lds.days_overdue > 0
        {status_clause}
        {rs_clause}
        ORDER BY l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (as_of,))
            return [dict(r) for r in cur.fetchall() or []]


def fetch_loans_maturity_base_rows(
    as_of: date,
    *,
    active_only: bool,
    restructure_scope: frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    status_clause = "AND l.status = 'active'" if active_only else ""
    rs_clause = restructure_scope_sql(restructure_scope)
    sql = f"""
        SELECT
            l.id AS loan_id,
            l.product_code,
            l.loan_type,
            l.scheme,
            COALESCE(ind.name, corp.trading_name, corp.legal_name, '') AS customer_name,
            lds.as_of_date AS state_as_of,
            lds.principal_not_due
        FROM loans l
        LEFT JOIN customers c ON c.id = l.customer_id
        LEFT JOIN individuals ind ON ind.customer_id = c.id
        LEFT JOIN corporates corp ON corp.customer_id = c.id
        INNER JOIN LATERAL (
            SELECT principal_not_due, as_of_date
            FROM loan_daily_state x
            WHERE x.loan_id = l.id AND x.as_of_date <= %s
            ORDER BY x.as_of_date DESC
            LIMIT 1
        ) lds ON TRUE
        WHERE lds.principal_not_due > 0
        {status_clause}
        {rs_clause}
        ORDER BY l.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (as_of,))
            return [dict(r) for r in cur.fetchall() or []]


def build_arrears_aging_report(
    as_of: date,
    *,
    active_only: bool,
    restructure_scope: frozenset[str] | None = None,
) -> pd.DataFrame:
    base = fetch_loans_arrears_base_rows(
        as_of, active_only=active_only, restructure_scope=restructure_scope
    )
    if not base:
        return pd.DataFrame()
    lids = [int(r["loan_id"]) for r in base]
    sched_map = fetch_latest_schedule_lines_batch(lids)
    daily_map = fetch_loan_daily_ancillary_series_batch(lids, as_of)

    rows_out: list[dict[str, Any]] = []
    for r in base:
        lid = int(r["loan_id"])
        lines = sched_map.get(lid) or []
        try:
            buckets = bucket_arrears_for_loan(
                as_of,
                principal_arrears=as_10dp(r.get("principal_arrears") or 0),
                interest_arrears=as_10dp(r.get("interest_arrears_balance") or 0),
                fees_charges=as_10dp(r.get("fees_charges_balance") or 0),
                penalty=as_10dp(r.get("penalty_interest_balance") or 0),
                default_int=as_10dp(r.get("default_interest_balance") or 0),
                schedule_lines=lines,
                daily_series=daily_map.get(lid),
            )
        except ValueError as ex:
            raise ValueError(f"Arrears ageing failed for loan_id={lid}: {ex}") from ex
        row = {
            "loan_id": lid,
            "customer_name": r.get("customer_name"),
            "product_code": r.get("product_code"),
            "scheme": r.get("scheme"),
            "state_as_of": r.get("state_as_of"),
            "days_overdue": r.get("days_overdue"),
            "total_outstanding_balance": float(as_10dp(r.get("total_exposure") or 0)),
            "total_delinquency_arrears": float(as_10dp(r.get("total_delinquency_arrears") or 0)),
        }
        for k in ARREARS_BUCKET_KEYS:
            row[k] = float(buckets[k])
        rows_out.append(row)

    return pd.DataFrame(rows_out)


def fetch_creditor_arrears_base_rows(
    as_of: date,
    *,
    active_only: bool,
) -> list[dict[str, Any]]:
    """
    Creditor drawdowns with **days_overdue > 0** from latest ``creditor_loan_daily_state`` on/before ``as_of``
    (same population idea as debtor arrears ageing).
    """
    status_clause = "AND cl.status = 'active'" if active_only else ""
    sql = f"""
        SELECT
            cl.id AS creditor_drawdown_id,
            cp.name AS lender_name,
            cl.creditor_facility_id,
            COALESCE(cl.creditor_loan_type_code, '') AS loan_type,
            lds.as_of_date AS state_as_of,
            lds.days_overdue,
            lds.principal_arrears,
            lds.interest_arrears_balance,
            lds.fees_charges_balance,
            lds.penalty_interest_balance,
            lds.default_interest_balance,
            COALESCE(lds.total_delinquency_arrears,
                lds.principal_arrears + lds.interest_arrears_balance
                + lds.default_interest_balance + lds.penalty_interest_balance + lds.fees_charges_balance
            ) AS total_delinquency_arrears,
            COALESCE(lds.total_exposure, 0) AS total_exposure
        FROM creditor_drawdowns cl
        JOIN creditor_facilities cf ON cf.id = cl.creditor_facility_id
        JOIN creditor_counterparties cp ON cp.id = cf.creditor_counterparty_id
        INNER JOIN LATERAL (
            SELECT *
            FROM creditor_loan_daily_state x
            WHERE x.creditor_drawdown_id = cl.id AND x.as_of_date <= %s
            ORDER BY x.as_of_date DESC
            LIMIT 1
        ) lds ON TRUE
        WHERE lds.days_overdue > 0
        {status_clause}
        ORDER BY cl.id
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (as_of,))
            return [dict(r) for r in cur.fetchall() or []]


def build_creditor_arrears_aging_report(
    as_of: date,
    *,
    active_only: bool,
) -> pd.DataFrame:
    """
    Creditor borrowings in arrears: same DPD bucket methodology as :func:`build_arrears_aging_report`,
    using ``creditor_schedule_lines`` and ``creditor_loan_daily_state``.
    """
    base = fetch_creditor_arrears_base_rows(as_of, active_only=active_only)
    if not base:
        return pd.DataFrame()
    dids = [int(r["creditor_drawdown_id"]) for r in base]
    sched_map = fetch_latest_creditor_schedule_lines_batch(dids)
    daily_map = fetch_creditor_daily_ancillary_series_batch(dids, as_of)

    rows_out: list[dict[str, Any]] = []
    for r in base:
        did = int(r["creditor_drawdown_id"])
        lines = sched_map.get(did) or []
        try:
            buckets = bucket_arrears_for_loan(
                as_of,
                principal_arrears=as_10dp(r.get("principal_arrears") or 0),
                interest_arrears=as_10dp(r.get("interest_arrears_balance") or 0),
                fees_charges=as_10dp(r.get("fees_charges_balance") or 0),
                penalty=as_10dp(r.get("penalty_interest_balance") or 0),
                default_int=as_10dp(r.get("default_interest_balance") or 0),
                schedule_lines=lines,
                daily_series=daily_map.get(did),
            )
        except ValueError as ex:
            raise ValueError(f"Arrears ageing failed for creditor_drawdown_id={did}: {ex}") from ex
        row = {
            "creditor_drawdown_id": did,
            "lender_name": r.get("lender_name"),
            "creditor_facility_id": r.get("creditor_facility_id"),
            "loan_type": r.get("loan_type"),
            "state_as_of": r.get("state_as_of"),
            "days_overdue": r.get("days_overdue"),
            "total_outstanding_balance": float(as_10dp(r.get("total_exposure") or 0)),
            "total_delinquency_arrears": float(as_10dp(r.get("total_delinquency_arrears") or 0)),
        }
        for k in ARREARS_BUCKET_KEYS:
            row[k] = float(buckets[k])
        rows_out.append(row)

    return pd.DataFrame(rows_out)


def build_maturity_profile_report(
    as_of: date,
    *,
    active_only: bool,
    view_type: str = "principal",
    restructure_scope: frozenset[str] | None = None,
) -> pd.DataFrame:
    base = fetch_loans_maturity_base_rows(
        as_of, active_only=active_only, restructure_scope=restructure_scope
    )
    if not base:
        return pd.DataFrame()
    lids = [int(r["loan_id"]) for r in base]
    sched_map = fetch_latest_schedule_lines_batch(lids)

    rows_out: list[dict[str, Any]] = []
    for r in base:
        lid = int(r["loan_id"])
        lines = sched_map.get(lid) or []
        pnd = as_10dp(r.get("principal_not_due") or 0)
        buckets = bucket_maturity_for_loan(as_of, principal_not_due=pnd, schedule_lines=lines, view_type=view_type)
        bucket_sum = as_10dp(sum(buckets[k] for k in MATURITY_BUCKET_KEYS))
        
        # Recon diff only applies to principal view. For cash flow, the bucket sum naturally exceeds PND.
        recon_diff = float(as_10dp(bucket_sum - pnd)) if view_type == "principal" else None

        row = {
            "loan_id": lid,
            "customer_name": r.get("customer_name"),
            "product_code": r.get("product_code"),
            "loan_type": r.get("loan_type"),
            "scheme": r.get("scheme"),
            "state_as_of": r.get("state_as_of"),
            "principal_not_due": float(pnd),
            "bucket_sum": float(bucket_sum),
            "recon_diff": recon_diff,
        }
        for k in MATURITY_BUCKET_KEYS:
            row[k] = float(buckets[k])
        rows_out.append(row)

    return pd.DataFrame(rows_out)


def build_creditor_maturity_profile_report(
    as_of: date,
    *,
    active_only: bool = True,
    view_type: str = "principal",
) -> pd.DataFrame:
    """
    Creditor-side maturity from **schedule lines only**: each future instalment’s scheduled principal (or
    principal + interest in cash-flow view) is placed in the tenor bucket for its due date. Same bucket
    boundaries as :func:`build_maturity_profile_report`, but **no** reconciliation to ``principal_not_due``
    or other daily-state balances.
    """
    base = fetch_creditor_schedule_maturity_drawdown_rows(active_only=active_only)
    if not base:
        return pd.DataFrame()
    dids = [int(r["creditor_drawdown_id"]) for r in base]
    sched_map = fetch_latest_creditor_schedule_lines_batch(dids)

    rows_out: list[dict[str, Any]] = []
    for r in base:
        did = int(r["creditor_drawdown_id"])
        lines = sched_map.get(did) or []
        buckets = bucket_maturity_from_future_scheduled_cashflows(
            as_of, schedule_lines=lines, view_type=view_type
        )
        bucket_sum = as_10dp(sum(buckets[k] for k in MATURITY_BUCKET_KEYS))
        if bucket_sum <= 0:
            continue

        row = {
            "creditor_drawdown_id": did,
            "lender_name": r.get("lender_name"),
            "creditor_facility_id": r.get("creditor_facility_id"),
            "loan_type": r.get("loan_type"),
            "scheduled_future_total": float(bucket_sum),
            "bucket_sum": float(bucket_sum),
        }
        for k in MATURITY_BUCKET_KEYS:
            row[k] = float(buckets[k])
        rows_out.append(row)

    return pd.DataFrame(rows_out)


def build_debtor_creditor_maturity_gap_summary(
    as_of: date,
    *,
    active_only: bool,
    view_type: str = "principal",
    restructure_scope: frozenset[str] | None = None,
) -> pd.DataFrame:
    """
    **Liquidity gap (timing):** sum debtor maturity bucket amounts (daily-state weighted schedule) vs sum
    creditor maturity bucket amounts (**scheduled** future cashflows only) at the same as-of, using the same
    tenor band labels. Debtor methodology: :func:`build_maturity_profile_report`; creditor: :func:`build_creditor_maturity_profile_report`.

    Rows are tenor buckets (shortest first), then **TOTAL**. ``net_position`` = inflows − outflows per bucket;
    ``cumulative_position`` is the running sum of ``net_position`` down the buckets (excludes the TOTAL row
    in the cumulative logic — TOTAL repeats the final cumulative for convenience).
    """
    df_d = build_maturity_profile_report(
        as_of,
        active_only=active_only,
        view_type=view_type,
        restructure_scope=restructure_scope,
    )
    df_c = build_creditor_maturity_profile_report(
        as_of, active_only=active_only, view_type=view_type
    )

    sums_d: dict[str, Decimal] = {k: Decimal("0") for k in MATURITY_BUCKET_KEYS}
    sums_c: dict[str, Decimal] = {k: Decimal("0") for k in MATURITY_BUCKET_KEYS}
    if not df_d.empty:
        for k in MATURITY_BUCKET_KEYS:
            if k in df_d.columns:
                sums_d[k] = as_10dp(df_d[k].sum())
    if not df_c.empty:
        for k in MATURITY_BUCKET_KEYS:
            if k in df_c.columns:
                sums_c[k] = as_10dp(df_c[k].sum())

    rows_out: list[dict[str, Any]] = []
    cum = Decimal("0")
    tot_d = Decimal("0")
    tot_c = Decimal("0")
    for k, lbl in zip(MATURITY_BUCKET_KEYS, MATURITY_BUCKET_LABELS, strict=True):
        d_amt = sums_d[k]
        c_amt = sums_c[k]
        net = as_10dp(d_amt - c_amt)
        cum = as_10dp(cum + net)
        tot_d = as_10dp(tot_d + d_amt)
        tot_c = as_10dp(tot_c + c_amt)
        rows_out.append(
            {
                "bucket": lbl,
                "debtor_cash_inflows": float(d_amt),
                "creditor_cash_outflows": float(c_amt),
                "net_position": float(net),
                "cumulative_position": float(cum),
            }
        )
    rows_out.append(
        {
            "bucket": "TOTAL",
            "debtor_cash_inflows": float(tot_d),
            "creditor_cash_outflows": float(tot_c),
            "net_position": float(as_10dp(tot_d - tot_c)),
            "cumulative_position": float(cum),
        }
    )
    return pd.DataFrame(rows_out)


def build_regulatory_maturity_profile_report(
    as_of: date,
    *,
    active_only: bool,
    view_type: str = "principal",
    restructure_scope: frozenset[str] | None = None,
) -> pd.DataFrame:
    """Per-loan regulatory maturity buckets (same base population as debtor maturity profile)."""
    base = fetch_loans_maturity_base_rows(
        as_of, active_only=active_only, restructure_scope=restructure_scope
    )
    if not base:
        return pd.DataFrame()
    lids = [int(r["loan_id"]) for r in base]
    sched_map = fetch_latest_schedule_lines_batch(lids)

    rows_out: list[dict[str, Any]] = []
    for r in base:
        lid = int(r["loan_id"])
        lines = sched_map.get(lid) or []
        pnd = as_10dp(r.get("principal_not_due") or 0)
        buckets = bucket_regulatory_maturity_for_loan(
            as_of, principal_not_due=pnd, schedule_lines=lines, view_type=view_type
        )
        bucket_sum = as_10dp(sum(buckets[k] for k in REGULATORY_MATURITY_BUCKET_KEYS))
        recon_diff = float(as_10dp(bucket_sum - pnd)) if view_type == "principal" else None

        row = {
            "loan_id": lid,
            "customer_name": r.get("customer_name"),
            "product_code": r.get("product_code"),
            "loan_type": r.get("loan_type"),
            "scheme": r.get("scheme"),
            "state_as_of": r.get("state_as_of"),
            "principal_not_due": float(pnd),
            "bucket_sum": float(bucket_sum),
            "recon_diff": recon_diff,
        }
        for k in REGULATORY_MATURITY_BUCKET_KEYS:
            row[k] = float(buckets[k])
        rows_out.append(row)

    return pd.DataFrame(rows_out)


def build_regulatory_maturity_summary_table(
    as_of: date,
    *,
    active_only: bool,
    view_type: str = "principal",
    restructure_scope: frozenset[str] | None = None,
) -> pd.DataFrame:
    """
    Aggregated regulatory maturity profile: debtor **cash inflows** (principal or principal+interest per
    ``view_type``) summed by regulatory bucket. **Cash outflows** (maturing liabilities) are zero until wired.

    Rows: TOTAL, then i)–ix). ``net`` = inflow − outflow; ``cumulative`` = running sum of net down the buckets.
    """
    df_loans = build_regulatory_maturity_profile_report(
        as_of,
        active_only=active_only,
        view_type=view_type,
        restructure_scope=restructure_scope,
    )
    if df_loans.empty:
        return pd.DataFrame(
            columns=[
                "bucket",
                "cash_inflows",
                "cash_outflows",
                "net_position",
                "cumulative_position",
            ]
        )

    sums: dict[str, Decimal] = {k: Decimal("0") for k in REGULATORY_MATURITY_BUCKET_KEYS}
    for k in REGULATORY_MATURITY_BUCKET_KEYS:
        if k in df_loans.columns:
            sums[k] = as_10dp(df_loans[k].sum())

    total_in = as_10dp(sum(sums.values()))
    total_out = Decimal("0")
    total_net = as_10dp(total_in - total_out)

    rows: list[dict[str, Any]] = [
        {
            "bucket": "TOTAL INFLOWS/OUTFLOWS",
            "cash_inflows": float(total_in),
            "cash_outflows": float(total_out),
            "net_position": float(total_net),
            "cumulative_position": float(total_net),
        }
    ]

    cum = Decimal("0")
    for k, lbl in zip(REGULATORY_MATURITY_BUCKET_KEYS, REGULATORY_MATURITY_BUCKET_LABELS, strict=True):
        inf = sums[k]
        outf = Decimal("0")
        net = as_10dp(inf - outf)
        cum = as_10dp(cum + net)
        rows.append(
            {
                "bucket": lbl,
                "cash_inflows": float(inf),
                "cash_outflows": float(outf),
                "net_position": float(net),
                "cumulative_position": float(cum),
            }
        )

    return pd.DataFrame(rows)
