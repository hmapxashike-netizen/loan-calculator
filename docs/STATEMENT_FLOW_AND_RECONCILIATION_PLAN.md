# Statement flow, scheduled periods, and reconciliation (specification)

This document is the **target architecture** for customer and internal loan statements built as **discrete events** and a **running balance**. It aligns **accrual windows** and **period-to-date** semantics with the **current engine configuration** (`accrual_convention.py`, `eod/loan_daily_engine.py`) and with how **`loan_daily_state`** is populated at EOD.

**Related code (reference only):** `accrual_convention.py`, `eod/loan_daily_engine.py`, `eod/core.py`, `reporting/statements.py`, `loan_management/schedules.py` (`get_max_schedule_due_date_on_or_before`).

---

## GL posting and accounting journals — approval gate

**Default rule for this programme of work:** **do not change GL posting**, journal definitions, event types, or migration scripts **unless** a separate change is **explicitly approved** in writing (scope, regression risk, and cutover). The statement refactor should be a **presentation and reconciliation layer** on top of existing subledger (`loan_daily_state`) and existing repayment / unapplied facts.

**If** a future change proves unavoidable (e.g. a missing event type that cannot be derived from existing tables), the proposal must be raised **first** with: impact on posted history, idempotent replay, and parallel run vs old behaviour.

---

## Scheduled periods and accrual days (must match engine)

The system supports two conventions (product/system config: `accrual_start_convention`). **Implementation of the event list must branch on the same convention as EOD.**

### EFFECTIVE_DAY (include first calendar day of the period, exclude the due date as an accrual day)

Matches the docstring in `accrual_convention.py` and the window in `_find_schedule_entry_for_day`:

- For a schedule instalment with **`period_start`** and **`due_date`**:
  - Regular interest accrues on calendar days **`d`** such that **`period_start <= d < due_date`**.
- So the **first** accrual day is **`period_start` (inclusive)**.
- The **due date** is **not** a regular-interest accrual day; it is treated as **billing/settlement** (movement between buckets, etc.). That is what “**include the first day, exclude the last [accrual day]**” means for this convention: the **last day of the accrual window** is **`due_date - 1 calendar day`**, not the due date itself.

### NEXT_DAY (legacy)

- Accrual window: **`period_start < d <= due_date`**.
- First accrual day is the day **after** `period_start`; **due date** **is** the last accrual day.

### Which period covers “today”?

For each calendar date, the engine selects the single schedule entry whose window contains that date. If no entry applies, scheduled regular interest for that day is zero.

### Last contractual due and the open period (cross-version)

After recast, the **latest** schedule row set may omit historical dues. **Anchor** the start of the current interest period using **all saved schedule versions** (e.g. latest due date on or before a boundary date), consistent with `get_max_schedule_due_date_on_or_before` and EOD period-to-date reset logic that considers **all** version due dates.

---

## Period-to-date and “accrued to date” on the statement

**Definition (aligned with persisted columns and engine intent):**

- **`regular_interest_period_to_date`** (and penalty/default analogues) in **`loan_daily_state`** is the running sum of **`regular_interest_daily`** (etc.) **within the current schedule period**, with **resets** after each **contractual due date** (see schema comment on `regular_interest_period_to_date` and EOD persistence in `eod/core.py`).

**For statement presentation and roll-ups (“accrued to date” narrative):**

- **Accrued-to-date amount** for the **open** (incomplete) period should equal the **summation of daily accruals** over the **same** calendar days the engine would include under the active convention, from:
  - **Period start:** last contractual due date that starts the period (**inclusive** under EFFECTIVE_DAY as **`period_start`**), derived consistently across schedule versions where needed.
  - **Through date (end of accrual window for the label):**
    - **Default (EOD-complete):** through **`min(statement_end, system_business_date) - 1 calendar day`** when the statement row represents “interest accrued **through yesterday**” relative to that row date (matches the existing stub pattern that uses `stub_period_end = end - 1` for the narrative window).
    - **Intraday / same-day capture:** when business rules persist or display activity **through system date** (e.g. intraday receipts or same-day state visible to the customer), the **through date** may extend to **system business date** (or the agreed “as at” time) **only** where product policy and `loan_daily_state` / EOD completeness flags explicitly allow it—so “accrued to date” does not claim EOD that has not run.

**Reliability:** rolled-up lines (e.g. one line “Regular interest (dd/mm/yy to dd/mm/yy)”) must equal the **sum of the underlying per-day event amounts** in the event ledger for that window (or match `*_period_to_date` at the same `as_of_date` when EOD and version boundaries are consistent).

---

## 1. Principles

1. **Balance is always a running total**  
   After sorting events in a strict order,  
   `Balance[i] = Balance[i-1] + Debits[i] - Credits[i]`  
   with **no** mid-statement overwrite from a “static” closing column for display.  
   **Verification:** compare the final running balance to `loan_daily_state` in a **separate reconciliation step**—do not rewrite the running column from stock mid-stream.

2. **Every line is an event**  
   Each row has: `event_date`, `event_type`, `narration`, `debit`, `credit`, optional `repayment_id` / `reference`, and optional bucket hints for audit only (not for balance math).

3. **Double-entry consistency**  
   Anything that **does not change total loan exposure** must not appear as both a charge and a payment, or as two charges. Typical **zero-net-internal** events: accrued → interest arrears **billing** if **total outstanding** (sum of the same buckets as closing) is **unchanged**—exclude from “New Charges” in the simple identity or show as paired lines / supplemental detail only.

---

## 2. Balance scope (critical)

Identity:

$$\text{Opening} + \text{New Charges} - \text{Payments} = \text{Closing}$$

Only holds if **Opening** and **Closing** use the **same** measure as the sum of flows.

**Recommended definition (aligns with current `_total_outstanding` in reporting):**

- **Closing balance** = principal (not due + arrears) + interest (accrued + arrears) + default + penalty + fees.

**Unapplied funds** are usually **not** inside that total; they are a **separate running column** (customer money in suspense). If unapplied is folded into one “balance,” the identity must be rewritten.

**Plan: two running columns on the customer view**

- **Loan outstanding** (for the identity above).  
- **Unapplied funds** (running total from `unapplied_funds_ledger`).

---

## 3. Complete event taxonomy (discrete records)

### 3.1 Charges (increase loan outstanding)

| Event | Source of truth | Granularity |
|--------|-----------------|-------------|
| Principal disbursement (and fee capitalisation at drawdown if it increases exposure) | Loan origination / drawdown facts + loan row | One or more lines as today |
| Regular interest accrual | `loan_daily_state.regular_interest_daily` per EOD date | **One line per calendar day** (only on days in the engine accrual window per convention) |
| Default interest accrual | `default_interest_daily` | One line per day |
| Penalty interest accrual | `penalty_interest_daily` | One line per day |
| Fees & charges | Delta in `fees_charges_balance` or fee posting events | One line per posting event; if no event table, **daily delta** as discrete lines |

**Explicitly exclude from “charges” (net-zero on total outstanding):**

- Pure **accrued → interest arrears** billing if total outstanding unchanged—unless shown as paired/zero-net or supplemental.

### 3.2 Credits (decrease loan outstanding or move cash to unapplied)

| Event | Source of truth |
|--------|-----------------|
| Payment allocated to loan buckets | `loan_repayment_allocation` (+ reversals) per `value_date` |
| Unapplied credit (overpayment) | `unapplied_funds_ledger` |
| Liquidation of unapplied applied to loan | Split: unapplied column vs loan bucket effects; must net correctly |

**Reversals:** discrete opposite-sign events (same `repayment_id` where applicable).

### 3.3 Non-cash structural events

- Recast / re-amortisation: explicit adjustment lines.  
- Modifications / waivers / write-downs: one event per business act.  
- FX / rounding true-ups: explicit adjustment events.

---

## 4. Ordering rules

1. **Primary sort:** `event_date` (value date / EOD date).  
2. **Intra-day order (fixed policy):** e.g. opening snapshot → accruals (regular, penalty, default, fees) → receipt allocations → unapplied movements → structural adjustments.  
3. **Document** the chosen order in **`docs/STATEMENT_EVENT_ORDER.md`** (to be added when implementation starts) and reference **`accrual_start_convention`** for which calendar days accrual lines exist.

---

## 5. Opening balance

- **Opening** = `loan_daily_state` **total outstanding** at end of **`start_date - 1`** (or an explicitly documented “morning of start_date” alternative—pick one product-wide).  
- Prefer an **explicit opening row** for customer readability (Debits/Credits 0, Balance = Opening).

---

## 6. Reliability equation (operational)

For **loan outstanding**:

$$\text{Closing} = \text{Opening} + \sum \text{Charge debits} - \sum \text{Payment credits} + \text{adjustments}$$

**Automated check:** after building the event list, compute running balance; compare to `loan_daily_state` at `end_date` (same components); tolerance 0 at 10dp; on failure flag `STATEMENT_RECONCILIATION_FAILED` with bucket diff.

**Unapplied:** separate identity from ledger.

---

## 7. Data pipeline (implementation shape)

1. **Event builder** (new module or refactor `reporting/statements.py`):  
   - EOD date range → **daily** accrual rows respecting **EFFECTIVE_DAY vs NEXT_DAY** windows.  
   - Repayments + allocations + reversals.  
   - Unapplied ledger.  
   - Drawdown / structural events.

2. **No snap-to-daily-state inside the running balance loop** (per presentation rule); **reconcile after** the loop.

3. **Customer-facing roll-up:** e.g. “Accrued interest (29/03–05/04)” = sum of underlying daily events for display; **underlying daily rows** remain source of truth for audit/reconcile.

---

## 8. Edge cases checklist

- Partial EOD: omit or mark provisional accrual rows when EOD incomplete.  
- Recast mid-period: explicit adjustments; resume daily accruals under new schedule windows.  
- Same-day receipt + accrual: fixed intraday order.  
- LIQ from unapplied: both columns.  
- Interest in suspense: include in outstanding if `loan_daily_state` totals include it.

---

## 9. Migration

- Build event list + running balance **in parallel** with current statement.  
- Golden tests: clean loan, arrears, overpayment, LIQ, recast, reversal, **both accrual conventions**.  
- Remove balance snapping only when reconcile passes.  
- Update customer-facing help for daily vs rolled-up lines.

---

## 10. Caveat

Running total is for **presentation consistency**. **Reliability** is still proven against **`loan_daily_state`** (or GL); the subledger remains the independent check—not abandoned.

---

## 11. Billing lines (accrued → arrears) — product choice

Whether customers see **billing** as explicit rows affects whether the **simple three-term identity** holds line-by-line without paired entries. **Default recommendation:** keep **billing internal** on the customer statement unless product requests transparency; if shown, use **paired zero-net** lines or a supplemental schedule.

---

## Implementation status (slices)

| Slice | Status | Location |
|-------|--------|----------|
| **1** — EOD accrual + fee-delta events, running balance, reconcile hook, intra-day order doc | Done | `reporting/statement_events.py`, `docs/STATEMENT_EVENT_ORDER.md`, `preview_statement_eod_flow_events` in `reporting/statements.py`, `tests/test_statement_events.py` |
| **2** — Drawdown + repayment bucket events (`alloc_interest_accrued` added to allocation query), full merge without fee-balance delta | Done | `loan_management/allocation_queries.py`, `build_complete_loan_flow_events`, `preview_statement_eod_flow_events(..., eod_only=…)` |
| **2b** — Unapplied ledger as separate running column (``meta['unapplied_delta']`` + dual running) | Done | ``build_unapplied_ledger_statement_events``, ``apply_dual_running_customer_events``, ``build_merged_customer_flow_events`` |
| **3** — Customer-facing flow statement (no balance snap) | Done | ``generate_customer_facing_flow_statement`` in ``reporting/statements.py`` |
| **3b** — Wire Streamlit/UI to flow statement; roll-ups from daily events | Planned | — |
