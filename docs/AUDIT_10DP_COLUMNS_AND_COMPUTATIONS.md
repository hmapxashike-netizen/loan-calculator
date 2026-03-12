# Audit: 10 Decimal Place Storage and Computations

**Date:** 2025-03-12  
**Scope:** All numeric columns by table; all engines and computations that produce/save values.

---

## Part 1: Column-by-Table Storage Precision

### 1.1 Tables with 10dp Columns (NUMERIC(22,10))

Only **loan_daily_state** has columns that store to 10 decimal places (after migration 25):

| Table | Column | Storage | Notes |
|-------|--------|---------|-------|
| loan_daily_state | regular_interest_daily | NUMERIC(22,10) | 10dp ✓ |
| loan_daily_state | penalty_interest_daily | NUMERIC(22,10) | 10dp ✓ |
| loan_daily_state | default_interest_daily | NUMERIC(22,10) | 10dp ✓ |
| loan_daily_state | regular_interest_period_to_date | NUMERIC(22,10) | 10dp ✓ |
| loan_daily_state | penalty_interest_period_to_date | NUMERIC(22,10) | 10dp ✓ |
| loan_daily_state | default_interest_period_to_date | NUMERIC(22,10) | 10dp ✓ |

**Migration 25** (`schema/25_loan_daily_state_highprec_daily.sql`) upgrades these from NUMERIC(18,2) to NUMERIC(22,10).

---

### 1.2 Tables with 2dp Columns (NUMERIC(18,2) or similar)

| Table | Column | Storage | Notes |
|-------|--------|---------|-------|
| **loans** | facility, principal, installment, total_payment | NUMERIC(18,2) | 2dp (currency) |
| **loans** | admin_fee_amount, drawdown_fee_amount, arrangement_fee_amount | NUMERIC(18,2) | 2dp (currency) |
| **loans** | annual_rate, monthly_rate | NUMERIC(12,6) | 6dp (rate) |
| **loans** | drawdown_fee, arrangement_fee, admin_fee | NUMERIC(8,6) | 6dp (rate) |
| **schedule_lines** | payment, principal, interest, principal_balance, total_outstanding | NUMERIC(18,2) | 2dp |
| **loan_repayments** | amount | NUMERIC(18,2) | 2dp |
| **loan_repayment_allocation** | alloc_principal_not_due, alloc_principal_arrears, alloc_interest_accrued, alloc_interest_arrears, alloc_default_interest, alloc_penalty_interest, alloc_fees_charges | NUMERIC(18,2) | 2dp |
| **loan_repayment_allocation** | alloc_principal_total, alloc_interest_total, alloc_fees_total | NUMERIC(18,2) | 2dp |
| **loan_daily_state** | principal_not_due, principal_arrears | NUMERIC(18,2) | 2dp |
| **loan_daily_state** | interest_accrued_balance, interest_arrears_balance | NUMERIC(18,2) | 2dp |
| **loan_daily_state** | default_interest_balance, penalty_interest_balance, fees_charges_balance | NUMERIC(18,2) | 2dp |
| **loan_daily_state** | total_exposure, credits, net_allocation, unallocated | NUMERIC(18,2) | 2dp |
| **unapplied_funds** | amount | NUMERIC(18,2) | 2dp |
| **loan_modifications** | new_annual_rate | NUMERIC(12,6) | 6dp |
| **loan_modifications** | new_principal | NUMERIC(18,2) | 2dp |
| **loan_recasts** | new_installment | NUMERIC(18,2) | 2dp |
| **agents** (11_sectors_subsectors_agents) | commission_rate_pct | NUMERIC(6,2) | 2dp |
| **customers** (03_customers) | shareholding_pct | NUMERIC(6,2) | 2dp |

---

### 1.3 Non-Numeric Columns (Excluded from 10dp Audit)

- **Dates:** disbursement_date, start_date, end_date, maturity_date, as_of_date, payment_date, value_date, created_at, updated_at, etc.
- **Integers:** id, loan_id, customer_id, term, period_number, days_overdue, version, etc.
- **Text/VARCHAR:** name, email, status, reference, event_type, narration, etc.
- **JSONB:** metadata, details

---

## Part 2: Engines and Computations by Column

### 2.1 10dp Columns – Computation Audit

#### regular_interest_daily

| Stage | Location | Precision | Status |
|-------|----------|-----------|--------|
| Compute | `loan_daily_engine._scheduled_interest_for_day()` | Raw Decimal (entry.interest_component / total_days) | No explicit 10dp quantize |
| Pass to EOD | `eod._run_loan_engine_for_date()` | `engine_loan.last_regular_interest_daily` (Decimal) | ✓ |
| Save | `loan_management.save_loan_daily_state()` | Passed as Decimal; DB NUMERIC(22,10) stores 10dp | ✓ |

**Gap:** Engine does not quantize to 10dp before assignment. The raw Decimal has full precision; DB rounds on insert. **Low risk** – DB enforces 10dp.

---

#### penalty_interest_daily

| Stage | Location | Precision | Status |
|-------|----------|-----------|--------|
| Compute (engine) | `loan_daily_engine._accrue_default_and_penalty_interest()` | Raw Decimal (basis × rate / 30) | No 10dp quantize |
| Compute (EOD) | `eod._run_loan_engine_for_date()` | Raw Decimal (basis × penalty_rate / 30) | No 10dp quantize |
| Used in EOD | EOD uses its own computation (not engine) when in arrears | Decimal | ✓ |
| Save | `save_loan_daily_state(default_interest_daily=..., penalty_interest_daily=...)` | Decimal; DB NUMERIC(22,10) | ✓ |

**Gap:** EOD computes default/penalty daily as raw Decimal. No explicit `as_accrual_10dp()` before save. DB stores 10dp on insert. **Low risk**.

---

#### default_interest_daily

| Stage | Location | Precision | Status |
|-------|----------|-----------|--------|
| Compute (EOD) | `eod._run_loan_engine_for_date()` | Raw Decimal (int_arr_opening × rate / 30) | No 10dp quantize |
| Save | `save_loan_daily_state()` | Decimal; DB NUMERIC(22,10) | ✓ |

**Gap:** Same as penalty – no explicit quantize. DB enforces 10dp. **Low risk**.

---

#### regular_interest_period_to_date

| Stage | Location | Precision | Status |
|-------|----------|-----------|--------|
| Compute (EOD) | `eod._run_loan_engine_for_date()` | `float(yesterday) + float(engine_loan.last_regular_interest_daily)` | **Uses float** |
| Save | `save_loan_daily_state()` | float; DB NUMERIC(22,10) | ⚠️ |

**Gap:** EOD uses **float** for accumulation. Float has ~15–17 significant digits; over many days this can introduce rounding error. Engine uses `as_accrual_10dp()` internally, but EOD recomputes from table + daily using float. **Medium risk** – consider Decimal + `as_accrual_10dp()` for consistency.

---

#### penalty_interest_period_to_date

| Stage | Location | Precision | Status |
|-------|----------|-----------|--------|
| Compute (EOD) | `eod._run_loan_engine_for_date()` | `D(str(yesterday)) + default_interest_daily_save` (Decimal) | ✓ |
| Save | `save_loan_daily_state()` | Decimal; DB NUMERIC(22,10) | ✓ |

**Gap:** No explicit 10dp quantize on accumulation. Decimal arithmetic preserves precision; DB rounds. **Low risk**.

---

#### default_interest_period_to_date

| Stage | Location | Precision | Status |
|-------|----------|-----------|--------|
| Compute (EOD) | `eod._run_loan_engine_for_date()` | `D(str(yesterday)) + default_interest_daily_save` (Decimal) | ✓ |
| Save | `save_loan_daily_state()` | Decimal; DB NUMERIC(22,10) | ✓ |

**Gap:** Same as penalty period-to-date. **Low risk**.

---

### 2.2 2dp Columns – Computation Audit (Summary)

| Column Group | Compute Path | Precision | Status |
|--------------|--------------|-----------|--------|
| **Balance columns** (principal_not_due, principal_arrears, interest_accrued_balance, etc.) | Engine uses `as_money()` (2dp); EOD passes float; allocation uses float | 2dp | ✓ |
| **Allocation columns** (alloc_*) | `compute_waterfall_allocation()` returns float; `round(remaining, 2)` for unapplied | 2dp | ✓ |
| **loan_repayments.amount** | User input / API; stored as NUMERIC(18,2) | 2dp | ✓ |
| **schedule_lines** (payment, principal, interest, etc.) | Amortisation logic; typically 2dp | 2dp | ✓ |
| **loans** (principal, facility, fees, etc.) | Capture/amortisation; 2dp | 2dp | ✓ |
| **unapplied_funds.amount** | From allocation overflow; 2dp | 2dp | ✓ |
| **net_allocation, unallocated, credits** | Sums of allocation amounts; 2dp | 2dp | ✓ |

---

### 2.3 Engine Summary

| Engine / Module | 10dp Usage | 2dp Usage |
|-----------------|------------|-----------|
| **loan_daily_engine** | `as_accrual_10dp()` for period_to_date only; daily values not quantized | `as_money()` for all balance buckets |
| **eod** | Passes Decimal for daily/period_to_date; **regular_interest_period_to_date uses float** | Balance columns as float |
| **loan_management.save_loan_daily_state** | Accepts Decimal/float; DB stores 10dp for daily/period_to_date columns | Balance columns as float |
| **loan_management.compute_waterfall_allocation** | N/A | Float; alloc amounts to 2dp |
| **loan_management.allocate_repayment_waterfall** | Reads daily from DB; passes through | Allocation amounts 2dp |
| **statements** | `_q2()` (2dp), `_q3()` (3dp) for display | Reads from DB |

---

## Part 3: Findings and Recommendations

### 3.1 Confirmed 10dp Storage

- **loan_daily_state** daily and period_to_date columns are NUMERIC(22,10) after migration 25.
- All other numeric columns are 2dp (or 6dp for rates) as intended.

### 3.2 Computation Gaps

1. **regular_interest_period_to_date uses float**  
   EOD computes: `float(yesterday_saved.get("regular_interest_period_to_date", 0)) + float(engine_loan.last_regular_interest_daily)`.  
   **Recommendation:** Use Decimal + `as_accrual_10dp()` for consistency with penalty/default period_to_date and to avoid float accumulation.

2. **Daily columns not explicitly quantized**  
   Engine and EOD produce raw Decimal values. DB enforces 10dp on insert.  
   **Recommendation (optional):** Add `as_accrual_10dp()` before save for clarity and consistency in Python.

3. **allocate_repayment_waterfall / reallocate**  
   When `allocate_repayment_waterfall` runs after EOD (e.g. reallocate), it reads `regular_interest_daily`, `penalty_interest_daily`, `default_interest_daily` from DB and passes them through to UPDATE. Values remain 10dp from DB. ✓

### 3.3 Tables Not Requiring 10dp

- **loans, schedule_lines, loan_repayments, loan_repayment_allocation, unapplied_funds, loan_modifications, loan_recasts** – All monetary amounts are 2dp by design. No change needed.

---

## Summary Table

| Table | 10dp Columns | 2dp Columns | Computation Status |
|-------|--------------|-------------|---------------------|
| loan_daily_state | 6 (daily + period_to_date) | 11 (balances, exposure, credits, net_allocation, unallocated) | ⚠️ regular_interest_period_to_date uses float |
| loans | 0 | 9 (principal, facility, fees, etc.) | ✓ |
| schedule_lines | 0 | 5 | ✓ |
| loan_repayments | 0 | 1 (amount) | ✓ |
| loan_repayment_allocation | 0 | 10 | ✓ |
| unapplied_funds | 0 | 1 (amount) | ✓ |
| loan_modifications | 0 | 1 (new_principal) | ✓ |
| loan_recasts | 0 | 1 (new_installment) | ✓ |

**Conclusion:** Migration 29 aligns ALL numeric columns (except dates) to NUMERIC(22,10). All engines and computations use `as_10dp()` from `decimal_utils` for consistent 10dp storage and computation.
