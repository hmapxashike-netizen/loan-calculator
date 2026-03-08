# Allocation vs balance: why alloc can exceed balance (and how to fix)

## What you saw

- **Loan 9, 30.11.2025:** `interest_arrears_balance = 101.75`
- **Repayment 2** (value_date 1.12.2025, amount 160): `alloc_interest_arrears = 160`

So we allocated **160** to interest arrears when the balance due was only **101.75** — i.e. we over-allocated.

## Where it breaks down

1. **Which state we use**  
   For a receipt with `value_date = 2025-12-01`, allocation loads:
   - `loan_daily_state` with `as_of_date <= value_date`  
   - `ORDER BY as_of_date DESC LIMIT 1`  
   So we use the **latest** daily state on or before 1.12 (either 1.12 or 30.11).

2. **Why 160 got into the DB**  
   With the **current** logic we cap per bucket: `to_alloc = min(remaining, bucket_balance)`, so we would never allocate 160 to interest arrears if the state we read had `interest_arrears_balance = 101.75`. So the 160 allocation could only happen if:
   - the allocation was run **before** the per-bucket cap was added, or
   - the state row we read had `interest_arrears_balance >= 160` (e.g. wrong or stale EOD for that date).

3. **Correct behaviour**  
   For the state with `interest_arrears_balance = 101.75`, we should allocate **at most 101.75** to interest arrears. The rest of the 160 (58.25) should go to the next buckets in the waterfall (e.g. principal arrears 53.03) and any remainder to **unapplied funds**.

## Why “reallocate” was still writing 160

Reallocation does: (1) **reverse** = add the *saved* allocation back to `loan_daily_state`, (2) then **allocate** again.

- Saved allocation had `alloc_interest_arrears = 160`.
- Current state had `interest_arrears_balance = 0` (because we’d already subtracted 160).
- After reverse: `0 + 160 = 160` → we “restored” to **160**.
- Allocation then read that state and was allowed to allocate 160 again.

So the engine was working, but the **state passed into it** was wrong: we restored using the wrong allocation instead of the correct pre-receipt balance.

## Fix (in code)

After reversing, we now **recompute** the correct daily state for the receipt’s date by running **EOD for that date**. EOD overwrites the row with engine output (accruals/due-date only; no repayment), so `interest_arrears_balance` is correct (e.g. 101.75). Then allocation runs with that state and caps at 101.75.

So **reallocate_repayment** now: reverse → **run EOD for eff_date** → allocate.

## Fix (what you run)

```bash
python scripts/correct_receipts_for_date.py 2025-12-01
```
Then:
```bash
python scripts/check_allocation.py 2
```
You should see `alloc_interest_arrears` ≤ 101.75 (e.g. 101.75), with the rest to other buckets or unapplied.

## Why principal/interest arrears stay reduced

**Allocation runs at EOD.** For each date, EOD runs the engine (accruals only), then `apply_allocations_for_loan_date`: for that loan and date, it loads all posted receipts with that value date, applies each in order (compute allocation from current state, write allocation row, credit unapplied if any, subtract from state), and saves the final state. So saved state = accruals − that day’s allocations. Principal/interest arrears (and other buckets) are reduced by receipts and are not overwritten.

## Going forward

The code now:
   - Caps each bucket: `to_alloc = min(remaining, bucket_balance)`.
   - Validates after the loop: if any allocation exceeds that bucket’s balance (tolerance 0.01), it **raises** and does not save.
   - Uses and reports the **state as_of_date** in the error message so you can see which snapshot was used.
   - **EOD** subtracts allocation totals for each date before saving `loan_daily_state`, so balances 1–5 remain reduced by allocated receipts.

