# Scripts (utilities & one-offs)

These are **not** part of the running LMS application. They are maintenance, export, and example scripts. Run from the **project root** so that `config` and other modules resolve correctly.

## How to correct an existing loan (before recent allocation/EOD changes)

If a loan (and its receipts) were captured or updated **before** the current logic (allocation at save, EOD subtracting allocations, reversal undoing allocation), use one of these:

**Option A – Correct a single date (e.g. one day with wrong allocations)**  
1. Run EOD for that date, then re-allocate all receipts for that date:
   ```bash
   python scripts/correct_receipts_for_date.py 2025-12-01
   ```
2. Optionally check allocation: `python scripts/check_allocation.py <repayment_id>` or `--loan <id> --from ... --to ...`.

**Option B – Correct a range of dates (e.g. whole loan history)**  
Run EOD and reallocate for every day in the range (from first receipt/disbursement to last date you care about):
   ```bash
   python scripts/run_eod_date_range.py 2025-10-08 2025-12-31
   ```
   This processes **all** active loans for each date; your loan is corrected along with others.

**Option C – Correct a single receipt**  
1. Ensure daily state exists for the receipt’s value date (run EOD for that date if needed).
2. Re-allocate that receipt only:
   ```bash
   python scripts/reallocate_repayment.py <repayment_id>
   ```
   If you’re unsure about state, use Option A for that date instead (EOD + reallocate all receipts for the date).

**Check results**  
- Allocation: `python scripts/check_allocation.py --loan <loan_id> --from <start> --to <end>`
- Unapplied: `python scripts/check_unapplied_funds.py --loan <loan_id>`
- Daily state / buckets: `python scripts/check_loan_daily_accrual.py --loan <loan_id>` (if present)

## Procedure to correct reallocation for a date

Order matters. Reallocation uses **loan_daily_state** for the receipt’s value_date, so daily state must be correct first.

1. **Run EOD for that date** so accruals and buckets in `loan_daily_state` are correct.
2. **Then re-allocate** receipts for that date so allocation uses the updated state.

**One script that does both (recommended):**
```bash
python scripts/correct_receipts_for_date.py 2025-12-01
```
This runs EOD for 1.12.2025, then reallocates every receipt with value_date 1.12.2025.

**Or manually:** `python scripts/run_eod_date_range.py 2025-12-01 2025-12-01` then `python scripts/reallocate_receipts_for_date.py 2025-12-01`.

| Script | Purpose |
|--------|---------|
| `export_loan_tables.py` | Export loans, daily state, repayments, allocation to CSV in `lms_exports/`. |
| `cleanup_loan_daily_state.py` | Delete `loan_daily_state` rows where date is before loan disbursement/start. |
| `delete_loans_1_to_7.py` | Delete loans 1–7 and all related data (CASCADE). Use `--yes` to skip confirm. |
| `reallocate_repayment.py` | Correct an already-saved receipt: reverse allocation + unapplied, then re-allocate (e.g. `python scripts/reallocate_repayment.py 2`). |
| `reallocate_receipts_for_date.py` | Re-allocate all receipts with value_date on a given date (run **after** EOD for that date). |
| `correct_receipts_for_date.py` | **EOD for the date, then** re-allocate all receipts for that date (use this to correct receipts for 1.12.2025 etc.). |
| `run_eod_date_range.py` | Run EOD for each day in a date range to correct accruals or backfill (e.g. `python scripts/run_eod_date_range.py 2025-10-08 2025-12-01`). |
| `check_allocation.py` | Show allocation in DB for a receipt or for a loan (e.g. `python scripts/check_allocation.py 2` or `--loan 9 --from 2025-11-01 --to 2025-12-31`). |
| `check_unapplied_funds.py` | Show unapplied funds (suspense) in DB (e.g. `python scripts/check_unapplied_funds.py` or `--loan 9 --status pending`). |
| `bootstrap_admin.py` | Create or reset an admin user in the `users` table. Edit email/password in file. |
| `check_admin_password.py` | Check if a password matches the stored hash for a user. Edit email/password in file. |

**Run from project root:**

```bash
python scripts/export_loan_tables.py
python scripts/delete_loans_1_to_7.py
python scripts/cleanup_loan_daily_state.py
python scripts/reallocate_repayment.py 2   # correct receipt 2
python scripts/run_eod_date_range.py 2025-10-08 2025-12-01   # correct accruals for date range
python scripts/bootstrap_admin.py
python scripts/check_admin_password.py
```
