# Subaccounts, grandchild codes, and posting resolution

## Migration

Run:

`python scripts/run_migration_50.py`

This widens `accounts.code` / `account_template` codes to `VARCHAR(32)`, adds `accounts.subaccount_resolution`, creates `disbursement_bank_options` and `product_gl_subaccount_map`, and adds `loans.disbursement_bank_option_id`.

Until migration runs, new UI and `save_loan` inserts that reference the new column will fail against an old database.

## Grandchild account codes

- **Pattern:** `CCCCCCC-NN` where `CCCCCCC` is the normal 7-character code of the **parent** account and `NN` is `01`–`99`.
- Example: parent `A100001` → children `A100001-01`, `A100001-02`, …
- Helpers live in `accounting.core`: `split_account_code`, `suggest_next_grandchild_account_code`, `parse_seven_char_account_code`.

## Backward compatibility

- If the account for a `system_tag` has **no active children**, resolution is unchanged: that row is the posting account (same as historical behaviour).
- If the tagged account **has** children and **`subaccount_resolution` is NULL**, posting still **fails** with a clear message (same class of error as before: you must not post to a roll-up parent without a rule).
- Optional maps and loan bank selection only apply when you opt in via **resolution mode** + data.

## `subaccount_resolution` on `accounts`

| Value          | Meaning |
|----------------|---------|
| *(empty)*      | Legacy: tagged parent with children → error until you map tags to leaves or set a mode. |
| `PRODUCT`      | For this tag, use `product_gl_subaccount_map` keyed by `loans.product_code` + `system_tag`. **Requires `loan_id` on `post_event`.** |
| `LOAN_CAPTURE` | For **`cash_operating` only**, use the loan’s `disbursement_bank_option_id` → `disbursement_bank_options.gl_account_id`. **Requires `loan_id`.** |
| `JOURNAL`      | Automated posting must supply `payload["account_overrides"][system_tag] = "<uuid>"` (manual / integration responsibility). |

## API

- `AccountingService.post_event(..., loan_id=None)` — when set, enables PRODUCT and LOAN_CAPTURE resolution for template lines.
- `payload["account_overrides"]` — optional `dict` of `system_tag` → posting account UUID (wins over resolution rules for that tag).

## Loan capture

- **Operating bank for this loan** (step 1) stores `disbursement_bank_option_id` into loan `details` / drafts and persists on `loans` at save.
- Used only when COA uses **LOAN_CAPTURE** on the tagged cash parent.

## UI (Accounting → Chart of Accounts)

Expanders cover **subaccount resolution**, **disbursement banks**, and **product → GL** maps. **Add Custom Account** can suggest the next `BASE-NN` code under a parent.

## Operational notes

1. **Product maps** use `products.code` (e.g. `TERM-USD`), matching `loans.product_code`.
2. **LOAN_CAPTURE** is intentionally limited to **`cash_operating`** to avoid ambiguous bank semantics on non-cash tags.
3. **JOURNAL** mode documents that teller/automation must pass explicit UUID overrides for those tags.
4. **Historical journals** are unchanged; new rules apply to **new** postings that use the resolver.
