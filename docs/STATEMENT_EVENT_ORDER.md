# Statement event ordering (intra-day)

Used by `reporting/statement_events.py` `sort_ordinal` and any future event builder.

**Primary sort:** `event_date` ascending.

**Within the same calendar date (ascending `sort_ordinal`):**

| Ordinal | Event family | Notes |
|--------|----------------|-------|
| 10 | Opening / carry-forward | Optional explicit opening row (debit/credit 0) |
| 15 | Drawdown | Principal + fee lines at disbursement (`DISBURSEMENT`) |
| 20 | Regular interest accrual (EOD) | `regular_interest_daily` |
| 21 | Penalty interest accrual (EOD) | `penalty_interest_daily` |
| 22 | Default interest accrual (EOD) | `default_interest_daily` |
| 30 | Fees & charges balance movement | Delta in `fees_charges_balance` (EOD-only slice; omit when using bucket 56) |
| 40 | Principal / structural | Recast adjustment (future) |
| 50–56 | Receipt allocations | `alloc_principal_total` … `alloc_fees_charges` (`PAYMENT_*`) |
| 60 | Unapplied ledger | Credits, liquidations (future; separate running column) |

**Tie-break:** `event_type`, then `narration`.

**Accrual windows:** Calendar inclusion of regular interest follows `accrual_start_convention` (`EFFECTIVE_DAY` vs `NEXT_DAY`); EOD already writes zero or non-zero `*_daily` per day accordingly—event builders emit rows from persisted dailies.

See `docs/STATEMENT_FLOW_AND_RECONCILIATION_PLAN.md` for period-to-date narrative and GL approval gate.
