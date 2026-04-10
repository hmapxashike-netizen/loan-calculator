-- Run on farndacred_db after 24_loan_daily_state_net_allocation.sql
--
-- Increase the daily-accrual and period-to-date columns from NUMERIC(18,2) to
-- NUMERIC(22,10).  Balance and exposure columns remain at NUMERIC(18,2) because
-- they represent monetary amounts (dollar-and-cent granularity is correct there).
--
-- Why: daily accrual values are computed as  balance × rate / 30  which produces
-- an irrational fraction (e.g. 33.333333...).  Storing only 2dp accumulates a
-- rounding error of up to 0.10 per period that appears as a "cosmetic residual"
-- in per-day identity checks.  10dp reduces that residual to < 0.000001 per day.

ALTER TABLE loan_daily_state
    ALTER COLUMN regular_interest_daily       TYPE NUMERIC(22, 10),
    ALTER COLUMN penalty_interest_daily       TYPE NUMERIC(22, 10),
    ALTER COLUMN default_interest_daily       TYPE NUMERIC(22, 10),
    ALTER COLUMN regular_interest_period_to_date  TYPE NUMERIC(22, 10),
    ALTER COLUMN penalty_interest_period_to_date  TYPE NUMERIC(22, 10),
    ALTER COLUMN default_interest_period_to_date  TYPE NUMERIC(22, 10);
