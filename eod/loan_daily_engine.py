"""
Daily loan state engine: accrual, due-date transitions, payment allocation.

Used by EOD to compute and persist loan_daily_state. All waterfall (payment allocation)
order comes from configuration (system/product); nothing is hardcoded here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from typing import Dict, List, Optional

from decimal_utils import as_10dp


# Configure decimal for currency calculations
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def _q10(value: Decimal) -> Decimal:
    """Quantize to 10dp; alias for decimal_utils.as_10dp for engine-internal use."""
    return as_10dp(value)


# Config bucket names (from waterfall_profiles) -> Loan balance attribute name.
# interest_arrears_balance in config is "interest_arrears" on Loan.
CONFIG_BUCKET_TO_ENGINE_ATTR: Dict[str, str] = {
    "fees_charges_balance": "fees_charges_balance",
    "penalty_interest_balance": "penalty_interest_balance",
    "default_interest_balance": "default_interest_balance",
    "interest_arrears_balance": "interest_arrears",
    "interest_accrued_balance": "interest_accrued_balance",
    "principal_arrears": "principal_arrears",
    "principal_not_due": "principal_not_due",
}

@dataclass
class LoanConfig:
    """
    Configuration parameters for a loan.

    All rates are expressed as monthly decimals (e.g. 0.10 = 10% per month).
    Waterfall order comes from configuration (system/product); pass waterfall_bucket_order.
    """

    regular_rate_per_month: Decimal
    default_interest_absolute_rate_per_month: Decimal
    penalty_interest_absolute_rate_per_month: Decimal

    grace_period_days: int = 0
    penalty_on_principal_arrears_only: bool = True
    # Ordered list of bucket names from config (waterfall_profiles). Required; no hardcoded fallback.
    waterfall_bucket_order: Optional[List[str]] = None

    # Interest calculation method:
    # - flat = always on original_principal
    # - reducing_balance = on outstanding principal (principal_not_due + principal_arrears)
    flat_interest: bool = False


@dataclass
class ScheduleEntry:
    """
    Represents a single scheduled instalment.

    period_start: first calendar day of this interest period (disbursement or previous due date).
    due_date: instalment due date — first day of the *next* period; regular interest accrues only
    on days ``period_start <= d < due_date``. Daily amount is ``interest_component / (due_date - period_start).days``.
    """

    period_start: date
    due_date: date
    principal_component: Decimal
    interest_component: Decimal


@dataclass
class Loan:
    """
    Daily loan state: balance buckets, accrual, due-date transitions, payment allocation.
    """

    loan_id: str
    disbursement_date: date
    original_principal: Decimal
    config: LoanConfig
    schedule: List[ScheduleEntry]

    # Currency metadata
    currency: str = "USD"
    base_currency: str = "ZWL"
    fx_rate_at_disbursement: Decimal = Decimal("1")
    fx_source: str = "RBZ"
    fx_rate_history: Dict[date, Decimal] = field(default_factory=dict)

    # Balance buckets
    principal_not_due: Decimal = field(init=False)
    principal_arrears: Decimal = field(default=Decimal("0"))

    interest_accrued_balance: Decimal = field(default=Decimal("0"))
    interest_arrears: Decimal = field(default=Decimal("0"))

    default_interest_balance: Decimal = field(default=Decimal("0"))
    penalty_interest_balance: Decimal = field(default=Decimal("0"))
    fees_charges_balance: Decimal = field(default=Decimal("0"))

    interest_accrued_table: Dict[date, Decimal] = field(default_factory=dict)

    last_regular_interest_daily: Decimal = field(default=Decimal("0"))
    last_default_interest_daily: Decimal = field(default=Decimal("0"))
    last_penalty_interest_daily: Decimal = field(default=Decimal("0"))

    current_date: Optional[date] = None
    days_overdue: int = 0

    # Period-to-date sums (current schedule period) for efficient statements
    current_period_start: date = field(default_factory=lambda: date(9999, 12, 31), init=False)
    regular_interest_period_to_date: Decimal = field(default=Decimal("0"), init=False)
    penalty_interest_period_to_date: Decimal = field(default=Decimal("0"), init=False)
    default_interest_period_to_date: Decimal = field(default=Decimal("0"), init=False)

    def __post_init__(self) -> None:
        self.original_principal = _q10(self.original_principal)
        self.principal_not_due = self.original_principal
        self.schedule.sort(key=lambda s: s.due_date)
        self.fx_rate_history[self.disbursement_date] = self.fx_rate_at_disbursement
        self.current_period_start = self.disbursement_date

    def process_day(self, current_date: date) -> None:
        """
        Run all daily processes for a given calendar date:
        regular interest accrual, due-date transitions, default and penalty interest.
        """
        self.current_date = current_date

        self.last_regular_interest_daily = Decimal("0")
        self.last_default_interest_daily = Decimal("0")
        self.last_penalty_interest_daily = Decimal("0")

        scheduled_interest_today = self._scheduled_interest_for_day(current_date)
        self.last_regular_interest_daily = _q10(scheduled_interest_today)
        if scheduled_interest_today > 0:
            self.interest_accrued_balance = _q10(
                self.interest_accrued_balance + scheduled_interest_today
            )

        self.interest_accrued_table[current_date] = self.interest_accrued_balance
        due_entries_today = [s for s in self.schedule if s.due_date == current_date]
        self._apply_due_date_transitions(current_date)

        if self.principal_arrears > 0 or self.interest_arrears > 0:
            self.days_overdue += 1
        else:
            self.days_overdue = 0

        if self.days_overdue > self.config.grace_period_days:
            self._accrue_default_and_penalty_interest()

        # Period-to-date: accumulate regular_interest_daily, default_interest_daily, penalty_interest_daily
        # up to and including due date; restart accumulating on day after due date.
        # Use 10dp (not 2dp) so sum of daily accruals matches schedule total (e.g. Oct 31 days = 876.53).
        yesterday = current_date - timedelta(days=1)
        due_entries_yesterday = [s for s in self.schedule if s.due_date == yesterday]
        if due_entries_yesterday:
            # Day after due date: reset and start new period with today's daily
            self.current_period_start = current_date
            self.regular_interest_period_to_date = _q10(self.last_regular_interest_daily)
            self.penalty_interest_period_to_date = _q10(self.last_penalty_interest_daily)
            self.default_interest_period_to_date = _q10(self.last_default_interest_daily)
        else:
            # Accumulate today's daily (includes due date; reset happens day after)
            self.regular_interest_period_to_date = _q10(
                self.regular_interest_period_to_date + self.last_regular_interest_daily
            )
            self.penalty_interest_period_to_date = _q10(
                self.penalty_interest_period_to_date + self.last_penalty_interest_daily
            )
            self.default_interest_period_to_date = _q10(
                self.default_interest_period_to_date + self.last_default_interest_daily
            )

    def record_fx_rate(self, on_date: date, fx_rate: Decimal) -> None:
        if on_date in self.fx_rate_history:
            raise ValueError(
                f"An FX rate for {on_date} is already recorded. "
                "Use accounting reversals to correct historical postings."
            )
        self.fx_rate_history[on_date] = fx_rate

    def get_fx_rate_for_date(self, on_date: date) -> Decimal:
        if on_date in self.fx_rate_history:
            return self.fx_rate_history[on_date]
        prior_dates = [d for d in self.fx_rate_history.keys() if d < on_date]
        if prior_dates:
            return self.fx_rate_history[max(prior_dates)]
        return self.fx_rate_at_disbursement

    @property
    def outstanding_principal(self) -> Decimal:
        return _q10(self.principal_not_due + self.principal_arrears)

    def outstanding_principal_base(self, on_date: Optional[date] = None) -> Decimal:
        if on_date is None:
            on_date = self.current_date or self.disbursement_date
        return _q10(self.outstanding_principal * self.get_fx_rate_for_date(on_date))

    def _scheduled_interest_for_day(self, d: date) -> Decimal:
        """
        Daily regular interest for date ``d`` from the saved schedule.

        Accrual window: ``period_start <= d < due_date`` (disbursement and each due date start a
        period; accrue through the day before the next due). If no period covers ``d``, return 0.
        """
        entry = self._find_schedule_entry_for_day(d)
        if entry is None:
            return Decimal("0")
        total_days = (entry.due_date - entry.period_start).days
        if total_days <= 0:
            return Decimal("0")
        return entry.interest_component / Decimal(total_days)

    def _find_schedule_entry_for_day(self, d: date) -> Optional[ScheduleEntry]:
        """Return the schedule entry whose accrual window ``period_start <= d < due_date`` contains ``d``."""
        for entry in self.schedule:
            if entry.period_start <= d < entry.due_date:
                return entry
        return None

    def _apply_due_date_transitions(self, d: date) -> None:
        """
        Move principal not-due into arrears per schedule line. For regular interest, bill the
        full period amount T (schedule ``interest_component`` = sum of daily accruals for
        ``period_start <= day < due_date``) out of ``interest_accrued_balance`` and into
        ``interest_arrears``. Accrued cannot go negative; T still bills to arrears if accrued
        was reduced earlier (e.g. mid-period allocation to accrued).
        """
        due_entries = [s for s in self.schedule if s.due_date == d]
        for entry in due_entries:
            principal_to_move = min(self.principal_not_due, entry.principal_component)
            self.principal_not_due = _q10(self.principal_not_due - principal_to_move)
            self.principal_arrears = _q10(self.principal_arrears + principal_to_move)

            T = _q10(entry.interest_component)
            self.interest_accrued_balance = _q10(
                max(Decimal("0"), self.interest_accrued_balance - T)
            )
            self.interest_arrears = _q10(self.interest_arrears + T)

    def _accrue_default_and_penalty_interest(self) -> None:
        # Daily default/penalty = balance * (rate_per_month as decimal) / 30.
        # E.g. interest_arrears 987.05 at 2% per month: 987.05 * 0.02 / 30 = 0.66 per day.
        if (
            self.interest_arrears > 0
            and self.config.default_interest_absolute_rate_per_month > 0
        ):
            default_today = (
                self.interest_arrears
                * self.config.default_interest_absolute_rate_per_month
                / Decimal("30")
            )
            self.last_default_interest_daily = _q10(default_today)
            self.default_interest_balance = _q10(
                self.default_interest_balance + default_today
            )

        if self.config.penalty_interest_absolute_rate_per_month > 0:
            if self.config.penalty_on_principal_arrears_only:
                basis = self.principal_arrears
            else:
                basis = self.principal_arrears + self.principal_not_due

            if basis > 0:
                # E.g. principal_arrears 494.17 at 2% per month: 494.17 * 0.02 / 30 = 0.33 per day.
                penalty_today = (
                    basis
                    * self.config.penalty_interest_absolute_rate_per_month
                    / Decimal("30")
                )
                self.last_penalty_interest_daily = _q10(penalty_today)
                self.penalty_interest_balance = _q10(
                    self.penalty_interest_balance + penalty_today
                )

    def add_fee(self, amount: Decimal) -> None:
        self.fees_charges_balance = _q10(
            self.fees_charges_balance + _q10(amount)
        )

    def process_payment(self, amount: Decimal) -> Dict[str, Decimal]:
        """
        Allocate a payment across buckets using config-driven waterfall order.
        Returns dict of (engine attribute name -> amount applied).
        Waterfall order must be provided via config; there is no hardcoded default.

        Note: In the current flow, receipt allocation is done in loan_management
        (allocate_repayment_waterfall / compute_waterfall_allocation), not here.
        EOD only uses process_day (accrual); it subtracts pre-stored allocations
        from engine state. This method is for consistency and any future use
        (e.g. in-memory what-if). EOD passes the same normalized bucket order
        as loan_management so behaviour would match if process_payment were used.
        """
        order = self.config.waterfall_bucket_order
        if not order:
            raise ValueError(
                "Waterfall bucket order is not configured. "
                "Please set waterfall_profiles (e.g. 'standard') in System configuration "
                "and ensure payment_waterfall is set, then retry."
            )
        remaining = _q10(amount)
        allocations: Dict[str, Decimal] = {}

        for config_bucket in order:
            if remaining <= 0:
                break
            attr = CONFIG_BUCKET_TO_ENGINE_ATTR.get(config_bucket)
            if not attr or not hasattr(self, attr):
                continue
            current_balance = getattr(self, attr)
            if current_balance <= 0:
                continue
            pay_amount = min(current_balance, remaining)
            setattr(self, attr, _q10(current_balance - pay_amount))
            allocations[attr] = allocations.get(attr, Decimal("0")) + pay_amount
            remaining = _q10(remaining - pay_amount)

        return allocations


__all__ = [
    "LoanConfig",
    "ScheduleEntry",
    "Loan",
    "CONFIG_BUCKET_TO_ENGINE_ATTR",
]
