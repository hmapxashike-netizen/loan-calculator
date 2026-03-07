from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from enum import Enum
import calendar
from typing import Dict, List, Optional


# Configure decimal for currency calculations
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


MONEY_QUANT = Decimal("0.01")


def as_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANT)


class WaterfallType(str, Enum):
    STANDARD = "standard"  # Fees → Penalty → Default → Int Arrears → Prin Arrears → Prin Not Due
    BORROWER_FRIENDLY = "borrower_friendly"  # Reverse of standard


@dataclass
class LoanConfig:
    """
    Configuration parameters for a loan.

    All rates are expressed as monthly decimals (e.g. 0.10 = 10% per month).

    Absolute rates already include any margin over the regular rate, i.e.:
      - default absolute rate = regular rate + default margin
      - penalty absolute rate = regular rate + penalty margin
    """

    regular_rate_per_month: Decimal
    default_interest_absolute_rate_per_month: Decimal
    penalty_interest_absolute_rate_per_month: Decimal

    grace_period_days: int = 0
    penalty_on_principal_arrears_only: bool = True
    waterfall_type: WaterfallType = WaterfallType.STANDARD

    # Interest calculation method:
    # - flat = always on original_principal
    # - reducing_balance = on outstanding principal (principal_not_due + principal_arrears)
    flat_interest: bool = False


@dataclass
class ScheduleEntry:
    """
    Represents a single scheduled instalment.

    period_start: first day the interest for this instalment starts accruing
    due_date: instalment due date (exclusive upper bound of accrual period)
    """

    period_start: date
    due_date: date
    principal_component: Decimal
    interest_component: Decimal


@dataclass
class Loan:
    """
    Core loan engine tracking all balance "buckets".
    """

    loan_id: str
    disbursement_date: date
    original_principal: Decimal
    config: LoanConfig
    schedule: List[ScheduleEntry]

    # Currency metadata
    currency: str = "USD"  # Transaction currency, e.g. USD
    base_currency: str = "ZWL"  # Reporting / regulatory currency, e.g. ZWL
    fx_rate_at_disbursement: Decimal = Decimal("1")  # transaction -> base at disbursement
    fx_source: str = "RBZ"  # e.g. RBZ official, market

    # FX rate history (e.g. daily or reporting-date snapshots)
    fx_rate_history: Dict[date, Decimal] = field(default_factory=dict)

    # Balance buckets
    principal_not_due: Decimal = field(init=False)
    principal_arrears: Decimal = field(default=Decimal("0"))

    interest_accrued_balance: Decimal = field(default=Decimal("0"))
    interest_arrears: Decimal = field(default=Decimal("0"))

    default_interest_balance: Decimal = field(default=Decimal("0"))
    penalty_interest_balance: Decimal = field(default=Decimal("0"))
    fees_charges_balance: Decimal = field(default=Decimal("0"))

    # Daily snapshots for interest_accrued_balance (audit trail)
    interest_accrued_table: Dict[date, Decimal] = field(default_factory=dict)

    # Last daily interest computations (for reporting / Excel)
    last_regular_interest_daily: Decimal = field(default=Decimal("0"))
    last_default_interest_daily: Decimal = field(default=Decimal("0"))
    last_penalty_interest_daily: Decimal = field(default=Decimal("0"))

    # Derived / tracking fields
    current_date: Optional[date] = None
    days_overdue: int = 0

    def __post_init__(self) -> None:
        self.original_principal = as_money(self.original_principal)
        self.principal_not_due = self.original_principal
        # Ensure schedule is sorted
        self.schedule.sort(key=lambda s: s.due_date)

        # Record initial FX rate snapshot
        self.fx_rate_history[self.disbursement_date] = self.fx_rate_at_disbursement

    # ---------------------------
    # Core daily processing
    # ---------------------------

    def process_day(self, current_date: date) -> None:
        """
        Run all daily processes for a given calendar date:
        - regular interest accrual (matching principle, with mid‑month handling)
        - due-date state transitions for principal and interest
        - default and penalty interest after grace period
        """
        self.current_date = current_date

        # Reset daily interest trackers
        self.last_regular_interest_daily = Decimal("0")
        self.last_default_interest_daily = Decimal("0")
        self.last_penalty_interest_daily = Decimal("0")

        # 1. Regular interest accrual into interest_accrued_balance
        scheduled_interest_today = self._scheduled_interest_for_day(current_date)
        self.last_regular_interest_daily = scheduled_interest_today
        if scheduled_interest_today > 0:
            self.interest_accrued_balance = as_money(
                self.interest_accrued_balance + scheduled_interest_today
            )

        # Snapshot accrued balance for audit
        self.interest_accrued_table[current_date] = self.interest_accrued_balance

        # 2. On any due date, move principal to arrears and accrued interest to interest arrears
        self._apply_due_date_transitions(current_date)

        # 3. Update days_overdue counter
        if self.principal_arrears > 0 or self.interest_arrears > 0:
            self.days_overdue += 1
        else:
            self.days_overdue = 0

        # 4. Default and penalty interest (only after grace period)
        if self.days_overdue > self.config.grace_period_days:
            self._accrue_default_and_penalty_interest()

    # ---------------------------
    # FX rate handling / exposures
    # ---------------------------

    def record_fx_rate(self, on_date: date, fx_rate: Decimal) -> None:
        """
        Store an FX rate (transaction -> base) for a given calendar date.
        Once written for a date, we do not overwrite it to preserve the audit trail;
        callers should correct via higher-level reversing logic if needed.
        """
        if on_date in self.fx_rate_history:
            # Do not silently overwrite historical FX; caller must decide how to correct.
            raise ValueError(
                f"An FX rate for {on_date} is already recorded. "
                f"Use accounting reversals to correct historical postings."
            )
        self.fx_rate_history[on_date] = fx_rate

    def get_fx_rate_for_date(self, on_date: date) -> Decimal:
        """
        Retrieve the FX rate to use for a given date.
        Uses the rate recorded for that exact date, or the latest prior date;
        falls back to the disbursement FX rate if none found.
        """
        if on_date in self.fx_rate_history:
            return self.fx_rate_history[on_date]

        # Find the latest rate before on_date
        prior_dates = [d for d in self.fx_rate_history.keys() if d < on_date]
        if prior_dates:
            latest = max(prior_dates)
            return self.fx_rate_history[latest]

        return self.fx_rate_at_disbursement

    @property
    def outstanding_principal(self) -> Decimal:
        """
        Total principal exposure in transaction currency.
        """
        return as_money(self.principal_not_due + self.principal_arrears)

    def outstanding_principal_base(self, on_date: Optional[date] = None) -> Decimal:
        """
        Principal exposure in base currency for the given date (or current date).
        """
        if on_date is None:
            if self.current_date is None:
                on_date = self.disbursement_date
            else:
                on_date = self.current_date
        rate = self.get_fx_rate_for_date(on_date)
        return as_money(self.outstanding_principal * rate)

    # ---------------------------
    # Interest accrual (matching principle)
    # ---------------------------

    def _scheduled_interest_for_day(self, d: date) -> Decimal:
        """
        Compute the regular interest accrual for calendar day d.

        Primary behaviour (schedule-matching):
        - Find the schedule entry whose accrual period covers d: [period_start, due_date)
        - Daily accrual = schedule interest for that period / number of days in the period.
        - Same amount every day in the period; sum over period equals schedule interest.

        Fallback behaviour (after last scheduled period):
        - If there is outstanding principal but no schedule entry covering d,
          accrue interest on the total outstanding principal using the regular
          monthly rate, spread evenly across the calendar month.
        """
        entry = self._find_schedule_entry_for_day(d)
        if entry is not None:
            total_days = (entry.due_date - entry.period_start).days
            if total_days <= 0:
                return Decimal("0")
            # Flat daily accrual: schedule interest / days in scheduled period
            return entry.interest_component / Decimal(total_days)

        # Fallback: after last scheduled period, accrue interest on principal
        if self.config.flat_interest:
            basis = self.original_principal
        else:
            basis = self.principal_not_due + self.principal_arrears

        if basis <= 0:
            return Decimal("0")

        days_in_calendar_month = calendar.monthrange(d.year, d.month)[1]
        daily_rate = self.config.regular_rate_per_month / Decimal(days_in_calendar_month)
        return basis * daily_rate

    def _find_schedule_entry_for_day(self, d: date) -> Optional[ScheduleEntry]:
        for entry in self.schedule:
            if entry.period_start <= d < entry.due_date:
                return entry
        return None

    # ---------------------------
    # Due date transitions
    # ---------------------------

    def _apply_due_date_transitions(self, d: date) -> None:
        """
        On a due date:
        - Move scheduled principal from principal_not_due to principal_arrears.
        - Post scheduled interest into interest_arrears (full schedule amount so arrears
          match the schedule); reduce accrued only by what was actually accrued to avoid
          double-counting. Daily rounding can make sum(accruals) < entry.interest_component.
        """
        due_entries = [s for s in self.schedule if s.due_date == d]
        for entry in due_entries:
            # Principal: move from not-due to arrears
            principal_to_move = min(self.principal_not_due, entry.principal_component)
            self.principal_not_due = as_money(self.principal_not_due - principal_to_move)
            self.principal_arrears = as_money(self.principal_arrears + principal_to_move)

            # Interest: post full scheduled interest to arrears (schedule is source of truth).
            # Remove from accrued only what we actually accrued for this period to avoid
            # double-counting; rounding can make accrued < entry.interest_component.
            amount_from_accrued = min(
                self.interest_accrued_balance, entry.interest_component
            )
            self.interest_accrued_balance = as_money(
                self.interest_accrued_balance - amount_from_accrued
            )
            self.interest_arrears = as_money(
                self.interest_arrears + entry.interest_component
            )

    # ---------------------------
    # Default & penalty interest
    # ---------------------------

    def _accrue_default_and_penalty_interest(self) -> None:
        # Default interest on interest arrears
        if (
            self.interest_arrears > 0
            and self.config.default_interest_absolute_rate_per_month > 0
        ):
            default_today = (
                self.interest_arrears
                * self.config.default_interest_absolute_rate_per_month
                / Decimal("30")
            )
            self.last_default_interest_daily = default_today
            self.default_interest_balance = as_money(
                self.default_interest_balance + default_today
            )

        # Penalty interest on principal
        if self.config.penalty_interest_absolute_rate_per_month > 0:
            if self.config.penalty_on_principal_arrears_only:
                basis = self.principal_arrears
            else:
                basis = self.principal_arrears + self.principal_not_due

            if basis > 0:
                penalty_today = (
                    basis
                    * self.config.penalty_interest_absolute_rate_per_month
                    / Decimal("30")
                )
                self.last_penalty_interest_daily = penalty_today
                self.penalty_interest_balance = as_money(
                    self.penalty_interest_balance + penalty_today
                )

    # ---------------------------
    # Fees & charges
    # ---------------------------

    def add_fee(self, amount: Decimal) -> None:
        self.fees_charges_balance = as_money(
            self.fees_charges_balance + as_money(amount)
        )

    # ---------------------------
    # Payment allocation (waterfall)
    # ---------------------------

    def process_payment(self, amount: Decimal) -> Dict[str, Decimal]:
        """
        Allocate a payment amount across the configured waterfall.
        Returns a dict detailing how much was applied to each bucket.
        """
        remaining = as_money(amount)
        allocations: Dict[str, Decimal] = {
            "fees_charges": Decimal("0"),
            "penalty_interest": Decimal("0"),
            "default_interest": Decimal("0"),
            "interest_arrears": Decimal("0"),
            "principal_arrears": Decimal("0"),
            "principal_not_due": Decimal("0"),
        }

        def apply(bucket_name: str) -> None:
            nonlocal remaining
            if remaining <= 0:
                return

            balance_attr = {
                "fees_charges": "fees_charges_balance",
                "penalty_interest": "penalty_interest_balance",
                "default_interest": "default_interest_balance",
                "interest_arrears": "interest_arrears",
                "principal_arrears": "principal_arrears",
                "principal_not_due": "principal_not_due",
            }[bucket_name]

            current_balance = getattr(self, balance_attr)
            if current_balance <= 0:
                return

            pay_amount = min(current_balance, remaining)
            setattr(self, balance_attr, as_money(current_balance - pay_amount))
            allocations[bucket_name] = as_money(allocations[bucket_name] + pay_amount)
            remaining = as_money(remaining - pay_amount)

        if self.config.waterfall_type == WaterfallType.STANDARD:
            order = [
                "fees_charges",
                "penalty_interest",
                "default_interest",
                "interest_arrears",
                "principal_arrears",
                "principal_not_due",
            ]
        else:
            order = [
                "principal_not_due",
                "principal_arrears",
                "interest_arrears",
                "default_interest",
                "penalty_interest",
                "fees_charges",
            ]

        for bucket in order:
            apply(bucket)

        return allocations


__all__ = [
    "LoanConfig",
    "ScheduleEntry",
    "Loan",
    "WaterfallType",
]

