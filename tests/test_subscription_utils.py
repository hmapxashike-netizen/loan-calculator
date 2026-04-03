"""Pure tests for subscription billing period math."""

from datetime import date

from subscription.subscription_utils import extend_period, get_billing_end_date, next_period_start, subscription_band


def test_monthly_april_3_ends_april_30():
    assert get_billing_end_date(date(2026, 4, 3), "Monthly") == date(2026, 4, 30)


def test_quarterly_jan_15_ends_mar_31():
    assert get_billing_end_date(date(2026, 1, 15), "Quarterly") == date(2026, 3, 31)


def test_next_period_start():
    assert next_period_start(date(2026, 3, 31)) == date(2026, 4, 1)


def test_extend_period():
    end = get_billing_end_date(date(2026, 1, 10), "Monthly")
    nxt = extend_period(end, "Monthly")
    assert nxt == get_billing_end_date(next_period_start(end), "Monthly")


def test_subscription_band_warning():
    band, d, _ = subscription_band(today=date(2026, 5, 5), due_date=date(2026, 5, 1), access_terminated_at=None)
    assert d == 4
    from subscription.subscription_utils import SubscriptionBand

    assert band == SubscriptionBand.WARNING


def test_subscription_band_view_only_frozen_has_cap():
    from subscription.subscription_utils import SubscriptionBand

    band, d, frozen = subscription_band(
        today=date(2026, 5, 20), due_date=date(2026, 5, 1), access_terminated_at=None
    )
    assert band == SubscriptionBand.VIEW_ONLY_FROZEN
    assert frozen == date(2026, 5, 8)
