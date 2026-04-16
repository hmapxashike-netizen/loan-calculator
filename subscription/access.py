"""Streamlit subscription access: snapshot refresh, menu filtering, and page guards."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

import streamlit as st

from subscription import repository as sub_repo
from db.tenant_registry import get_stored_tenant_schema
from subscription.subscription_utils import SubscriptionBand, subscription_band

RESTRICTED_NAV_SECTIONS = frozenset({"Portfolio reports", "Subscription"})

BASIC_TIER_EXCLUDED_SECTIONS = frozenset(
    {
        "Notifications",
        "Document Management",
        "Portfolio reports",
    }
)


@dataclass(frozen=True)
class SubscriptionAccessSnapshot:
    band: SubscriptionBand
    tier_name: str
    days_overdue: int
    period_start: date | None
    period_end: date | None
    frozen_effective_date: date | None
    restricted_nav: bool
    terminated: bool
    basic_tier: bool
    message: str | None
    enforcement_skipped: bool
    grace_access_active: bool = False


def _today() -> date:
    return date.today()


def refresh_subscription_access_snapshot(user: dict[str, Any] | None) -> SubscriptionAccessSnapshot:
    """
    Load tenant subscription row, compute band, persist ``subscription_access_snapshot``,
    auto-set ``access_terminated_at`` when entering 90+ day delinquency.
    """
    role = (user or {}).get("role") or ""
    if role in ("VENDOR", "SUPERADMIN"):
        # Platform operators manage billing across tenants; do not block them on tenant delinquency.
        snap = SubscriptionAccessSnapshot(
            band=SubscriptionBand.CURRENT,
            tier_name="Premium",
            days_overdue=0,
            period_start=None,
            period_end=None,
            frozen_effective_date=None,
            restricted_nav=False,
            terminated=False,
            basic_tier=False,
            message=None,
            enforcement_skipped=True,
            grace_access_active=False,
        )
        st.session_state["subscription_access_snapshot"] = snap
        st.session_state.pop("subscription_frozen_effective_date", None)
        return snap

    if role == "BORROWER":
        snap = SubscriptionAccessSnapshot(
            band=SubscriptionBand.CURRENT,
            tier_name="Premium",
            days_overdue=0,
            period_start=None,
            period_end=None,
            frozen_effective_date=None,
            restricted_nav=False,
            terminated=False,
            basic_tier=False,
            message=None,
            enforcement_skipped=True,
            grace_access_active=False,
        )
        st.session_state["subscription_access_snapshot"] = snap
        return snap

    tenant_schema = get_stored_tenant_schema()
    if not tenant_schema:
        snap = SubscriptionAccessSnapshot(
            band=SubscriptionBand.CURRENT,
            tier_name="Premium",
            days_overdue=0,
            period_start=None,
            period_end=None,
            frozen_effective_date=None,
            restricted_nav=False,
            terminated=False,
            basic_tier=False,
            message="No tenant context; subscription enforcement skipped.",
            enforcement_skipped=True,
            grace_access_active=False,
        )
        st.session_state["subscription_access_snapshot"] = snap
        return snap

    try:
        row = sub_repo.get_tenant_subscription_row(tenant_schema)
    except Exception as e:
        snap = SubscriptionAccessSnapshot(
            band=SubscriptionBand.CURRENT,
            tier_name="Premium",
            days_overdue=0,
            period_start=None,
            period_end=None,
            frozen_effective_date=None,
            restricted_nav=False,
            terminated=False,
            basic_tier=False,
            message=f"Subscription load failed ({e}); enforcement skipped.",
            enforcement_skipped=True,
            grace_access_active=False,
        )
        st.session_state["subscription_access_snapshot"] = snap
        return snap

    if not row:
        snap = SubscriptionAccessSnapshot(
            band=SubscriptionBand.CURRENT,
            tier_name="Basic",
            days_overdue=0,
            period_start=None,
            period_end=None,
            frozen_effective_date=None,
            restricted_nav=False,
            terminated=False,
            basic_tier=True,
            message=None,
            enforcement_skipped=False,
            grace_access_active=False,
        )
        st.session_state["subscription_access_snapshot"] = snap
        return snap

    ps, pe, term = sub_repo.row_dates(row)
    tier_name = str(row.get("tier_name") or "Basic")
    today = _today()
    grace_until = sub_repo.grace_access_until_date(row)
    # Inclusive end date: full access for enforcement through grace_until.
    grace_active = grace_until is not None and today <= grace_until

    band, d_over, frozen = subscription_band(today=today, due_date=pe, access_terminated_at=term)

    if not grace_active and band == SubscriptionBand.TERMINATED and term is None and d_over > 90:
        try:
            sub_repo.set_tenant_access_terminated(tenant_schema, terminated=True)
        except Exception:
            pass

    restricted = band == SubscriptionBand.RESTRICTED_NAV
    terminated = band == SubscriptionBand.TERMINATED
    basic_tier = tier_name.strip().lower() == "basic"

    msg: str | None = None
    if band == SubscriptionBand.WARNING:
        msg = f"Subscription payment is {d_over} day(s) overdue. Please settle to avoid restricted access."
    elif band == SubscriptionBand.VIEW_ONLY_FROZEN:
        msg = (
            f"Subscription is {d_over} days overdue. The app is in view-only mode; "
            f"business date is capped to {frozen}."
        )
    elif band == SubscriptionBand.RESTRICTED_NAV:
        msg = (
            f"Subscription is severely overdue ({d_over} days). Only Reports and Subscription are available."
        )
    elif band == SubscriptionBand.TERMINATED:
        msg = "Subscription access has been terminated. Contact your administrator."

    if grace_active:
        band = SubscriptionBand.CURRENT
        restricted = False
        terminated = False
        frozen = None
        msg = f"Grace access until {grace_until.isoformat()} (inclusive). Full access applies; settle subscription as agreed."

    snap = SubscriptionAccessSnapshot(
        band=band,
        tier_name=tier_name,
        days_overdue=d_over,
        period_start=ps,
        period_end=pe,
        frozen_effective_date=frozen,
        restricted_nav=restricted,
        terminated=terminated,
        basic_tier=basic_tier,
        message=msg,
        enforcement_skipped=False,
        grace_access_active=grace_active,
    )
    st.session_state["subscription_access_snapshot"] = snap

    if band == SubscriptionBand.VIEW_ONLY_FROZEN and frozen is not None:
        st.session_state["subscription_frozen_effective_date"] = frozen
    else:
        st.session_state.pop("subscription_frozen_effective_date", None)

    return snap


def get_subscription_snapshot() -> SubscriptionAccessSnapshot | None:
    raw = st.session_state.get("subscription_access_snapshot")
    return raw if isinstance(raw, SubscriptionAccessSnapshot) else None


def filter_menu_for_subscription(
    menu: dict[str, Callable],
    snapshot: SubscriptionAccessSnapshot | None,
    *,
    role: str,
) -> dict[str, Callable]:
    """Apply restricted-nav and Basic-tier exclusions to sidebar menu."""
    if role == "BORROWER" or snapshot is None or snapshot.enforcement_skipped:
        return menu
    if role not in (
        "ADMIN",
        "LOAN_OFFICER",
        "LOAN_SUPERVISOR",
        "SUPERADMIN",
        "ACCOUNTS_OFFICER",
        "ACCOUNTS_SUPERVISOR",
        "VIEWER",
    ):
        return menu

    out = dict(menu)
    if snapshot.restricted_nav:
        allowed = RESTRICTED_NAV_SECTIONS
        out = {k: v for k, v in menu.items() if k in allowed}
        return out

    if snapshot.basic_tier:
        for name in BASIC_TIER_EXCLUDED_SECTIONS:
            out.pop(name, None)
    return out


def render_subscription_banners(snapshot: SubscriptionAccessSnapshot | None) -> None:
    if snapshot is None or snapshot.enforcement_skipped or not snapshot.message:
        return
    if snapshot.grace_access_active:
        st.info(snapshot.message)
        return
    if snapshot.band == SubscriptionBand.WARNING:
        st.warning(snapshot.message)
    elif snapshot.band in (
        SubscriptionBand.VIEW_ONLY_FROZEN,
        SubscriptionBand.RESTRICTED_NAV,
    ):
        st.info(snapshot.message)
    elif snapshot.band == SubscriptionBand.TERMINATED:
        st.error(snapshot.message)


def check_access(
    *,
    nav_section: str,
    snapshot: SubscriptionAccessSnapshot | None = None,
) -> bool:
    """
    Guard a top-level loan app section. Returns True if rendering may proceed.
    If restricted/terminated violates policy, shows message and stops.
    """
    snap = snapshot or get_subscription_snapshot()
    if snap is None or snap.enforcement_skipped:
        return True
    if snap.terminated:
        st.error("Your organisation's subscription has been terminated.")
        st.stop()
    if snap.restricted_nav and nav_section not in RESTRICTED_NAV_SECTIONS:
        st.error("This section is not available while subscription is in restricted status.")
        st.stop()
    return True


def premium_bank_reconciliation_enabled() -> bool:
    snap = get_subscription_snapshot()
    if snap is None or snap.enforcement_skipped:
        return True
    return not snap.basic_tier


def basic_tier_hide_loan_capture() -> bool:
    snap = get_subscription_snapshot()
    if snap is None or snap.enforcement_skipped:
        return False
    return snap.basic_tier
