#!/usr/bin/env python3
"""
Create N test customers and N loans: January 2025 disbursements, term 6–24 months,
penalty interest 10%, rates/fees from each product's product_config JSON.

Mix: ~90% consumer-loan products; remainder covers other loan types. Every active
product receives at least one loan when len(active_products) <= N.

Usage:
  python scripts/seed_bulk_test_portfolio.py --count 500
  python scripts/seed_bulk_test_portfolio.py --count 500 --dry-run
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import date, datetime, timedelta

import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from customers.core import create_individual  # noqa: E402
from loan_management.cash_gl import get_cached_source_cash_account_entries  # noqa: E402
from loan_management.product_catalog import get_product_config_from_db, list_products  # noqa: E402
from loan_management.product_catalog import load_system_config_from_db  # noqa: E402
from loan_management.save_loan import save_loan  # noqa: E402
from loans import (  # noqa: E402
    add_months,
    days_in_month,
    get_amortization_schedule,
    get_bullet_schedule,
    get_term_loan_amortization_schedule,
    repayment_dates,
    consumer_level_payment,
)
from utils.rates import pct_to_monthly  # noqa: E402

DISPLAY_BY_DB_LT = {
    "consumer_loan": "Consumer Loan",
    "term_loan": "Term Loan",
    "bullet_loan": "Bullet Loan",
    "customised_repayments": "Customised Repayments",
}


def _rate_basis(product_cfg: dict | None) -> str:
    gls = (product_cfg or {}).get("global_loan_settings") or {}
    rb = gls.get("rate_basis")
    return rb if rb in {"Per month", "Per annum"} else "Per annum"


def _flat_rate(product_cfg: dict | None) -> bool:
    gls = (product_cfg or {}).get("global_loan_settings") or {}
    return gls.get("interest_method") == "Flat rate"


def _default_currencies(sys_cfg: dict | None, loan_key: str, fallback: str = "USD") -> str:
    cfg = sys_cfg or {}
    loan_curr_cfg = cfg.get("loan_default_currencies", {}) or {}
    cc = loan_curr_cfg.get(loan_key, cfg.get("base_currency", fallback))
    accepted = cfg.get("accepted_currencies", [cfg.get("base_currency", fallback)])
    if cc not in accepted:
        return accepted[0] if accepted else fallback
    return cc


def compute_consumer_pair(
    *,
    loan_required: float,
    loan_term: int,
    disbursement_date: datetime,
    product_cfg: dict,
    use_anniversary: bool,
    scheme_label: str,
    sys_cfg: dict | None,
) -> tuple[dict, pd.DataFrame]:
    dr = (product_cfg.get("default_rates") or {}).get("consumer_loan") or {}
    ip = dr.get("interest_pct")
    af = dr.get("admin_fee_pct")
    if ip is None or af is None:
        raise ValueError("product_config.default_rates.consumer_loan incomplete")
    prb = _rate_basis(product_cfg)
    flat = _flat_rate(product_cfg)
    interest_pct_month = pct_to_monthly(float(ip), prb)
    if interest_pct_month is None:
        raise ValueError("invalid interest_pct for consumer_loan")
    # Match app.compute_consumer_schedule (internal basis is always per-month decimal).
    base_rate = float(interest_pct_month) / 100.0
    admin_fee = float(af) / 100.0
    total_facility = loan_required / (1.0 - admin_fee)
    amount_display = loan_required
    additional = float(sys_cfg.get("consumer_default_additional_rate_pct", 0.0) or 0.0) / 100.0
    total_monthly_rate = base_rate + additional
    monthly_installment = consumer_level_payment(total_facility, total_monthly_rate, int(loan_term))
    default_first = add_months(disbursement_date, 1).date()
    if not use_anniversary:
        default_first = default_first.replace(day=days_in_month(default_first.year, default_first.month))
    first_rep = datetime.combine(default_first, datetime.min.time())
    schedule_dates = repayment_dates(disbursement_date, first_rep, int(loan_term), use_anniversary)
    end_date = schedule_dates[-1] if schedule_dates else add_months(disbursement_date, loan_term)
    df_schedule = get_amortization_schedule(
        total_facility,
        total_monthly_rate,
        int(loan_term),
        disbursement_date,
        monthly_installment,
        flat_rate=flat,
        schedule_dates=schedule_dates,
    )
    details = {
        "principal": total_facility,
        "disbursed_amount": amount_display,
        "term": loan_term,
        "monthly_rate": total_monthly_rate,
        "admin_fee": admin_fee,
        "scheme": scheme_label,
        "disbursement_date": disbursement_date,
        "start_date": disbursement_date,
        "end_date": end_date,
        "first_repayment_date": first_rep,
        "installment": monthly_installment,
        "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
        "currency": _default_currencies(sys_cfg, "consumer_loan"),
    }
    return details, df_schedule


def compute_term_pair(
    *,
    loan_required: float,
    loan_term: int,
    disbursement_date: datetime,
    product_cfg: dict,
    use_anniversary: bool,
    sys_cfg: dict | None,
) -> tuple[dict, pd.DataFrame]:
    dr = (product_cfg.get("default_rates") or {}).get("term_loan") or {}
    for k in ("interest_pct", "drawdown_pct", "arrangement_pct"):
        if dr.get(k) is None:
            raise ValueError(f"product_config.default_rates.term_loan.{k} missing")
    rate_pct = float(dr["interest_pct"])
    dd_fee = float(dr["drawdown_pct"]) / 100.0
    arr_fee = float(dr["arrangement_pct"]) / 100.0
    prb = _rate_basis(product_cfg)
    flat = _flat_rate(product_cfg)
    total_fee = dd_fee + arr_fee
    amount_display = loan_required
    total_facility = loan_required / (1.0 - total_fee)
    annual_rate = (rate_pct / 100.0) * 12.0 if prb == "Per month" else (rate_pct / 100.0)
    default_first = add_months(disbursement_date, 1).date()
    if not use_anniversary:
        default_first = default_first.replace(day=days_in_month(default_first.year, default_first.month))
    first_rep = datetime.combine(default_first, datetime.min.time())
    schedule_dates = repayment_dates(disbursement_date, first_rep, int(loan_term), use_anniversary)
    grace_key = "none"
    df_schedule, installment = get_term_loan_amortization_schedule(
        total_facility,
        annual_rate,
        disbursement_date,
        schedule_dates,
        grace_key,
        0,
        flat_rate=flat,
    )
    end_date = schedule_dates[-1] if schedule_dates else disbursement_date
    details = {
        "principal": total_facility,
        "disbursed_amount": amount_display,
        "term": loan_term,
        "annual_rate": annual_rate,
        "drawdown_fee": dd_fee,
        "arrangement_fee": arr_fee,
        "disbursement_date": disbursement_date,
        "start_date": disbursement_date,
        "end_date": end_date,
        "first_repayment_date": first_rep,
        "installment": installment,
        "grace_type": "No Grace Period",
        "moratorium_months": 0,
        "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
        "currency": _default_currencies(sys_cfg, "term_loan"),
    }
    return details, df_schedule


def compute_bullet_pair(
    *,
    loan_required: float,
    loan_term: int,
    disbursement_date: datetime,
    product_cfg: dict,
    use_anniversary: bool,
    sys_cfg: dict | None,
) -> tuple[dict, pd.DataFrame]:
    dr = (product_cfg.get("default_rates") or {}).get("bullet_loan") or {}
    for k in ("interest_pct", "drawdown_pct", "arrangement_pct"):
        if dr.get(k) is None:
            raise ValueError(f"product_config.default_rates.bullet_loan.{k} missing")
    rate_pct = float(dr["interest_pct"])
    dd_fee = float(dr["drawdown_pct"]) / 100.0
    arr_fee = float(dr["arrangement_pct"]) / 100.0
    prb = _rate_basis(product_cfg)
    flat = _flat_rate(product_cfg)
    total_fee = dd_fee + arr_fee
    amount_display = loan_required
    total_facility = loan_required / (1.0 - total_fee)
    annual_rate = (rate_pct / 100.0) * 12.0 if prb == "Per month" else (rate_pct / 100.0)
    default_first = add_months(disbursement_date, 1).date()
    if not use_anniversary:
        default_first = default_first.replace(day=days_in_month(default_first.year, default_first.month))
    first_rep = datetime.combine(default_first, datetime.min.time())
    schedule_dates = repayment_dates(disbursement_date, first_rep, int(loan_term), use_anniversary)
    end_date = schedule_dates[-1] if schedule_dates else add_months(disbursement_date, loan_term)
    df_schedule = get_bullet_schedule(
        total_facility,
        annual_rate,
        disbursement_date,
        end_date,
        "straight",
        schedule_dates,
        flat_rate=flat,
    )
    total_payment = float(df_schedule["Payment"].sum())
    details = {
        "principal": total_facility,
        "disbursed_amount": amount_display,
        "term": loan_term,
        "annual_rate": annual_rate,
        "drawdown_fee": dd_fee,
        "arrangement_fee": arr_fee,
        "disbursement_date": disbursement_date,
        "start_date": disbursement_date,
        "end_date": end_date,
        "total_payment": total_payment,
        "bullet_type": "straight",
        "first_repayment_date": first_rep,
        "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
        "currency": _default_currencies(sys_cfg, "bullet_loan"),
    }
    return details, df_schedule


def compute_customised_pair(
    *,
    loan_required: float,
    loan_term: int,
    disbursement_date: datetime,
    product_cfg: dict,
    use_anniversary: bool,
    sys_cfg: dict | None,
) -> tuple[dict, pd.DataFrame]:
    """Same amortisation engine as term loan; product uses customised_repayments default_rates."""
    dr = (product_cfg.get("default_rates") or {}).get("customised_repayments") or {}
    for k in ("interest_pct", "drawdown_pct", "arrangement_pct"):
        if dr.get(k) is None:
            raise ValueError(f"product_config.default_rates.customised_repayments.{k} missing")
    rate_pct = float(dr["interest_pct"])
    dd_fee = float(dr["drawdown_pct"]) / 100.0
    arr_fee = float(dr["arrangement_pct"]) / 100.0
    prb = _rate_basis(product_cfg)
    flat = _flat_rate(product_cfg)
    total_fee = dd_fee + arr_fee
    amount_display = loan_required
    total_facility = loan_required / (1.0 - total_fee)
    annual_rate = (rate_pct / 100.0) * 12.0 if prb == "Per month" else (rate_pct / 100.0)
    default_first = add_months(disbursement_date, 1).date()
    if not use_anniversary:
        default_first = default_first.replace(day=days_in_month(default_first.year, default_first.month))
    first_rep = datetime.combine(default_first, datetime.min.time())
    schedule_dates = repayment_dates(disbursement_date, first_rep, int(loan_term), use_anniversary)
    grace_key = "none"
    df_schedule, installment = get_term_loan_amortization_schedule(
        total_facility,
        annual_rate,
        disbursement_date,
        schedule_dates,
        grace_key,
        0,
        flat_rate=flat,
    )
    end_date = schedule_dates[-1] if schedule_dates else disbursement_date
    total_payment = float(df_schedule["Payment"].sum())
    details = {
        "principal": total_facility,
        "disbursed_amount": amount_display,
        "term": loan_term,
        "annual_rate": annual_rate,
        "drawdown_fee": dd_fee,
        "arrangement_fee": arr_fee,
        "disbursement_date": disbursement_date,
        "start_date": disbursement_date,
        "end_date": end_date,
        "first_repayment_date": first_rep,
        "installment": installment,
        "grace_type": "No Grace Period",
        "moratorium_months": 0,
        "total_payment": total_payment,
        "payment_timing": "anniversary" if use_anniversary else "last_day_of_month",
        "currency": _default_currencies(sys_cfg, "customised_repayments"),
    }
    return details, df_schedule


def _jan_2025_random_datetime() -> datetime:
    d0 = date(2025, 1, 1)
    d1 = date(2025, 1, 31)
    span = (d1 - d0).days
    d = d0 + timedelta(days=random.randint(0, span))
    return datetime(d.year, d.month, d.day)


def _pick_product_assignments(products: list[dict], n: int, consumer_fraction: float) -> list[dict]:
    """
    Target ~consumer_fraction consumer loans; include every active product at least once when
    len(products) <= n (cannot cover all products when len(products) > n).
    """
    if not products:
        raise ValueError("No active products.")
    cons = [p for p in products if (p.get("loan_type") or "").strip().lower() == "consumer_loan"]
    non = [p for p in products if (p.get("loan_type") or "").strip().lower() != "consumer_loan"]
    n_cons_target = int(round(n * consumer_fraction))

    def _count_cons(rows: list[dict]) -> int:
        return sum(1 for x in rows if (x.get("loan_type") or "").strip().lower() == "consumer_loan")

    if len(products) > n:
        out: list[dict] = []
        while len(out) < n:
            rem = n - len(out)
            need_cons = n_cons_target - _count_cons(out)
            p_cons = max(0.0, min(1.0, need_cons / rem)) if rem else 0.0
            if random.random() < p_cons and cons:
                out.append(random.choice(cons))
            elif non:
                out.append(random.choice(non))
            else:
                out.append(random.choice(cons))
        random.shuffle(out)
        return out

    out = list(products)
    random.shuffle(out)
    while len(out) < n:
        rem = n - len(out)
        need_cons = n_cons_target - _count_cons(out)
        p_cons = max(0.0, min(1.0, need_cons / rem)) if rem else 0.0
        if random.random() < p_cons and cons:
            out.append(random.choice(cons))
        elif non:
            out.append(random.choice(non))
        else:
            out.append(random.choice(cons))
    random.shuffle(out)
    return out[:n]


def _apply_penalty_and_gl(
    details: dict,
    product_cfg: dict,
    *,
    penalty_pct: float,
    cash_gl_id: str | None,
) -> dict:
    d = dict(details)
    pq = product_cfg.get("penalty_interest_quotation") or "Per annum"
    prb = _rate_basis(product_cfg)
    pm = pct_to_monthly(float(penalty_pct), prb)
    d["penalty_rate_pct"] = float(pm if pm is not None else 0.0)
    d["penalty_quotation"] = pq
    if cash_gl_id:
        d["cash_gl_account_id"] = cash_gl_id
    return d


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed customers + loans from product defaults.")
    ap.add_argument("--count", type=int, default=500, help="Customers and loans to create.")
    ap.add_argument("--dry-run", action="store_true", help="Plan only; no DB writes.")
    ap.add_argument("--seed", type=int, default=None, help="RNG seed for repeatability.")
    args = ap.parse_args()
    n = max(1, min(args.count, 50_000))
    if args.seed is not None:
        random.seed(args.seed)

    sys_cfg = load_system_config_from_db() or {}
    products = list_products(active_only=True) or []
    if not products:
        print("No active products. Create products under System configurations → Products.", file=sys.stderr)
        sys.exit(2)

    cash_entries = get_cached_source_cash_account_entries() or []
    cash_gl = str(cash_entries[0]["id"]) if cash_entries else None
    if not cash_gl and not args.dry_run:
        print(
            "Source cash GL cache is empty. Build it under Accounting maintenance or this script may fail on save_loan.",
            file=sys.stderr,
        )

    assignments = _pick_product_assignments(products, n, consumer_fraction=0.90)
    cons_n = sum(1 for p in assignments if (p.get("loan_type") or "").strip().lower() == "consumer_loan")
    print(f"Planned: {n} loans — {cons_n} consumer ({100.0 * cons_n / n:.1f}%), {n - cons_n} non-consumer.")

    if args.dry_run:
        codes = [f"{p.get('code')} ({p.get('loan_type')})" for p in assignments[:15]]
        print("Sample products:", ", ".join(codes), "…" if n > 15 else "")
        return

    created_customers: list[int] = []
    created_loans: list[int] = []
    errors: list[str] = []

    for i in range(1, n + 1):
        pcode = (assignments[i - 1].get("code") or "").strip()
        lt_raw = (assignments[i - 1].get("loan_type") or "").strip().lower()
        label = DISPLAY_BY_DB_LT.get(lt_raw, "Term Loan")
        try:
            cid = create_individual(
                name=f"Bulk Test Client {i:04d}",
                national_id=f"BT{i:06d}",
                phone1=f"+2637{random.randint(10000000, 99999999)}",
                email1=f"bulk{i:04d}@example.test",
                migration_ref=f"BULK2025-{i:05d}",
            )
            created_customers.append(cid)
        except Exception as e:
            errors.append(f"customer {i}: {e}")
            continue

        cfg = get_product_config_from_db(pcode) or {}
        loan_term = random.randint(6, 24)
        loan_required = float(random.randint(2_000, 50_000))
        disb = _jan_2025_random_datetime()
        use_anniversary = random.choice([True, False])
        scheme_name = f"{assignments[i - 1].get('name') or pcode} ({pcode})"

        try:
            if lt_raw == "consumer_loan":
                details, df_s = compute_consumer_pair(
                    loan_required=loan_required,
                    loan_term=loan_term,
                    disbursement_date=disb,
                    product_cfg=cfg,
                    use_anniversary=use_anniversary,
                    scheme_label=scheme_name,
                    sys_cfg=sys_cfg,
                )
            elif lt_raw == "term_loan":
                details, df_s = compute_term_pair(
                    loan_required=loan_required,
                    loan_term=loan_term,
                    disbursement_date=disb,
                    product_cfg=cfg,
                    use_anniversary=use_anniversary,
                    sys_cfg=sys_cfg,
                )
            elif lt_raw == "bullet_loan":
                details, df_s = compute_bullet_pair(
                    loan_required=loan_required,
                    loan_term=loan_term,
                    disbursement_date=disb,
                    product_cfg=cfg,
                    use_anniversary=use_anniversary,
                    sys_cfg=sys_cfg,
                )
            elif lt_raw == "customised_repayments":
                details, df_s = compute_customised_pair(
                    loan_required=loan_required,
                    loan_term=loan_term,
                    disbursement_date=disb,
                    product_cfg=cfg,
                    use_anniversary=use_anniversary,
                    sys_cfg=sys_cfg,
                )
            else:
                raise ValueError(f"unsupported loan_type {lt_raw}")
            details = _apply_penalty_and_gl(details, cfg, penalty_pct=10.0, cash_gl_id=cash_gl)
            lid = save_loan(cid, label, details, df_s, product_code=pcode)
            created_loans.append(int(lid))
        except Exception as e:
            errors.append(f"loan {i} product {pcode}: {e}")

    print(f"Customers created: {len(created_customers)}")
    print(f"Loans created: {len(created_loans)}")
    if errors:
        print(f"Errors ({len(errors)}), first 10:")
        for line in errors[:10]:
            print(" ", line)


if __name__ == "__main__":
    main()
