#!/usr/bin/env python3
"""
Print how a receipt was allocated across waterfall buckets (from loan_repayment_allocation).

Examples (from project root, with LMS_DATABASE_URL / config set like the app):

  python scripts/breakdown_repayment_allocation.py --loan-id 1 --amount 100000
  python scripts/breakdown_repayment_allocation.py --repayment-id 42
  python scripts/breakdown_repayment_allocation.py --loan-id 1 --amount 100000 --tolerance 0.02

Uses the same DB URL as the app: config.get_database_url() / env LMS_DATABASE_URL.

"Delinquency (5 buckets)" = principal_arrears + interest_arrears + default + penalty + fees
(same components as statement total delinquency when comparing to loan_daily_state).
"""
from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

from config import get_database_url  # noqa: E402


def _f(x) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _conn():
    return psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Break down waterfall allocation for a loan repayment (DB-backed)."
    )
    p.add_argument("--loan-id", type=int, default=None, help="Filter repayments to this loan.")
    p.add_argument("--repayment-id", type=int, default=None, help="Exact repayment row (overrides --amount).")
    p.add_argument(
        "--amount",
        type=str,
        default=None,
        help="Match receipt amount (e.g. 100000). Use with --loan-id if multiple loans exist.",
    )
    p.add_argument(
        "--tolerance",
        type=str,
        default="0.01",
        help="Amount match tolerance (default 0.01).",
    )
    args = p.parse_args()

    if args.repayment_id is None and args.amount is None:
        p.error("Provide --repayment-id or --amount (and usually --loan-id).")

    tol = _f(args.tolerance)
    amt = _f(args.amount) if args.amount is not None else None

    sql = """
        SELECT
            lr.id AS repayment_id,
            lr.loan_id,
            lr.amount AS receipt_amount,
            lr.payment_date,
            lr.value_date,
            COALESCE(lr.value_date, lr.payment_date) AS eff_date,
            lr.reference,
            lr.status,
            lra.id AS allocation_row_id,
            lra.alloc_principal_not_due,
            lra.alloc_principal_arrears,
            lra.alloc_interest_accrued,
            lra.alloc_interest_arrears,
            lra.alloc_default_interest,
            lra.alloc_penalty_interest,
            lra.alloc_fees_charges,
            lra.alloc_principal_total,
            lra.alloc_interest_total,
            lra.alloc_fees_total,
            lra.alloc_total,
            lra.unallocated,
            lra.event_type,
            lra.created_at AS allocation_created_at
        FROM loan_repayments lr
        LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
        WHERE 1=1
    """
    params: list = []
    if args.repayment_id is not None:
        sql += " AND lr.id = %s"
        params.append(args.repayment_id)
    else:
        if args.loan_id is not None:
            sql += " AND lr.loan_id = %s"
            params.append(args.loan_id)
        sql += " AND lr.amount BETWEEN %s AND %s"
        lo = amt - tol
        hi = amt + tol
        params.extend([lo, hi])

    sql += " ORDER BY lr.id DESC"

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    if not rows:
        print("No matching repayment found.", file=sys.stderr)
        return 1
    if len(rows) > 1:
        print(
            f"Multiple matches ({len(rows)}). Use --repayment-id to pick one. Candidates:\n",
            file=sys.stderr,
        )
        for r in rows[:20]:
            print(
                f"  repayment_id={r['repayment_id']} loan_id={r['loan_id']} "
                f"amount={r['receipt_amount']} eff_date={r['eff_date']}",
                file=sys.stderr,
            )
        return 2

    r = dict(rows[0])
    if r.get("allocation_row_id") is None:
        print(
            f"Repayment {r['repayment_id']} exists but has no loan_repayment_allocation row.",
            file=sys.stderr,
        )
        return 3

    def col(name: str) -> Decimal:
        return _f(r.get(name))

    pr_nd = col("alloc_principal_not_due")
    pr_ar = col("alloc_principal_arrears")
    int_acc = col("alloc_interest_accrued")
    int_ar = col("alloc_interest_arrears")
    def_i = col("alloc_default_interest")
    pen = col("alloc_penalty_interest")
    fees = col("alloc_fees_charges")

    principal_total = col("alloc_principal_total")
    interest_total = col("alloc_interest_total")
    fees_total = col("alloc_fees_total")
    alloc_total = col("alloc_total")
    unalloc = col("unallocated")

    # Recompute if stored totals missing (older rows)
    sum_components = pr_nd + pr_ar + int_acc + int_ar + def_i + pen + fees
    delinq_5 = pr_ar + int_ar + def_i + pen + fees

    receipt = col("receipt_amount")

    print("=" * 72)
    print(" REPAYMENT ALLOCATION BREAKDOWN")
    print("=" * 72)
    print(f"  repayment_id     : {r['repayment_id']}")
    print(f"  loan_id          : {r['loan_id']}")
    print(f"  eff_date         : {r['eff_date']}")
    print(f"  reference        : {r.get('reference')}")
    print(f"  receipt_amount   : {receipt}")
    print(f"  event_type       : {r.get('event_type')}")
    print()
    print("  Per-bucket allocation (loan_repayment_allocation)")
    print("  " + "-" * 68)
    print(f"  alloc_principal_not_due     {pr_nd:>20}")
    print(f"  alloc_principal_arrears     {pr_ar:>20}")
    print(f"  alloc_interest_accrued      {int_acc:>20}")
    print(f"  alloc_interest_arrears      {int_ar:>20}")
    print(f"  alloc_default_interest      {def_i:>20}")
    print(f"  alloc_penalty_interest      {pen:>20}")
    print(f"  alloc_fees_charges          {fees:>20}")
    print("  " + "-" * 68)
    print(f"  Sum of seven lines above    {sum_components:>20}")
    print()
    print("  Aggregates (stored)")
    print(f"  alloc_principal_total       {principal_total:>20}")
    print(f"  alloc_interest_total        {interest_total:>20}")
    print(f"  alloc_fees_total            {fees_total:>20}")
    print(f"  alloc_total                 {alloc_total:>20}")
    print(f"  unallocated                 {unalloc:>20}")
    print()
    print("  Derived (for reconciliation vs loan_daily_state / statements)")
    print(f"  Delinquency (5 buckets)*    {delinq_5:>20}")
    print("    * principal_arrears + interest_arrears + default + penalty + fees")
    print()
    chk = alloc_total + unalloc if alloc_total or unalloc else sum_components + unalloc
    print(f"  Check: alloc_total + unallocated = {alloc_total + unalloc}")
    print(f"  Check: receipt_amount          = {receipt}")
    if abs(chk - receipt) > Decimal("0.02"):
        print("  WARNING: alloc_total + unallocated does not match receipt (see DB / migrations).")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
