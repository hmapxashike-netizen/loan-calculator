"""
Backfill script: add fee amount columns and populate them for all existing loans.

Run once after deploying schema/28_loan_fee_amounts.sql, or use this script
in place of the SQL migration -- it does both steps (ALTER + UPDATE).

  python scripts/backfill_fee_amounts.py [--dry-run] [--loan-id LOAN_ID]

Options
  --dry-run    Print the rows that would be updated without committing.
  --loan-id N  Process a single loan only (useful for spot-checks).

Identity enforced:
  disbursed_amount + admin_fee_amount + drawdown_fee_amount + arrangement_fee_amount = principal
"""

from __future__ import annotations

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
import psycopg2.extras
from config import get_database_url


DDL = """
ALTER TABLE loans
    ADD COLUMN IF NOT EXISTS admin_fee_amount       NUMERIC(18, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS drawdown_fee_amount    NUMERIC(18, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS arrangement_fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0;

COMMENT ON COLUMN loans.admin_fee_amount       IS 'Absolute admin fee amount at disbursement = principal * admin_fee rate.';
COMMENT ON COLUMN loans.drawdown_fee_amount    IS 'Absolute drawdown fee amount at disbursement = principal * drawdown_fee rate.';
COMMENT ON COLUMN loans.arrangement_fee_amount IS 'Absolute arrangement fee amount at disbursement = principal * arrangement_fee rate.';
"""


def _fee_amt(principal: float, rate) -> float:
    if not rate:
        return 0.0
    return round(float(principal) * float(rate), 2)


def run(dry_run: bool, loan_id) -> None:
    conn = psycopg2.connect(get_database_url())
    conn.autocommit = False

    with conn.cursor() as cur:
        print("Applying DDL (ADD COLUMN IF NOT EXISTS) ...")
        cur.execute(DDL)

        base_query = """
            SELECT id, principal, admin_fee, drawdown_fee, arrangement_fee,
                   admin_fee_amount, drawdown_fee_amount, arrangement_fee_amount
            FROM loans
            WHERE admin_fee_amount = 0
              AND drawdown_fee_amount = 0
              AND arrangement_fee_amount = 0
        """
        params = ()
        if loan_id is not None:
            base_query += " AND id = %s"
            params = (loan_id,)
        base_query += " ORDER BY id"

        cur.execute(base_query, params)
        rows = cur.fetchall()

    if not rows:
        print("No loans require backfilling.")
        conn.close()
        return

    label = "DRY RUN -- " if dry_run else ""
    print(f"{label}Backfilling {len(rows)} loan(s)...\n")
    print(f"{'ID':>8}  {'Principal':>14}  {'Admin fee $':>12}  {'Drawdown fee $':>14}  {'Arrangement fee $':>18}  {'Sum=principal?':>16}")
    print("-" * 100)

    updates = []
    errors = []

    for row in rows:
        lid = row[0]
        principal = float(row[1] or 0)
        admin_amt      = _fee_amt(principal, row[2])
        drawdown_amt   = _fee_amt(principal, row[3])
        arrangement_amt = _fee_amt(principal, row[4])
        disbursed_amt  = round(principal - admin_amt - drawdown_amt - arrangement_amt, 2)
        check_sum      = round(disbursed_amt + admin_amt + drawdown_amt + arrangement_amt, 2)
        identity_ok    = abs(check_sum - principal) < 0.01
        flag = "" if identity_ok else " *** MISMATCH ***"
        print(
            f"{lid:>8}  {principal:>14.2f}  {admin_amt:>12.2f}  {drawdown_amt:>14.2f}  "
            f"{arrangement_amt:>18.2f}  {check_sum:>14.2f}{flag}"
        )
        if not identity_ok:
            errors.append(f"Loan {lid}: disbursed+fees={check_sum} != principal={principal}")
        updates.append((admin_amt, drawdown_amt, arrangement_amt, lid))

    if errors:
        print("\nIdentity check failures:")
        for e in errors:
            print(f"  {e}")

    if dry_run:
        print("\nDry-run complete -- no changes committed.")
        conn.close()
        return

    if errors:
        answer = input(f"\n{len(errors)} mismatch(es). Continue anyway? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            conn.close()
            return

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE loans
            SET admin_fee_amount       = %s,
                drawdown_fee_amount    = %s,
                arrangement_fee_amount = %s
            WHERE id = %s
            """,
            updates,
        )

    conn.commit()
    conn.close()
    print(f"\nCommitted. {len(updates)} loan(s) updated.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill fee amount columns on the loans table.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without committing.")
    parser.add_argument("--loan-id", type=int, default=None, help="Restrict to a single loan ID.")
    args = parser.parse_args()
    run(dry_run=args.dry_run, loan_id=args.loan_id)