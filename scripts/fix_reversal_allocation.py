"""
Backfill allocation for a reversal row that has no allocation.

When a receipt is reversed, the reversal row should get unallocation_parent_reversed
(negative amounts mirroring the original). If the original had no allocation at
reversal time, the reversal row was created without an allocation row.

This script looks up the original receipt's allocation and inserts the missing
unallocation_parent_reversed row for the reversal.

Run from project root:  python scripts/fix_reversal_allocation.py 15
"""
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/fix_reversal_allocation.py <reversal_repayment_id>")
        print("Example: python scripts/fix_reversal_allocation.py 15")
        sys.exit(1)
    try:
        reversal_id = int(sys.argv[1])
    except ValueError:
        print("repayment_id must be an integer.")
        sys.exit(1)

    from config import get_database_url
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from loan_management import _get_allocation_sum_for_repayment, _connection

    conn = psycopg2.connect(get_database_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, loan_id, amount, original_repayment_id,
                       COALESCE(value_date, payment_date) AS eff_date
                FROM loan_repayments
                WHERE id = %s
                """,
                (reversal_id,),
            )
            row = cur.fetchone()
        if not row:
            print(f"Repayment {reversal_id} not found.")
            sys.exit(1)
        if float(row["amount"] or 0) >= 0:
            print(f"Repayment {reversal_id} is not a reversal (amount is not negative).")
            sys.exit(1)
        orig_id = row.get("original_repayment_id")
        if not orig_id:
            print(f"Repayment {reversal_id} has no original_repayment_id (not a reversal row).")
            sys.exit(1)

        alloc = _get_allocation_sum_for_repayment(orig_id, conn)
        if not alloc:
            print(f"Original repayment {orig_id} has no allocation. Cannot backfill reversal.")
            sys.exit(1)

        def _f(v):
            return float(v or 0)

        rev = {
            "alloc_principal_not_due": -_f(alloc["alloc_principal_not_due"]),
            "alloc_principal_arrears": -_f(alloc["alloc_principal_arrears"]),
            "alloc_interest_accrued": -_f(alloc["alloc_interest_accrued"]),
            "alloc_interest_arrears": -_f(alloc["alloc_interest_arrears"]),
            "alloc_default_interest": -_f(alloc["alloc_default_interest"]),
            "alloc_penalty_interest": -_f(alloc["alloc_penalty_interest"]),
            "alloc_fees_charges": -_f(alloc["alloc_fees_charges"]),
        }
        rev["alloc_principal_total"] = rev["alloc_principal_not_due"] + rev["alloc_principal_arrears"]
        rev["alloc_interest_total"] = (
            rev["alloc_interest_accrued"] + rev["alloc_interest_arrears"]
            + rev["alloc_default_interest"] + rev["alloc_penalty_interest"]
        )
        rev["alloc_fees_total"] = rev["alloc_fees_charges"]

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO loan_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total,
                    event_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    reversal_id,
                    rev["alloc_principal_not_due"],
                    rev["alloc_principal_arrears"],
                    rev["alloc_interest_accrued"],
                    rev["alloc_interest_arrears"],
                    rev["alloc_default_interest"],
                    rev["alloc_penalty_interest"],
                    rev["alloc_fees_charges"],
                    rev["alloc_principal_total"],
                    rev["alloc_interest_total"],
                    rev["alloc_fees_total"],
                    "unallocation_parent_reversed",
                ),
            )
        conn.commit()
        print(f"Inserted unallocation_parent_reversed for repayment {reversal_id} (reversal of {orig_id}).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
