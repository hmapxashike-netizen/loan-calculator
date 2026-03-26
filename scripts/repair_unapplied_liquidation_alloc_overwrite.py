"""
Repair DB rows corrupted when reverse_repayment() used ON CONFLICT DO UPDATE on
loan_repayment_allocation, overwriting a positive unapplied_funds_allocation with
negative unallocation_parent_reversed on the *same* repayment_id.

EOD then nets allocations incorrectly (e.g. interest_arrears_balance +343.86 too high on
the liquidation value_date). New reversals insert a separate repayment; this script fixes
legacy data by splitting: restore positive allocation on the liquidation repayment and
move the negative leg to a new system reversal repayment (same pattern as current code).

Run from project root:

  python scripts/repair_unapplied_liquidation_alloc_overwrite.py --dry-run
  python scripts/repair_unapplied_liquidation_alloc_overwrite.py --loan-id 9

Then replay EOD for the affected loan and date range, e.g.:

  python scripts/run_single_loan_eod_range.py 9 2025-06-01 2025-08-31

If repair finds nothing, liquidation rows may differ from the narrow template — inspect DB:

  python scripts/repair_unapplied_liquidation_alloc_overwrite.py --loan-id 9 --diagnose
"""
from __future__ import annotations

import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_database_url
from decimal_utils import as_10dp


def _f10(x) -> float:
    return float(as_10dp(float(x or 0)))


def _run_diagnose(loan_id: int) -> None:
    """Print system liquidation / unapplied allocations (omitted from standard loan CSV export)."""
    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    lr.id AS repayment_id,
                    lr.loan_id,
                    lr.reference,
                    lr.status,
                    lr.amount AS repayment_amount,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    lr.original_repayment_id,
                    lra.id AS lra_id,
                    lra.event_type,
                    lra.alloc_interest_arrears,
                    lra.alloc_total,
                    lra.source_repayment_id
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND (
                    COALESCE(lr.reference, '') ILIKE '%%Unapplied funds allocation%%'
                    OR COALESCE(lr.reference, '') ILIKE '%%Reversal of unapplied funds allocation%%'
                    OR lra.event_type IN ('unapplied_funds_allocation', 'unallocation_parent_reversed')
                  )
                ORDER BY value_date, lr.id
                """,
                (loan_id,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"No system unapplied liquidation / allocation rows for loan_id={loan_id}.")
        print(
            "If balance is still wrong, cause may be elsewhere (different loan, or no EOD "
            "liquidation in this database)."
        )
        return

    print(f"loan_id={loan_id}: system / unapplied-related repayments ({len(rows)} allocation row(s)):\n")
    for r in rows:
        print(
            f"  repayment_id={r['repayment_id']} value_date={r['value_date']} status={r['status']} "
            f"ref={r['reference']!r} amt={r['repayment_amount']} orig_rid={r.get('original_repayment_id')}\n"
            f"    lra_id={r['lra_id']} event={r['event_type']} src={r.get('source_repayment_id')} "
            f"alloc_ia={r['alloc_interest_arrears']} alloc_total={r['alloc_total']}"
        )

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    (COALESCE(lr.value_date, lr.payment_date))::date AS d,
                    COALESCE(SUM(lra.alloc_interest_arrears), 0) AS sum_ia,
                    COUNT(*) AS n_alloc_rows
                FROM loan_repayments lr
                JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND lr.status IN ('posted', 'reversed')
                GROUP BY 1
                HAVING COALESCE(SUM(lra.alloc_interest_arrears), 0) <> 0
                ORDER BY 1
                """,
                (loan_id,),
            )
            per_day = cur.fetchall()
    finally:
        conn.close()

    if per_day:
        print("\nPer day — SUM(alloc_interest_arrears), all receipts (posted+reversed):")
        for r in per_day:
            print(f"  {r['d']}: sum_ia={r['sum_ia']} ({r['n_alloc_rows']} allocation row(s) that day)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Split overwritten unapplied liquidation allocations into +parent / +child reversal rows."
    )
    parser.add_argument("--loan-id", type=int, default=None, help="Only this loan_id (optional).")
    parser.add_argument("--dry-run", action="store_true", help="Print actions only; no DB changes.")
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="List unapplied/liquidation rows for --loan-id (no repair).",
    )
    args = parser.parse_args()

    if args.diagnose:
        if args.loan_id is None:
            print("--diagnose requires --loan-id")
            return 2
        _run_diagnose(args.loan_id)
        return 0

    conn = psycopg2.connect(get_database_url())
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT
                    lra.id AS lra_id,
                    lra.repayment_id AS liq_rid,
                    lr.loan_id,
                    (COALESCE(lr.value_date, lr.payment_date))::date AS vd,
                    lr.status AS lr_status,
                    lra.event_type AS lra_event,
                    lra.alloc_principal_not_due, lra.alloc_principal_arrears,
                    lra.alloc_interest_accrued, lra.alloc_interest_arrears,
                    lra.alloc_default_interest, lra.alloc_penalty_interest, lra.alloc_fees_charges,
                    lra.alloc_principal_total, lra.alloc_interest_total, lra.alloc_fees_total,
                    lra.alloc_total, lra.unallocated, lra.source_repayment_id
                FROM loan_repayment_allocation lra
                JOIN loan_repayments lr ON lr.id = lra.repayment_id
                WHERE COALESCE(lr.reference, '') ILIKE 'Unapplied funds allocation%%'
                  AND COALESCE(lra.alloc_total, 0) < 0
                  AND NOT EXISTS (
                    SELECT 1 FROM loan_repayments lr2
                    WHERE lr2.original_repayment_id = lr.id
                      AND COALESCE(lr2.reference, '') ILIKE 'Reversal of unapplied funds allocation%%'
                  )
            """
            params: list = []
            if args.loan_id is not None:
                sql += " AND lr.loan_id = %s"
                params.append(args.loan_id)
            cur.execute(sql, params)
            corrupt = list(cur.fetchall())
    finally:
        conn.close()

    if not corrupt:
        print("No matching corrupted rows found.")
        if args.loan_id is not None:
            print(
                f"Tip: python scripts/repair_unapplied_liquidation_alloc_overwrite.py "
                f"--loan-id {args.loan_id} --diagnose"
            )
        return 0

    print(f"Found {len(corrupt)} corrupted liquidation allocation row(s).")

    conn = psycopg2.connect(get_database_url())
    try:
        conn.autocommit = False
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for row in corrupt:
                liq_rid = int(row["liq_rid"])
                loan_id = int(row["loan_id"])
                vd = row["vd"]
                lra_id = int(row["lra_id"])

                cur.execute(
                    """
                    SELECT 1 FROM loan_repayments lr2
                    WHERE lr2.original_repayment_id = %s
                      AND COALESCE(lr2.reference, '') ILIKE 'Reversal of unapplied funds allocation%%'
                    LIMIT 1
                    """,
                    (liq_rid,),
                )
                if cur.fetchone():
                    print(f"Skip liq_rid={liq_rid} loan_id={loan_id}: reversal row already exists.")
                    continue

                apr = _f10(row["alloc_principal_not_due"])
                apa = _f10(row["alloc_principal_arrears"])
                aia = _f10(row["alloc_interest_accrued"])
                aiar = _f10(row["alloc_interest_arrears"])
                adi = _f10(row["alloc_default_interest"])
                api = _f10(row["alloc_penalty_interest"])
                afc = _f10(row["alloc_fees_charges"])
                src = row["source_repayment_id"]
                if src is None:
                    cur.execute(
                        """
                        SELECT source_repayment_id
                        FROM unapplied_funds
                        WHERE allocation_repayment_id = %s AND entry_type = 'debit'
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (liq_rid,),
                    )
                    uf = cur.fetchone()
                    src = int(uf["source_repayment_id"]) if uf and uf.get("source_repayment_id") else None

                rev_total = apr + apa + aia + aiar + adi + api + afc
                unalloc = _f10(row.get("unallocated"))

                pos_apr, pos_apa = -apr, -apa
                pos_aia, pos_aiar = -aia, -aiar
                pos_adi, pos_api, pos_afc = -adi, -api, -afc
                pos_prin_tot = pos_apr + pos_apa
                pos_int_tot = pos_aia + pos_aiar + pos_adi + pos_api
                pos_fees_tot = pos_afc
                pos_alloc_total = pos_prin_tot + pos_int_tot + pos_fees_tot
                pos_unalloc = -unalloc if unalloc else 0.0

                print(
                    f"Repair loan_id={loan_id} liquidation_repayment_id={liq_rid} date={vd} "
                    f"alloc_total_was={rev_total}"
                )

                if args.dry_run:
                    continue

                cur.execute(
                    """
                    INSERT INTO loan_repayments (
                        loan_id, amount, payment_date, reference, value_date, status, original_repayment_id
                    )
                    VALUES (%s, %s, %s, %s, %s, 'reversed', %s)
                    RETURNING id
                    """,
                    (
                        loan_id,
                        _f10(pos_alloc_total),
                        vd,
                        "Reversal of unapplied funds allocation",
                        vd,
                        liq_rid,
                    ),
                )
                row_new = cur.fetchone()
                new_rid = int(row_new["id"]) if row_new else None
                if new_rid is None:
                    raise RuntimeError(f"INSERT reversal repayment failed for liq_rid={liq_rid}")

                cur.execute(
                    """
                    INSERT INTO loan_repayment_allocation (
                        repayment_id,
                        alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total,
                        alloc_total, unallocated, event_type, source_repayment_id
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        'unallocation_parent_reversed', %s
                    )
                    """,
                    (
                        new_rid,
                        _f10(apr), _f10(apa), _f10(aia), _f10(aiar), _f10(adi), _f10(api), _f10(afc),
                        _f10(apr + apa),
                        _f10(aia + aiar + adi + api),
                        _f10(afc),
                        _f10(rev_total),
                        _f10(unalloc),
                        src,
                    ),
                )

                cur.execute(
                    """
                    UPDATE loan_repayment_allocation
                    SET
                        alloc_principal_not_due = %s,
                        alloc_principal_arrears = %s,
                        alloc_interest_accrued = %s,
                        alloc_interest_arrears = %s,
                        alloc_default_interest = %s,
                        alloc_penalty_interest = %s,
                        alloc_fees_charges = %s,
                        alloc_principal_total = %s,
                        alloc_interest_total = %s,
                        alloc_fees_total = %s,
                        alloc_total = %s,
                        unallocated = %s,
                        event_type = 'unapplied_funds_allocation',
                        source_repayment_id = COALESCE(source_repayment_id, %s)
                    WHERE id = %s
                    """,
                    (
                        _f10(pos_apr),
                        _f10(pos_apa),
                        _f10(pos_aia),
                        _f10(pos_aiar),
                        _f10(pos_adi),
                        _f10(pos_api),
                        _f10(pos_afc),
                        _f10(pos_prin_tot),
                        _f10(pos_int_tot),
                        _f10(pos_fees_tot),
                        _f10(pos_alloc_total),
                        _f10(pos_unalloc),
                        src,
                        lra_id,
                    ),
                )

        if args.dry_run:
            conn.rollback()
            print("Dry-run: rolled back.")
        else:
            conn.commit()
            print("Committed. Run single-loan EOD range for affected loans/dates.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
