"""In-place repayment reallocation (waterfall re-run with policy-compliant state updates)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from decimal_utils import as_10dp

from .allocation_queries import (
    _get_allocation_sum_for_repayment,
    _get_opening_balances_for_repayment,
    get_net_allocation_for_loan_date,
    get_unallocated_for_loan_date,
)
from .cash_gl import _post_event_for_loan
from .db import RealDictCursor, _connection
from .product_catalog import load_system_config_from_db
from .serialization import _date_conv
from .unapplied_recast import _credit_unapplied_funds
from .unapplied_refs import _unapplied_original_reference, _unapplied_reversal_reference
from .waterfall_core import _get_waterfall_config, compute_waterfall_allocation


def reallocate_repayment(
    repayment_id: int,
    *,
    system_config: dict | None = None,
    use_yesterday_state: bool = False,
) -> None:
    """
    Correct an already-saved receipt: reverse its allocation and unapplied credits,
    then re-run allocation (e.g. after fixing waterfall config or logic).
    Allocation is done at save receipt, not at EOD.

    When use_yesterday_state=True, uses loan_daily_state from the day before as
    state_before (avoids engine divergence when engine has wrong arrears).
    """
    opening_balances: dict[str, float] | None = None
    st_prev_realloc: dict | None = None
    opening_days_od: int = 0
    svc_unapplied = None

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT lr.id, lr.loan_id, lr.amount, lr.reference,
                       COALESCE(lr.value_date, lr.payment_date) AS eff_date
                FROM loan_repayments lr
                WHERE lr.id = %s
                """,
                (repayment_id,),
            )
            rrow = cur.fetchone()
            if not rrow:
                raise ValueError(f"Repayment {repayment_id} not found.")
            amount = float(rrow["amount"] or 0)
            if amount <= 0:
                # Reversals (negative) are never reallocated; they get unallocation_parent_reversed
                # at creation (reverse_repayment) or via clear script's fix-reversed section.
                return
            if (rrow.get("reference") or "") == "Unapplied funds allocation":
                # System liquidation repayments must not be reallocated as cash receipts.
                return
            loan_id = int(rrow["loan_id"])
            eff_date = rrow["eff_date"]
            if hasattr(eff_date, "date"):
                eff_date = eff_date.date()

            alloc_row = _get_allocation_sum_for_repayment(repayment_id, conn)
            if not alloc_row:
                # No prior allocation; ensure state exists then allocate
                conn.commit()
                from .repayment_waterfall import allocate_repayment_waterfall

                allocate_repayment_waterfall(repayment_id, system_config=system_config)
                return

            # Skip no-op: if allocation is already all zeros (all unapplied), reallocate would produce nothing
            def _fz(v):
                return abs(float(v or 0)) < 1e-6
            if all(
                _fz(alloc_row.get(k))
                for k in (
                    "alloc_principal_not_due", "alloc_principal_arrears",
                    "alloc_interest_accrued", "alloc_interest_arrears",
                    "alloc_default_interest", "alloc_penalty_interest", "alloc_fees_charges",
                )
            ):
                return

            # Idempotency: skip if allocation would be unchanged (prevents duplicates from double-calls).
            conn.commit()
            from eod.core import run_single_loan_eod

            prev_cal = eff_date - timedelta(days=1)
            run_single_loan_eod(loan_id, prev_cal, sys_cfg=system_config)
            with _connection() as _idem_conn:
                with _idem_conn.cursor(cursor_factory=RealDictCursor) as _idem_cur:
                    _idem_cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (4021001, loan_id))
                    opening_balances, st_prev_realloc, opening_days_od = _get_opening_balances_for_repayment(
                        _idem_cur, loan_id, eff_date, repayment_id
                    )
            cfg = system_config or load_system_config_from_db() or {}
            profile_key, bucket_order = _get_waterfall_config(cfg)
            desired_alloc, desired_unapplied = compute_waterfall_allocation(
                amount, opening_balances, bucket_order, profile_key,
                state_as_of=eff_date, repayment_id=repayment_id,
            )
            desired_remaining_arrears = float(
                as_10dp(
                    max(0.0, opening_balances.get("interest_arrears_balance", 0.0) - desired_alloc.get("alloc_interest_arrears", 0.0))
                    + max(0.0, opening_balances.get("default_interest_balance", 0.0) - desired_alloc.get("alloc_default_interest", 0.0))
                    + max(0.0, opening_balances.get("penalty_interest_balance", 0.0) - desired_alloc.get("alloc_penalty_interest", 0.0))
                    + max(0.0, opening_balances.get("principal_arrears", 0.0) - desired_alloc.get("alloc_principal_arrears", 0.0))
                    + max(0.0, opening_balances.get("fees_charges_balance", 0.0) - desired_alloc.get("alloc_fees_charges", 0.0))
                )
            )
            if desired_unapplied > 1e-6 and desired_remaining_arrears > 1e-6:
                raise ValueError(
                    f"Policy violation for repayment {repayment_id}: unapplied={desired_unapplied} while "
                    f"arrears still outstanding={desired_remaining_arrears}."
                )
            def _same_10dp(a, b):
                return as_10dp(a or 0) == as_10dp(b or 0)
            if all(
                _same_10dp(alloc_row.get(k), desired_alloc.get(k, 0))
                for k in (
                    "alloc_principal_not_due", "alloc_principal_arrears",
                    "alloc_interest_accrued", "alloc_interest_arrears",
                    "alloc_default_interest", "alloc_penalty_interest", "alloc_fees_charges",
                )
            ) and _same_10dp(alloc_row.get("unallocated", 0), desired_unapplied):
                # Allocation/unapplied is already correct, but we may still be
                # missing the unapplied ledger credit and/or GL journal backing.
                # Backfill it idempotently (post_event replaces by (event_id,event_tag)).
                if desired_unapplied > 1e-6:
                    cur.execute(
                        """
                        SELECT COALESCE(SUM(amount), 0)
                        FROM unapplied_funds
                        WHERE repayment_id = %s
                          AND entry_type = 'credit'
                          AND reference = 'Overpayment'
                        """,
                        (repayment_id,),
                    )
                    _credit_row = cur.fetchone()
                    if isinstance(_credit_row, dict):
                        existing_credit_raw = next(iter(_credit_row.values()), 0)
                    else:
                        existing_credit_raw = (_credit_row[0] if _credit_row else 0)
                    existing_credit = float(as_10dp(existing_credit_raw or 0))
                    if existing_credit + 1e-6 < desired_unapplied:
                        # Guard: never allow GL overpayment credit without matching ledger credit.
                        missing_credit = float(as_10dp(desired_unapplied - existing_credit))
                        _credit_unapplied_funds(conn, loan_id, repayment_id, missing_credit, eff_date)
                    try:
                        from accounting.service import AccountingService
                        from decimal import Decimal

                        svc_unapplied = AccountingService()
                    except Exception:
                        svc_unapplied = None

                    if svc_unapplied is not None:
                        credit_ref = _unapplied_original_reference(
                            "credit",
                            loan_id=loan_id,
                            repayment_id=repayment_id,
                            value_date=eff_date,
                        )

                        # Only post if header doesn't exist yet.
                        cur.execute(
                            """
                            SELECT 1
                            FROM journal_entries
                            WHERE event_tag = %s AND event_id = %s
                            LIMIT 1
                            """,
                            ("UNAPPLIED_FUNDS_OVERPAYMENT", credit_ref),
                        )
                        if cur.fetchone() is None:
                            _post_event_for_loan(
                                svc_unapplied,
                                loan_id,
                                repayment_id=repayment_id,
                                event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                                reference=credit_ref,
                                description="Unapplied funds on overpayment",
                                event_id=credit_ref,
                                created_by="system",
                                entry_date=eff_date,
                                amount=Decimal(str(desired_unapplied)),
                            )
                return

            # Insert debit rows to offset prior unapplied credits (ledger-style)
            # Also fix GL lineage for unapplied overpayment by reversing prior
            # UNAPPLIED_FUNDS_OVERPAYMENT journals (only if they exist), and
            # posting a new credit journal for the new_unapplied amount.
            try:
                from accounting.service import AccountingService

                svc_unapplied = AccountingService()
            except Exception:
                svc_unapplied = None

            cur.execute(
                """
                SELECT id, amount, value_date
                FROM unapplied_funds
                WHERE repayment_id = %s AND amount > 0
                ORDER BY id DESC
                """,
                (repayment_id,),
            )
            prev_unapplied_rows = cur.fetchall()
            for uf_row in prev_unapplied_rows:
                amt = float(as_10dp(-float(uf_row["amount"] or 0)))
                cur.execute(
                    """
                    INSERT INTO unapplied_funds (loan_id, amount, value_date, entry_type, reference, source_repayment_id, currency)
                    VALUES (%s, %s, %s, 'debit', 'Reallocate (remove prior unapplied)', %s, 'USD')
                    """,
                    (loan_id, amt, eff_date, repayment_id),
                )

                # GL reversal: only post if the original credit journal exists.
                if svc_unapplied is not None and float(uf_row.get("amount") or 0) > 1e-6:
                    from decimal import Decimal

                    uf_value_date = uf_row.get("value_date") or eff_date
                    if hasattr(uf_value_date, "date"):
                        uf_value_date = uf_value_date.date() if callable(getattr(uf_value_date, "date")) else uf_value_date

                    orig_ref = _unapplied_original_reference(
                        "credit",
                        loan_id=loan_id,
                        repayment_id=repayment_id,
                        value_date=uf_value_date,
                    )
                    # If there is no original UNAPPLIED_FUNDS_OVERPAYMENT journal,
                    # do not create a reversal-only entry.
                    cur.execute(
                        """
                        SELECT 1
                        FROM journal_entries
                        WHERE event_tag = %s AND event_id = %s
                        LIMIT 1
                        """,
                        ("UNAPPLIED_FUNDS_OVERPAYMENT", orig_ref),
                    )
                    if cur.fetchone() is not None:
                        rev_ref = _unapplied_reversal_reference(orig_ref)
                        _post_event_for_loan(
                            svc_unapplied,
                            loan_id,
                            repayment_id=repayment_id,
                            event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                            reference=rev_ref,
                            description=f"Reversal of unapplied overpayment: {orig_ref}",
                            event_id=rev_ref,
                            created_by="system",
                            entry_date=eff_date,
                            amount=Decimal(str(float(uf_row.get("amount") or 0))),
                            is_reversal=True,
                        )

    # Override existing allocation row in place (policy: one row per repayment_id).
    # Same opening-basis policy as allocate_repayment_waterfall (closing eff_date-1 minus earlier receipts).
    _ = use_yesterday_state  # retained for backward compatibility; intentionally ignored.
    if opening_balances is None:
        raise RuntimeError("reallocate_repayment: opening balances missing after idempotency gate")

    state_before = opening_balances
    cfg = system_config or load_system_config_from_db() or {}
    profile_key, bucket_order = _get_waterfall_config(cfg)
    new_alloc, new_unapplied = compute_waterfall_allocation(
        amount, state_before, bucket_order, profile_key,
        state_as_of=eff_date, repayment_id=repayment_id,
    )
    new_remaining_arrears = float(
        as_10dp(
            max(0.0, state_before.get("interest_arrears_balance", 0.0) - new_alloc.get("alloc_interest_arrears", 0.0))
            + max(0.0, state_before.get("default_interest_balance", 0.0) - new_alloc.get("alloc_default_interest", 0.0))
            + max(0.0, state_before.get("penalty_interest_balance", 0.0) - new_alloc.get("alloc_penalty_interest", 0.0))
            + max(0.0, state_before.get("principal_arrears", 0.0) - new_alloc.get("alloc_principal_arrears", 0.0))
            + max(0.0, state_before.get("fees_charges_balance", 0.0) - new_alloc.get("alloc_fees_charges", 0.0))
        )
    )
    if new_unapplied > 1e-6 and new_remaining_arrears > 1e-6:
        raise ValueError(
            f"Policy violation for repayment {repayment_id}: unapplied={new_unapplied} while "
            f"arrears still outstanding={new_remaining_arrears}."
        )

    new_apr = new_alloc.get("alloc_principal_not_due", 0.0)
    new_apa = new_alloc.get("alloc_principal_arrears", 0.0)
    new_aia = new_alloc.get("alloc_interest_accrued", 0.0)
    new_aiar = min(new_alloc.get("alloc_interest_arrears", 0.0), state_before.get("interest_arrears_balance", 0.0))
    new_adi = new_alloc.get("alloc_default_interest", 0.0)
    new_api = new_alloc.get("alloc_penalty_interest", 0.0)
    new_afc = new_alloc.get("alloc_fees_charges", 0.0)
    new_prin_total = new_apr + new_apa
    new_int_total = new_aia + new_aiar + new_adi + new_api
    new_fees_total = new_afc

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM loan_repayment_allocation
                WHERE repayment_id = %s
                LIMIT 1
                """,
                (repayment_id,),
            )
            alloc_row_id = cur.fetchone()
            if not alloc_row_id:
                raise ValueError(f"No allocation row for repayment {repayment_id}; cannot reallocate.")
            alloc_row_id = int(alloc_row_id["id"])

            cur.execute(
                """
                UPDATE loan_repayment_allocation SET
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
                    unallocated = %s
                WHERE id = %s
                """,
                (
                    float(as_10dp(new_apr)), float(as_10dp(new_apa)),
                    float(as_10dp(new_aia)), float(as_10dp(new_aiar)),
                    float(as_10dp(new_adi)), float(as_10dp(new_api)), float(as_10dp(new_afc)),
                    float(as_10dp(new_prin_total)), float(as_10dp(new_int_total)), float(as_10dp(new_fees_total)),
                    float(as_10dp(new_prin_total + new_int_total + new_fees_total)),
                    float(as_10dp(new_unapplied)),
                    alloc_row_id,
                ),
            )

            if new_unapplied > 1e-6:
                _credit_unapplied_funds(conn, loan_id, repayment_id, new_unapplied, eff_date)
                # GL credit posting for unapplied overpayment.
                if svc_unapplied is not None:
                    from decimal import Decimal

                    credit_ref = _unapplied_original_reference(
                        "credit",
                        loan_id=loan_id,
                        repayment_id=repayment_id,
                        value_date=eff_date,
                    )
                    _post_event_for_loan(
                        svc_unapplied,
                        loan_id,
                        repayment_id=repayment_id,
                        event_type="UNAPPLIED_FUNDS_OVERPAYMENT",
                        reference=credit_ref,
                        description="Unapplied funds on overpayment",
                        event_id=credit_ref,
                        created_by="system",
                        entry_date=eff_date,
                        amount=Decimal(str(new_unapplied)),
                    )

            new_interest_accrued = max(0.0, state_before["interest_accrued_balance"] - new_aia)
            new_interest_arrears = max(0.0, state_before["interest_arrears_balance"] - new_aiar)
            new_principal_not_due = max(0.0, state_before["principal_not_due"] - new_apr)
            new_principal_arrears = max(0.0, state_before["principal_arrears"] - new_apa)
            new_default_interest = max(0.0, state_before["default_interest_balance"] - new_adi)
            new_penalty_interest = max(0.0, state_before["penalty_interest_balance"] - new_api)
            new_fees_charges = max(0.0, state_before["fees_charges_balance"] - new_afc)
            new_reg_susp = max(
                0.0,
                float(
                    as_10dp(
                        float(state_before.get("regular_interest_in_suspense_balance", 0) or 0) - new_aia
                    )
                ),
            )
            new_pen_susp = max(0.0, float(as_10dp(new_penalty_interest)))
            new_def_susp = max(0.0, float(as_10dp(new_default_interest)))

            # Daily/period columns from closing(eff_date-1) (st_prev_realloc); aligned with allocate_repayment_waterfall.
            _sp = st_prev_realloc or {}
            reg_daily = float(_sp.get("regular_interest_daily", 0) or 0)
            def_daily = float(_sp.get("default_interest_daily", 0) or 0)
            pen_daily = float(_sp.get("penalty_interest_daily", 0) or 0)
            reg_period = float(_sp.get("regular_interest_period_to_date", 0) or 0)
            def_period = float(_sp.get("default_interest_period_to_date", 0) or 0)
            pen_period = float(_sp.get("penalty_interest_period_to_date", 0) or 0)

            total_exposure = (
                new_principal_not_due + new_principal_arrears + new_interest_accrued + new_interest_arrears
                + new_default_interest + new_penalty_interest + new_fees_charges
            )
            eff_date_val = _date_conv(eff_date) or (eff_date.date() if isinstance(eff_date, datetime) else eff_date)
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date_val, conn=conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date_val, conn=conn)
            days_overdue = int(opening_days_od)
            if new_interest_arrears + new_default_interest + new_penalty_interest + new_principal_arrears <= 1e-6:
                days_overdue = 0

            cur.execute(
                """
                UPDATE loan_daily_state SET
                    regular_interest_daily = %s,
                    principal_not_due = %s,
                    principal_arrears = %s,
                    interest_accrued_balance = %s,
                    interest_arrears_balance = %s,
                    default_interest_daily = %s,
                    default_interest_balance = %s,
                    penalty_interest_daily = %s,
                    penalty_interest_balance = %s,
                    fees_charges_balance = %s,
                    days_overdue = %s,
                    total_delinquency_arrears = %s,
                    total_exposure = %s,
                    regular_interest_period_to_date = %s,
                    penalty_interest_period_to_date = %s,
                    default_interest_period_to_date = %s,
                    net_allocation = %s,
                    unallocated = %s,
                    regular_interest_in_suspense_balance = %s,
                    penalty_interest_in_suspense_balance = %s,
                    default_interest_in_suspense_balance = %s,
                    total_interest_in_suspense_balance = %s
                WHERE loan_id = %s AND as_of_date = %s
                """,
                (
                    reg_daily, new_principal_not_due, new_principal_arrears,
                    new_interest_accrued, new_interest_arrears,
                    def_daily, new_default_interest,
                    pen_daily, new_penalty_interest,
                    new_fees_charges,
                    days_overdue,
                    float(as_10dp(new_principal_arrears + new_interest_arrears + new_default_interest + new_penalty_interest + new_fees_charges)),
                    total_exposure,
                    reg_period, pen_period, def_period,
                    net_alloc, unalloc,
                    new_reg_susp,
                    new_pen_susp,
                    new_def_susp,
                    float(
                        as_10dp(
                            Decimal(str(new_reg_susp))
                            + Decimal(str(new_pen_susp))
                            + Decimal(str(new_def_susp))
                        )
                    ),
                    loan_id, eff_date_val,
                ),
            )
