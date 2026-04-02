"""Unapplied funds: references, balances, EOD arrears application, recast, credit rows."""

from __future__ import annotations

from datetime import date
from typing import Any

from decimal_utils import as_10dp

from .db import RealDictCursor, _connection
from .loan_daily_state import get_loan_daily_state_balances, save_loan_daily_state

def _unapplied_original_reference(
    entry_kind: str,
    *,
    loan_id: int,
    repayment_id: int,
    value_date: date,
) -> str:
    """
    Deterministic reference for unapplied funds lifecycle (PDF-driven).

    Reversal references are built as: `REV-` + this string.
    """
    # Keep format stable so reversal can reference the exact original journal reference.
    return f"{entry_kind}: Repayment ID {repayment_id} on LoanID {loan_id} - {value_date.isoformat()}"


def _unapplied_reversal_reference(original_reference: str) -> str:
    """Prefix original reference with REV- per PDF reversal rule."""
    return f"REV-{original_reference}"


def get_unapplied_ledger_balance(loan_id: int, as_of_date: date) -> float:
    """Balance = SUM(amount) for loan_id, value_date <= as_of_date (ledger-style single table)."""
    return get_unapplied_balance(loan_id, as_of_date)


def get_unapplied_balance(loan_id: int, as_of_date: date) -> float:
    """Balance = SUM(amount) for the loan with value_date <= as_of_date (ledger-style)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM unapplied_funds
                WHERE loan_id = %s AND value_date <= %s
                """,
                (loan_id, as_of_date),
            )
            row = cur.fetchone()
            return float(row[0] or 0)


def get_loans_with_unapplied_balance(as_of_date: date) -> list[int]:
    """Loan IDs with unapplied balance > 0 as of the given date (ledger-style)."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT loan_id
                FROM unapplied_funds
                WHERE value_date <= %s
                GROUP BY loan_id
                HAVING COALESCE(SUM(amount), 0) > 0
                ORDER BY loan_id
                """,
                (as_of_date,),
            )
            return [int(r[0]) for r in cur.fetchall()]


def apply_unapplied_funds_to_arrears_eod(
    loan_id: int,
    as_of_date: date,
    sys_cfg: dict,
) -> float:
    """
    Apply pending unapplied funds towards arrears (waterfall order) for a loan.
    Only runs if unapplied > 0 and any arrears > 0 (interest_arrears, penalty, default, principal_arrears).
    Constraints: allocate no more than unapplied balance, no more than each bucket balance.
    Creates a system repayment and allocation with event_type='unapplied_funds_allocation'.
    Returns amount applied (0 if none).
    """
    from . import _legacy as _lm

    BUCKET_TO_ALLOC = _lm.BUCKET_TO_ALLOC
    STANDARD_SKIP_BUCKETS = _lm.STANDARD_SKIP_BUCKETS
    _get_waterfall_config = _lm._get_waterfall_config
    _post_event_for_loan = _lm._post_event_for_loan
    get_net_allocation_for_loan_date = _lm.get_net_allocation_for_loan_date
    get_unallocated_for_loan_date = _lm.get_unallocated_for_loan_date

    unapplied_as_of = get_unapplied_balance(loan_id, as_of_date)
    # To prevent double-liquidation during EOD replays, cap to the overall unapplied balance
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(amount), 0) FROM unapplied_funds WHERE loan_id = %s", (loan_id,))
            overall_unapplied = float(cur.fetchone()[0] or 0)
    unapplied = round(max(0.0, min(unapplied_as_of, overall_unapplied)), 2)

    if unapplied <= 1e-6:
        return 0.0

    state = get_loan_daily_state_balances(loan_id, as_of_date)
    if not state:
        return 0.0

    # Arrears buckets only (waterfall order, excluding interest_accrued and principal_not_due for standard)
    interest_arrears = float(state.get("interest_arrears_balance") or 0)
    penalty_balance = float(state.get("penalty_interest_balance") or 0)
    default_balance = float(state.get("default_interest_balance") or 0)
    principal_arrears = float(state.get("principal_arrears") or 0)
    fees_balance = float(state.get("fees_charges_balance") or 0)

    arrears_total = interest_arrears + penalty_balance + default_balance + principal_arrears + fees_balance
    if arrears_total <= 1e-6:
        return 0.0

    profile_key, bucket_order = _get_waterfall_config(sys_cfg)
    # Standard profile skips interest_accrued_balance and principal_not_due
    skip = STANDARD_SKIP_BUCKETS if profile_key == "standard" else ()

    target_to_apply = round(min(unapplied, arrears_total), 2)
    if target_to_apply <= 1e-6:
        return 0.0

    # Consume FIFO by source; create one debit + allocation per source for lineage
    with _connection() as conn:
        # Optional GL posting: unapplied liquidation requires transaction templates.
        try:
            from accounting_service import AccountingService
            svc_liq = AccountingService()
        except Exception:
            svc_liq = None

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH source_balances AS (
                    SELECT
                        COALESCE(uf.repayment_id, uf.source_repayment_id) AS source_repayment_id,
                        MIN(uf.value_date) AS first_value_date,
                        COALESCE(SUM(uf.amount), 0) AS overall_amount,
                        COALESCE(SUM(CASE WHEN uf.value_date <= %s THEN uf.amount ELSE 0 END), 0) AS as_of_amount
                    FROM unapplied_funds uf
                    WHERE uf.loan_id = %s
                      AND COALESCE(uf.repayment_id, uf.source_repayment_id) IS NOT NULL
                    GROUP BY COALESCE(uf.repayment_id, uf.source_repayment_id)
                )
                SELECT
                    sb.source_repayment_id AS repayment_id,
                    LEAST(sb.overall_amount, sb.as_of_amount) AS amount,
                    sb.first_value_date
                FROM source_balances sb
                WHERE sb.overall_amount > 0 AND sb.as_of_amount > 0
                ORDER BY sb.first_value_date, sb.source_repayment_id
                """,
                (as_of_date, loan_id),
            )
            credit_rows = cur.fetchall()

        # Build consumption per source (FIFO)
        remaining_to_consume = target_to_apply
        consumptions: list[tuple[int | None, float]] = []  # (source_repayment_id, amount)
        for row in credit_rows:
            if remaining_to_consume <= 1e-6:
                break
            uf_amount = float(row["amount"] or 0)
            consume = min(uf_amount, remaining_to_consume)
            consume = float(as_10dp(consume))
            if consume <= 0:
                continue
            src_rep = row.get("repayment_id")
            consumptions.append((int(src_rep) if src_rep is not None else None, consume))
            remaining_to_consume -= consume
        amount_applied = float(as_10dp(sum(c for _s, c in consumptions)))
        if amount_applied <= 1e-6:
            # Nothing actually consumable from unapplied ledger -> no mutation.
            return 0.0

        # Per-source: debit unapplied_funds only.
        # Rule: loan_repayments holds ONLY teller receipts + reversals.
        #       EOD unapplied-to-arrears events are tracked in unapplied_funds (debits)
        #       and loan_daily_state only; no system repayment or allocation row is created.
        bucket_balances: dict[str, float] = {
            "principal_not_due": float(state.get("principal_not_due") or 0),
            "principal_arrears": principal_arrears,
            "interest_accrued_balance": float(state.get("interest_accrued_balance") or 0),
            "interest_arrears_balance": interest_arrears,
            "default_interest_balance": default_balance,
            "penalty_interest_balance": penalty_balance,
            "fees_charges_balance": fees_balance,
        }
        profile_key, bucket_order = _get_waterfall_config(sys_cfg)
        skip = STANDARD_SKIP_BUCKETS if profile_key == "standard" else ()
        alloc_principal_not_due = 0.0
        alloc_principal_arrears = 0.0
        alloc_interest_accrued = 0.0
        alloc_interest_arrears = 0.0
        alloc_default_interest = 0.0
        alloc_penalty_interest = 0.0
        alloc_fees_charges = 0.0

        for src_repayment_id, consumed in consumptions:
            remaining = consumed
            src_alloc: dict[str, float] = {k: 0.0 for k in BUCKET_TO_ALLOC}

            for bucket_name in bucket_order:
                if bucket_name not in BUCKET_TO_ALLOC or bucket_name in skip:
                    continue
                alloc_key, state_key = BUCKET_TO_ALLOC[bucket_name]
                bucket_balance = max(0.0, bucket_balances.get(state_key, 0))
                to_alloc = min(remaining, bucket_balance)
                src_alloc[alloc_key] = to_alloc
                remaining -= to_alloc
                bucket_balances[state_key] = bucket_balance - to_alloc
                if remaining <= 1e-6:
                    break

            apr = src_alloc.get("alloc_principal_not_due", 0.0)
            apa = src_alloc.get("alloc_principal_arrears", 0.0)
            aia = src_alloc.get("alloc_interest_accrued", 0.0)
            aiar = src_alloc.get("alloc_interest_arrears", 0.0)
            adi = src_alloc.get("alloc_default_interest", 0.0)
            api = src_alloc.get("alloc_penalty_interest", 0.0)
            afc = src_alloc.get("alloc_fees_charges", 0.0)
            alloc_principal_not_due += apr
            alloc_principal_arrears += apa
            alloc_interest_accrued += aia
            alloc_interest_arrears += aiar
            alloc_default_interest += adi
            alloc_penalty_interest += api
            alloc_fees_charges += afc

            with conn.cursor() as cur:
                # Create a deterministic system repayment that represents this liquidation leg.
                # This allows reverse_repayment() to reverse liquidations by parent receipt.
                cur.execute(
                    """
                    INSERT INTO loan_repayments (
                        loan_id, amount, payment_date, reference, value_date, status
                    ) VALUES (%s, %s, %s, %s, %s, 'posted')
                    RETURNING id
                    """,
                    (
                        loan_id,
                        float(as_10dp(-consumed)),
                        as_of_date,
                        "Unapplied funds allocation",
                        as_of_date,
                    ),
                )
                liquidation_repayment_id = int(cur.fetchone()[0])

                # Debit unapplied_funds: link back to the source teller receipt.
                cur.execute(
                    """
                    INSERT INTO unapplied_funds (
                        loan_id, amount, value_date, entry_type, reference,
                        allocation_repayment_id, source_repayment_id, currency
                    )
                    VALUES (%s, %s, %s, 'debit', 'Applied to arrears (EOD)', %s, %s, 'USD')
                    """,
                    (
                        loan_id,
                        float(as_10dp(-consumed)),
                        as_of_date,
                        liquidation_repayment_id,
                        src_repayment_id,
                    ),
                )

                # Persist liquidation lineage for reversal cascade.
                alloc_principal_total = apr + apa
                alloc_interest_total = aia + aiar + adi + api
                alloc_fees_total = afc
                alloc_total = alloc_principal_total + alloc_interest_total + alloc_fees_total
                cur.execute(
                    """
                    INSERT INTO loan_repayment_allocation (
                        repayment_id,
                        alloc_principal_not_due, alloc_principal_arrears,
                        alloc_interest_accrued, alloc_interest_arrears,
                        alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                        alloc_principal_total, alloc_interest_total, alloc_fees_total,
                        alloc_total, unallocated,
                        event_type,
                        source_repayment_id
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        'unapplied_funds_allocation', %s
                    )
                    """,
                    (
                        liquidation_repayment_id,
                        float(as_10dp(apr)),
                        float(as_10dp(apa)),
                        float(as_10dp(aia)),
                        float(as_10dp(aiar)),
                        float(as_10dp(adi)),
                        float(as_10dp(api)),
                        float(as_10dp(afc)),
                        float(as_10dp(alloc_principal_total)),
                        float(as_10dp(alloc_interest_total)),
                        float(as_10dp(alloc_fees_total)),
                        float(as_10dp(alloc_total)),
                        float(as_10dp(0.0)),
                        src_repayment_id,
                    ),
                )

                # GL postings for liquidation: dedicated events debit unapplied_funds (not bank).
                if svc_liq is not None:
                    liq_ref = _unapplied_original_reference(
                        "liquidation",
                        loan_id=loan_id,
                        repayment_id=src_repayment_id,
                        value_date=as_of_date,
                    )
                    from decimal import Decimal
                    if apr > 1e-6:
                        _post_event_for_loan(svc_liq, loan_id, repayment_id=src_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_NOT_YET_DUE",
                            reference=liq_ref,
                            description=f"Unapplied liquidation: principal not yet due ({liq_ref})",
                            event_id=liq_ref,
                            created_by="system",
                            entry_date=as_of_date,
                            amount=Decimal(str(apr)),
                        )
                    if apa > 1e-6:
                        _post_event_for_loan(svc_liq, loan_id, repayment_id=src_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS",
                            reference=liq_ref,
                            description=f"Unapplied liquidation: principal arrears ({liq_ref})",
                            event_id=liq_ref,
                            created_by="system",
                            entry_date=as_of_date,
                            amount=Decimal(str(apa)),
                        )
                    if aia > 1e-6:
                        _post_event_for_loan(svc_liq, loan_id, repayment_id=src_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST_NOT_YET_DUE",
                            reference=liq_ref,
                            description=f"Unapplied liquidation: interest accrued ({liq_ref})",
                            event_id=liq_ref,
                            created_by="system",
                            entry_date=as_of_date,
                            amount=Decimal(str(aia)),
                        )
                    if aiar > 1e-6:
                        _post_event_for_loan(svc_liq, loan_id, repayment_id=src_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST",
                            reference=liq_ref,
                            description=f"Unapplied liquidation: interest arrears ({liq_ref})",
                            event_id=liq_ref,
                            created_by="system",
                            entry_date=as_of_date,
                            amount=Decimal(str(aiar)),
                        )
                    if adi > 1e-6:
                        _post_event_for_loan(svc_liq, loan_id, repayment_id=src_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_DEFAULT_INTEREST",
                            reference=liq_ref,
                            description=f"Unapplied liquidation: default interest ({liq_ref})",
                            event_id=liq_ref,
                            created_by="system",
                            entry_date=as_of_date,
                            amount=Decimal(str(adi)),
                        )
                    if api > 1e-6:
                        _post_event_for_loan(svc_liq, loan_id, repayment_id=src_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_PENALTY_INTEREST",
                            reference=liq_ref,
                            description=f"Unapplied liquidation: penalty interest ({liq_ref})",
                            event_id=liq_ref,
                            created_by="system",
                            entry_date=as_of_date,
                            amount=Decimal(str(api)),
                        )
                    if afc > 1e-6:
                        _post_event_for_loan(svc_liq, loan_id, repayment_id=src_repayment_id,
                            event_type="UNAPPLIED_LIQUIDATION_PASS_THROUGH_COST_RECOVERY",
                            reference=liq_ref,
                            description=f"Unapplied liquidation: fees/charges ({liq_ref})",
                            event_id=liq_ref,
                            created_by="system",
                            entry_date=as_of_date,
                            amount=Decimal(str(afc)),
                        )

        alloc_principal_total = alloc_principal_not_due + alloc_principal_arrears
        alloc_interest_total = (
            alloc_interest_accrued + alloc_interest_arrears
            + alloc_default_interest + alloc_penalty_interest
        )
        alloc_fees_total = alloc_fees_charges

        # Update loan_daily_state once (total reduction)
        new_principal_not_due = max(0.0, float(state.get("principal_not_due") or 0) - alloc_principal_not_due)
        new_principal_arrears = max(0.0, principal_arrears - alloc_principal_arrears)
        new_interest_accrued = max(0.0, float(state.get("interest_accrued_balance") or 0) - alloc_interest_accrued)
        new_interest_arrears = max(0.0, interest_arrears - alloc_interest_arrears)
        new_default_interest = max(0.0, default_balance - alloc_default_interest)
        new_penalty_interest = max(0.0, penalty_balance - alloc_penalty_interest)
        new_fees_charges = max(0.0, fees_balance - alloc_fees_charges)

        # Use the daily accrual values already saved by EOD for this date.
        # Never re-derive from the engine (which ignores grace periods and allocations).
        daily_state = get_loan_daily_state_balances(loan_id, as_of_date)
        if daily_state:
            reg_daily  = float(daily_state.get("regular_interest_daily", 0) or 0)
            def_daily  = float(daily_state.get("default_interest_daily", 0) or 0)
            pen_daily  = float(daily_state.get("penalty_interest_daily", 0) or 0)
            reg_period = float(daily_state.get("regular_interest_period_to_date", 0) or 0)
            def_period = float(daily_state.get("default_interest_period_to_date", 0) or 0)
            pen_period = float(daily_state.get("penalty_interest_period_to_date", 0) or 0)
        else:
            reg_daily = def_daily = pen_daily = 0.0
            reg_period = def_period = pen_period = 0.0

        arrears_after = new_interest_arrears + new_default_interest + new_penalty_interest + new_principal_arrears
        days_overdue = int(state.get("days_overdue") or 0)
        if arrears_after <= 1e-6:
            days_overdue = 0

        net_alloc = get_net_allocation_for_loan_date(loan_id, as_of_date, conn=conn)
        unalloc = get_unallocated_for_loan_date(loan_id, as_of_date, conn=conn)

        open_reg_susp = float(state.get("regular_interest_in_suspense_balance") or 0)
        new_reg_susp = max(0.0, float(as_10dp(open_reg_susp - alloc_interest_accrued)))
        new_pen_susp = max(0.0, float(as_10dp(new_penalty_interest)))
        new_def_susp = max(0.0, float(as_10dp(new_default_interest)))

        save_loan_daily_state(
            loan_id=loan_id,
            as_of_date=as_of_date,
            regular_interest_daily=reg_daily,
            principal_not_due=new_principal_not_due,
            principal_arrears=new_principal_arrears,
            interest_accrued_balance=new_interest_accrued,
            interest_arrears_balance=new_interest_arrears,
            default_interest_daily=def_daily,
            default_interest_balance=new_default_interest,
            penalty_interest_daily=pen_daily,
            penalty_interest_balance=new_penalty_interest,
            fees_charges_balance=new_fees_charges,
            days_overdue=days_overdue,
            regular_interest_period_to_date=reg_period,
            penalty_interest_period_to_date=pen_period,
            default_interest_period_to_date=def_period,
            net_allocation=net_alloc,
            unallocated=unalloc,
            regular_interest_in_suspense_balance=new_reg_susp,
            penalty_interest_in_suspense_balance=new_pen_susp,
            default_interest_in_suspense_balance=new_def_susp,
            conn=conn,
        )

    return amount_applied


def get_unapplied_entries(loan_id: int, through_date: date) -> list[tuple[date, float]]:
    """
    Unapplied entries for statement from loan_repayment_allocation.unallocated only.
    Returns list of (value_date, amount) for in-memory aggregation.
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (COALESCE(lr.value_date, lr.payment_date))::date AS value_date,
                    COALESCE(lra.unallocated, 0) AS amount
                FROM loan_repayments lr
                LEFT JOIN loan_repayment_allocation lra ON lra.repayment_id = lr.id
                WHERE lr.loan_id = %s
                  AND (COALESCE(lr.value_date, lr.payment_date))::date <= %s
                  AND NOT (
                    COALESCE(lr.reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.customer_reference, '') ILIKE '%%napplied funds allocation%%'
                    OR COALESCE(lr.company_reference, '') ILIKE '%%napplied funds allocation%%'
                  )
                ORDER BY value_date, lr.id
                """,
                (loan_id, through_date),
            )
            rows = cur.fetchall()
    out: list[tuple[date, float]] = []
    for r in rows:
        vd = r[0]
        if hasattr(vd, "date"):
            vd = vd.date() if callable(getattr(vd, "date")) else vd
        out.append((vd, float(r[1] or 0)))
    return out


def get_unapplied_ledger_entries_for_statement(
    loan_id: int,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """
    Statement-facing unapplied ledger lines from the unapplied_funds_ledger view.
    This view mirrors the export ledger logic:
      - Credits/reversals from receipt allocation (loan_repayment_allocation totals vs amount)
      - Liquidations from loan_repayment_allocation with event_type='unapplied_funds_allocation'
    Returns entry_kind in ('credit','reversal','liquidation') with unapplied_running_balance.
    Includes all entries with value_date <= end_date so running balance is correct.
    """
    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    ufl.repayment_id,
                    ufl.repayment_key,
                    ufl.loan_id,
                    ufl.value_date,
                    ufl.entry_kind,
                    ufl.liquidation_repayment_id,
                    ufl.unapplied_delta,
                    ufl.alloc_prin_arrears,
                    ufl.alloc_int_arrears,
                    ufl.alloc_penalty_int,
                    ufl.alloc_default_int,
                    ufl.alloc_fees_charges,
                    ufl.unapplied_running_balance,
                    ufl.parent_repayment_id,
                    ufl.reversal_of_id,
                    lr.amount AS source_receipt_amount
                FROM unapplied_funds_ledger ufl
                LEFT JOIN loan_repayments lr ON lr.id = ufl.repayment_id
                WHERE ufl.loan_id = %s
                  AND ufl.value_date <= %s
                ORDER BY ufl.value_date, ufl.repayment_id, ufl.entry_kind
                """,
                (loan_id, end_date),
            )
            return [dict(r) for r in cur.fetchall()]


def get_unapplied_repayment_ids(loan_id: int, as_of_date: date) -> set[int]:
    """Repayment IDs that created unapplied credits for this loan with value_date <= as_of_date."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT repayment_id FROM unapplied_funds
                WHERE loan_id = %s AND value_date <= %s AND amount > 0 AND repayment_id IS NOT NULL
                """,
                (loan_id, as_of_date),
            )
            return {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}


def apply_unapplied_funds_recast(
    unapplied_funds_id: int,
    *,
    as_of: date | None = None,
) -> None:
    """
    Apply unapplied funds to the loan via recast logic (separate from allocation).
    Reclassifies accrued interest → interest arrears and principal not due → principal arrears
    (up to the unapplied amount), then applies the payment to those buckets. Call only after
    funds have been credited to Unapplied (e.g. from Unapplied tab).
    """
    from . import _legacy as _lm

    _post_event_for_loan = _lm._post_event_for_loan
    get_net_allocation_for_loan_date = _lm.get_net_allocation_for_loan_date
    get_unallocated_for_loan_date = _lm.get_unallocated_for_loan_date

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, loan_id, amount, value_date, repayment_id
                FROM unapplied_funds
                WHERE id = %s
                """,
                (unapplied_funds_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} not found.")
            if float(row.get("amount") or 0) <= 0:
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} has no positive balance (already consumed or debit).")
            # Prevent double-apply: check if debit already exists for this credit
            cur.execute(
                "SELECT 1 FROM unapplied_funds WHERE source_unapplied_id = %s LIMIT 1",
                (unapplied_funds_id,),
            )
            if cur.fetchone():
                raise ValueError(f"Unapplied funds entry {unapplied_funds_id} was already applied via recast.")
            loan_id = int(row["loan_id"])
            amount = float(row["amount"])
            value_date = row["value_date"]
            if hasattr(value_date, "date"):
                value_date = value_date.date()
            eff_date = as_of or value_date
            source_repayment_id = int(row["repayment_id"]) if row.get("repayment_id") is not None else None
            if source_repayment_id is None:
                raise ValueError("Recast requires source repayment_id on the unapplied credit row.")

            # Ensure exact-date state exists before any mutation.
            from eod import run_single_loan_eod
            run_single_loan_eod(loan_id, eff_date)

            cur.execute(
                """
                SELECT as_of_date, principal_not_due, principal_arrears, interest_accrued_balance,
                       interest_arrears_balance, default_interest_balance,
                       penalty_interest_balance, fees_charges_balance, days_overdue,
                       regular_interest_daily, default_interest_daily, penalty_interest_daily,
                       regular_interest_period_to_date, penalty_interest_period_to_date, default_interest_period_to_date,
                       COALESCE(regular_interest_in_suspense_balance, 0) AS regular_interest_in_suspense_balance,
                       COALESCE(penalty_interest_in_suspense_balance, 0) AS penalty_interest_in_suspense_balance,
                       COALESCE(default_interest_in_suspense_balance, 0) AS default_interest_in_suspense_balance
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date = %s
                FOR UPDATE
                """,
                (loan_id, eff_date),
            )
            st_row = cur.fetchone()
            if not st_row:
                raise ValueError(f"No exact loan_daily_state for loan_id={loan_id} on {eff_date}.")
            balances = {k: float(st_row.get(k, 0) or 0) for k in (
                "principal_not_due", "principal_arrears", "interest_accrued_balance",
                "interest_arrears_balance", "default_interest_balance",
                "penalty_interest_balance", "fees_charges_balance", "days_overdue"
            )}
            # Preserve accruals for eff_date: use existing row if it's for eff_date, else engine state
            st_row_as_of = st_row.get("as_of_date")
            if hasattr(st_row_as_of, "date"):
                st_row_as_of = st_row_as_of.date() if callable(getattr(st_row_as_of, "date")) else st_row_as_of
            if st_row_as_of == eff_date:
                acc_daily = (float(st_row.get("regular_interest_daily") or 0), float(st_row.get("default_interest_daily") or 0), float(st_row.get("penalty_interest_daily") or 0))
                acc_period = (float(st_row.get("regular_interest_period_to_date") or 0), float(st_row.get("penalty_interest_period_to_date") or 0), float(st_row.get("default_interest_period_to_date") or 0))
            else:
                from eod import get_engine_state_for_loan_date
                _eng = get_engine_state_for_loan_date(loan_id, eff_date)
                if _eng:
                    acc_daily = (_eng.get("regular_interest_daily", 0) or 0, _eng.get("default_interest_daily", 0) or 0, _eng.get("penalty_interest_daily", 0) or 0)
                    acc_period = (_eng.get("regular_interest_period_to_date", 0) or 0, _eng.get("penalty_interest_period_to_date", 0) or 0, _eng.get("default_interest_period_to_date", 0) or 0)
                else:
                    acc_daily = (0.0, 0.0, 0.0)
                    acc_period = (0.0, 0.0, 0.0)

            remaining = amount
            move_accrued_to_arrears = min(balances["interest_accrued_balance"], remaining)
            move_accrued_to_arrears = round(move_accrued_to_arrears, 2)
            remaining -= move_accrued_to_arrears
            move_principal_not_due_to_arrears = 0.0
            if remaining > 1e-6:
                move_principal_not_due_to_arrears = min(balances["principal_not_due"], remaining)
                move_principal_not_due_to_arrears = round(move_principal_not_due_to_arrears, 2)

            new_interest_accrued = round(balances["interest_accrued_balance"] - move_accrued_to_arrears, 2)
            new_interest_arrears = round(balances["interest_arrears_balance"] - move_accrued_to_arrears, 2)
            new_principal_not_due = round(balances["principal_not_due"] - move_principal_not_due_to_arrears, 2)
            new_principal_arrears = round(balances["principal_arrears"] - move_principal_not_due_to_arrears, 2)
            net_alloc = get_net_allocation_for_loan_date(loan_id, eff_date, conn=conn)
            unalloc = get_unallocated_for_loan_date(loan_id, eff_date, conn=conn)
            liquidation_amount = move_accrued_to_arrears + move_principal_not_due_to_arrears
            # Safety: recast should not consume a different total than we allocate to
            # the liquidation buckets; otherwise reversal cascade would not reconcile.
            if abs(float(as_10dp(liquidation_amount)) - float(as_10dp(amount))) > 1e-6:
                raise ValueError(
                    "Recast allocation mismatch: unapplied amount does not equal "
                    "principal_not_due_to_arrears + interest_accrued_to_arrears."
                )

            rec_reg_susp = float(st_row.get("regular_interest_in_suspense_balance") or 0)
            rec_pen_susp = float(balances["penalty_interest_balance"])
            rec_def_susp = float(balances["default_interest_balance"])

            save_loan_daily_state(
                loan_id=loan_id,
                as_of_date=eff_date,
                regular_interest_daily=acc_daily[0],
                principal_not_due=new_principal_not_due,
                principal_arrears=new_principal_arrears,
                interest_accrued_balance=new_interest_accrued,
                interest_arrears_balance=new_interest_arrears,
                default_interest_daily=acc_daily[1],
                default_interest_balance=balances["default_interest_balance"],
                penalty_interest_daily=acc_daily[2],
                penalty_interest_balance=balances["penalty_interest_balance"],
                fees_charges_balance=balances["fees_charges_balance"],
                days_overdue=int(balances["days_overdue"]),
                regular_interest_period_to_date=acc_period[0],
                penalty_interest_period_to_date=acc_period[1],
                default_interest_period_to_date=acc_period[2],
                net_allocation=net_alloc,
                unallocated=unalloc,
                regular_interest_in_suspense_balance=rec_reg_susp,
                penalty_interest_in_suspense_balance=rec_pen_susp,
                default_interest_in_suspense_balance=rec_def_susp,
                conn=conn,
            )

            # Create a deterministic system repayment representing this recast liquidation leg.
            try:
                from accounting_service import AccountingService
                svc_unapplied = AccountingService()
            except Exception:
                svc_unapplied = None

            cur.execute(
                """
                INSERT INTO loan_repayments (
                    loan_id, amount, payment_date, reference, value_date, status
                ) VALUES (%s, %s, %s, %s, %s, 'posted')
                RETURNING id
                """,
                (
                    loan_id,
                    float(as_10dp(-liquidation_amount)),
                    eff_date,
                    "Unapplied funds allocation",
                    eff_date,
                ),
            )
            liquidation_repayment_id = int(cur.fetchone()[0])

            # Insert liquidation lineage allocation so reverse_repayment can cascade.
            cur.execute(
                """
                INSERT INTO loan_repayment_allocation (
                    repayment_id,
                    alloc_principal_not_due, alloc_principal_arrears,
                    alloc_interest_accrued, alloc_interest_arrears,
                    alloc_default_interest, alloc_penalty_interest, alloc_fees_charges,
                    alloc_principal_total, alloc_interest_total, alloc_fees_total,
                    alloc_total, unallocated,
                    event_type,
                    source_repayment_id
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    'unapplied_funds_allocation', %s
                )
                """,
                (
                    liquidation_repayment_id,
                    float(as_10dp(0.0)),
                    float(as_10dp(move_principal_not_due_to_arrears)),
                    float(as_10dp(0.0)),
                    float(as_10dp(move_accrued_to_arrears)),
                    float(as_10dp(0.0)),
                    float(as_10dp(0.0)),
                    float(as_10dp(0.0)),
                    float(as_10dp(move_principal_not_due_to_arrears)),
                    float(as_10dp(move_accrued_to_arrears)),
                    float(as_10dp(0.0)),
                    float(as_10dp(liquidation_amount)),
                    float(as_10dp(0.0)),
                    source_repayment_id,
                ),
            )

            liq_ref = _unapplied_original_reference(
                "liquidation",
                loan_id=loan_id,
                repayment_id=source_repayment_id,
                value_date=eff_date,
            )

            # Insert debit row (ledger-style, no UPDATE)
            cur.execute(
                """
                INSERT INTO unapplied_funds (
                    loan_id, amount, value_date, entry_type, reference,
                    allocation_repayment_id, source_repayment_id, source_unapplied_id, currency
                )
                VALUES (%s, %s, %s, 'debit', 'Applied via recast', %s, %s, %s, 'USD')
                """,
                (
                    loan_id,
                    float(as_10dp(-liquidation_amount)),
                    eff_date,
                    liquidation_repayment_id,
                    source_repayment_id,
                    unapplied_funds_id,
                ),
            )

            # GL postings for recast liquidation.
            # For recast, liquidated amounts land in (principal_arrears, interest_arrears).
            if svc_unapplied is not None:
                from decimal import Decimal

                if move_principal_not_due_to_arrears > 1e-6:
                    _post_event_for_loan(
                        svc_unapplied,
                        loan_id,
                        repayment_id=source_repayment_id,
                        event_type="UNAPPLIED_LIQUIDATION_PRINCIPAL_ARREARS",
                        reference=liq_ref,
                        description=f"Recast liquidation: principal arrears ({liq_ref})",
                        event_id=liq_ref,
                        created_by="system",
                        entry_date=eff_date,
                        amount=Decimal(str(move_principal_not_due_to_arrears)),
                    )
                if move_accrued_to_arrears > 1e-6:
                    _post_event_for_loan(
                        svc_unapplied,
                        loan_id,
                        repayment_id=source_repayment_id,
                        event_type="UNAPPLIED_LIQUIDATION_REGULAR_INTEREST",
                        reference=liq_ref,
                        description=f"Recast liquidation: interest arrears ({liq_ref})",
                        event_id=liq_ref,
                        created_by="system",
                        entry_date=eff_date,
                        amount=Decimal(str(move_accrued_to_arrears)),
                    )



def _credit_unapplied_funds(
    conn,
    loan_id: int,
    repayment_id: int,
    amount: float,
    value_date: date,
    currency: str = "USD",
) -> None:
    """Insert a credit row into unapplied_funds (ledger-style, append-only)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO unapplied_funds (loan_id, repayment_id, amount, currency, value_date, entry_type, reference)
            VALUES (%s, %s, %s, %s, %s, 'credit', 'Overpayment')
            """,
            (loan_id, repayment_id, float(as_10dp(amount)), currency, value_date),
        )


