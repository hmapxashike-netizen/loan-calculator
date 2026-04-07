"""GL posting helpers for loan modification approval (write-off, top-up)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp

from .approval_journal import build_loan_approval_journal_payload
from .cash_gl import _post_event_for_loan
from .db import RealDictCursor, _connection

_CAPITALISE_ORDER: tuple[tuple[str, str], ...] = (
    ("principal_arrears", "principal_arrears"),
    ("interest_accrued_balance", "regular_interest_accrued"),
    ("interest_arrears_balance", "regular_interest_arrears"),
    ("default_interest_balance", "default_interest_asset"),
    ("penalty_interest_balance", "penalty_interest_asset"),
    ("fees_charges_balance", "fees_charges_arrears"),
)


def post_principal_writeoff_for_loan(
    loan_id: int,
    amount: Decimal | float,
    *,
    entry_date: date,
    created_by: str = "system",
    unique_suffix: str = "",
) -> None:
    """Post PRINCIPAL_WRITEOFF journals (allowance vs loan principal) when templates exist."""
    if amount is None or float(amount) <= 0:
        return
    amt = as_10dp(Decimal(str(amount)))
    payload: dict[str, Any] = {
        "allowance_credit_losses": amt,
        "loan_principal": amt,
    }
    try:
        from accounting.service import AccountingService

        svc = AccountingService()
        _post_event_for_loan(
            svc,
            int(loan_id),
            event_type="PRINCIPAL_WRITEOFF",
            reference=f"MOD-WRITELOFF-{loan_id}",
            description=f"Loan modification principal write-off (loan {loan_id})",
            event_id=f"MOD-WO-{loan_id}-{entry_date.isoformat()}-{unique_suffix or 'x'}",
            created_by=created_by,
            entry_date=entry_date,
            payload=payload,
        )
    except Exception as e:
        print(f"post_principal_writeoff_for_loan failed: {e}")


def post_modification_topup_disbursement(
    loan_id: int,
    topup_amount: Decimal | float,
    *,
    entry_date: date,
    cash_gl_account_id: str | None = None,
    created_by: str = "system",
    unique_suffix: str = "",
) -> None:
    """
    Post additional disbursement as LOAN_APPROVAL-style entry (Dr principal, Cr cash).
    Fees zero; gross principal increase equals cash out.
    """
    if topup_amount is None or float(topup_amount) <= 0:
        return
    ta = float(as_10dp(Decimal(str(topup_amount))))
    details = {
        "principal": ta,
        "disbursed_amount": ta,
        "drawdown_fee": 0.0,
        "arrangement_fee": 0.0,
        "admin_fee": 0.0,
    }
    payload: dict[str, Any] = dict(build_loan_approval_journal_payload(details))
    _cash = str(cash_gl_account_id or "").strip()
    if _cash:
        payload["account_overrides"] = {"cash_operating": _cash}
    try:
        from accounting.service import AccountingService

        svc = AccountingService()
        _post_event_for_loan(
            svc,
            int(loan_id),
            event_type="LOAN_APPROVAL",
            reference=f"MOD-TOPUP-{loan_id}",
            description=f"Loan modification top-up disbursement (loan {loan_id})",
            event_id=f"MOD-TOPUP-{loan_id}-{entry_date.isoformat()}-{unique_suffix or 'x'}",
            created_by=created_by,
            entry_date=entry_date,
            payload=payload,
        )
    except Exception as e:
        print(f"post_modification_topup_disbursement failed: {e}")


def post_restructure_fee_charge_for_loan(
    loan_id: int,
    fee_amount: Decimal | float,
    *,
    entry_date: date,
    created_by: str = "system",
    unique_suffix: str = "",
) -> None:
    """
    Post restructure fee charge:
      DR loan principal, CR deferred fee liability.
    """
    if fee_amount is None or float(fee_amount) <= 0:
        return
    fa = float(as_10dp(Decimal(str(fee_amount))))
    payload = {
        "loan_principal": as_10dp(fa),
        "deferred_fee_liability": as_10dp(fa),
    }
    try:
        from accounting.service import AccountingService

        svc = AccountingService()
        _post_event_for_loan(
            svc,
            int(loan_id),
            event_type="RESTRUCTURE_FEE_CHARGE",
            reference=f"MOD-RESTRUCT-FEE-{loan_id}",
            description=f"Loan modification restructure fee charge (loan {loan_id})",
            event_id=f"MOD-RFEE-{loan_id}-{entry_date.isoformat()}-{unique_suffix or 'x'}",
            created_by=created_by,
            entry_date=entry_date,
            payload=payload,
        )
    except Exception as e:
        print(f"post_restructure_fee_charge_for_loan failed: {e}")


def execute_restructure_capitalisation_for_loan(
    loan_id: int,
    *,
    restructure_date: date,
    restructure_amount: float,
    created_by: str = "system",
    unique_suffix: str = "",
) -> dict[str, float]:
    """
    Move non-principal balances into principal_not_due so restructure amount is fully supported.

    Journals:
      DR loan_principal (principal not due branch)
      CR each bucket moved from (principal arrears / interest / fees buckets)
    """
    target = float(as_10dp(restructure_amount or 0.0))
    if target <= 1e-10:
        return {"moved_total": 0.0, "needed": 0.0}

    with _connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM loan_daily_state
                WHERE loan_id = %s AND as_of_date = %s
                FOR UPDATE
                """,
                (int(loan_id), restructure_date),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(
                    f"No loan_daily_state row for loan {loan_id} on {restructure_date}. "
                    "Run EOD through restructure date first."
                )
            st = dict(row)
            pnd = float(as_10dp(st.get("principal_not_due") or 0.0))
            needed = float(as_10dp(max(0.0, target - pnd)))
            if needed <= 1e-10:
                return {"moved_total": 0.0, "needed": 0.0}

            moved_by_state: dict[str, float] = {}
            moved_by_tag: dict[str, float] = {}
            rem = needed
            for state_key, tag in _CAPITALISE_ORDER:
                avail = float(as_10dp(st.get(state_key) or 0.0))
                if rem <= 1e-10:
                    break
                take = float(as_10dp(min(rem, avail)))
                if take <= 1e-10:
                    continue
                moved_by_state[state_key] = take
                moved_by_tag[tag] = float(as_10dp(moved_by_tag.get(tag, 0.0) + take))
                rem = float(as_10dp(rem - take))

            if rem > 1e-6:
                raise ValueError(
                    "Restructure amount requires capitalisation beyond available balances. "
                    f"Needed additional {rem:,.10f}."
                )

            moved_total = float(as_10dp(sum(moved_by_state.values())))
            new_pnd = float(as_10dp(pnd + moved_total))
            new_prin_arr = float(
                as_10dp(
                    max(
                        0.0,
                        float(as_10dp(st.get("principal_arrears") or 0.0))
                        - float(as_10dp(moved_by_state.get("principal_arrears") or 0.0)),
                    )
                )
            )
            new_int_acc = float(
                as_10dp(
                    max(
                        0.0,
                        float(as_10dp(st.get("interest_accrued_balance") or 0.0))
                        - float(as_10dp(moved_by_state.get("interest_accrued_balance") or 0.0)),
                    )
                )
            )
            new_int_arr = float(
                as_10dp(
                    max(
                        0.0,
                        float(as_10dp(st.get("interest_arrears_balance") or 0.0))
                        - float(as_10dp(moved_by_state.get("interest_arrears_balance") or 0.0)),
                    )
                )
            )
            new_def = float(
                as_10dp(
                    max(
                        0.0,
                        float(as_10dp(st.get("default_interest_balance") or 0.0))
                        - float(as_10dp(moved_by_state.get("default_interest_balance") or 0.0)),
                    )
                )
            )
            new_pen = float(
                as_10dp(
                    max(
                        0.0,
                        float(as_10dp(st.get("penalty_interest_balance") or 0.0))
                        - float(as_10dp(moved_by_state.get("penalty_interest_balance") or 0.0)),
                    )
                )
            )
            new_fees = float(
                as_10dp(
                    max(
                        0.0,
                        float(as_10dp(st.get("fees_charges_balance") or 0.0))
                        - float(as_10dp(moved_by_state.get("fees_charges_balance") or 0.0)),
                    )
                )
            )
            old_reg_susp = float(as_10dp(st.get("regular_interest_in_suspense_balance") or 0.0))
            old_pen_susp = float(as_10dp(st.get("penalty_interest_in_suspense_balance") or 0.0))
            old_def_susp = float(as_10dp(st.get("default_interest_in_suspense_balance") or 0.0))
            new_reg_susp = float(
                as_10dp(max(0.0, old_reg_susp - float(as_10dp(moved_by_state.get("interest_accrued_balance") or 0.0))))
            )
            new_pen_susp = float(
                as_10dp(max(0.0, old_pen_susp - float(as_10dp(moved_by_state.get("penalty_interest_balance") or 0.0))))
            )
            new_def_susp = float(
                as_10dp(max(0.0, old_def_susp - float(as_10dp(moved_by_state.get("default_interest_balance") or 0.0))))
            )
            total_exp = float(as_10dp(new_pnd + new_prin_arr + new_int_acc + new_int_arr + new_def + new_pen + new_fees))
            delin = float(as_10dp(new_prin_arr + new_int_arr + new_def + new_pen + new_fees))
            days_overdue = int(st.get("days_overdue") or 0)
            if delin <= 1e-10:
                days_overdue = 0

            cur.execute(
                """
                UPDATE loan_daily_state
                SET principal_not_due = %s,
                    principal_arrears = %s,
                    interest_accrued_balance = %s,
                    interest_arrears_balance = %s,
                    default_interest_balance = %s,
                    penalty_interest_balance = %s,
                    fees_charges_balance = %s,
                    days_overdue = %s,
                    total_delinquency_arrears = %s,
                    total_exposure = %s,
                    regular_interest_in_suspense_balance = %s,
                    penalty_interest_in_suspense_balance = %s,
                    default_interest_in_suspense_balance = %s,
                    total_interest_in_suspense_balance = %s
                WHERE loan_id = %s AND as_of_date = %s
                """,
                (
                    new_pnd,
                    new_prin_arr,
                    new_int_acc,
                    new_int_arr,
                    new_def,
                    new_pen,
                    new_fees,
                    days_overdue,
                    delin,
                    total_exp,
                    new_reg_susp,
                    new_pen_susp,
                    new_def_susp,
                    float(as_10dp(new_reg_susp + new_pen_susp + new_def_susp)),
                    int(loan_id),
                    restructure_date,
                ),
            )
            if cur.rowcount == 0:
                raise ValueError("Could not update loan_daily_state for restructuring capitalisation.")

            payload: dict[str, Any] = {"loan_principal": as_10dp(moved_total)}
            payload.update({k: as_10dp(v) for k, v in moved_by_tag.items() if float(v) > 1e-10})
            try:
                from accounting.service import AccountingService

                svc = AccountingService()
                _post_event_for_loan(
                    svc,
                    int(loan_id),
                    event_type="LOAN_RESTRUCTURE_CAPITALISE",
                    reference=f"MOD-CAP-{loan_id}",
                    description=f"Loan modification capitalisation (loan {loan_id})",
                    event_id=f"MOD-CAP-{loan_id}-{restructure_date.isoformat()}-{unique_suffix or 'x'}",
                    created_by=created_by,
                    entry_date=restructure_date,
                    payload=payload,
                )
            except Exception as e:
                raise ValueError(
                    f"Capitalisation journals could not be posted. Check LOAN_RESTRUCTURE_CAPITALISE templates. ({e})"
                ) from e

    out = {"moved_total": moved_total, "needed": needed, "new_principal_not_due": new_pnd}
    for k, v in moved_by_tag.items():
        out[f"moved_{k}"] = float(as_10dp(v))
    return out
