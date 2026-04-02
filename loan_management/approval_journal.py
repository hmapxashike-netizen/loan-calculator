"""LOAN_APPROVAL journal payload amounts (double-entry alignment with fees)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from decimal_utils import as_10dp


def build_loan_approval_journal_payload(details: dict[str, Any]) -> dict[str, Decimal]:
    """
    Amounts for LOAN_APPROVAL (Dr loan principal, Cr cash, Cr deferred fee liability).

    Total debits must equal total credits: gross loan asset at inception must equal
    cash disbursed plus deferred fees. Using only facility `principal` for the debit
    breaks double-entry when that field holds net disbursed while fees are non-zero.

    Schema intent (loan fee columns): disbursed_amount + fee amounts aligns with facility.
    Here the principal debit is disbursed + deferred fees so journals always balance.
    """
    prin_amt = Decimal(str(as_10dp(details.get("principal", details.get("facility", 0)))))
    disb_amt = Decimal(str(as_10dp(details.get("disbursed_amount", details.get("principal", 0)))))

    drawdown_fee = Decimal(
        str(
            as_10dp(
                details.get("drawdown_fee_amount")
                or (float(prin_amt) * float(details.get("drawdown_fee") or 0))
            )
        )
    )
    arrangement_fee = Decimal(
        str(
            as_10dp(
                details.get("arrangement_fee_amount")
                or (float(prin_amt) * float(details.get("arrangement_fee") or 0))
            )
        )
    )
    admin_fee = Decimal(
        str(
            as_10dp(
                details.get("admin_fee_amount")
                or (float(prin_amt) * float(details.get("admin_fee") or 0))
            )
        )
    )
    total_fees = as_10dp(drawdown_fee + arrangement_fee + admin_fee)
    disb_amt = as_10dp(disb_amt)
    gross_loan_principal = as_10dp(disb_amt + total_fees)

    return {
        "loan_principal": gross_loan_principal,
        "cash_operating": disb_amt,
        "deferred_fee_liability": total_fees,
    }
