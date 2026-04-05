"""Flow statement running columns after reorder (e.g. roll-up)."""

from datetime import date
from decimal import Decimal

from reporting.statements import recalculate_flow_statement_running_balances


def test_recalculate_flow_balances_after_row_swap():
    rows = [
        {
            "Due Date": date(2025, 1, 2),
            "Narration": "a",
            "Debits": 10.0,
            "Credits": 0.0,
            "Balance": 999.0,
            "Unapplied funds": 1.0,
        },
        {
            "Due Date": date(2025, 1, 2),
            "Narration": "b",
            "Debits": 0.0,
            "Credits": 5.0,
            "Balance": 888.0,
            "Unapplied funds": 2.0,
        },
        {
            "Due Date": date(2025, 1, 3),
            "Narration": "Total outstanding (flow) as at 2025-01-03",
            "Debits": 0.0,
            "Credits": 0.0,
            "Balance": 0.0,
            "Unapplied funds": 0.0,
        },
    ]
    recalculate_flow_statement_running_balances(
        rows,
        opening_loan=Decimal("100"),
        opening_unapplied=Decimal("0"),
    )
    assert rows[0]["Balance"] == 110.0
    assert rows[1]["Balance"] == 105.0
    assert rows[2]["Balance"] == 105.0


def test_recalculate_flow_respects_unapplied_delta_before_loan_move():
    rows = [
        {
            "Due Date": date(2025, 1, 1),
            "Narration": "Unapplied credit",
            "Debits": 0.0,
            "Credits": 0.0,
            "Balance": 0.0,
            "Unapplied funds": 0.0,
            "_unapplied_delta": "3",
        },
        {
            "Due Date": date(2025, 1, 1),
            "Narration": "Repayment",
            "Debits": 0.0,
            "Credits": 2.0,
            "Balance": 0.0,
            "Unapplied funds": 0.0,
        },
    ]
    recalculate_flow_statement_running_balances(
        rows,
        opening_loan=Decimal("100"),
        opening_unapplied=Decimal("1"),
    )
    assert rows[0]["Unapplied funds"] == 4.0
    assert rows[0]["Balance"] == 100.0
    assert rows[1]["Balance"] == 98.0
    assert rows[1]["Unapplied funds"] == 4.0
