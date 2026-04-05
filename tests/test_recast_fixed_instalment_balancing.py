from datetime import date

import reamortisation as r


def test_recast_rate_normalization_supports_legacy_decimal_and_percent():
    assert r._normalize_recast_annual_rate(12) == 0.12
    assert r._normalize_recast_annual_rate(1.2) == 1.2
    assert r._normalize_recast_annual_rate(0.12) == 0.12


def test_maintain_instalment_final_balancing_keeps_first_fixed(monkeypatch):
    monkeypatch.setattr(r, "get_loan", lambda loan_id: {"id": loan_id, "annual_rate": 12, "installment": 100.0})
    monkeypatch.setattr(r, "_last_due_date_from_loan", lambda loan: date(2025, 6, 1))

    df, inst = r._build_recast_schedule_maintain_instalment(
        loan_id=1,
        recast_date=date(2025, 1, 1),
        new_principal_balance=420.0,
        fixed_instalment=100.0,
        balancing_position="final_installment",
    )

    assert inst == 100.0
    assert float(df.iloc[1]["Monthly Installment"]) == 100.0
    assert float(df.iloc[-1]["Principal Balance"]) == 0.0


def test_maintain_instalment_rejects_next_balancing(monkeypatch):
    monkeypatch.setattr(r, "get_loan", lambda loan_id: {"id": loan_id, "annual_rate": 12, "installment": 100.0})
    monkeypatch.setattr(r, "_last_due_date_from_loan", lambda loan: date(2025, 6, 1))

    try:
        r._build_recast_schedule_maintain_instalment(
            loan_id=1,
            recast_date=date(2025, 1, 1),
            new_principal_balance=420.0,
            fixed_instalment=100.0,
            balancing_position="next_installment",
        )
        assert False, "Expected ValueError for unsupported next_installment balancing."
    except ValueError as ex:
        assert "Only 'final_installment'" in str(ex)


def test_prepay_upcoming_installments_zeroes_covered_periods(monkeypatch):
    monkeypatch.setattr(r, "get_loan", lambda loan_id: {"id": loan_id, "annual_rate": 12, "installment": 100.0})
    monkeypatch.setattr(r, "_last_due_date_from_loan", lambda loan: date(2025, 6, 1))

    df, inst = r._build_recast_schedule_prepay_upcoming_installments(
        loan_id=1,
        recast_date=date(2025, 1, 1),
        new_principal_balance=420.0,
        fixed_instalment=100.0,
        prepayment_amount=220.0,
    )

    assert inst == 100.0
    # First two dues should be fully covered by prepayment pool.
    assert float(df.iloc[1]["Monthly Installment"]) == 0.0
    assert float(df.iloc[2]["Monthly Installment"]) == 0.0
    # First uncovered due becomes balancing transition (non-zero and <= fixed instalment).
    assert 0.0 < float(df.iloc[3]["Monthly Installment"]) <= 100.0


def test_prepay_upcoming_installments_resumes_contractual_due_after_pool(monkeypatch):
    monkeypatch.setattr(r, "get_loan", lambda loan_id: {"id": loan_id, "annual_rate": 12, "installment": 100.0})
    monkeypatch.setattr(r, "get_latest_schedule_version", lambda loan_id: 1)
    monkeypatch.setattr(
        r,
        "get_schedule_lines",
        lambda loan_id, version=None: [
            {"Period": 0, "Date": "01-Jan-2025"},
            {"Period": 1, "Date": "31-Jan-2025"},
            {"Period": 2, "Date": "28-Feb-2025"},
            {"Period": 3, "Date": "31-Mar-2025"},
            {"Period": 4, "Date": "30-Apr-2025"},
            {"Period": 5, "Date": "31-May-2025"},
            {"Period": 6, "Date": "30-Jun-2025"},
        ],
    )

    df, inst = r._build_recast_schedule_prepay_upcoming_installments(
        loan_id=1,
        recast_date=date(2025, 1, 1),
        new_principal_balance=650.0,
        fixed_instalment=100.0,
        prepayment_amount=220.0,
    )

    assert inst == 100.0
    dues = [float(v) for v in df["Monthly Installment"].tolist()[1:]]
    # Starting dues are reduced while pool is consumed.
    assert dues[0] == 0.0
    assert dues[1] == 0.0
    assert 0.0 < dues[2] < 100.0
    # Then dues resume to contractual instalment from replaced schedule.
    assert dues[3] == 100.0
    assert dues[4] == 100.0
    lowers = [v for v in dues if 0.0 < v < 100.0]
    assert len(lowers) <= 1


def test_recast_maintain_term_preserves_existing_due_dates(monkeypatch):
    monkeypatch.setattr(r, "get_loan", lambda loan_id: {"id": loan_id, "annual_rate": 12, "installment": 100.0})
    monkeypatch.setattr(r, "get_latest_schedule_version", lambda loan_id: 1)
    monkeypatch.setattr(
        r,
        "get_schedule_lines",
        lambda loan_id, version=None: [
            {"Period": 0, "Date": "01-Jan-2025"},
            {"Period": 1, "Date": "31-Jan-2025"},
            {"Period": 2, "Date": "28-Feb-2025"},
            {"Period": 3, "Date": "31-Mar-2025"},
            {"Period": 4, "Date": "30-Apr-2025"},
        ],
    )
    df, _ = r._build_recast_schedule(loan_id=1, recast_date=date(2025, 1, 15), new_principal_balance=300.0)
    assert list(df["Date"])[1:] == ["31-Jan-2025", "28-Feb-2025", "31-Mar-2025", "30-Apr-2025"]


def test_recast_maintain_term_keeps_equal_instalments(monkeypatch):
    monkeypatch.setattr(r, "get_loan", lambda loan_id: {"id": loan_id, "annual_rate": 12, "installment": 100.0})
    monkeypatch.setattr(r, "get_latest_schedule_version", lambda loan_id: 1)
    monkeypatch.setattr(
        r,
        "get_schedule_lines",
        lambda loan_id, version=None: [
            {"Period": 0, "Date": "01-Jan-2025"},
            {"Period": 1, "Date": "31-Jan-2025"},
            {"Period": 2, "Date": "28-Feb-2025"},
            {"Period": 3, "Date": "31-Mar-2025"},
            {"Period": 4, "Date": "30-Apr-2025"},
        ],
    )
    df, inst = r._build_recast_schedule(loan_id=1, recast_date=date(2025, 1, 15), new_principal_balance=300.0)

    dues = [float(v) for v in df["Monthly Installment"].tolist()[1:]]
    assert len(dues) == 4
    assert all(v == float(inst) for v in dues)
    assert float(df.iloc[-1]["Principal Balance"]) == 0.0


def test_recast_maintain_instalment_preserves_existing_due_dates(monkeypatch):
    monkeypatch.setattr(r, "get_loan", lambda loan_id: {"id": loan_id, "annual_rate": 12, "installment": 100.0})
    monkeypatch.setattr(r, "get_latest_schedule_version", lambda loan_id: 1)
    monkeypatch.setattr(
        r,
        "get_schedule_lines",
        lambda loan_id, version=None: [
            {"Period": 0, "Date": "01-Jan-2025"},
            {"Period": 1, "Date": "31-Jan-2025"},
            {"Period": 2, "Date": "28-Feb-2025"},
            {"Period": 3, "Date": "31-Mar-2025"},
        ],
    )
    df, _ = r._build_recast_schedule_maintain_instalment(
        loan_id=1,
        recast_date=date(2025, 1, 15),
        new_principal_balance=250.0,
        fixed_instalment=100.0,
        balancing_position="final_installment",
    )
    assert list(df["Date"])[1:4] == ["31-Jan-2025", "28-Feb-2025", "31-Mar-2025"]


def test_maintain_instalment_consumes_prepayment_from_last_due(monkeypatch):
    monkeypatch.setattr(r, "get_loan", lambda loan_id: {"id": loan_id, "annual_rate": 12, "installment": 100.0})
    monkeypatch.setattr(r, "get_latest_schedule_version", lambda loan_id: 1)
    monkeypatch.setattr(
        r,
        "get_schedule_lines",
        lambda loan_id, version=None: [
            {"Period": 0, "Date": "01-Jan-2025"},
            {"Period": 1, "Date": "31-Jan-2025"},
            {"Period": 2, "Date": "28-Feb-2025"},
            {"Period": 3, "Date": "31-Mar-2025"},
            {"Period": 4, "Date": "30-Apr-2025"},
        ],
    )

    df, inst = r.build_recast_schedule_for_mode(
        loan_id=1,
        recast_date=date(2025, 1, 1),
        new_principal_balance=300.0,
        mode="maintain_instalment",
        balancing_position="final_installment",
        prepayment_amount=220.0,
    )

    assert inst == 100.0
    dues = [float(v) for v in df["Monthly Installment"].tolist()[1:]]
    # Pool must be consumed from the tail backwards.
    assert dues[0] == 100.0
    assert 0.0 < dues[1] < 100.0
    assert dues[-1] == 0.0
    assert dues[-2] == 0.0
    lowers = [v for v in dues if 0.0 < v < 100.0]
    assert len(lowers) <= 1
