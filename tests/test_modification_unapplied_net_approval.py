from __future__ import annotations

from contextlib import contextmanager

import loan_management.approval_drafts as ad
import loan_management as lm
import reamortisation as r


class _DummyCursor:
    def execute(self, *_args, **_kwargs):
        return None

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyConn:
    def cursor(self, *args, **kwargs):
        return _DummyCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@contextmanager
def _dummy_connection():
    yield _DummyConn()


def test_modification_approval_liquidates_before_apply(monkeypatch):
    calls: list[str] = []
    unapplied = iter([{"eligible_total": 10.0}, {"eligible_total": 0.0}])
    monkeypatch.setattr(
        ad,
        "get_loan_approval_draft",
        lambda draft_id: {
            "id": draft_id,
            "status": "PENDING",
            "loan_id": 7,
            "loan_type": "term_loan",
            "customer_id": 1,
            "schedule_json": [{"Period": 0, "Date": "01-Jan-2025"}],
            "details_json": {
                "approval_action": "LOAN_MODIFICATION",
                "restructure_date": "2026-04-05",
                "carry_amount": "80.00",
                "topup_amount": "0.00",
                "modification_loan_details": {},
            },
        },
    )
    monkeypatch.setattr(ad, "_ensure_loan_approval_drafts_table", lambda conn: None)
    monkeypatch.setattr(ad, "_connection", _dummy_connection)
    monkeypatch.setattr(ad, "get_loan_daily_state_balances_for_recast_preview", lambda loan_id, rd: ({"total_exposure": 100.0}, rd))
    monkeypatch.setattr(ad, "get_unapplied_balance_for_restructure", lambda loan_id, rd: next(unapplied))
    monkeypatch.setattr(
        ad,
        "execute_unapplied_liquidation_for_restructure",
        lambda loan_id, rd, **kwargs: calls.append("liquidate"),
    )
    monkeypatch.setattr(
        ad,
        "execute_restructure_capitalisation_for_loan",
        lambda *args, **kwargs: calls.append("capitalise"),
    )
    monkeypatch.setattr(ad, "post_principal_writeoff_for_loan", lambda *args, **kwargs: calls.append("writeoff"))
    monkeypatch.setattr(ad, "post_modification_topup_disbursement", lambda *args, **kwargs: calls.append("topup"))
    monkeypatch.setattr(lm, "get_loan", lambda loan_id: {"id": loan_id})
    monkeypatch.setattr(
        r,
        "apply_loan_modification_from_approval_schedule",
        lambda *args, **kwargs: calls.append("apply"),
    )

    out = ad.approve_loan_approval_draft(1, approved_by="tester")
    assert out == 7
    assert "liquidate" in calls and "capitalise" in calls and "apply" in calls
    assert calls.index("liquidate") < calls.index("apply")
    assert calls.index("capitalise") < calls.index("apply")


def test_modification_approval_rejects_when_carry_exceeds_current_net(monkeypatch):
    monkeypatch.setattr(
        ad,
        "get_loan_approval_draft",
        lambda draft_id: {
            "id": draft_id,
            "status": "PENDING",
            "loan_id": 9,
            "loan_type": "term_loan",
            "customer_id": 1,
            "schedule_json": [{"Period": 0, "Date": "01-Jan-2025"}],
            "details_json": {
                "approval_action": "LOAN_MODIFICATION",
                "restructure_date": "2026-04-05",
                "carry_amount": "85.00",
                "topup_amount": "0.00",
                "modification_loan_details": {},
            },
        },
    )
    monkeypatch.setattr(ad, "get_loan_daily_state_balances_for_recast_preview", lambda loan_id, rd: ({"total_exposure": 100.0}, rd))
    monkeypatch.setattr(ad, "get_unapplied_balance_for_restructure", lambda loan_id, rd: {"eligible_total": 20.0})
    monkeypatch.setattr(
        ad,
        "execute_unapplied_liquidation_for_restructure",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not liquidate")),
    )
    monkeypatch.setattr(
        ad,
        "execute_restructure_capitalisation_for_loan",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not capitalise")),
    )

    try:
        ad.approve_loan_approval_draft(2, approved_by="tester")
        assert False, "Expected net-cap validation failure."
    except ValueError as ex:
        assert "exceeds current Net" in str(ex)


def test_split_modification_keeps_topup_and_runs_liquidation(monkeypatch):
    calls: list[str] = []
    ids = iter([101, 102])
    unapplied = iter([{"eligible_total": 20.0}, {"eligible_total": 0.0}])
    monkeypatch.setattr(
        ad,
        "get_loan_approval_draft",
        lambda draft_id: {
            "id": draft_id,
            "status": "PENDING",
            "loan_id": 11,
            "loan_type": "term_loan",
            "customer_id": 1,
            "product_code": "P1",
            "schedule_json": [{"Period": 0, "Date": "01-Jan-2025"}],
            "schedule_json_secondary": [{"Period": 0, "Date": "01-Jan-2025"}],
            "details_json": {
                "approval_action": "LOAN_MODIFICATION_SPLIT",
                "restructure_date": "2026-04-05",
                "topup_amount": "5.00",
                "split_net_by_leg": ["50.00", "20.00"],
                "split_leg_count": 2,
                "split_loan_details_list": [{}, {}],
                "split_loan_types": ["term_loan", "term_loan"],
                "split_product_codes": ["P1", "P1"],
            },
        },
    )
    monkeypatch.setattr(ad, "_ensure_loan_approval_drafts_table", lambda conn: None)
    monkeypatch.setattr(ad, "_connection", _dummy_connection)
    monkeypatch.setattr(ad, "get_loan_daily_state_balances_for_recast_preview", lambda loan_id, rd: ({"total_exposure": 100.0}, rd))
    monkeypatch.setattr(ad, "get_unapplied_balance_for_restructure", lambda loan_id, rd: next(unapplied))
    monkeypatch.setattr(
        ad,
        "execute_unapplied_liquidation_for_restructure",
        lambda loan_id, rd, **kwargs: calls.append("liquidate"),
    )
    monkeypatch.setattr(
        ad,
        "execute_restructure_capitalisation_for_loan",
        lambda *args, **kwargs: calls.append("capitalise"),
    )
    monkeypatch.setattr(ad, "post_principal_writeoff_for_loan", lambda *args, **kwargs: calls.append("writeoff"))
    monkeypatch.setattr(ad, "post_modification_topup_disbursement", lambda *args, **kwargs: calls.append("topup"))
    monkeypatch.setattr(ad, "terminate_loan", lambda *args, **kwargs: calls.append("terminate"))
    monkeypatch.setattr(lm, "get_loan", lambda loan_id: {"id": loan_id, "cash_gl_account_id": "1"})
    monkeypatch.setattr(ad, "save_loan", lambda *args, **kwargs: next(ids))

    out = ad.approve_loan_approval_draft(3, approved_by="tester")
    assert out == 101
    assert "liquidate" in calls and "capitalise" in calls and "topup" in calls and "terminate" in calls
    assert calls.index("liquidate") < calls.index("topup")
    assert calls.index("capitalise") < calls.index("topup")
