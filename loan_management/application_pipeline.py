"""Orchestration: send application to approval queue and link booked loans (additive APIs only)."""

from __future__ import annotations

from decimal import Decimal
from datetime import date, datetime
from typing import Any

import pandas as pd

from .commission_invoice_pdf import build_agent_commission_invoice_pdf_bytes

from decimal_utils import as_10dp

from agents import get_agent

from .approval_drafts import save_loan_approval_draft
from .db import RealDictCursor, _connection
from .loan_applications import (
    STATUS_BOOKED,
    STATUS_SENT_FOR_APPROVAL,
    get_loan_application,
    set_loan_approval_draft_application_id,
    update_application_status,
)
from .loan_pipeline_config import non_terminal_for_submit
from .schema_ddl import _ensure_loan_applications_schema


def ensure_agent_commission_accrual_for_loan(
    loan_id: int,
    *,
    application_id: int | None = None,
    created_by: str = "system",
    post_gl: bool = True,
) -> int | None:
    """Ensure a commission accrual row exists for a booked loan; optionally post accrual GL."""
    commission_ctx: dict[str, Any] | None = None
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM loans WHERE id = %s", (int(loan_id),))
            lr = cur.fetchone()
            if not lr:
                raise ValueError(f"loans id={loan_id} not found.")
            loan = dict(lr)
            agid = loan.get("agent_id")
            if agid is None:
                return None

            cur.execute(
                "SELECT * FROM agent_commission_accruals WHERE loan_id = %s",
                (int(loan_id),),
            )
            existing = cur.fetchone()
            if existing:
                ex = dict(existing)
                if application_id is not None and ex.get("application_id") is None:
                    cur.execute(
                        """
                        UPDATE agent_commission_accruals
                        SET application_id = %s
                        WHERE id = %s
                        """,
                        (int(application_id), int(ex["id"])),
                    )
                return int(ex["id"])

            net_proceeds = loan.get("disbursed_amount") or loan.get("principal") or 0
            principal_d = as_10dp(net_proceeds)
            agent = get_agent(int(agid))
            rate = (agent or {}).get("commission_rate_pct")
            rate_d = as_10dp(rate) if rate is not None else None
            comm = Decimal("0")
            if rate_d is not None:
                comm = as_10dp(principal_d * rate_d / Decimal("100"))
            if comm <= 0:
                return None

            cur.execute(
                """
                INSERT INTO agent_commission_accruals (
                    loan_id, application_id, agent_id, principal_at_booking,
                    commission_rate_pct_snapshot, commission_amount, accrual_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'PENDING_POST')
                RETURNING id
                """,
                (
                    int(loan_id),
                    int(application_id) if application_id is not None else None,
                    int(agid),
                    float(principal_d),
                    float(rate_d) if rate_d is not None else None,
                    float(comm),
                ),
            )
            ins = cur.fetchone()
            if ins:
                commission_ctx = {
                    "accrual_id": int(ins["id"]),
                    "loan_id": int(loan_id),
                    "amount": as_10dp(comm),
                    "created_by": created_by,
                    "disbursement_date": loan.get("disbursement_date") or loan.get("start_date"),
                }

    if not commission_ctx:
        return None
    if post_gl:
        entry_date = None
        try:
            _raw_dt = commission_ctx.get("disbursement_date")
            if _raw_dt:
                entry_date = (
                    _raw_dt
                    if isinstance(_raw_dt, date)
                    else datetime.fromisoformat(str(_raw_dt)).date()
                )
        except Exception:
            entry_date = None
        try:
            posted = _post_commission_event_if_templates(
                event_type="AGENT_COMMISSION_ACCRUAL",
                payload=_commission_event_payload(commission_ctx["amount"]),
                reference=f"AGCOMM-ACCRUAL-{commission_ctx['accrual_id']}",
                description=f"Agent commission accrual for loan {commission_ctx['loan_id']}",
                event_id=f"AGCOMM_ACCRUAL:{commission_ctx['accrual_id']}",
                created_by=str(commission_ctx["created_by"]),
                entry_date=entry_date,
                loan_id=int(commission_ctx["loan_id"]),
            )
        except Exception:
            posted = False
        with _connection() as conn:
            _ensure_loan_applications_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_commission_accruals
                    SET accrual_status = %s
                    WHERE id = %s
                    """,
                    ("ACCRUED" if posted else "PENDING_POST", int(commission_ctx["accrual_id"])),
                )
    return int(commission_ctx["accrual_id"])


def _commission_event_payload(amount: Decimal) -> dict[str, float]:
    amt = float(as_10dp(amount))
    return {
        "deferred_fee_commission_asset": amt,
        "accrued_expenses": amt,
    }


def _commission_payment_payload(amount: Decimal) -> dict[str, float]:
    amt = float(as_10dp(amount))
    return {
        "accrued_expenses": amt,
        # Backward compatibility for tenants still using legacy template mapping.
        "fees_commission_expense": amt,
        "cash_operating": amt,
    }


def _commission_recognition_payload(amount: Decimal) -> dict[str, float]:
    amt = float(as_10dp(amount))
    return {
        "fees_commission_expense": amt,
        "deferred_fee_commission_asset": amt,
    }


def _post_commission_event_if_templates(
    *,
    event_type: str,
    payload: dict[str, float],
    reference: str,
    description: str,
    event_id: str,
    created_by: str,
    entry_date: date | None = None,
    loan_id: int | None = None,
    cash_gl_account_id: str | None = None,
) -> bool:
    from accounting.service import AccountingService

    pl = dict(payload)
    if cash_gl_account_id:
        pl["account_overrides"] = {"cash_operating": str(cash_gl_account_id).strip()}
    svc = AccountingService()
    sim = svc.simulate_event(event_type, payload=pl)
    if not sim.lines:
        return False
    svc.post_event(
        event_type=event_type,
        reference=reference,
        description=description,
        event_id=event_id,
        created_by=created_by,
        entry_date=entry_date,
        payload=pl,
        loan_id=loan_id,
    )
    return True


def submit_application_for_approval(
    application_id: int,
    customer_id: int,
    loan_type: str,
    details: dict[str, Any],
    schedule_df: pd.DataFrame,
    *,
    product_code: str | None = None,
    created_by: str | None = None,
) -> int:
    """
    Create a PENDING loan_approval_draft (existing API) and link it to the application.
    Does not modify save_loan_approval_draft; sets application_id in a follow-up UPDATE.
    """
    app = get_loan_application(int(application_id))
    if not app:
        raise ValueError(f"loan_applications id={application_id} not found.")
    st = (app.get("status") or "").upper()
    if st in ("BOOKED", "DECLINED", "WITHDRAWN", "SUPERSEDED"):
        raise ValueError(f"Application not submittable in status {st}.")
    if app.get("superseded_by_id"):
        raise ValueError("Application was superseded.")
    if not non_terminal_for_submit(st):
        raise ValueError(f"Application is not eligible for submission in status {st}.")
    if st == STATUS_SENT_FOR_APPROVAL:
        raise ValueError("Application was already sent for approval.")

    draft_id = save_loan_approval_draft(
        int(customer_id),
        str(loan_type),
        details,
        schedule_df,
        product_code=product_code,
        created_by=created_by or "application_pipeline",
        status="PENDING",
    )
    set_loan_approval_draft_application_id(int(draft_id), int(application_id))
    update_application_status(int(application_id), STATUS_SENT_FOR_APPROVAL)
    return int(draft_id)


def link_loan_to_application(
    loan_id: int,
    application_id: int,
    *,
    skip_commission_accrual: bool = False,
) -> None:
    """
    After a loan exists (e.g. approve_loan_approval_draft returned loan_id), attach pipeline:
    sets loans.source_application_id, loan_applications.loan_id + BOOKED,
    optional agent_commission_accruals row when loan.agent_id is set.
    Idempotent when the same loan_id is already linked.
    """
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM loan_applications WHERE id = %s AND deleted_at IS NULL",
                (int(application_id),),
            )
            app = cur.fetchone()
            if not app:
                raise ValueError(f"loan_applications id={application_id} not found.")
            app = dict(app)

            if app.get("superseded_by_id"):
                raise ValueError("Application was superseded.")

            cur.execute("SELECT * FROM loans WHERE id = %s", (int(loan_id),))
            lr = cur.fetchone()
            if not lr:
                raise ValueError(f"loans id={loan_id} not found.")
            loan = dict(lr)

            if (
                app.get("loan_id") is not None
                and int(app["loan_id"]) == int(loan_id)
                and (app.get("status") or "").upper() == STATUS_BOOKED
            ):
                src = loan.get("source_application_id")
                if src is None or int(src) != int(application_id):
                    cur.execute(
                        """
                        UPDATE loans SET source_application_id = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (int(application_id), int(loan_id)),
                    )
                return

            existing_lid = app.get("loan_id")
            if existing_lid is not None and int(existing_lid) != int(loan_id):
                raise ValueError("Application already booked to a different loan.")

            cid_app = app.get("customer_id")
            cid_loan = loan.get("customer_id")
            if cid_app is not None and cid_loan is not None and int(cid_app) != int(cid_loan):
                raise ValueError("Loan customer_id does not match application customer_id.")

            aid_app = app.get("agent_id")
            aid_loan = loan.get("agent_id")
            if (
                aid_app is not None
                and aid_loan is not None
                and int(aid_app) != int(aid_loan)
            ):
                raise ValueError("Loan agent_id does not match application agent_id.")

            cur.execute(
                """
                UPDATE loan_applications
                SET loan_id = %s, status = %s, updated_at = NOW()
                WHERE id = %s AND deleted_at IS NULL
                """,
                (int(loan_id), STATUS_BOOKED, int(application_id)),
            )
            cur.execute(
                """
                UPDATE loans SET source_application_id = %s, updated_at = NOW() WHERE id = %s
                """,
                (int(application_id), int(loan_id)),
            )

            if skip_commission_accrual:
                return
    if not skip_commission_accrual:
        ensure_agent_commission_accrual_for_loan(
            int(loan_id),
            application_id=int(application_id),
            created_by="application_pipeline",
            post_gl=True,
        )


def generate_agent_commission_invoices(
    *,
    period_start: date,
    period_end: date,
    agent_ids: list[int],
    created_by: str = "loan_applications_ui",
) -> list[int]:
    if not agent_ids:
        return []
    if period_end < period_start:
        raise ValueError("Period end must be on or after period start.")
    out_ids: list[int] = []
    ids = sorted({int(x) for x in agent_ids})
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for aid in ids:
                cur.execute(
                    """
                    SELECT
                        aca.id,
                        aca.loan_id,
                        aca.application_id,
                        aca.commission_amount
                    FROM agent_commission_accruals aca
                    JOIN loans l ON l.id = aca.loan_id
                    WHERE aca.agent_id = %s
                      AND COALESCE(aca.invoice_id, 0) = 0
                      AND COALESCE(aca.commission_amount, 0) > 0
                      AND l.disbursement_date BETWEEN %s AND %s
                    ORDER BY l.disbursement_date, aca.id
                    """,
                    (aid, period_start, period_end),
                )
                rows = [dict(r) for r in cur.fetchall()]
                if not rows:
                    continue
                total = as_10dp(sum((Decimal(str(r["commission_amount"])) for r in rows), start=Decimal("0")))
                cur.execute(
                    """
                    INSERT INTO agent_commission_invoices (
                        invoice_number, agent_id, period_start, period_end, invoice_date,
                        total_commission, status, created_by
                    )
                    VALUES (
                        CONCAT('AGINV-', TO_CHAR(CURRENT_DATE, 'YYYYMMDD'), '-', %s, '-', nextval('loan_ref_seq')::text),
                        %s, %s, %s, CURRENT_DATE, %s, 'ISSUED', %s
                    )
                    RETURNING id
                    """,
                    (aid, aid, period_start, period_end, float(total), created_by),
                )
                inv_id = int(cur.fetchone()["id"])
                out_ids.append(inv_id)
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO agent_commission_invoice_lines (
                            invoice_id, accrual_id, loan_id, application_id, commission_amount
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            inv_id,
                            int(r["id"]),
                            int(r["loan_id"]),
                            int(r["application_id"]) if r.get("application_id") is not None else None,
                            float(as_10dp(r["commission_amount"])),
                        ),
                    )
                cur.execute(
                    """
                    UPDATE agent_commission_accruals
                    SET invoice_id = %s, accrual_status = 'INVOICED'
                    WHERE id = ANY(%s)
                    """,
                    (inv_id, [int(r["id"]) for r in rows]),
                )
    return out_ids


def list_agent_commission_invoices(
    *,
    status: str | None = None,
    limit: int = 200,
) -> list[dict]:
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if status:
                cur.execute(
                    """
                    SELECT i.*, a.name AS agent_name
                    FROM agent_commission_invoices i
                    LEFT JOIN agents a ON a.id = i.agent_id
                    WHERE i.status = %s
                    ORDER BY i.created_at DESC
                    LIMIT %s
                    """,
                    (status.strip().upper(), max(1, min(int(limit), 2000))),
                )
            else:
                cur.execute(
                    """
                    SELECT i.*, a.name AS agent_name
                    FROM agent_commission_invoices i
                    LEFT JOIN agents a ON a.id = i.agent_id
                    ORDER BY i.created_at DESC
                    LIMIT %s
                    """,
                    (max(1, min(int(limit), 2000)),),
                )
            return [dict(r) for r in cur.fetchall()]


def get_agent_commission_invoice_detail(invoice_id: int) -> dict[str, Any] | None:
    """
    Invoice header data, full ``agents`` row, and summarised loan lines for PDF/print.

    Line rows include borrower display name, disbursement date, net proceeds (disbursed_amount),
    and commission for that loan.
    """
    iid = int(invoice_id)
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT i.*
                FROM agent_commission_invoices i
                WHERE i.id = %s
                """,
                (iid,),
            )
            inv_row = cur.fetchone()
            if not inv_row:
                return None
            invoice = dict(inv_row)

            aid = invoice.get("agent_id")
            agent_row: dict[str, Any] | None = None
            if aid is not None:
                cur.execute(
                    """
                    SELECT *
                    FROM agents
                    WHERE id = %s
                    """,
                    (int(aid),),
                )
                ag = cur.fetchone()
                agent_row = dict(ag) if ag else None

            cur.execute(
                """
                SELECT
                    lic.id AS line_id,
                    lic.loan_id,
                    lic.application_id,
                    lic.commission_amount,
                    l.disbursement_date,
                    COALESCE(l.disbursed_amount, 0) AS disbursed_amount,
                    COALESCE(NULLIF(TRIM(ind.name), ''),
                             NULLIF(TRIM(corp.trading_name), ''),
                             NULLIF(TRIM(corp.legal_name), ''),
                             NULLIF(TRIM(c.name), ''),
                             '') AS borrower_name
                FROM agent_commission_invoice_lines lic
                JOIN loans l ON l.id = lic.loan_id
                LEFT JOIN customers c ON c.id = l.customer_id
                LEFT JOIN individuals ind ON ind.customer_id = c.id
                LEFT JOIN corporates corp ON corp.customer_id = c.id
                WHERE lic.invoice_id = %s
                ORDER BY l.id, lic.id
                """,
                (iid,),
            )
            lines = [dict(r) for r in cur.fetchall()]

    return {"invoice": invoice, "agent": agent_row or {}, "lines": lines}


def get_agent_commission_invoice_pdf_bytes(invoice_id: int) -> bytes | None:
    """Render ``get_agent_commission_invoice_detail`` as a PDF, or ``None`` if missing / ReportLab absent."""
    detail = get_agent_commission_invoice_detail(int(invoice_id))
    if not detail:
        return None
    return build_agent_commission_invoice_pdf_bytes(detail=detail)


def mark_agent_commission_invoice_paid(
    invoice_id: int,
    *,
    payment_date: date | None = None,
    created_by: str = "loan_applications_ui",
    cash_gl_account_id: str | None = None,
) -> None:
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM agent_commission_invoices WHERE id = %s",
                (int(invoice_id),),
            )
            inv = cur.fetchone()
            if not inv:
                raise ValueError("Invoice not found.")
            inv = dict(inv)
            if str(inv.get("status") or "").upper() == "PAID":
                return
            total = as_10dp(inv.get("total_commission") or 0)
            if total <= 0:
                raise ValueError("Invoice total is zero; nothing to pay.")

    posted = _post_commission_event_if_templates(
        event_type="AGENT_COMMISSION_PAYMENT",
        payload=_commission_payment_payload(total),
        reference=f"AGCOMM-PAY-{int(invoice_id)}",
        description=f"Agent commission payment invoice {int(invoice_id)}",
        event_id=f"AGCOMM_PAY:{int(invoice_id)}",
        created_by=created_by,
        entry_date=payment_date or date.today(),
        cash_gl_account_id=cash_gl_account_id,
    )
    if not posted:
        raise ValueError(
            "AGENT_COMMISSION_PAYMENT templates are not configured; cannot mark invoice as paid."
        )

    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_commission_invoices
                SET status = 'PAID',
                    paid_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (int(invoice_id),),
            )
            cur.execute(
                """
                UPDATE agent_commission_accruals
                SET accrual_status = 'PAID', paid_at = NOW()
                WHERE invoice_id = %s
                """,
                (int(invoice_id),),
            )


def recognise_agent_commission_eom(
    *,
    as_of_date: date,
    created_by: str = "loan_applications_ui",
) -> int:
    posted = 0
    with _connection() as conn:
        _ensure_loan_applications_schema(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    aca.id,
                    aca.loan_id,
                    aca.commission_amount,
                    COALESCE(aca.recognised_months, 0) AS recognised_months,
                    COALESCE(aca.recognised_amount, 0) AS recognised_amount,
                    l.term,
                    COALESCE(l.disbursement_date, l.start_date) AS booking_date
                FROM agent_commission_accruals aca
                JOIN loans l ON l.id = aca.loan_id
                WHERE COALESCE(aca.commission_amount, 0) > 0
                  AND COALESCE(l.disbursement_date, l.start_date) IS NOT NULL
                  AND COALESCE(l.disbursement_date, l.start_date)::date <= %s
                ORDER BY aca.id
                """,
                (as_of_date,),
            )
            rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        total = as_10dp(r.get("commission_amount") or 0)
        term = int(r.get("term") or 0)
        if total <= 0 or term <= 0:
            continue
        bdate = r.get("booking_date")
        if hasattr(bdate, "date"):
            bdate = bdate.date()
        if not isinstance(bdate, date):
            continue
        months_elapsed = (as_of_date.year - bdate.year) * 12 + (as_of_date.month - bdate.month) + 1
        if months_elapsed < 1:
            continue
        target_months = min(term, months_elapsed)
        recognised_months = int(r.get("recognised_months") or 0)
        remaining_months = target_months - recognised_months
        if remaining_months <= 0:
            continue
        recognised_amount = as_10dp(r.get("recognised_amount") or 0)
        monthly = as_10dp(total / Decimal(term))
        for i in range(remaining_months):
            seq = recognised_months + i + 1
            if seq >= term:
                amt = as_10dp(total - as_10dp(recognised_amount + monthly * i))
            else:
                amt = monthly
            if amt <= 0:
                continue
            did_post = _post_commission_event_if_templates(
                event_type="COMMISSION_AMORTISATION",
                payload=_commission_recognition_payload(amt),
                reference=f"AGCOMM-AMORT-{int(r['id'])}-{seq}",
                description=f"Monthly commission amortisation for accrual {int(r['id'])}",
                event_id=f"AGCOMM_AMORT:{int(r['id'])}:{as_of_date.year:04d}{as_of_date.month:02d}:{seq}",
                created_by=created_by,
                entry_date=as_of_date,
                loan_id=int(r["loan_id"]) if r.get("loan_id") is not None else None,
            )
            if did_post:
                posted += 1
                with _connection() as conn:
                    _ensure_loan_applications_schema(conn)
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE agent_commission_accruals
                            SET recognised_months = recognised_months + 1,
                                recognised_amount = recognised_amount + %s,
                                recognised_at = CASE
                                    WHEN recognised_months + 1 >= %s THEN NOW()
                                    ELSE recognised_at
                                END,
                                accrual_status = CASE
                                    WHEN accrual_status = 'PAID' THEN 'PAID'
                                    ELSE 'RECOGNISED'
                                END
                            WHERE id = %s
                            """,
                            (float(as_10dp(amt)), int(term), int(r["id"])),
                        )
    return posted
