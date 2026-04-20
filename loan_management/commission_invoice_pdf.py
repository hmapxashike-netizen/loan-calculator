"""PDF export for agent commission invoices (ReportLab)."""

from __future__ import annotations

from html import escape
from io import BytesIO
from typing import Any

from decimal_utils import as_10dp


def _fmt_dmy(d: Any) -> str:
    if d is None:
        return "—"
    if hasattr(d, "strftime"):
        return d.strftime("%d/%m/%Y")
    return str(d)


def _fmt_usd(amount: Any) -> str:
    try:
        v = float(as_10dp(amount))
    except Exception:
        v = float(amount or 0)
    return f"USD {v:,.2f}"


def _clean_cell(s: Any, max_len: int = 44) -> str:
    t = (str(s) if s is not None else "").strip()
    if len(t) > max_len:
        return t[: max_len - 1] + "…"
    return t


def build_agent_commission_invoice_pdf_bytes(*, detail: dict[str, Any]) -> bytes | None:
    """
    Build a printable commission invoice PDF.

    ``detail`` must match :func:`get_agent_commission_invoice_detail` output:
    ``invoice``, ``agent``, ``lines``.
    """
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.platypus import (
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except ImportError:
        return None

    inv = detail.get("invoice") or {}
    agent = detail.get("agent") or {}
    lines = detail.get("lines") or []

    invoice_no = escape(str(inv.get("invoice_number") or f"#{inv.get('id')}"))
    inv_date = _fmt_dmy(inv.get("invoice_date"))
    ps = inv.get("period_start")
    pe = inv.get("period_end")
    total = inv.get("total_commission") or 0

    aname = escape(str(agent.get("name") or "—"))
    tin = escape(str(agent.get("tin_number") or "").strip() or "—")
    addr_parts = [
        str(agent.get("address_line1") or "").strip(),
        str(agent.get("address_line2") or "").strip(),
        " ".join(
            x
            for x in [
                str(agent.get("city") or "").strip(),
                str(agent.get("country") or "").strip(),
            ]
            if x
        ).strip(),
    ]
    addr_display = ", ".join(escape(p) for p in addr_parts if p) or "—"
    phones = []
    if str(agent.get("phone1") or "").strip():
        phones.append(escape(str(agent.get("phone1")).strip()))
    if str(agent.get("phone2") or "").strip():
        phones.append(escape(str(agent.get("phone2")).strip()))
    phone_display = " · ".join(phones) if phones else "—"
    email_disp = escape(str(agent.get("email") or "").strip() or "—")

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        rightMargin=40,
        leftMargin=40,
        topMargin=44,
        bottomMargin=52,
    )
    styles = getSampleStyleSheet()
    small = ParagraphStyle(
        name="Small",
        parent=styles["Normal"],
        fontSize=9,
        leading=11,
    )
    story: list[Any] = []

    story.append(Paragraph("Agent commission invoice", styles["Title"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>Invoice #:</b> {invoice_no} &nbsp; <b>Date:</b> {escape(inv_date)}", styles["Normal"]))
    story.append(Spacer(1, 14))

    story.append(Paragraph("<b>Agent (bill to)</b>", styles["Normal"]))
    story.append(Paragraph(f"<b>{aname}</b>", styles["Normal"]))
    story.append(Paragraph(f"<b>TIN:</b> {tin}", small))
    story.append(Paragraph(f"<b>Address:</b> {addr_display}", small))
    story.append(Paragraph(f"<b>Contact:</b> {phone_display} &nbsp; <b>Email:</b> {email_disp}", small))
    story.append(Spacer(1, 14))

    summary_txt = (
        f"Commission for the period {_fmt_dmy(ps)} to {_fmt_dmy(pe)} is "
        f"<b>{escape(_fmt_usd(total))}</b>."
    )
    story.append(Paragraph(summary_txt, styles["Normal"]))
    story.append(Spacer(1, 12))

    hdr = ["Loan", "Borrower", "Disbursement", "Net proceeds", "Commission"]
    table_data: list[list[Any]] = [hdr]
    for row in lines:
        loan_id = row.get("loan_id")
        table_data.append(
            [
                str(int(loan_id)) if loan_id is not None else "—",
                _clean_cell(row.get("borrower_name") or "—"),
                _fmt_dmy(row.get("disbursement_date")),
                _fmt_usd(row.get("disbursed_amount")),
                _fmt_usd(row.get("commission_amount")),
            ]
        )

    if len(table_data) == 1:
        table_data.append(["—", "No lines", "—", "—", "—"])

    col_widths = [52, 156, 72, 72, 84]
    t = Table(table_data, colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 7),
                ("TOPPADDING", (0, 0), (-1, 0), 7),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("ALIGN", (0, 0), (0, -1), "LEFT"),
                ("ALIGN", (1, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 10))
    story.append(Paragraph(f"<b>Total commission:</b> {escape(_fmt_usd(total))}", styles["Normal"]))

    story.append(Spacer(1, 36))
    story.append(Paragraph("<b>Agent acknowledgement</b>", styles["Normal"]))
    story.append(Spacer(1, 8))
    story.append(
        Paragraph(
            "I confirm the commission summary above for the stated period.",
            small,
        )
    )
    story.append(Spacer(1, 36))
    sig_style = ParagraphStyle(
        name="SigLine",
        parent=small,
        fontSize=10,
        leading=14,
        spaceBefore=8,
    )
    story.append(Paragraph("Agent signature: _________________________________", sig_style))
    story.append(Spacer(1, 10))
    story.append(Paragraph("Date: _________________________________", sig_style))

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()
