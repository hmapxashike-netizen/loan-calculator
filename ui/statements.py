"""Statements: customer loan statement + GL tab (Streamlit)."""

from __future__ import annotations

import base64
import html as html_module
import json
import uuid
from datetime import datetime
from io import BytesIO

import pandas as pd
import streamlit as st

from display_formatting import format_display_amount

from ui.streamlit_feedback import run_with_spinner

def _make_statement_pdf(df, customer_name, cust_id, loan_id, start_fmt, end_fmt, statement_title):
    """Build PDF bytes for statement with header (customer, ID, period) and table. statement_title e.g. 'Loan Statement (Internal – Daily)' or 'Customer loan statement'."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
    except ImportError:
        return None
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    styles = getSampleStyleSheet()
    story = []
    story.append(Paragraph(statement_title, styles["Title"]))
    story.append(Paragraph(f"<b>Customer:</b> {customer_name}", styles["Normal"]))
    story.append(Paragraph(f"<b>Customer ID:</b> {cust_id or '—'}", styles["Normal"]))
    story.append(Paragraph(f"<b>Loan ID:</b> {loan_id}", styles["Normal"]))
    story.append(Paragraph(f"<b>Period covered:</b> {start_fmt} to {end_fmt}", styles["Normal"]))
    story.append(Spacer(1, 16))
    # Table: header row + data rows (stringify for reportlab)
    df_str = df.fillna("").astype(str)
    table_data = [df_str.columns.tolist()] + df_str.values.tolist()
    t = Table(table_data, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    story.append(t)
    doc.build(story)
    buf.seek(0)
    return buf.getvalue()


def _statement_table_html(
    df,
    display_headers: dict[str, str],
    center_columns: list[str] | None = None,
    *,
    get_system_config,
) -> str:
    """Build a full-width HTML table from the statement dataframe. display_headers maps column name -> display label.
    center_columns: optional list of column names to center (e.g. last 4 columns for customer statement)."""
    import html

    center_set = set(center_columns or [])

    def _cell_html(v, col_name: str) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        if col_name == "Due Date":
            if hasattr(v, "strftime"):
                return html.escape(v.strftime("%d %b %y"))
            s = str(v).strip()
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                try:
                    from datetime import date as _date

                    d = _date.fromisoformat(s[:10])
                    return html.escape(d.strftime("%d %b %y"))
                except ValueError:
                    pass
            return html.escape(s[:16] if len(s) > 16 else s)
        if isinstance(v, (int, float)):
            try:
                return format_display_amount(float(v), system_config=get_system_config())
            except (TypeError, ValueError):
                return html.escape(str(v))
        return html.escape(str(v))

    def _th_td_classes(col_name: str) -> str:
        parts: list[str] = []
        if col_name in center_set:
            parts.append("center")
        if col_name == "Due Date":
            parts.append("stmt-due-date")
        return f' class="{" ".join(parts)}"' if parts else ""

    cols = df.columns.tolist()
    headers = [display_headers.get(c, c) for c in cols]
    th_parts = []
    for i, h in enumerate(headers):
        cname = cols[i] if i < len(cols) else None
        cls = _th_td_classes(cname) if cname else ""
        th_parts.append(f"<th{cls}>{html.escape(h)}</th>")
    th = "".join(th_parts)
    rows = []
    for _, r in df.iterrows():
        td_parts = []
        for c in cols:
            cls = _th_td_classes(c)
            td_parts.append(f"<td{cls}>{_cell_html(r.get(c), c)}</td>")
        rows.append(f"<tr>{''.join(td_parts)}</tr>")
    tbody = "\n".join(rows)
    return f'<table class="stmt-table"><thead><tr>{th}</tr></thead><tbody>{tbody}</tbody></table>'


_STMT_PRINT_WINDOW_CSS = (
    "body{margin:0;padding:12px;font-family:system-ui,Segoe UI,sans-serif;color:#0f172a;}"
    ".stmt-header{margin-bottom:0.5rem;padding:0.5rem 0.75rem;border:1px solid #e2e8f0;border-radius:4px;"
    "background:#f8fafc;font-size:1.05rem;}"
    ".stmt-table{width:100%;border-collapse:collapse;font-size:0.95rem;background:#fff;table-layout:auto;}"
    ".stmt-table th,.stmt-table td{border:1px solid #e2e8f0;padding:0.35rem 0.45rem;text-align:left;}"
    ".stmt-table th{background:#f1f5f9;font-weight:600;}"
    ".stmt-table td.num,.stmt-table th.num{text-align:right;}"
    ".stmt-table td.center,.stmt-table th.center{text-align:center;}"
    ".stmt-table th.stmt-due-date,.stmt-table td.stmt-due-date{width:1%;white-space:nowrap;max-width:5.75rem;"
    "font-size:0.88rem;text-align:center;vertical-align:middle;}"
    ".stmt-table tbody tr:nth-child(even){background:#f8fafc;}"
    ".stmt-closing{margin-top:0.75rem;text-align:center;font-size:1rem;padding:0.5rem;"
    "border-top:1px solid #e2e8f0;color:#334155;}"
    "@media print{body{padding:0}@page{margin:12mm}}"
)


def _statement_export_bar_html(
    *,
    csv_bytes: bytes,
    pdf_bytes: bytes | None,
    print_inner_html: str,
    csv_file_name: str,
    pdf_file_name: str,
) -> str:
    """Single-row HTML: equal-width green Download CSV / Download PDF / Print (statement-only print)."""
    csv_b64 = base64.standard_b64encode(csv_bytes).decode("ascii")
    print_b64 = base64.standard_b64encode(print_inner_html.encode("utf-8")).decode("ascii")
    uid = uuid.uuid4().hex[:12]
    csv_fn = html_module.escape(csv_file_name, quote=True)
    pdf_fn = html_module.escape(pdf_file_name, quote=True)
    if pdf_bytes:
        pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("ascii")
        pdf_el = (
            f'<a class="stmt-exp" href="data:application/pdf;base64,{pdf_b64}" '
            f'download="{pdf_fn}">Download PDF</a>'
        )
    else:
        pdf_el = '<span class="stmt-exp stmt-exp--disabled">Download PDF</span>'
    css_js = json.dumps(_STMT_PRINT_WINDOW_CSS)
    return f"""<div style="width:100%;box-sizing:border-box;">
<style>
.stmt-exp-wrap {{ display:flex; gap:0.65rem; width:100%; align-items:stretch; box-sizing:border-box; }}
.stmt-exp {{
  flex: 1 1 0;
  min-width: 0;
  text-align: center;
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 2.85rem;
  padding: 0.45rem 0.5rem;
  box-sizing: border-box;
  background: #16a34a;
  color: #fff !important;
  border: none;
  border-radius: 6px;
  font-size: 0.9rem;
  font-weight: 600;
  cursor: pointer;
  text-decoration: none !important;
  font-family: system-ui, Segoe UI, sans-serif;
}}
.stmt-exp:hover {{ background: #15803d !important; color: #fff !important; }}
.stmt-exp--disabled {{
  background: #94a3b8 !important;
  cursor: not-allowed;
  pointer-events: none;
  color: #fff !important;
}}
.stmt-exp--disabled:hover {{ background: #94a3b8 !important; color: #fff !important; }}
</style>
<div class="stmt-exp-wrap">
<a class="stmt-exp" href="data:text/csv;charset=utf-8;base64,{csv_b64}" download="{csv_fn}">Download CSV</a>
{pdf_el}
<button type="button" class="stmt-exp" id="stmt-print-{uid}">Print</button>
</div>
<textarea id="stmt-b64-{uid}" style="position:absolute;left:-9999px;width:1px;height:1px;opacity:0;pointer-events:none" aria-hidden="true" readonly>{print_b64}</textarea>
<script>
(function() {{
  var btn = document.getElementById('stmt-print-{uid}');
  var ta = document.getElementById('stmt-b64-{uid}');
  if (!btn || !ta) return;
  var printCss = {css_js};
  btn.addEventListener('click', function() {{
    var b64 = ta.value.trim();
    var binary = atob(b64);
    var bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    var h = new TextDecoder('utf-8').decode(bytes);
    var w = window.open('', '_blank');
    if (!w) return;
    w.document.open();
    w.document.write('<!DOCTYPE html><html><head><meta charset="utf-8"><title>Statement</title><style>' + printCss + '</style></head><body>');
    w.document.write(h);
    w.document.write('</body></html>');
    w.document.close();
    w.focus();
    setTimeout(function() {{ w.print(); w.close(); }}, 400);
  }});
}})();
</script>
</div>"""


def render_statements_ui(
    *,
    loan_management_available: bool,
    loan_management_error: str,
    customers_available: bool,
    customers_error: str,
    get_system_config,
    get_system_date,
    list_customers,
    get_display_name,
    money_df_column_config,
) -> None:
        """
        Generate statements on demand (no persistence).
        Customer loan statement: select customer/loan, date range; search by customer name or Loan ID.
        GL / ledger statements (later).
        """
        import pandas as pd

        if not loan_management_available:
            st.error(loan_management_error or "Loan management not available.")
            return
        if not customers_available:
            st.error(customers_error or "Customers module not available.")
            return
    
        try:
            from reporting.statements import generate_customer_facing_statement
        except ImportError as e:
            st.error(f"Statements module not available: {e}")
            return
    
        # Short labels for allocation columns (display only)
        _alloc_display = {
            "Portion of Credit Allocated to Interest": "Credit to Interest",
            "Credit Allocated to Fees": "Credit to Fees",
            "Credit Allocated to Capital": "Credit to Principal",
            "Arrears": "Arrears",
        }
    
        def _normalize_customer_id(v):
            if isinstance(v, dict):
                v = v.get("id")
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None
    
        def _customer_label(cid):
            cid_n = _normalize_customer_id(cid)
            if cid_n is None:
                return f"Customer #{cid}"
            try:
                nm = get_display_name(cid_n) if customers_available else ""
            except Exception:
                nm = ""
            # Defensive: if a dict leaks through from any upstream helper/session state, avoid raw JSON UI labels.
            if isinstance(nm, dict):
                nm = (
                    (nm.get("individual") or {}).get("name")
                    or (nm.get("corporate") or {}).get("trading_name")
                    or (nm.get("corporate") or {}).get("legal_name")
                    or ""
                )
            nm_s = str(nm or "").strip()
            return nm_s if nm_s else f"Customer #{cid_n}"
    
        tab_loan, tab_gl = st.tabs(["Customer loan statement", "General Ledger"])
        with tab_loan:
            st.markdown("##### Customer loan statement")

            customers = list_customers() if customers_available else []
            fc0, fc1, fc2 = st.columns([1.05, 1.2, 1.2], gap="xsmall")
            with fc0:
                _stmt_search_raw = st.text_input(
                    "Search by name or Loan ID",
                    placeholder="Name / Loan ID",
                    key="stmt_search",
                )
            search = (_stmt_search_raw or "").strip()
            _loan_id_token = search.lstrip("#").strip() if search else ""

            preselect_cust_id = None
            preselect_loan_id = None

            from loan_management import get_loan

            if search:
                if _loan_id_token.isdigit():
                    lid = int(_loan_id_token)
                    loan = get_loan(lid)
                    if loan and loan.get("customer_id"):
                        preselect_cust_id = _normalize_customer_id(loan["customer_id"])
                        preselect_loan_id = lid
                    else:
                        st.warning(f"No loan found with ID **{lid}**.")
                if preselect_loan_id is None and not _loan_id_token.isdigit():
                    search_lower = search.lower()
                    customers = [
                        c for c in customers if search_lower in _customer_label(c.get("id")).lower()
                    ]
    
            if not customers and preselect_cust_id is None:
                st.info("No customers found. Create a customer or enter a valid Loan ID.")
            else:
                cust_options = [(_normalize_customer_id(c.get("id")), _customer_label(c.get("id"))) for c in customers]
                cust_options = [t for t in cust_options if t[0] is not None]
                cust_labels = [t[1] for t in cust_options]
                default_idx = 0
                if preselect_cust_id is not None:
                    try:
                        default_idx = next(i for i, t in enumerate(cust_options) if t[0] == preselect_cust_id)
                    except StopIteration:
                        cust_options.insert(0, (preselect_cust_id, _customer_label(preselect_cust_id)))
                        cust_labels.insert(0, cust_options[0][1])
                        default_idx = 0
                # Drive selectbox only via session_state (never mix index= with Session State API).
                if cust_labels:
                    if (
                        preselect_cust_id is not None
                        and 0 <= default_idx < len(cust_labels)
                    ):
                        st.session_state["stmt_cust"] = cust_labels[default_idx]
                    elif st.session_state.get("stmt_cust") not in cust_labels:
                        st.session_state["stmt_cust"] = cust_labels[default_idx]
                with fc1:
                    cust_sel = st.selectbox(
                        "Customer",
                        cust_labels,
                        key="stmt_cust",
                    )
                cust_id = cust_options[cust_labels.index(cust_sel)][0]
    
                from loan_management import get_loans_by_customer
                loans = get_loans_by_customer(cust_id)
                if not loans:
                    with fc2:
                        st.selectbox(
                            "Loan",
                            options=["—"],
                            key="stmt_loan_empty",
                            disabled=True,
                        )
                    st.info("No loans for this customer.")
                else:
                    loan_options = [
                        (l["id"], f"#{l['id']} {l.get('loan_type', '')} {l.get('principal', 0):,.0f}")
                        for l in loans
                    ]
                    loan_labels = [t[1] for t in loan_options]
                    default_loan_idx = 0
                    if preselect_loan_id is not None:
                        try:
                            default_loan_idx = next(i for i, t in enumerate(loan_options) if t[0] == preselect_loan_id)
                        except StopIteration:
                            default_loan_idx = 0
                    if loan_labels:
                        if (
                            preselect_loan_id is not None
                            and 0 <= default_loan_idx < len(loan_labels)
                        ):
                            st.session_state["stmt_loan"] = loan_labels[default_loan_idx]
                        elif st.session_state.get("stmt_loan") not in loan_labels:
                            st.session_state["stmt_loan"] = loan_labels[default_loan_idx]
                    with fc2:
                        loan_sel = st.selectbox(
                            "Loan",
                            loan_labels,
                            key="stmt_loan",
                        )
                    loan_id = loan_options[loan_labels.index(loan_sel)][0]

                    loan_info = get_loan(loan_id)
                    disbursement = loan_info.get("disbursement_date") or loan_info.get("start_date")
                    if hasattr(disbursement, "date"):
                        disbursement = disbursement.date()
                    elif isinstance(disbursement, str):
                        disbursement = datetime.fromisoformat(disbursement[:10]).date()
                    start_default = disbursement or get_system_date()
                    dr0, dr1, dr2, dr3, dr4, dr5 = st.columns(
                        [0.95, 0.95, 0.55, 0.55, 0.55, 1.05],
                        gap="xsmall",
                    )
                    with dr0:
                        start_date = st.date_input(
                            "Period start (disbursement)",
                            value=start_default,
                            key=f"stmt_start_{loan_id}",
                            disabled=True,
                            help="Fixed to loan disbursement date.",
                        )
                    with dr1:
                        end_date = st.date_input(
                            "Period end",
                            value=get_system_date(),
                            key="stmt_end",
                            help="Statement runs through this date.",
                        )
                    with dr2:
                        show_pa_billing = st.checkbox(
                            "Principal arrears billing",
                            value=True,
                            key="stmt_show_pa_billing",
                            help="Include principal arrears billing (informational) lines.",
                        )
                    with dr3:
                        show_arrears_col = st.checkbox(
                            "Arrears column",
                            value=True,
                            key="stmt_show_arrears_col",
                            help="Show arrears column on the statement.",
                        )
                    with dr4:
                        show_unapplied_col = st.checkbox(
                            "Unapplied funds column",
                            value=True,
                            key="stmt_show_unapplied_col",
                            help="Show unapplied funds column on the statement.",
                        )
                    with dr5:
                        gen_stmt = st.button(
                            "Generate statement",
                            type="primary",
                            key="stmt_gen",
                            use_container_width=True,
                        )
    
                    if gen_stmt:
                        try:
                            def _do_generate_statement() -> None:
                                rows, meta = generate_customer_facing_statement(
                                    loan_id,
                                    start_date=start_date,
                                    end_date=end_date,
                                    include_principal_arrears_billing=show_pa_billing,
                                )
                                if not rows:
                                    st.info("No statement lines for this period.")
                                else:
                                    df = pd.DataFrame(rows)
                                    start = meta.get("start_date")
                                    end = meta.get("end_date")
                                    cust_id = _normalize_customer_id(meta.get("customer_id"))
                                    customer_name = _customer_label(cust_id) if cust_id is not None else "—"
                                    start_fmt = start.strftime("%d%b%Y") if hasattr(start, "strftime") else str(start)
                                    end_fmt = end.strftime("%d%b%Y") if hasattr(end, "strftime") else str(end)
                                    gen = meta.get("generated_at")
                                    generated_fmt = (
                                        gen.strftime("%d %b %Y, %H:%M:%S")
                                        if gen and hasattr(gen, "strftime")
                                        else (str(gen) if gen else "")
                                    )

                                    statement_title = "Customer loan statement"
                                    numeric_cols = ["Debits", "Credits", "Balance", "Arrears", "Unapplied funds"]
                                    for c in numeric_cols:
                                        if c in df.columns:
                                            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

                                    visible_df = df.copy()
                                    if not show_arrears_col and "Arrears" in visible_df.columns:
                                        visible_df = visible_df.drop(columns=["Arrears"])
                                    if not show_unapplied_col and "Unapplied funds" in visible_df.columns:
                                        visible_df = visible_df.drop(columns=["Unapplied funds"])

                                    # Full-width statement: HTML table (no Streamlit dataframe width limits)
                                    display_headers = {**_alloc_display}
                                    closing_row = None
                                    if len(visible_df) > 0:
                                        last_narr = str(visible_df.iloc[-1].get("Narration") or "")
                                        if "Total outstanding" in last_narr:
                                            closing_row = visible_df.iloc[-1]
                                            stmt_df = visible_df.iloc[:-1]
                                        else:
                                            stmt_df = visible_df
                                    else:
                                        stmt_df = visible_df
                                    center_cols = [
                                        c
                                        for c in ["Debits", "Credits", "Balance", "Arrears", "Unapplied funds"]
                                        if c in stmt_df.columns
                                    ]
                                    table_html = _statement_table_html(
                                        stmt_df,
                                        display_headers,
                                        center_columns=center_cols,
                                        get_system_config=get_system_config,
                                    )
                                    closing_html = ""
                                    if closing_row is not None:
                                        due_d = closing_row.get("Due Date")
                                        bal = closing_row.get("Balance")
                                        unapp = closing_row.get("Unapplied funds")
                                        due_fmt = (
                                            due_d.strftime("%d %b %Y")
                                            if due_d and hasattr(due_d, "strftime")
                                            else str(due_d or "")
                                        )
                                        try:
                                            bal_fmt = f"{float(bal):,.2f}" if bal is not None else "0.00"
                                            unapp_fmt = f"{float(unapp):,.2f}" if unapp is not None else "0.00"
                                        except (TypeError, ValueError):
                                            bal_fmt = str(bal or "0.00")
                                            unapp_fmt = str(unapp or "0.00")
                                        if show_unapplied_col:
                                            closing_html = (
                                                f"<div class='stmt-closing'><strong>Closing balance as at {due_fmt}:</strong> {bal_fmt}  &nbsp;|&nbsp;  "
                                                f"<strong>Unapplied funds:</strong> {unapp_fmt}</div>"
                                            )
                                        else:
                                            closing_html = (
                                                f"<div class='stmt-closing'><strong>Closing balance as at {due_fmt}:</strong> {bal_fmt}</div>"
                                            )
                                    header_fragment = (
                                        "<div class='stmt-header'>"
                                        f"<strong style='font-size: 1.275rem; display: block; margin-bottom: 0.25rem;'>{statement_title}</strong>"
                                        f"<span style='display: block;'><strong>Customer:</strong> {customer_name}</span>"
                                        f"<span style='display: block;'><strong>Customer ID:</strong> {cust_id if cust_id is not None else '—'}</span>"
                                        f"<span style='display: block;'><strong>Loan ID:</strong> {loan_id}</span>"
                                        f"<span style='display: block; margin-top: 0.25rem;'><strong>Period covered:</strong> {start_fmt} to {end_fmt}</span>"
                                        "</div>"
                                    )
                                    stmt_inner = header_fragment + table_html + closing_html
                                    stmt_html = (
                                        "<style>"
                                        "main .block-container { max-width: 100% !important; padding-left: 1.5rem; padding-right: 1.5rem; } "
                                        "[data-testid='stSidebar'] { width: 16rem !important; } "
                                        ".stmt-view { width: 100%; max-width: 100%; overflow-x: auto; margin-top: 1rem; } "
                                        ".stmt-view .stmt-header { margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid #e2e8f0; border-radius: 4px; background: #f8fafc; font-size: 1.1rem; } "
                                        ".stmt-view .stmt-table { width: 100%; border-collapse: collapse; font-size: 1.1rem; background: #fff; } "
                                        ".stmt-view .stmt-table th, .stmt-view .stmt-table td { border: 1px solid #e2e8f0; padding: 0.35rem 0.45rem; text-align: left; } "
                                        ".stmt-view .stmt-table th { background: #f1f5f9; font-weight: 600; } "
                                        ".stmt-view .stmt-table td.num, .stmt-view .stmt-table th.num { text-align: right; } "
                                        ".stmt-view .stmt-table td.center, .stmt-view .stmt-table th.center { text-align: center; } "
                                        ".stmt-view .stmt-table th.stmt-due-date, .stmt-view .stmt-table td.stmt-due-date { "
                                        "width: 1%; white-space: nowrap; max-width: 5.75rem; font-size: 0.92rem; "
                                        "text-align: center; vertical-align: middle; } "
                                        ".stmt-view .stmt-table tbody tr:nth-child(even) { background: #f8fafc; } "
                                        ".stmt-closing { margin-top: 0.75rem; text-align: center; font-size: 1.1rem; padding: 0.5rem; border-top: 1px solid #e2e8f0; color: #334155; } "
                                        "</style>"
                                        "<div class='stmt-view'>"
                                        + stmt_inner
                                        + "</div>"
                                    )
                                    st.markdown(stmt_html, unsafe_allow_html=True)

                                    for note in meta.get("notifications") or []:
                                        st.warning(note)

                                    st.markdown(
                                        "<div style='margin-top: 1rem; padding-top: 0.75rem; border-top: 1px solid #e2e8f0; color: #64748b; font-size: 1.125rem;'>"
                                        f"For the period from {start_fmt} to {end_fmt}<br>"
                                        f"<strong>Generated:</strong> {generated_fmt}"
                                        "</div>",
                                        unsafe_allow_html=True,
                                    )
                                    # CSV with header (comment lines at top) so all formats include header
                                    stmt_slug = "customer"
                                    csv_header_lines = [
                                        f"# {statement_title}",
                                        f"# Customer: {customer_name}",
                                        f"# Customer ID: {cust_id if cust_id is not None else '—'}",
                                        f"# Loan ID: {loan_id}",
                                        f"# Period covered: {start_fmt} to {end_fmt}",
                                        "#",
                                    ]
                                    buf = BytesIO()
                                    buf.write(("\n".join(csv_header_lines) + "\n").encode("utf-8"))
                                    visible_df.to_csv(
                                        buf,
                                        index=False,
                                        date_format="%Y-%m-%d",
                                        float_format="%.2f",
                                    )
                                    buf.seek(0)
                                    csv_raw = buf.getvalue()
                                    pdf_bytes = _make_statement_pdf(
                                        visible_df,
                                        customer_name,
                                        cust_id,
                                        loan_id,
                                        start_fmt,
                                        end_fmt,
                                        statement_title,
                                    )
                                    _csv_name = f"loan_statement_{stmt_slug}_{loan_id}_{start_date}_{end_date}.csv"
                                    _pdf_name = f"loan_statement_{stmt_slug}_{loan_id}_{start_date}_{end_date}.pdf"
                                    st.components.v1.html(
                                        _statement_export_bar_html(
                                            csv_bytes=csv_raw,
                                            pdf_bytes=pdf_bytes,
                                            print_inner_html=stmt_inner,
                                            csv_file_name=_csv_name,
                                            pdf_file_name=_pdf_name,
                                        ),
                                        height=96,
                                    )

                            run_with_spinner("Generating statement…", _do_generate_statement)
                        except Exception as ex:
                            st.error(str(ex))
                            st.exception(ex)
    
        with tab_gl:
            from decimal import Decimal
    
            from decimal_utils import as_10dp
    
            def _gl_journal_amount(v):
                """Match DB/posting precision (10dp); avoids misleading 2dp-only grid values."""
                try:
                    return float(as_10dp(Decimal(str(v if v is not None else 0))))
                except Exception:
                    return float(v or 0)
    
            st.markdown("##### General Ledger")
            st.markdown(
                "<span style='font-size:0.975rem;color:#64748b;'>From · To · Account</span>",
                unsafe_allow_html=True,
            )
    
            from accounting.service import AccountingService
            svc = AccountingService()
    
            sys_date = get_system_date()
            gl_col1, gl_col2, gl_col3 = st.columns([0.95, 0.95, 2.2])
            with gl_col1:
                gl_start = st.date_input("From", value=sys_date.replace(day=1), key="stmt_gl_start")
            with gl_col2:
                gl_end = st.date_input("To", value=sys_date, key="stmt_gl_end")
            with gl_col3:
                all_accounts = svc.list_accounts()
                account_options = ["All"] + [f"{a['code']} - {a['name']}" for a in all_accounts]
                gl_account_sel = st.selectbox("Account", account_options, key="stmt_gl_acct")
    
            account_filter = None if gl_account_sel == "All" else gl_account_sel.split(" - ")[0]
    
            if account_filter:
                # If a parent account is selected, allow user to choose between Rollup Summary or Full Ledger.
                _is_parent = svc.is_parent_account(account_filter)
                _view_mode = "Parent Summary (Rollup)"
    
                if _is_parent:
                    _view_mode = st.radio(
                        "View Mode",
                        ["Parent Summary (Rollup)", "Full Ledger (this account & subaccounts)"],
                        horizontal=True,
                        help="Rollup lists each immediate child with descendants rolled up, plus any journals posted directly on the parent. "
                        "Full Ledger lists every posted line on this account and all active subaccounts for the date range (opening/closing match the rollup total).",
                    )
    
                if _is_parent and _view_mode == "Parent Summary (Rollup)":
                    st.markdown(f"#### Account Statement (Parent Summary): {gl_account_sel}")
                    child_rows = svc.get_child_account_summaries(account_filter, gl_start, gl_end)
    
                    def _fmt_bal(d, c):
                        net = float(d or 0) - float(c or 0)
                        if net > 0:
                            return f"{net:,.2f}", "Dr"
                        elif net < 0:
                            return f"{-net:,.2f}", "Cr"
                        return "0.00", "-"
    
                    summary_rows = []
                    total_dr = 0.0
                    total_cr = 0.0
                    sum_ob_d = 0.0
                    sum_ob_c = 0.0
                    for ch in child_rows:
                        ob_d = float(ch.get("ob_debit") or 0)
                        ob_c = float(ch.get("ob_credit") or 0)
                        p_d = float(ch.get("period_debit") or 0)
                        p_c = float(ch.get("period_credit") or 0)
    
                        sum_ob_d += ob_d
                        sum_ob_c += ob_c
                        total_dr += p_d
                        total_cr += p_c
    
                        ob_val, ob_side = _fmt_bal(ob_d, ob_c)
                        cb_val, cb_side = _fmt_bal(ob_d + p_d, ob_c + p_c)
    
                        summary_rows.append(
                            {
                                "Child Account": f"{ch['code']} - {ch['name']}",
                                "Opening Balance": f"{ob_val} {ob_side}" if ob_side != "-" else "0.00",
                                "Debit": f"{p_d:,.2f}" if p_d else "",
                                "Credit": f"{p_c:,.2f}" if p_c else "",
                                "Closing Balance": f"{cb_val} {cb_side}" if cb_side != "-" else "0.00",
                            }
                        )
    
                    if summary_rows:
                        tobv, tobs = _fmt_bal(sum_ob_d, sum_ob_c)
                        tcbv, tcbs = _fmt_bal(sum_ob_d + total_dr, sum_ob_c + total_cr)
                        summary_rows.append(
                            {
                                "Child Account": "— Total (subtree) —",
                                "Opening Balance": f"{tobv} {tobs}" if tobs != "-" else "0.00",
                                "Debit": f"{total_dr:,.2f}" if total_dr else "",
                                "Credit": f"{total_cr:,.2f}" if total_cr else "",
                                "Closing Balance": f"{tcbv} {tcbs}" if tcbs != "-" else "0.00",
                            }
                        )
    
                    import pandas as pd
    
                    df_summary = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(
                        columns=["Child Account", "Opening Balance", "Debit", "Credit", "Closing Balance"]
                    )
                    st.dataframe(df_summary, use_container_width=True, hide_index=True)
                    if summary_rows:
                        st.caption(f"Flow totals for period: Debit {total_dr:,.2f} | Credit {total_cr:,.2f}")
    
                else:
                    ledger = svc.get_account_ledger(
                        account_filter,
                        start_date=gl_start,
                        end_date=gl_end,
                        include_descendants=bool(_is_parent),
                    )
                    if ledger:
                        _ledger_title = (
                            f"Account Statement (subtree): {ledger['account']['code']} - {ledger['account']['name']}"
                            if ledger.get("include_descendants")
                            else f"Account Statement: {ledger['account']['code']} - {ledger['account']['name']}"
                        )
                        st.markdown(f"#### {_ledger_title}")
    
                        rows = []
                        # 1. Opening Balance Row
                        ob_debit = float(ledger['opening_balance']['ob_debit'] or 0)
                        ob_credit = float(ledger['opening_balance']['ob_credit'] or 0)
    
                        running_net = ob_debit - ob_credit
    
                        def format_bal(net):
                            if net > 0:
                                return f"{net:,.2f}", "Dr"
                            elif net < 0:
                                return f"{-net:,.2f}", "Cr"
                            else:
                                return "0.00", "-"
    
                        ob_val, ob_type = format_bal(running_net)
    
                        rows.append({
                            "Date": gl_start.strftime("%Y-%m-%d") if gl_start else "",
                            "Reference": "",
                            "Description": "Opening Balance",
                            "Debit": f"{ob_debit:,.2f}" if ob_debit else "",
                            "Credit": f"{ob_credit:,.2f}" if ob_credit else "",
                            "Balance": ob_val,
                            "Dr/Cr": ob_type
                        })
    
                        total_dr = ob_debit
                        total_cr = ob_credit
    
                        for tx in ledger['transactions']:
                            dr = float(tx['debit'] or 0)
                            cr = float(tx['credit'] or 0)
                            total_dr += dr
                            total_cr += cr
                            running_net += (dr - cr)
    
                            b_val, b_type = format_bal(running_net)
                            event_id = str(tx.get("event_id") or "")
                            is_reversal = event_id.startswith("REV-")
                            # If this is a reversal journal, show the explicit description
                            # (we set those on reversal postings). Otherwise, keep template memo.
                            desc = tx['description'] if (is_reversal or "Reversal of" in str(tx.get("description") or "")) else (tx['memo'] if tx.get('memo') else tx.get('description'))
    
                            rows.append({
                                "Date": tx['entry_date'].strftime("%Y-%m-%d") if tx['entry_date'] else "",
                                "Reference": tx['reference'] or "",
                                "Description": desc or "",
                                "Debit": f"{dr:,.2f}" if dr else "",
                                "Credit": f"{cr:,.2f}" if cr else "",
                                "Balance": b_val,
                                "Dr/Cr": b_type
                            })
    
                        # Calculate closing (only show balance & Dr/Cr, not totals as a row)
                        cb_val, cb_type = format_bal(running_net)
                        rows.append({
                            "Date": gl_end.strftime("%Y-%m-%d") if gl_end else "",
                            "Reference": "",
                            "Description": "Closing Balance",
                            "Debit": "",
                            "Credit": "",
                            "Balance": cb_val,
                            "Dr/Cr": cb_type
                        })
    
                        df_ledger = pd.DataFrame(rows)
                        st.dataframe(df_ledger, use_container_width=True, hide_index=True)
                        st.caption(f"Totals for period: Debit {total_dr:,.2f} | Credit {total_cr:,.2f}")
    
                    else:
                        st.info("Account not found.")
            else:
                entries = svc.get_journal_entries(start_date=gl_start, end_date=gl_end, account_code=account_filter)
                if entries:
                    flat_rows = []
                    for entry in entries:
                        ok = entry.get("double_entry_balanced", True)
                        for line in entry["lines"]:
                            flat_rows.append({
                                "Date": entry["entry_date"],
                                "Reference": entry["reference"],
                                "Event": entry["event_tag"],
                                "Balanced": "OK" if ok else "NO",
                                "Account": f"{line['account_name']} ({line['account_code']})",
                                "Debit": _gl_journal_amount(line.get("debit")),
                                "Credit": _gl_journal_amount(line.get("credit")),
                            })
    
                    df_all = pd.DataFrame(flat_rows) if flat_rows else pd.DataFrame(
                        columns=["Date", "Reference", "Event", "Balanced", "Account", "Debit", "Credit"]
                    )
                    _gl_je_cols = {
                        "Debit": st.column_config.NumberColumn(format="%.10f", step=1e-10),
                        "Credit": st.column_config.NumberColumn(format="%.10f", step=1e-10),
                    }
                    st.dataframe(
                        df_all,
                        use_container_width=True,
                        hide_index=True,
                        column_config=money_df_column_config(df_all, overrides=_gl_je_cols),
                    )
                    st.caption(
                        "Debit/Credit columns use 10 decimal places. "
                        "If amounts still end in .00 only, run `python scripts/run_migration_56.py` on this database "
                        "so `journal_items` columns are NUMERIC(28,10); then re-run EOD for affected dates if needed."
                    )
    
                    if not df_all.empty:
                        st.caption(
                            f"Totals for period: Debit {format_display_amount(df_all['Debit'].sum(), system_config=get_system_config())} | "
                            f"Credit {format_display_amount(df_all['Credit'].sum(), system_config=get_system_config())}"
                        )
                        if "Balanced" in df_all.columns and (df_all["Balanced"] == "NO").any():
                            st.warning(
                                "Some rows are from journal headers that fail double-entry at **2dp** "
                                "(see **Balanced** column). New postings are blocked if materially unbalanced; these may be historical."
                            )
                else:
                    st.info("No journal entries found for the selected filters.")
    
