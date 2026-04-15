"""Statements: customer loan statement + GL tab (Streamlit)."""

from __future__ import annotations

import base64
import html as html_module
import json
import re
import uuid
import zipfile
from datetime import date, datetime
from io import BytesIO

import pandas as pd
import streamlit as st

from display_formatting import format_display_amount
from reporting.statements import PERIODIC_NUMERIC_HEADINGS
from style import BRAND_GREEN

from ui.components import inject_tertiary_hyperlink_css_once
from ui.streamlit_feedback import run_with_spinner

# Default Streamlit server.maxMessageSize WebSocket limit (keep payloads below this).
_GL_STREAMLIT_MAX_MESSAGE_BYTES = 200 * 1024 * 1024
# st.dataframe ships the full table to the browser; cap displayed rows to avoid MessageSizeError.
_GL_UI_MAX_ROWS = 20_000
# Streamlit hides the dataframe toolbar "Download as CSV" above ~150k rows (client-side export).
_GL_SERVER_EXPORT_MIN_ROWS = 150_000
# ZIP server exports when the raw CSV is at least this large (typical GL text compresses well).
_GL_ZIP_IF_CSV_BYTES = 5 * 1024 * 1024
# Raw CSV at/above Streamlit’s default cap must be zipped (and use strong compression).
_GL_ZIP_IF_RAW_CSV_200MB = 200 * 1024 * 1024


def _gl_safe_export_stem(
    *,
    kind: str,
    account_code: str | None,
    gl_start,
    gl_end,
    loan_id: int | None = None,
    product_code: str | None = None,
) -> str:
    acct = re.sub(r"[^\w.\-]+", "_", (account_code or "all").strip())[:48] or "all"
    stem = f"gl_{kind}_{acct}_{gl_start}_{gl_end}"
    if loan_id is not None:
        stem += f"_loan{int(loan_id)}"
    if product_code:
        stem += "_" + (re.sub(r"[^\w.\-]+", "_", str(product_code).strip())[:32] or "product")
    return stem


def _gl_dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    df_export = df.copy()
    if "Date" in df_export.columns:
        c = df_export["Date"]
        if pd.api.types.is_datetime64_any_dtype(c):
            df_export["Date"] = c.dt.strftime("%Y-%m-%d")
        elif c.dtype == object:

            def _fmt_date_cell(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return ""
                if hasattr(v, "strftime"):
                    return v.strftime("%Y-%m-%d")
                return str(v)

            df_export["Date"] = c.map(_fmt_date_cell)
    buf = BytesIO()
    df_export.to_csv(buf, index=False, encoding="utf-8-sig", lineterminator="\n")
    return buf.getvalue()


def _gl_zip_csv_bytes(csv_bytes: bytes, *, stem: str, level: int) -> bytes:
    zbuf = BytesIO()
    with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED, compresslevel=level) as zf:
        zf.writestr(f"{stem}.csv", csv_bytes)
    return zbuf.getvalue()


def _gl_pack_csv_export(csv_bytes: bytes, *, stem: str) -> tuple[bytes, str, str] | None:
    """Build payload for ``st.download_button``. Returns None if still above Streamlit’s ~200 MB cap."""
    margin = 2 * 1024 * 1024
    max_payload = _GL_STREAMLIT_MAX_MESSAGE_BYTES - margin
    n = len(csv_bytes)
    prefer_zip = n >= _GL_ZIP_IF_CSV_BYTES or n >= _GL_ZIP_IF_RAW_CSV_200MB
    must_zip = n > max_payload

    if not prefer_zip and not must_zip:
        return csv_bytes, f"{stem}.csv", "text/csv"

    level_first = 9 if (n >= _GL_ZIP_IF_RAW_CSV_200MB or must_zip) else 6
    zb = _gl_zip_csv_bytes(csv_bytes, stem=stem, level=level_first)
    if len(zb) > max_payload and level_first == 6:
        zb = _gl_zip_csv_bytes(csv_bytes, stem=stem, level=9)
    if len(zb) > max_payload:
        return None
    return zb, f"{stem}.zip", "application/zip"


def _gl_dataframe_for_ui(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    if df is None or df.empty or len(df) <= _GL_UI_MAX_ROWS:
        return df, False
    return df.iloc[:_GL_UI_MAX_ROWS].copy(), True


def _render_gl_server_csv_export(
    df: pd.DataFrame,
    *,
    kind: str,
    account_code: str | None,
    gl_start,
    gl_end,
    button_key: str,
    ui_truncated: bool,
    loan_id: int | None = None,
    product_code: str | None = None,
) -> None:
    if df is None or df.empty:
        return
    need_export = ui_truncated or len(df) >= _GL_SERVER_EXPORT_MIN_ROWS
    if not need_export:
        return
    inject_tertiary_hyperlink_css_once()
    if ui_truncated:
        st.warning(
            f"Showing only the first {_GL_UI_MAX_ROWS:,} of {len(df):,} rows in the table so the page stays "
            f"under Streamlit’s default **~200 MB** browser payload limit. Use **Download CSV (server)** for the full extract."
        )
    if len(df) >= _GL_SERVER_EXPORT_MIN_ROWS:
        st.caption(
            "This table is large: the in-table **Download as CSV** control may be hidden by Streamlit. "
            "Use the server export below."
        )
    stem = _gl_safe_export_stem(
        kind=kind,
        account_code=account_code,
        gl_start=gl_start,
        gl_end=gl_end,
        loan_id=loan_id,
        product_code=product_code,
    )
    csv_b = _gl_dataframe_to_csv_bytes(df)
    packed = _gl_pack_csv_export(csv_b, stem=stem)
    if packed is None:
        st.error(
            "The export is still larger than Streamlit’s default **200 MB** message limit after ZIP compression. "
            "Narrow the date range or account filter, or raise `server.maxMessageSize` in `.streamlit/config.toml`."
        )
        return
    data, fname, mime = packed
    help_txt = "Server-built UTF-8 CSV (with BOM), full precision as in the full extract."
    if fname.endswith(".zip"):
        if len(csv_b) >= _GL_ZIP_IF_RAW_CSV_200MB:
            help_txt += " Zipped (max compression) because the raw CSV is over 200 MB."
        else:
            help_txt += " Zipped because the CSV is large or to stay under Streamlit’s message size limit."
    st.download_button(
        label="Download CSV (server)",
        data=data,
        file_name=fname,
        mime=mime,
        key=button_key,
        type="tertiary",
        help=help_txt,
    )


# Money columns for both periodic and customer/flow statements (10dp in row dicts; CSV keeps full precision).
_STATEMENT_MONEY_COLUMNS = frozenset(
    {
        "Debits",
        "Credits",
        "Balance",
        "Arrears",
        "Unapplied funds",
        *PERIODIC_NUMERIC_HEADINGS,
    }
)

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
                return html.escape(v.strftime("%d-%b-%Y"))
            s = str(v).strip()
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                try:
                    from datetime import date as _date

                    d = _date.fromisoformat(s[:10])
                    return html.escape(d.strftime("%d-%b-%Y"))
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
        if col_name in _STATEMENT_MONEY_COLUMNS:
            parts.append("num")
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
    "font-size:0.88rem;text-align:left;vertical-align:middle;}"
    ".stmt-table tbody tr:nth-child(even){background:#f8fafc;}"
    ".stmt-closing{margin-top:0.75rem;text-align:center;font-size:1rem;padding:0.5rem;"
    "border-top:1px solid #e2e8f0;color:#334155;}"
    "@media print{body{padding:0}@page{margin:12mm}}"
)


def _statement_print_button_html(*, print_inner_html: str | None) -> str:
    """Single print action rendered as a link."""
    uid = uuid.uuid4().hex[:12]
    css_js = json.dumps(_STMT_PRINT_WINDOW_CSS)
    is_enabled = bool(print_inner_html)
    link_html = (
        f'<a href="#" class="stmt-link" id="stmt-print-{uid}">Print</a>'
        if is_enabled
        else '<span class="stmt-link stmt-link--disabled">Print</span>'
    )
    ta_html = (
        f'<textarea id="stmt-b64-{uid}" style="position:absolute;left:-9999px;width:1px;height:1px;opacity:0;pointer-events:none" '
        f'aria-hidden="true" readonly>{base64.standard_b64encode((print_inner_html or "").encode("utf-8")).decode("ascii")}</textarea>'
        if is_enabled
        else ""
    )
    script_html = (
        f"""<script>
(function() {{
  var btn = document.getElementById('stmt-print-{uid}');
  var ta = document.getElementById('stmt-b64-{uid}');
  if (!btn || !ta) return;
  var printCss = {css_js};
  btn.addEventListener('click', function(e) {{
    if (e && e.preventDefault) e.preventDefault();
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
</script>"""
        if is_enabled
        else ""
    )
    return f"""<div style="width:100%;box-sizing:border-box;">
<style>
.stmt-link {{
  color: {BRAND_GREEN} !important;
  text-decoration: underline !important;
  font-size: 0.8rem;
  font-weight: 600;
  cursor: pointer;
  font-family: system-ui, Segoe UI, sans-serif;
}}
.stmt-link:hover {{ filter: brightness(0.93) !important; }}
.stmt-link--disabled {{
  color: #94a3b8 !important;
  text-decoration: none !important;
  cursor: not-allowed;
  pointer-events: none;
}}
</style>
{link_html}
{ta_html}
{script_html}
</div>"""


def _statement_download_pdf_button_html(*, pdf_bytes: bytes | None, pdf_file_name: str) -> str:
    """Single download action rendered as a link."""
    if pdf_bytes:
        pdf_fn = html_module.escape(pdf_file_name, quote=True)
        pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("ascii")
        link = (
            f'<a class="stmt-link" href="data:application/pdf;base64,{pdf_b64}" '
            f'download="{pdf_fn}">Download PDF</a>'
        )
    else:
        link = '<span class="stmt-link stmt-link--disabled">Download PDF</span>'
    return f"""<div style="width:100%;box-sizing:border-box;">
<style>
.stmt-link {{
  color: {BRAND_GREEN} !important;
  text-decoration: underline !important;
  font-size: 0.8rem;
  font-weight: 600;
  cursor: pointer;
  font-family: system-ui, Segoe UI, sans-serif;
}}
.stmt-link:hover {{ filter: brightness(0.93) !important; }}
.stmt-link--disabled {{
  color: #94a3b8 !important;
  text-decoration: none !important;
  cursor: not-allowed;
  pointer-events: none;
}}
</style>
{link}
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
    list_products=None,
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
            from reporting.statements import (
                generate_customer_facing_flow_statement,
                generate_customer_facing_statement,
                recalculate_flow_statement_running_balances,
            )
            from reporting.statement_events import rollup_flow_statement_rows_for_display
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
    
        _stmt_sections = ["Customer loan statement", "General Ledger"]
        st.session_state.setdefault("statements_subnav", _stmt_sections[0])
        if st.session_state["statements_subnav"] not in _stmt_sections:
            st.session_state["statements_subnav"] = _stmt_sections[0]
        st.markdown(
            '<p class="farnda-statements-section-nav" aria-hidden="true"></p>',
            unsafe_allow_html=True,
        )
        st.radio(
            "Statements section",
            _stmt_sections,
            key="statements_subnav",
            horizontal=True,
            label_visibility="collapsed",
        )
        _stmt_active = st.session_state["statements_subnav"]

        if _stmt_active == "Customer loan statement":
            st.markdown(
                """
<style>
button[aria-label="Generate statement"],
button[aria-label="Generate"]{
  min-height:1.6rem !important;
  height:1.6rem !important;
  width:100% !important;
  padding:0.05rem 0.3rem !important;
  font-size:0.7rem !important;
}
</style>
""",
                unsafe_allow_html=True,
            )
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
                        [0.9, 0.9, 1.45, 0.6, 0.6, 0.9],
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
                    stmt_type_labels = [
                        "Customer Statement - Letterhead",
                        "Customer Statement - Daily",
                        "Customer Statement - Stock based (Legacy)",
                    ]
                    stmt_type_map = {
                        "Customer Statement - Letterhead": "letterhead",
                        "Customer Statement - Daily": "daily",
                        "Customer Statement - Stock based (Legacy)": "stock_legacy",
                    }
                    with dr2:
                        stmt_type_label = st.selectbox(
                            "Statement type",
                            stmt_type_labels,
                            index=0,
                            key="stmt_type",
                            help="Choose one of the 3 customer statement formats.",
                        )
                    stmt_type_key = stmt_type_map.get(stmt_type_label, "letterhead")

                    if stmt_type_key == "letterhead":
                        use_flow_statement = True
                        rollup_flow_accruals = True
                        flow_arrears_mode = "end_snapshot"
                        show_pa_billing = False
                        show_arrears_col = False
                        show_unapplied_col = True
                    elif stmt_type_key == "daily":
                        use_flow_statement = True
                        rollup_flow_accruals = False
                        flow_arrears_mode = "by_row_date"
                        show_pa_billing = True
                        show_arrears_col = True
                        show_unapplied_col = True
                    else:
                        use_flow_statement = False
                        rollup_flow_accruals = False
                        flow_arrears_mode = "end_snapshot"
                        show_pa_billing = True
                        show_arrears_col = True
                        show_unapplied_col = True

                    with dr3:
                        gen_stmt = st.button(
                            "Generate",
                            type="primary",
                            key="stmt_gen",
                            use_container_width=True,
                        )
                    with dr4:
                        print_btn_slot = st.empty()
                    with dr5:
                        pdf_btn_slot = st.empty()
                    with print_btn_slot:
                        st.components.v1.html(
                            _statement_print_button_html(print_inner_html=None),
                            height=34,
                        )
                    with pdf_btn_slot:
                        st.components.v1.html(
                            _statement_download_pdf_button_html(
                                pdf_bytes=None,
                                pdf_file_name="",
                            ),
                            height=34,
                        )
                    if stmt_type_key == "stock_legacy":
                        lg1, lg2, lg3 = st.columns(3, gap="xsmall")
                        with lg1:
                            show_pa_billing = st.checkbox(
                                "Principal arrears billing",
                                value=True,
                                key="stmt_show_pa_billing",
                                help="Include principal arrears billing (informational) lines.",
                            )
                        with lg2:
                            show_arrears_col = st.checkbox(
                                "Arrears column",
                                value=True,
                                key="stmt_show_arrears_col",
                                help="Show arrears column on the statement.",
                            )
                        with lg3:
                            show_unapplied_col = st.checkbox(
                                "Unapplied funds column",
                                value=True,
                                key="stmt_show_unapplied_col",
                                help="Show unapplied funds column on the statement.",
                            )
                    else:
                        show_unapplied_col = True
    
                    if gen_stmt:
                        try:
                            def _do_generate_statement() -> None:
                                cust_for_auth = _normalize_customer_id(loan_info.get("customer_id"))
                                allowed_ids = [cust_for_auth] if cust_for_auth is not None else None
                                if use_flow_statement:
                                    rows, meta = generate_customer_facing_flow_statement(
                                        loan_id,
                                        start_date=start_date,
                                        end_date=end_date,
                                        allowed_customer_ids=allowed_ids,
                                        arrears_mode=flow_arrears_mode,
                                    )
                                    if rollup_flow_accruals:
                                        disb_r = loan_info.get("disbursement_date") or loan_info.get("start_date")
                                        if hasattr(disb_r, "date"):
                                            disb_r = disb_r.date()
                                        elif isinstance(disb_r, str):
                                            from datetime import datetime as _dt

                                            disb_r = _dt.fromisoformat(disb_r[:10]).date()
                                        else:
                                            disb_r = None
                                        sys_biz = get_system_date()
                                        if hasattr(sys_biz, "date"):
                                            sys_biz = sys_biz.date()
                                        stmt_end = meta.get("end_date")
                                        if hasattr(stmt_end, "date"):
                                            stmt_end = stmt_end.date()
                                        rollup_cap = sys_biz
                                        if (
                                            isinstance(stmt_end, date)
                                            and isinstance(sys_biz, date)
                                        ):
                                            rollup_cap = min(stmt_end, sys_biz)
                                        rows = rollup_flow_statement_rows_for_display(
                                            rows,
                                            loan_id=loan_id,
                                            disbursement_date=disb_r,
                                            system_business_date=rollup_cap,
                                        )
                                        recalculate_flow_statement_running_balances(
                                            rows,
                                            opening_loan=meta.get("opening_loan"),
                                            opening_unapplied=meta.get("opening_unapplied"),
                                        )
                                    rows = [
                                        {k: v for k, v in r.items() if not str(k).startswith("_")}
                                        for r in rows
                                    ]
                                else:
                                    rows, meta = generate_customer_facing_statement(
                                        loan_id,
                                        start_date=start_date,
                                        end_date=end_date,
                                        include_principal_arrears_billing=show_pa_billing,
                                        allowed_customer_ids=allowed_ids,
                                    )
                                if not rows:
                                    st.info("No statement lines for this period.")
                                else:
                                    df = pd.DataFrame(rows)
                                    start = meta.get("start_date")
                                    end = meta.get("end_date")
                                    cust_id = _normalize_customer_id(meta.get("customer_id"))
                                    customer_name = _customer_label(cust_id) if cust_id is not None else "—"
                                    start_fmt = start.strftime("%d-%b-%Y") if hasattr(start, "strftime") else str(start)
                                    end_fmt = end.strftime("%d-%b-%Y") if hasattr(end, "strftime") else str(end)
                                    gen = meta.get("generated_at")
                                    generated_fmt = (
                                        gen.strftime("%d %b %Y, %H:%M:%S")
                                        if gen and hasattr(gen, "strftime")
                                        else (str(gen) if gen else "")
                                    )

                                    statement_title = stmt_type_label
                                    for c in df.columns:
                                        if c in _STATEMENT_MONEY_COLUMNS:
                                            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

                                    visible_df = df.copy()
                                    if not show_arrears_col and "Arrears" in visible_df.columns:
                                        visible_df = visible_df.drop(columns=["Arrears"])
                                    if not show_unapplied_col and "Unapplied funds" in visible_df.columns:
                                        visible_df = visible_df.drop(columns=["Unapplied funds"])

                                    # Full-width statement: HTML table (no Streamlit dataframe width limits)
                                    display_headers = {"Due Date": "Date", **_alloc_display}
                                    closing_row = None
                                    closing_row_full = None
                                    if len(df) > 0:
                                        full_last_narr = str(df.iloc[-1].get("Narration") or "")
                                        if "Total outstanding" in full_last_narr:
                                            closing_row_full = df.iloc[-1]
                                    if len(visible_df) > 0:
                                        last_narr = str(visible_df.iloc[-1].get("Narration") or "")
                                        if "Total outstanding" in last_narr:
                                            closing_row = visible_df.iloc[-1]
                                            stmt_df = visible_df.iloc[:-1]
                                        else:
                                            stmt_df = visible_df
                                    else:
                                        stmt_df = visible_df
                                    table_html = _statement_table_html(
                                        stmt_df,
                                        display_headers,
                                        center_columns=[],
                                        get_system_config=get_system_config,
                                    )
                                    closing_html = ""
                                    if closing_row is not None:
                                        due_d = closing_row.get("Due Date")
                                        bal = closing_row.get("Balance")
                                        unapp = closing_row.get("Unapplied funds")
                                        arr = (
                                            closing_row_full.get("Arrears")
                                            if closing_row_full is not None
                                            else closing_row.get("Arrears")
                                        )
                                        due_fmt = (
                                            due_d.strftime("%d-%b-%Y")
                                            if due_d and hasattr(due_d, "strftime")
                                            else str(due_d or "")
                                        )
                                        try:
                                            bal_fmt = f"{float(bal):,.2f}" if bal is not None else "0.00"
                                            unapp_fmt = f"{float(unapp):,.2f}" if unapp is not None else "0.00"
                                            arr_fmt = f"{float(arr):,.2f}" if arr is not None else "0.00"
                                        except (TypeError, ValueError):
                                            bal_fmt = str(bal or "0.00")
                                            unapp_fmt = str(unapp or "0.00")
                                            arr_fmt = str(arr or "0.00")
                                        if stmt_type_key == "letterhead":
                                            closing_html = (
                                                f"<div class='stmt-closing'><strong>Closing balance as at {due_fmt}:</strong> {bal_fmt}  &nbsp;|&nbsp;  "
                                                f"<strong>Unapplied funds:</strong> {unapp_fmt}  &nbsp;|&nbsp;  "
                                                f"<strong>Arrears (as at statement end date):</strong> {arr_fmt}</div>"
                                            )
                                        elif show_unapplied_col:
                                            closing_html = (
                                                f"<div class='stmt-closing'><strong>Closing balance as at {due_fmt}:</strong> {bal_fmt}  &nbsp;|&nbsp;  "
                                                f"<strong>Unapplied funds:</strong> {unapp_fmt}</div>"
                                            )
                                        if show_unapplied_col:
                                            pass
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
                                    stmt_slug = {
                                        "letterhead": "customer_letterhead",
                                        "daily": "customer_daily",
                                        "stock_legacy": "customer_stock_legacy",
                                    }.get(stmt_type_key, "customer")
                                    _pdf_name = f"loan_statement_{stmt_slug}_{loan_id}_{start_date}_{end_date}.pdf"
                                    pdf_bytes = _make_statement_pdf(
                                        visible_df,
                                        customer_name,
                                        cust_id,
                                        loan_id,
                                        start_fmt,
                                        end_fmt,
                                        statement_title,
                                    )

                                    with print_btn_slot:
                                        st.components.v1.html(
                                            _statement_print_button_html(print_inner_html=stmt_inner),
                                            height=34,
                                        )
                                    with pdf_btn_slot:
                                        st.components.v1.html(
                                            _statement_download_pdf_button_html(
                                                pdf_bytes=pdf_bytes,
                                                pdf_file_name=_pdf_name,
                                            ),
                                            height=34,
                                        )

                                    st.markdown(f"##### {statement_title}")
                                    st.caption(
                                        f"Customer: {customer_name} | Customer ID: {cust_id if cust_id is not None else '—'} | "
                                        f"Loan ID: {loan_id} | Period: {start_fmt} to {end_fmt}"
                                    )
                                    if "Due Date" in stmt_df.columns:
                                        stmt_df = stmt_df.copy()
                                        stmt_df["Due Date"] = stmt_df["Due Date"].map(
                                            lambda v: v.strftime("%d-%b-%Y") if hasattr(v, "strftime") else str(v or "")
                                        )
                                    stmt_df_display = stmt_df.rename(columns=display_headers)
                                    if money_df_column_config is not None:
                                        st.dataframe(
                                            stmt_df_display,
                                            column_config=money_df_column_config(
                                                stmt_df_display,
                                                overrides={
                                                    "Date": {
                                                        **st.column_config.TextColumn(),
                                                        "alignment": "left",
                                                    },
                                                    "Narration": {
                                                        **st.column_config.TextColumn(),
                                                        "alignment": "left",
                                                    },
                                                },
                                                column_disabled={},
                                                money_column_alignment="right",
                                            ),
                                            hide_index=True,
                                            width="stretch",
                                        )
                                    else:
                                        st.dataframe(stmt_df_display, hide_index=True, width="stretch")
                                    if closing_row is not None:
                                        if stmt_type_key == "letterhead":
                                            st.info(
                                                f"Closing balance as at {due_fmt}: {bal_fmt} | "
                                                f"Unapplied funds: {unapp_fmt} | "
                                                f"Arrears (as at statement end date): {arr_fmt}"
                                            )
                                        elif show_unapplied_col:
                                            st.info(
                                                f"Closing balance as at {due_fmt}: {bal_fmt} | Unapplied funds: {unapp_fmt}"
                                            )
                                        else:
                                            st.info(f"Closing balance as at {due_fmt}: {bal_fmt}")

                                    for note in meta.get("notifications") or []:
                                        st.warning(note)

                                    st.markdown(
                                        "<div style='margin-top:1rem;padding-top:0.75rem;border-top:1px solid #e2e8f0;"
                                        "color:#64748b;font-size:1rem;display:flex;gap:1rem;align-items:center;"
                                        "flex-wrap:wrap;white-space:nowrap;'>"
                                        f"<span><strong>Period:</strong> {start_fmt} to {end_fmt}</span>"
                                        f"<span><strong>Generated:</strong> {generated_fmt}</span>"
                                        "</div>",
                                        unsafe_allow_html=True,
                                    )
                            run_with_spinner("Generating statement…", _do_generate_statement)
                        except Exception as ex:
                            st.error(str(ex))
                            st.exception(ex)
    
        elif _stmt_active == "General Ledger":
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
                "<span style='font-size:0.975rem;color:#64748b;'>From · To · Account · Loan · Product</span>",
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
    
            gl_loan_err: str | None = None
            gl_loan_id_val: int | None = None
            gl_product_code_val: str | None = None
            gl_r2a, gl_r2b = st.columns([1.0, 2.2])
            with gl_r2a:
                gl_loan_in = st.text_input(
                    "Loan ID",
                    value="",
                    key="stmt_gl_loan_id",
                    placeholder="All loans",
                    help="Optional. Posted journals for this loan (event_id / reference patterns used by loan and EOD GL).",
                )
            with gl_r2b:
                _plabs = ["All"]
                _pcodes: list[str | None] = [None]
                _lp_fn = list_products
                if _lp_fn is None:
                    try:
                        from loan_management.product_catalog import list_products as _lp_fn
                    except Exception:
                        _lp_fn = None
                if callable(_lp_fn):
                    try:
                        for p in _lp_fn(active_only=False):
                            code = (p.get("code") or "").strip()
                            if not code:
                                continue
                            name = (p.get("name") or "").strip() or code
                            _plabs.append(f"{code} — {name}")
                            _pcodes.append(code)
                    except Exception:
                        pass
                _pi = st.selectbox(
                    "Product",
                    options=list(range(len(_plabs))),
                    format_func=lambda i: _plabs[i],
                    key="stmt_gl_product_idx",
                    help="Optional. Journals whose loan linkage matches loans with this product_code.",
                )
                if _pi < len(_pcodes):
                    gl_product_code_val = _pcodes[_pi]

            _loan_raw = (gl_loan_in or "").strip()
            if _loan_raw:
                try:
                    _n = int(_loan_raw)
                    if _n <= 0:
                        gl_loan_err = "Loan ID must be a positive integer."
                    else:
                        gl_loan_id_val = _n
                except ValueError:
                    gl_loan_err = "Loan ID must be a whole number."

            account_filter = None if gl_account_sel == "All" else gl_account_sel.split(" - ")[0]

            if not gl_loan_err and (gl_loan_id_val or gl_product_code_val):
                st.caption(
                    "Loan / product filters are applied in SQL on journal headers (event_id, reference patterns used by "
                    "loan approval and EOD). Entries without a loan key may be excluded."
                )

            if gl_loan_err:
                st.error(gl_loan_err)
            elif account_filter:
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
                    child_rows = svc.get_child_account_summaries(
                        account_filter,
                        gl_start,
                        gl_end,
                        loan_id=gl_loan_id_val,
                        product_code=gl_product_code_val,
                    )
    
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
    
                    df_summary = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(
                        columns=["Child Account", "Opening Balance", "Debit", "Credit", "Closing Balance"]
                    )
                    df_summary_ui, _gl_summary_trunc = _gl_dataframe_for_ui(df_summary)
                    st.dataframe(df_summary_ui, use_container_width=True, hide_index=True)
                    _render_gl_server_csv_export(
                        df_summary,
                        kind="parent_summary",
                        account_code=account_filter,
                        gl_start=gl_start,
                        gl_end=gl_end,
                        button_key="stmt_gl_srv_csv_parent_summary",
                        ui_truncated=_gl_summary_trunc,
                        loan_id=gl_loan_id_val,
                        product_code=gl_product_code_val,
                    )
                    if summary_rows:
                        st.caption(f"Flow totals for period: Debit {total_dr:,.2f} | Credit {total_cr:,.2f}")
    
                else:
                    ledger = svc.get_account_ledger(
                        account_filter,
                        start_date=gl_start,
                        end_date=gl_end,
                        include_descendants=bool(_is_parent),
                        loan_id=gl_loan_id_val,
                        product_code=gl_product_code_val,
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
                        df_ledger_ui, _gl_ledger_trunc = _gl_dataframe_for_ui(df_ledger)
                        st.dataframe(df_ledger_ui, use_container_width=True, hide_index=True)
                        _render_gl_server_csv_export(
                            df_ledger,
                            kind="account_ledger",
                            account_code=account_filter,
                            gl_start=gl_start,
                            gl_end=gl_end,
                            button_key="stmt_gl_srv_csv_ledger",
                            ui_truncated=_gl_ledger_trunc,
                            loan_id=gl_loan_id_val,
                            product_code=gl_product_code_val,
                        )
                        st.caption(f"Totals for period: Debit {total_dr:,.2f} | Credit {total_cr:,.2f}")
    
                    else:
                        st.info("Account not found.")
            else:
                entries = svc.get_journal_entries(
                    start_date=gl_start,
                    end_date=gl_end,
                    account_code=account_filter,
                    loan_id=gl_loan_id_val,
                    product_code=gl_product_code_val,
                )
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
                    df_all_ui, _gl_je_trunc = _gl_dataframe_for_ui(df_all)
                    _gl_je_cols = {
                        "Debit": st.column_config.NumberColumn(format="%.10f", step=1e-10),
                        "Credit": st.column_config.NumberColumn(format="%.10f", step=1e-10),
                    }
                    st.dataframe(
                        df_all_ui,
                        use_container_width=True,
                        hide_index=True,
                        column_config=money_df_column_config(df_all_ui, overrides=_gl_je_cols),
                    )
                    _render_gl_server_csv_export(
                        df_all,
                        kind="journal_entries",
                        account_code=None,
                        gl_start=gl_start,
                        gl_end=gl_end,
                        button_key="stmt_gl_srv_csv_je_all",
                        ui_truncated=_gl_je_trunc,
                        loan_id=gl_loan_id_val,
                        product_code=gl_product_code_val,
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
    
