"""
Farnda Cred global theme: soft glassmorphism, nav tiles, custom ring cursor, Inter typography.

Inject once per session via :func:`inject_farnda_global_styles_once` using :func:`inject_style_block`
(``st.html``) so CSS is not shown as plain text. Navigation icons use
Unicode symbols / emoji in :func:`format_navigation_label` because Streamlit ``st.radio``
options are plain text (Font Awesome / Lucide markup is not rendered there). Font Awesome
is still loaded for optional use in :func:`create_card` via ``icon_html``.
"""

from __future__ import annotations

import html
import urllib.parse

import streamlit as st

_SESSION_FLAG = "_farnda_global_style_v7"

# Pre-encoded SVG cursors (hotspot at geometric center).
_CURSOR_DEFAULT_SVG = """<svg xmlns='http://www.w3.org/2000/svg' width='32' height='32' viewBox='0 0 32 32'><circle cx='16' cy='16' r='10' fill='none' stroke='rgba(0,33,71,0.3)' stroke-width='1'/><circle cx='16' cy='16' r='2' fill='%23002147'/></svg>"""
_CURSOR_POINTER_SVG = """<svg xmlns='http://www.w3.org/2000/svg' width='36' height='36' viewBox='0 0 36 36'><circle cx='18' cy='18' r='12' fill='none' stroke='rgba(0,33,71,0.65)' stroke-width='1.5'/><circle cx='18' cy='18' r='2.5' fill='%23002147'/></svg>"""


def _cursor_data_uri(svg: str) -> str:
    q = urllib.parse.quote(svg, safe="")
    return f"url(\"data:image/svg+xml;charset=utf-8,{q}\")"


CURSOR_DEFAULT = _cursor_data_uri(_CURSOR_DEFAULT_SVG) + " 16 16, auto"
CURSOR_POINTER = _cursor_data_uri(_CURSOR_POINTER_SVG) + " 18 18, pointer"


FARNDA_GLOBAL_CSS = """
/* ---- Farnda Cred — foundation (Inter + Font Awesome via @import in inject) ---- */
html, body, input, button, textarea, select {
  font-family: "Inter", "Segoe UI", Roboto, system-ui, -apple-system, sans-serif !important;
}

.stApp {
  color: #0f172a;
  """ + f"cursor: {CURSOR_DEFAULT};" + """
}

/* Main canvas: soft grey + glass panel */
[data-testid="stAppViewContainer"] > .main {
  background: #F8FAFC !important;
}

[data-testid="stAppViewContainer"] > .main .block-container {
  background: rgba(255, 255, 255, 0.78) !important;
  backdrop-filter: blur(14px) saturate(140%);
  -webkit-backdrop-filter: blur(14px) saturate(140%);
  border-radius: 16px !important;
  border: 1px solid rgba(255, 255, 255, 0.65) !important;
  box-shadow: 0 10px 40px -12px rgba(15, 23, 42, 0.12) !important;
  padding-top: 1.25rem !important;
  padding-bottom: 2rem !important;
}

/* Sidebar: white shell */
[data-testid="stSidebar"] {
  background: #FFFFFF !important;
  border-right: 1px solid rgba(15, 23, 42, 0.06) !important;
  box-shadow: 4px 0 24px -8px rgba(15, 23, 42, 0.08) !important;
}

[data-testid="stSidebar"] > div:first-child {
  background: #FFFFFF !important;
}

[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
  color: #002147 !important;
  font-weight: 700 !important;
}

/* Headings */
.stApp h1, .stApp h2, .stApp h3,
[data-testid="stAppViewContainer"] .stMarkdown h1,
[data-testid="stAppViewContainer"] .stMarkdown h2,
[data-testid="stAppViewContainer"] .stMarkdown h3 {
  color: #002147 !important;
  font-weight: 700 !important;
}

/* Primary / secondary buttons (navy, rounded) */
.stApp button[data-testid="stBaseButton-primary"] {
  background-color: #002147 !important;
  color: #ffffff !important;
  border: none !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
  transition: background-color 0.18s ease, box-shadow 0.18s ease, transform 0.12s ease !important;
  box-shadow: 0 2px 8px rgba(0, 33, 71, 0.22) !important;
}

.stApp button[data-testid="stBaseButton-primary"]:hover {
  background-color: #001a38 !important;
  box-shadow: 0 4px 14px rgba(0, 33, 71, 0.28) !important;
  """ + f"cursor: {CURSOR_POINTER} !important;" + """
}

/* Sidebar primary (e.g. Log out): bright blue; main content keeps navy primary above */
[data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] {
  width: 100% !important;
  background-color: #2563eb !important;
  color: #ffffff !important;
  border: none !important;
  box-shadow: 0 2px 8px rgba(37, 99, 235, 0.32) !important;
}

[data-testid="stSidebar"] button[data-testid="stBaseButton-primary"]:hover {
  background-color: #1d4ed8 !important;
  box-shadow: 0 4px 14px rgba(37, 99, 235, 0.4) !important;
}

.stApp button[data-testid="stBaseButton-secondary"] {
  border-radius: 8px !important;
  border: 1px solid rgba(0, 33, 71, 0.35) !important;
  color: #002147 !important;
  font-weight: 600 !important;
  transition: background-color 0.18s ease, border-color 0.18s ease !important;
}

.stApp button[data-testid="stBaseButton-secondary"]:hover {
  background-color: rgba(0, 33, 71, 0.06) !important;
  border-color: #002147 !important;
  """ + f"cursor: {CURSOR_POINTER} !important;" + """
}

/* Clickable / interactive — ring cursor expands */
.stApp a,
.stApp button,
.stApp [role="button"],
.stApp summary,
.stApp input[type="checkbox"],
.stApp input[type="radio"],
.stApp .stSelectbox div[data-baseweb="select"],
.stApp [data-testid="stSidebar"] label,
.stApp div[data-testid="collapsedControl"] {
  """ + f"cursor: {CURSOR_POINTER} !important;" + """
}

/* ---- Sidebar radio → nav tiles (Base Web under data-testid="stRadioGroup"; verified Streamlit 1.54.0) ---- */
/* Hide Base Web radio disk (sibling right after the real <input type="radio">). */
[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"] > input[type="radio"] + div {
  display: none !important;
}

/* 1.54.x fallback: any vector ring inside the option label */
[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"] svg {
  display: none !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"] input[type="radio"] {
  position: absolute !important;
  opacity: 0 !important;
  width: 0 !important;
  height: 0 !important;
  margin: 0 !important;
  clip: rect(0, 0, 0, 0) !important;
}

/* Legacy Streamlit / alternate DOM: native circle + old flex wrapper */
[data-testid="stSidebar"] .stRadio > div > label > div:first-child {
  display: none !important;
}

[data-testid="stSidebar"] .stRadio svg {
  display: none !important;
}

[data-testid="stSidebar"] .stRadio input[type="radio"] {
  position: absolute !important;
  opacity: 0 !important;
  width: 0 !important;
  height: 0 !important;
  margin: 0 !important;
}

/* Tile stack: radiogroup is the flex column (not .stRadio > div, which is widget label + group). */
[data-testid="stSidebar"] [data-testid="stRadioGroup"] {
  display: flex !important;
  flex-direction: column !important;
  gap: 0.2rem !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"] {
  display: flex !important;
  align-items: center !important;
  margin: 0 !important;
  padding: 0.62rem 0.75rem 0.62rem 0.65rem !important;
  border-radius: 8px !important;
  border-left: 4px solid transparent !important;
  transition: background-color 0.15s ease, border-color 0.15s ease !important;
  font-weight: 500 !important;
  font-size: 0.94rem !important;
  color: #0f172a !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"]:hover {
  background-color: rgba(0, 33, 71, 0.04) !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"]:has(input:checked) {
  background-color: #E3F2FD !important;
  border-left-color: #002147 !important;
  color: #002147 !important;
  font-weight: 600 !important;
}

/* Pre-1.43: options lived under .stRadio > div with role radiogroup */
[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] {
  display: flex !important;
  flex-direction: column !important;
  gap: 0.2rem !important;
}

[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label {
  display: flex !important;
  align-items: center !important;
  margin: 0 !important;
  padding: 0.62rem 0.75rem 0.62rem 0.65rem !important;
  border-radius: 8px !important;
  border-left: 4px solid transparent !important;
  transition: background-color 0.15s ease, border-color 0.15s ease !important;
  font-weight: 500 !important;
  font-size: 0.94rem !important;
  color: #0f172a !important;
}

[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label:hover {
  background-color: rgba(0, 33, 71, 0.04) !important;
}

[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label:has(input:checked) {
  background-color: #E3F2FD !important;
  border-left-color: #002147 !important;
  color: #002147 !important;
  font-weight: 600 !important;
}

/* Sidebar: sticky compact logo (nav list scrolls beneath) */
[data-testid="stSidebar"] .farnda-sidebar-sticky-head {
  position: sticky !important;
  top: 0 !important;
  z-index: 100 !important;
  background: #ffffff !important;
  padding-bottom: 0.35rem !important;
  margin: 0 0 0.2rem 0 !important;
  border-bottom: 1px solid rgba(15, 23, 42, 0.07) !important;
}

[data-testid="stSidebar"] .farnda-sidebar-logo-wrap {
  background: #ffffff !important;
  border-radius: 8px !important;
  border: 1px solid rgba(15, 23, 42, 0.06) !important;
  box-shadow: 0 1px 6px rgba(15, 23, 42, 0.05) !important;
  padding: 0.2rem 0.35rem !important;
  margin: 0 !important;
}

[data-testid="stSidebar"] .farnda-sidebar-logo-img {
  max-height: 48px !important;
  width: auto !important;
  max-width: 100% !important;
  height: auto !important;
  display: block !important;
  margin: 0 auto !important;
  object-fit: contain !important;
}

[data-testid="stSidebar"] .farnda-sidebar-wordmark-fallback {
  margin: 0 !important;
  padding: 0.15rem 0 !important;
  font-weight: 700 !important;
  color: #002147 !important;
  font-size: 0.95rem !important;
  text-align: center !important;
}

/* Login / register: slogan under hero logo */
.farnda-auth-slogan {
  text-align: center !important;
  color: #374151 !important;
  font-size: 1.025rem !important;
  margin: 0.35rem 0 0.75rem 0 !important;
}

.farnda-user-card {
  border-radius: 12px !important;
  box-shadow: 0 4px 14px -2px rgba(15, 23, 42, 0.12) !important;
  border: 1px solid rgba(15, 23, 42, 0.08) !important;
}

.farnda-system-date {
  color: #2E7D32 !important;
}

/* Metric cards (create_card) */
.farnda-metric-card {
  background: rgba(248, 250, 252, 0.95);
  border-radius: 12px;
  padding: 1rem 1.15rem;
  box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.08);
  border: 1px solid rgba(15, 23, 42, 0.06);
  margin-bottom: 0.5rem;
}
.farnda-metric-card .farnda-metric-title {
  font-size: 0.8125rem;
  font-weight: 600;
  color: #64748b;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  margin: 0 0 0.35rem 0;
}
.farnda-metric-card .farnda-metric-value {
  font-size: 1.5rem;
  font-weight: 700;
  color: #002147;
  margin: 0;
  line-height: 1.2;
}
.farnda-metric-card .farnda-metric-delta {
  font-size: 0.875rem;
  margin-top: 0.35rem;
  font-weight: 600;
}
"""


def inject_style_block(css_rules: str) -> None:
    """
    Inject a ``<style>`` block using ``st.html``.

    Streamlit's markdown renderer sanitizes ``st.markdown(..., unsafe_allow_html=True)``
    and can strip ``<style>``, leaving raw CSS visible as page text. ``st.html`` applies
    the stylesheet without that leak; style-only payloads use the event container (no layout gap).
    """
    text = css_rules.strip()
    if not text:
        return
    body = f"<style>\n{text}\n</style>"
    html_fn = getattr(st, "html", None)
    if html_fn is not None:
        html_fn(body)
    else:
        st.markdown(body, unsafe_allow_html=True)


def inject_farnda_global_styles_once() -> None:
    """Inject fonts, FA CDN, and global CSS once per browser session."""
    if st.session_state.get(_SESSION_FLAG):
        return
    st.session_state[_SESSION_FLAG] = True
    _font_fa = (
        '@import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap");\n'
        '@import url("https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css");\n'
    )
    inject_style_block(_font_fa + FARNDA_GLOBAL_CSS.strip())


# Unicode / emoji prefixes for sidebar radio labels (plain text only in Streamlit).
NAV_LABEL_ICONS: dict[str, str] = {
    "Home": "\U0001F3E0",
    "Admin Dashboard": "\U0001F6E1",
    "Officer Dashboard": "\U0001F4BC",
    "Customers": "\U0001F464",
    "Loan management": "\U0001F4B0",
    "Loan Capture": "\U0001F4DD",
    "Portfolio reports": "\U0001F4CA",
    "Teller": "\U0001F3E7",
    "Reamortisation": "\U0001F504",
    "Statements": "\U0001F4C4",
    "Accounting": "\U0001F4D2",
    "Journals": "\U0001F4D6",
    "Notifications": "\U0001F514",
    "Document Management": "\U0001F4C1",
    "End of day": "\U0001F319",
    "System configurations": "\u2699\uFE0F",
    "View Schedule": "\U0001F4C5",
    "Loan Calculators": "\U0001F9EE",
    "Update Loans": "\U0001F504",
    "Interest In Suspense": "\u23F3",
    "Approve Loans": "\u2705",
}


def format_navigation_label(section_key: str) -> str:
    """Return sidebar display text with a leading icon/symbol for nav tiles."""
    icon = NAV_LABEL_ICONS.get(section_key, "\u25C6")
    return f"{icon}  {section_key}"


def create_card(
    title: str,
    value: str,
    *,
    delta_html: str | None = None,
    icon_html: str | None = None,
) -> str:
    """
    Render a metric card as HTML for ``st.markdown(..., unsafe_allow_html=True)``.

    ``icon_html`` optional Font Awesome snippet, e.g. ``'<i class="fa-solid fa-arrow-trend-up"></i>'``.
    ``delta_html`` optional small HTML under the value (e.g. colored trend).
    """
    t = html.escape(str(title))
    v = html.escape(str(value))
    icon_block = f'<span class="farnda-metric-icon" style="margin-right:0.35rem;">{icon_html}</span>' if icon_html else ""
    delta_block = (
        f'<div class="farnda-metric-delta">{delta_html}</div>'
        if delta_html
        else ""
    )
    return f"""<div class="farnda-metric-card">{icon_block}<p class="farnda-metric-title">{t}</p><p class="farnda-metric-value">{v}</p>{delta_block}</div>"""


__all__ = [
    "create_card",
    "CURSOR_DEFAULT",
    "CURSOR_POINTER",
    "FARNDA_GLOBAL_CSS",
    "format_navigation_label",
    "inject_farnda_global_styles_once",
    "inject_style_block",
    "NAV_LABEL_ICONS",
]
