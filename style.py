"""
Farnda Cred global theme + **brand heading system**.

Brand colours: :data:`BRAND_NAVY` (``#113C7A``), :data:`BRAND_GREEN` (``#5CC346``).

- **Headings**: use :func:`render_main_header`, :func:`render_sub_header`, :func:`render_sub_sub_header`
  (HTML ``div`` elements with classes ``main-header``, ``sub-header``, ``sub-sub-header``). Level 2 and 3
  share the **same** font size, weight, and navy (:data:`BRAND_NAVY`) so section titles match Admin
  Dashboard **Users**; only ``aria-level`` differs (2 vs 3). Prefer these helpers over ``st.title`` /
  ``st.header`` / ``st.subheader``.
- **CSS**: Brand heading rules ship inside :func:`inject_farnda_global_styles_once` via
  :func:`inject_style_block` (``st.html``, reliable on Streamlit 1.54+). The bundle reapplies when
  :data:`_FARNDA_CSS_BUNDLE_VERSION` changes (session key ``_farnda_global_css_bundle_version``), so
  heading/font tweaks are not stuck behind a one-shot flag. :func:`apply_custom_styles` injects
  heading CSS for standalone scripts when the full bundle has not run.

Sidebar styling covers ``st.sidebar.radio`` / ``stNavigation``. Font Awesome is loaded for
:func:`create_card` via ``icon_html``.

**Tabs:** All ``st.tabs`` in the main canvas use the **underline** look (see
:data:`TABS_UNDERLINE_ACTIVE` / :data:`TABS_UNDERLINE_TRACK`; active underline thickness
:data:`TAB_UNDERLINE_WIDTH_PX`). Loan management uses ``st.tabs`` (not ``st.segmented_control``) so
theme styles cannot force a boxed segmented bar. ``st.segmented_control`` / ``st.pills`` in main still
use the flat ``stButtonGroup`` overrides below. For custom HTML tab rows, use
``<nav class="farnda-tab-bar">`` with ``<a class="farnda-tab">`` and ``farnda-tab--active`` on the
current item.
"""

from __future__ import annotations

import html
import urllib.parse

import streamlit as st

# Session stores this **integer**; bump when global/brand heading CSS must refresh in the browser.
_FARNDA_CSS_BUNDLE_VERSION = 46
_SESSION_FLAG = "_farnda_global_css_bundle_version"

# Heading scale: 15% softer than prior (multiply size by 0.85; weights stepped to valid CSS values)
_HEADER_TONE = 0.85
_MAIN_HEADER_FONT_PX = round(52.5 * _HEADER_TONE, 3)  # was 52.5
_MAIN_HEADER_FONT_WEIGHT = 800  # was 900; ~15% lighter emphasis
# Level-2: base 20px tuned, then +60% vs that tuned size (colour stays BRAND_NAVY)
_SUB_HEADER_FONT_SCALE = 1.6
_SUB_HEADER_FONT_PX = round(20 * _HEADER_TONE * _SUB_HEADER_FONT_SCALE, 3)
_SUB_HEADER_FONT_WEIGHT = 600  # was 700
# Level 3 uses the same size/colour as level 2 (Admin “Users” look) — ``aria-level`` stays 3 for a11y.
_SUB_SUB_HEADER_FONT_PX = _SUB_HEADER_FONT_PX
_SUB_SUB_HEADER_FONT_WEIGHT = _SUB_HEADER_FONT_WEIGHT

# Brand palette (centralized for headings and future UI tokens)
BRAND_NAVY = "#113C7A"
BRAND_GREEN = "#5CC346"
# Labels, captions, and control values (avoid washing the whole UI in theme navy)
BRAND_TEXT_BODY = "#1e293b"
BRAND_TEXT_MUTED = "#475569"
BRAND_TEXT_SOFT = "#64748b"

# Default underline tabs (``st.tabs`` in main + optional ``nav.farnda-tab-bar`` for custom menus)
TABS_UNDERLINE_ACTIVE = BRAND_GREEN
TABS_UNDERLINE_TRACK = "rgba(17, 60, 122, 0.14)"
# Active tab underline thickness (px) for ``st.tabs`` / ``nav.farnda-tab-bar`` in main
TAB_UNDERLINE_WIDTH_PX = 3
# Alias for segmented/pills bar (same visual weight when those widgets are used)
LM_SEGMENT_UNDERLINE_PX = TAB_UNDERLINE_WIDTH_PX

_BRAND_STYLE_SESSION_KEY = "_farnda_brand_header_styles_v2"

# Bundled into :func:`inject_farnda_global_styles_once`; also injected alone by :func:`apply_custom_styles` for demos.
_BRAND_HEADER_CSS = f"""
/* Strip default Streamlit spacing on markdown blocks that only wrap our heading divs */
.stApp [data-testid="stMarkdownContainer"]:has(> div.main-header),
.stApp [data-testid="stMarkdownContainer"]:has(> div.sub-header),
.stApp [data-testid="stMarkdownContainer"]:has(> div.sub-sub-header),
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"]:has(> div.main-header),
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"]:has(> div.sub-header),
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"]:has(> div.sub-sub-header) {{
  margin: 0 !important;
  padding: 0 !important;
  text-align: left !important;
}}
.stApp [data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] div.main-header),
.stApp [data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] div.sub-header),
.stApp [data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] div.sub-sub-header),
[data-testid="stSidebar"] [data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] div.main-header),
[data-testid="stSidebar"] [data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] div.sub-header),
[data-testid="stSidebar"] [data-testid="stElementContainer"]:has([data-testid="stMarkdownContainer"] div.sub-sub-header) {{
  margin-top: 0 !important;
  margin-bottom: 0 !important;
  padding-top: 0 !important;
  padding-bottom: 0 !important;
}}
/* Level 1 — logo navy; size/weight toned 15% vs prior */
.stApp div.main-header,
.stApp [data-testid="stMarkdownContainer"] div.main-header,
[data-testid="stSidebar"] div.main-header,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.main-header {{
  color: {BRAND_NAVY} !important;
  font-size: {_MAIN_HEADER_FONT_PX}px !important;
  font-weight: {_MAIN_HEADER_FONT_WEIGHT} !important;
  text-align: left !important;
  margin: 0 !important;
  padding: 0 0 0.5rem 0 !important;
  line-height: 1.15 !important;
  letter-spacing: 0.02em !important;
  box-sizing: border-box !important;
  -webkit-font-smoothing: antialiased !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
}}
[data-testid="stMain"] [data-testid="stMarkdownContainer"] div.main-header {{
  color: {BRAND_NAVY} !important;
  font-size: {_MAIN_HEADER_FONT_PX}px !important;
  font-weight: {_MAIN_HEADER_FONT_WEIGHT} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
}}
/* Level 2 — replaces st.header (navy; same brand as level 1) */
.stApp div.sub-header,
.stApp [data-testid="stMarkdownContainer"] div.sub-header,
[data-testid="stSidebar"] div.sub-header,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.sub-header {{
  color: {BRAND_NAVY} !important;
  font-size: {_SUB_HEADER_FONT_PX}px !important;
  font-weight: {_SUB_HEADER_FONT_WEIGHT} !important;
  text-align: left !important;
  margin: 0 !important;
  padding: 0.35rem 0 0.25rem 0 !important;
  line-height: 1.3 !important;
  box-sizing: border-box !important;
  -webkit-font-smoothing: antialiased !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
}}
[data-testid="stMain"] [data-testid="stMarkdownContainer"] div.sub-header {{
  color: {BRAND_NAVY} !important;
  font-size: {_SUB_HEADER_FONT_PX}px !important;
  font-weight: {_SUB_HEADER_FONT_WEIGHT} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
}}
/* Level 3 — same visual scale as level 2 (replicates Admin Dashboard “Users” across the app) */
.stApp div.sub-sub-header,
.stApp [data-testid="stMarkdownContainer"] div.sub-sub-header,
[data-testid="stSidebar"] div.sub-sub-header,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.sub-sub-header {{
  color: {BRAND_NAVY} !important;
  font-size: {_SUB_SUB_HEADER_FONT_PX}px !important;
  font-weight: {_SUB_SUB_HEADER_FONT_WEIGHT} !important;
  text-align: left !important;
  margin: 0 !important;
  padding: 0.35rem 0 0.25rem 0 !important;
  line-height: 1.3 !important;
  box-sizing: border-box !important;
  -webkit-font-smoothing: antialiased !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
}}
[data-testid="stMain"] [data-testid="stMarkdownContainer"] div.sub-sub-header {{
  color: {BRAND_NAVY} !important;
  font-size: {_SUB_SUB_HEADER_FONT_PX}px !important;
  font-weight: {_SUB_SUB_HEADER_FONT_WEIGHT} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
}}
/* ---- Beat Streamlit theme: navy + font-size/weight on headings and descendants (st.markdown + st.html) ---- */
html body .stApp div.main-header,
html body .stApp [data-testid="stMarkdownContainer"] div.main-header,
html body [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] div.main-header,
html body [data-testid="stAppViewContainer"] > .main [data-testid="stMarkdownContainer"] div.main-header,
html body [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.main-header,
html body .stApp [data-testid="stHtml"] div.main-header,
html body [data-testid="stAppViewContainer"] [data-testid="stHtml"] div.main-header {{
  color: {BRAND_NAVY} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
  font-size: {_MAIN_HEADER_FONT_PX}px !important;
  font-weight: {_MAIN_HEADER_FONT_WEIGHT} !important;
}}
html body .stApp div.main-header *,
html body .stApp [data-testid="stMarkdownContainer"] div.main-header *,
html body [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] div.main-header *,
html body [data-testid="stAppViewContainer"] > .main [data-testid="stMarkdownContainer"] div.main-header *,
html body [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.main-header *,
html body .stApp [data-testid="stHtml"] div.main-header *,
html body [data-testid="stAppViewContainer"] [data-testid="stHtml"] div.main-header * {{
  color: {BRAND_NAVY} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
  font-size: {_MAIN_HEADER_FONT_PX}px !important;
  font-weight: {_MAIN_HEADER_FONT_WEIGHT} !important;
}}
html body .stApp div.sub-header,
html body .stApp [data-testid="stMarkdownContainer"] div.sub-header,
html body [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] div.sub-header,
html body [data-testid="stAppViewContainer"] > .main [data-testid="stMarkdownContainer"] div.sub-header,
html body [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.sub-header,
html body .stApp [data-testid="stHtml"] div.sub-header,
html body [data-testid="stAppViewContainer"] [data-testid="stHtml"] div.sub-header {{
  color: {BRAND_NAVY} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
  font-size: {_SUB_HEADER_FONT_PX}px !important;
  font-weight: {_SUB_HEADER_FONT_WEIGHT} !important;
}}
html body .stApp div.sub-header *,
html body .stApp [data-testid="stMarkdownContainer"] div.sub-header *,
html body [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] div.sub-header *,
html body [data-testid="stAppViewContainer"] > .main [data-testid="stMarkdownContainer"] div.sub-header *,
html body [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.sub-header *,
html body .stApp [data-testid="stHtml"] div.sub-header *,
html body [data-testid="stAppViewContainer"] [data-testid="stHtml"] div.sub-header * {{
  color: {BRAND_NAVY} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
  font-size: {_SUB_HEADER_FONT_PX}px !important;
  font-weight: {_SUB_HEADER_FONT_WEIGHT} !important;
}}
html body .stApp div.sub-sub-header,
html body .stApp [data-testid="stMarkdownContainer"] div.sub-sub-header,
html body [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] div.sub-sub-header,
html body [data-testid="stAppViewContainer"] > .main [data-testid="stMarkdownContainer"] div.sub-sub-header,
html body [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.sub-sub-header,
html body .stApp [data-testid="stHtml"] div.sub-sub-header,
html body [data-testid="stAppViewContainer"] [data-testid="stHtml"] div.sub-sub-header {{
  color: {BRAND_NAVY} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
  font-size: {_SUB_SUB_HEADER_FONT_PX}px !important;
  font-weight: {_SUB_SUB_HEADER_FONT_WEIGHT} !important;
}}
html body .stApp div.sub-sub-header *,
html body .stApp [data-testid="stMarkdownContainer"] div.sub-sub-header *,
html body [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] div.sub-sub-header *,
html body [data-testid="stAppViewContainer"] > .main [data-testid="stMarkdownContainer"] div.sub-sub-header *,
html body [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] div.sub-sub-header *,
html body .stApp [data-testid="stHtml"] div.sub-sub-header *,
html body [data-testid="stAppViewContainer"] [data-testid="stHtml"] div.sub-sub-header * {{
  color: {BRAND_NAVY} !important;
  -webkit-text-fill-color: {BRAND_NAVY} !important;
  font-size: {_SUB_SUB_HEADER_FONT_PX}px !important;
  font-weight: {_SUB_SUB_HEADER_FONT_WEIGHT} !important;
}}
"""

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
  color: """ + BRAND_TEXT_BODY + """ !important;
  """ + f"cursor: {CURSOR_DEFAULT};" + """
}

/* Form controls: neutral greys/slate so labels & values are not tinted brand blue */
.stApp [data-testid="stWidgetLabel"] p,
.stApp [data-testid="stWidgetLabel"] span,
.stApp [data-testid="stWidgetLabel"] label {
  color: """ + BRAND_TEXT_MUTED + """ !important;
}
.stApp [data-testid="stTextInput"] input,
.stApp [data-testid="stNumberInput"] input,
.stApp [data-testid="stTextArea"] textarea,
.stApp [data-testid="stDateInput"] input,
.stApp [data-testid="stTimeInput"] input {
  color: """ + BRAND_TEXT_BODY + """ !important;
}
.stApp [data-testid="stSelectbox"] [data-baseweb="select"] > div,
.stApp [data-testid="stMultiSelect"] [data-baseweb="select"] > div {
  color: """ + BRAND_TEXT_BODY + """ !important;
}
.stApp [data-testid="stCheckbox"] label,
.stApp [data-testid="stCheckbox"] label span {
  color: """ + BRAND_TEXT_MUTED + """ !important;
}
/* Horizontal radios in main (e.g. “Find loan by”) — keep readable grey; sidebar nav radios stay tiled below */
[data-testid="stMain"] [data-testid="stRadioGroup"] label[data-baseweb="radio"],
[data-testid="stMain"] [data-testid="stRadioGroup"] label[data-baseweb="radio"] p {
  color: """ + BRAND_TEXT_MUTED + """ !important;
}
.stApp [data-testid="stCaption"] {
  color: """ + BRAND_TEXT_SOFT + """ !important;
}
.stApp [data-testid="stCaption"] * {
  color: inherit !important;
}
/* ---- Underline tabs (``st.tabs`` in main + ``.farnda-tab-bar``); also ``.main`` if ``stMain`` absent ---- */
[data-testid="stMain"] [data-baseweb="tab-list"],
[data-testid="stAppViewContainer"] > .main [data-baseweb="tab-list"] {
  gap: 0.45rem !important;
  border-bottom: 1px solid """ + TABS_UNDERLINE_TRACK + """ !important;
  background: transparent !important;
  box-shadow: none !important;
  padding: 0 !important;
  min-height: auto !important;
}
[data-testid="stMain"] [data-baseweb="tab-list"] button[role="tab"],
[data-testid="stAppViewContainer"] > .main [data-baseweb="tab-list"] button[role="tab"] {
  background: transparent !important;
  border: none !important;
  border-radius: 0 !important;
  box-shadow: none !important;
  outline: none !important;
  margin: 0 0 -1px 0 !important;
  padding: 0.48rem 0.65rem 0.42rem !important;
  min-height: auto !important;
  border-bottom: """ + str(TAB_UNDERLINE_WIDTH_PX) + """px solid transparent !important;
  color: """ + BRAND_NAVY + """ !important;
  font-weight: 600 !important;
  letter-spacing: 0.02em !important;
}
[data-testid="stMain"] [data-baseweb="tab-list"] button[role="tab"] p,
[data-testid="stAppViewContainer"] > .main [data-baseweb="tab-list"] button[role="tab"] p {
  color: inherit !important;
}
[data-testid="stMain"] [data-baseweb="tab-list"] button[role="tab"]:hover,
[data-testid="stAppViewContainer"] > .main [data-baseweb="tab-list"] button[role="tab"]:hover {
  color: """ + BRAND_NAVY + """ !important;
  background: rgba(17, 60, 122, 0.04) !important;
}
[data-testid="stMain"] [data-baseweb="tab-list"] button[role="tab"][aria-selected="true"],
[data-testid="stAppViewContainer"] > .main [data-baseweb="tab-list"] button[role="tab"][aria-selected="true"] {
  color: """ + BRAND_NAVY + """ !important;
  border-bottom-color: """ + BRAND_GREEN + """ !important;
  font-weight: 600 !important;
  background: transparent !important;
}
/* Custom menus: <nav class="farnda-tab-bar" aria-label="…">…</nav> with <a class="farnda-tab"> / .farnda-tab--active */
[data-testid="stMain"] nav.farnda-tab-bar,
.stApp main nav.farnda-tab-bar {
  display: flex !important;
  flex-wrap: wrap !important;
  gap: 0.35rem !important;
  align-items: flex-end !important;
  border-bottom: 1px solid """ + TABS_UNDERLINE_TRACK + """ !important;
  margin: 0.1rem 0 0.75rem 0 !important;
  padding: 0 !important;
  background: transparent !important;
}
[data-testid="stMain"] .farnda-tab-bar .farnda-tab,
.stApp main .farnda-tab-bar .farnda-tab {
  display: inline-block !important;
  margin: 0 0 -1px 0 !important;
  padding: 0.42rem 0.75rem 0.48rem !important;
  border: none !important;
  border-bottom: """ + str(TAB_UNDERLINE_WIDTH_PX) + """px solid transparent !important;
  background: transparent !important;
  color: """ + BRAND_NAVY + """ !important;
  font-weight: 600 !important;
  font-size: 1rem !important;
  text-decoration: none !important;
  cursor: pointer !important;
  font-family: inherit !important;
  letter-spacing: 0.02em !important;
}
[data-testid="stMain"] .farnda-tab-bar .farnda-tab:hover,
.stApp main .farnda-tab-bar .farnda-tab:hover {
  color: """ + BRAND_NAVY + """ !important;
  background: rgba(17, 60, 122, 0.05) !important;
}
[data-testid="stMain"] .farnda-tab-bar .farnda-tab.farnda-tab--active,
.stApp main .farnda-tab-bar .farnda-tab.farnda-tab--active {
  color: """ + BRAND_NAVY + """ !important;
  border-bottom-color: """ + BRAND_GREEN + """ !important;
  font-weight: 600 !important;
}
/* ``st.segmented_control`` / ``st.pills`` (Baseweb button-group; theme often wins on segmented look) */
[data-testid="stMain"] [data-testid="stButtonGroup"],
[data-testid="stMain"] [data-testid="stButtonGroup"] > div,
[data-testid="stAppViewContainer"] > .main [data-testid="stButtonGroup"],
[data-testid="stAppViewContainer"] > .main [data-testid="stButtonGroup"] > div {
  background: transparent !important;
  border: none !important;
  border-radius: 0 !important;
  box-shadow: none !important;
  outline: none !important;
}
[data-testid="stMain"] [data-testid="stButtonGroup"] [data-baseweb="button-group"],
[data-testid="stAppViewContainer"] > .main [data-testid="stButtonGroup"] [data-baseweb="button-group"] {
  display: flex !important;
  flex-wrap: wrap !important;
  align-items: flex-end !important;
  gap: 0.5rem !important;
  column-gap: 0.65rem !important;
  row-gap: 0.25rem !important;
  background: transparent !important;
  border: none !important;
  border-radius: 0 !important;
  box-shadow: none !important;
  outline: none !important;
  padding: 0 !important;
  margin: 0.15rem 0 0.55rem 0 !important;
  border-bottom: none !important;
}
[data-testid="stMain"] [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"],
[data-testid="stAppViewContainer"] > .main [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"] {
  background: transparent !important;
  border: none !important;
  border-left: none !important;
  border-right: none !important;
  border-top: none !important;
  border-radius: 0 !important;
  box-shadow: none !important;
  outline: none !important;
  margin: 0 !important;
  padding: 0.48rem 0.2rem 0.42rem !important;
  min-height: auto !important;
  border-bottom: """ + str(LM_SEGMENT_UNDERLINE_PX) + """px solid transparent !important;
  color: """ + BRAND_NAVY + """ !important;
  font-weight: 600 !important;
  letter-spacing: 0.02em !important;
}
[data-testid="stMain"] [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"]:hover,
[data-testid="stAppViewContainer"] > .main [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"]:hover {
  color: """ + BRAND_NAVY + """ !important;
  background: rgba(17, 60, 122, 0.04) !important;
  border-bottom-color: transparent !important;
}
[data-testid="stMain"] [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"][aria-checked="true"],
[data-testid="stAppViewContainer"] > .main [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"][aria-checked="true"] {
  color: """ + BRAND_NAVY + """ !important;
  border-bottom-color: """ + BRAND_GREEN + """ !important;
  font-weight: 600 !important;
  background: transparent !important;
}
[data-testid="stMain"] [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"] p,
[data-testid="stMain"] [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"] span,
[data-testid="stAppViewContainer"] > .main [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"] p,
[data-testid="stAppViewContainer"] > .main [data-testid="stButtonGroup"] [data-baseweb="button-group"] [role="radio"] span {
  color: inherit !important;
}
/* Loan Capture shortcuts: marker row then horizontal block (inside Loan Capture tab panel) */
[data-testid="stMain"] [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"],
[data-testid="stAppViewContainer"] > .main [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] {
  margin-top: 0 !important;
  margin-bottom: 0.35rem !important;
  align-items: center !important;
}
[data-testid="stMain"] [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] button[data-testid="stBaseButton-tertiary"],
[data-testid="stMain"] [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] button[data-testid="baseButton-tertiary"],
[data-testid="stAppViewContainer"] > .main [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] button[data-testid="stBaseButton-tertiary"],
[data-testid="stAppViewContainer"] > .main [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] button[data-testid="baseButton-tertiary"] {
  color: """ + BRAND_TEXT_SOFT + """ !important;
  font-size: 0.8125rem !important;
  font-weight: 500 !important;
  text-decoration: underline !important;
  text-underline-offset: 0.15em !important;
  padding: 0.08rem 0.2rem !important;
  min-height: auto !important;
  gap: 0.28rem !important;
}
[data-testid="stMain"] [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] button[data-testid="stBaseButton-tertiary"]:hover,
[data-testid="stMain"] [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] button[data-testid="baseButton-tertiary"]:hover,
[data-testid="stAppViewContainer"] > .main [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] button[data-testid="stBaseButton-tertiary"]:hover,
[data-testid="stAppViewContainer"] > .main [data-testid="stElementContainer"]:has(.farnda-lm-subnav-secondary)
  + [data-testid="stElementContainer"] [data-testid="stHorizontalBlock"] button[data-testid="baseButton-tertiary"]:hover {
  color: """ + BRAND_NAVY + """ !important;
}
/* Primary buttons: keep white label on navy/green — reset inherited label rules */
.stApp button[data-testid="stBaseButton-primary"] p,
.stApp button[data-testid="stBaseButton-primary"] span,
.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] p,
.stApp [data-testid="stSidebar"] button[kind="primary"] p {
  color: #ffffff !important;
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

/* streamlit-option-menu: baseline iframe height; main._apply_sidebar_option_menu_iframe_height tightens per menu count */
[data-testid="stSidebar"] iframe[data-testid="stIFrame"] {
  min-height: max(520px, calc(100dvh - 200px)) !important;
  max-height: none !important;
}

/* Custom sidebar nav tiles (pure HTML anchors, no radio/button widgets). */
[data-testid="stSidebar"] .farnda-nav-stack {
  display: block !important;
  width: 100% !important;
  margin: 0 !important;
  padding: 0 !important;
  border: 1px solid rgba(0, 33, 71, 0.2) !important;
  border-radius: 8px !important;
  overflow: hidden !important;
}

[data-testid="stSidebar"] .farnda-nav-tile {
  display: block !important;
  width: 100% !important;
  box-sizing: border-box !important;
  clear: both !important;
  float: none !important;
  margin: 0 !important;
  padding: 0.48rem 0.6rem !important;
  border-bottom: 1px solid rgba(0, 33, 71, 0.14) !important;
  color: #0f172a !important;
  text-decoration: none !important;
  text-align: left !important;
  line-height: 1.12 !important;
  font-size: 0.86rem !important;
  font-weight: 500 !important;
  letter-spacing: 0.03em !important;
  white-space: normal !important;
}

[data-testid="stSidebar"] .farnda-nav-stack .farnda-nav-tile:last-child {
  border-bottom: none !important;
}

[data-testid="stSidebar"] .farnda-nav-tile:hover {
  background: rgba(0, 33, 71, 0.05) !important;
  color: #0f172a !important;
  text-decoration: none !important;
}

[data-testid="stSidebar"] .farnda-nav-tile.farnda-nav-tile--active {
  background: #dbeafe !important;
  color: #1e3a8a !important;
  font-weight: 600 !important;
}

[data-testid="stSidebar"] .farnda-nav-tile:visited {
  color: inherit !important;
}

/* Native Streamlit h1–h3 fallback (prefer render_*_header helpers) */
.stApp h1,
[data-testid="stAppViewContainer"] .stMarkdown h1 {
  color: #113C7A !important;
  font-size: """ + f"{_MAIN_HEADER_FONT_PX}px" + """ !important;
  font-weight: """ + str(_MAIN_HEADER_FONT_WEIGHT) + """ !important;
  text-align: left !important;
}
.stApp h2,
[data-testid="stAppViewContainer"] .stMarkdown h2 {
  color: """ + BRAND_NAVY + """ !important;
  -webkit-text-fill-color: """ + BRAND_NAVY + """ !important;
  font-size: """ + f"{_SUB_HEADER_FONT_PX}px" + """ !important;
  font-weight: """ + str(_SUB_HEADER_FONT_WEIGHT) + """ !important;
  text-align: left !important;
}
.stApp h3,
[data-testid="stAppViewContainer"] .stMarkdown h3 {
  color: #113C7A !important;
  font-size: """ + f"{_SUB_SUB_HEADER_FONT_PX}px" + """ !important;
  font-weight: """ + str(_SUB_SUB_HEADER_FONT_WEIGHT) + """ !important;
  text-align: left !important;
}

/* Primary / secondary buttons (brand navy, rounded) */
.stApp button[data-testid="stBaseButton-primary"] {
  background-color: #113C7A !important;
  color: #ffffff !important;
  border: none !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
  transition: background-color 0.18s ease, box-shadow 0.18s ease, transform 0.12s ease !important;
  box-shadow: 0 2px 8px rgba(0, 33, 71, 0.22) !important;
}

.stApp button[data-testid="stBaseButton-primary"]:hover {
  background-color: #0d2f63 !important;
  box-shadow: 0 4px 14px rgba(0, 33, 71, 0.28) !important;
  """ + f"cursor: {CURSOR_POINTER} !important;" + """
}

/* Sidebar buttons: Streamlit 1.54 wrapper spacing + compact stacked tiles */
.stApp [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
  gap: 0 !important;
  row-gap: 0 !important;
}

.stApp [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {
  margin: 0 !important;
  padding: 0 !important;
}

.stApp [data-testid="stSidebar"] [data-testid="stButton"] {
  margin: 0 !important;
  padding: 0 !important;
}

.stApp [data-testid="stSidebar"] .stButton {
  margin: 0 !important;
  padding: 0 !important;
}

.stApp [data-testid="stSidebar"] [data-testid="stButton"] > div {
  margin: 0 !important;
  padding: 0 !important;
}

.stApp [data-testid="stSidebar"] [data-testid="stElementContainer"] {
  margin-top: 0 !important;
  margin-bottom: 0 !important;
  padding-top: 0 !important;
  padding-bottom: 0 !important;
}

.stApp [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > [data-testid="stElementContainer"] {
  margin: 0 !important;
  padding: 0 !important;
}

/* Sidebar buttons: blue primary (override theme red), left-aligned label + emoji */
.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"],
.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-secondary"] {
  width: 100% !important;
  min-height: 1.6rem !important;
  margin: 0 !important;
  padding: 0.14rem 0.45rem !important;
  border-radius: 6px !important;
  display: flex !important;
  flex-direction: row !important;
  align-items: center !important;
  justify-content: flex-start !important;
  text-align: left !important;
  line-height: 1.2 !important;
  letter-spacing: 0.03em !important;
  background-image: none !important;
}

/* Streamlit 1.54 fallback: sidebar buttons may be emitted as BaseWeb kind buttons without testids */
.stApp [data-testid="stSidebar"] button[kind="primary"],
.stApp [data-testid="stSidebar"] button[kind="secondary"] {
  width: 100% !important;
  min-height: 1.6rem !important;
  margin: 0 !important;
  padding: 0.14rem 0.45rem !important;
  border-radius: 6px !important;
  display: flex !important;
  flex-direction: row !important;
  align-items: center !important;
  justify-content: flex-start !important;
  text-align: left !important;
  line-height: 1.2 !important;
}

.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] > div,
.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-secondary"] > div,
.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] > span,
.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-secondary"] > span {
  width: 100% !important;
  display: flex !important;
  flex-direction: row !important;
  align-items: center !important;
  justify-content: flex-start !important;
  text-align: left !important;
}

.stApp [data-testid="stSidebar"] button[kind="primary"] > div,
.stApp [data-testid="stSidebar"] button[kind="secondary"] > div,
.stApp [data-testid="stSidebar"] button[kind="primary"] > span,
.stApp [data-testid="stSidebar"] button[kind="secondary"] > span {
  width: 100% !important;
  display: flex !important;
  flex-direction: row !important;
  align-items: center !important;
  justify-content: flex-start !important;
  text-align: left !important;
}

.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] p,
.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-secondary"] p {
  text-align: left !important;
  width: 100% !important;
  margin: 0 !important;
}

.stApp [data-testid="stSidebar"] button[kind="primary"] p,
.stApp [data-testid="stSidebar"] button[kind="secondary"] p {
  text-align: left !important;
  width: 100% !important;
  margin: 0 !important;
}

.stApp [data-testid="stSidebar"] button[data-testid^="stBaseButton"] [data-testid="stMarkdownContainer"] {
  width: 100% !important;
  text-align: left !important;
}

.stApp [data-testid="stSidebar"] button[data-testid^="stBaseButton"] [data-testid="stMarkdownContainer"] p {
  display: flex !important;
  align-items: center !important;
  justify-content: flex-start !important;
  gap: 0.25rem !important;
  margin: 0 !important;
}

.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] {
  background-color: #2563eb !important;
  color: #ffffff !important;
  border: none !important;
  box-shadow: none !important;
}

.stApp [data-testid="stSidebar"] button[kind="primary"] {
  background-color: #2563eb !important;
  color: #ffffff !important;
  border: none !important;
  box-shadow: none !important;
}

.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"]:hover,
.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"]:focus-visible {
  background-color: #1d4ed8 !important;
  color: #ffffff !important;
  box-shadow: none !important;
}

.stApp [data-testid="stSidebar"] button[kind="primary"]:hover,
.stApp [data-testid="stSidebar"] button[kind="primary"]:focus-visible {
  background-color: #1d4ed8 !important;
  color: #ffffff !important;
  box-shadow: none !important;
}

.stApp [data-testid="stSidebar"] button[data-testid="stBaseButton-secondary"] {
  background-color: #ffffff !important;
  color: #002147 !important;
  box-shadow: none !important;
}

.stApp [data-testid="stSidebar"] button[kind="secondary"] {
  background-color: #ffffff !important;
  color: #002147 !important;
  box-shadow: none !important;
  border: 1px solid rgba(0, 33, 71, 0.35) !important;
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

/* Extra safety for 1.54 variants: suppress any pseudo radio marker drawn on the label. */
[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"]::before,
[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"]::after {
  display: none !important;
  content: none !important;
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

[data-testid="stSidebar"] .stRadio > div > label::before,
[data-testid="stSidebar"] .stRadio > div > label::after {
  display: none !important;
  content: none !important;
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
  gap: 0 !important;
  border: 1px solid rgba(0, 33, 71, 0.2) !important;
  border-radius: 8px !important;
  overflow: hidden !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"] {
  display: flex !important;
  align-items: center !important;
  justify-content: flex-start !important;
  margin: 0 !important;
  padding: 0.46rem 0.62rem 0.46rem 0.55rem !important;
  border-radius: 0 !important;
  border-left: none !important;
  border-bottom: 1px solid rgba(0, 33, 71, 0.14) !important;
  transition: background-color 0.15s ease, border-color 0.15s ease !important;
  font-weight: 500 !important;
  font-size: 0.86rem !important;
  line-height: 1.1 !important;
  color: #0f172a !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"]:last-child {
  border-bottom: none !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"]:hover {
  background-color: rgba(0, 33, 71, 0.04) !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"]:has(input:checked) {
  background-color: #dbeafe !important;
  color: #1e3a8a !important;
  font-weight: 600 !important;
}

[data-testid="stSidebar"] [data-testid="stRadioGroup"] label[data-baseweb="radio"] p {
  margin: 0 !important;
  width: 100% !important;
  text-align: left !important;
}

/* app.py “Section” nav: space under tile stack so last row (e.g. Subscription) is not tight to divider */
[data-testid="stSidebar"] .stRadio {
  margin-bottom: 0.85rem !important;
}

/* Pre-1.43: options lived under .stRadio > div with role radiogroup */
[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] {
  display: flex !important;
  flex-direction: column !important;
  gap: 0 !important;
  border: 1px solid rgba(0, 33, 71, 0.2) !important;
  border-radius: 8px !important;
  overflow: hidden !important;
}

[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label {
  display: flex !important;
  align-items: center !important;
  justify-content: flex-start !important;
  margin: 0 !important;
  padding: 0.46rem 0.62rem 0.46rem 0.55rem !important;
  border-radius: 0 !important;
  border-left: none !important;
  border-bottom: 1px solid rgba(0, 33, 71, 0.14) !important;
  transition: background-color 0.15s ease, border-color 0.15s ease !important;
  font-weight: 500 !important;
  font-size: 0.86rem !important;
  line-height: 1.1 !important;
  color: #0f172a !important;
}

[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label:last-child {
  border-bottom: none !important;
}

[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label:hover {
  background-color: rgba(0, 33, 71, 0.04) !important;
}

[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label:has(input:checked) {
  background-color: #dbeafe !important;
  color: #1e3a8a !important;
  font-weight: 600 !important;
}

/* ---- Streamlit 1.54+ st.navigation sidebar (data-testid stSidebarNav / stSidebarNavItems) ---- */
/* No .st-emotion-cache-* (hashes change). Do not hide stMarkdownContainer here — it can remove real labels. */
[data-testid="stSidebarNav"] input[type="radio"],
[data-testid="stSidebarNavItems"] input[type="radio"],
[data-testid="stSidebarNavItems"] [role="radiogroup"] input[type="radio"] {
  display: none !important;
  position: absolute !important;
  opacity: 0 !important;
  width: 0 !important;
  height: 0 !important;
  margin: 0 !important;
  padding: 0 !important;
  clip: rect(0, 0, 0, 0) !important;
  pointer-events: none !important;
}

/* Base Web disk: div immediately after the hidden radio (same idea as stRadioGroup). */
[data-testid="stSidebarNavItems"] label[data-baseweb="radio"] > input[type="radio"] + div,
[data-testid="stSidebarNavItems"] label > input[type="radio"] + div {
  display: none !important;
}

/* Radio-style nav only — do not hide SVGs on <a> multipage links (material icons). */
[data-testid="stSidebarNavItems"] [role="radiogroup"] label svg,
[data-testid="stSidebarNavItems"] label[data-baseweb="radio"] svg {
  display: none !important;
}

[data-testid="stSidebarNavItems"] [role="radiogroup"] label,
[data-testid="stSidebarNavItems"] label[data-baseweb="radio"] {
  display: flex !important;
  align-items: center !important;
  width: 100% !important;
  max-width: 100% !important;
  box-sizing: border-box !important;
  margin: 0.1rem 0 0.1rem -12px !important;
  padding: 0.62rem 0.75rem 0.62rem 10px !important;
  border-radius: 8px !important;
  border-left: 4px solid transparent !important;
  cursor: pointer !important;
  transition: background-color 0.2s ease, border-color 0.2s ease !important;
  font-weight: 500 !important;
  font-size: 0.94rem !important;
  background: transparent !important;
}

[data-testid="stSidebarNavItems"] [role="radiogroup"] label:hover,
[data-testid="stSidebarNavItems"] label[data-baseweb="radio"]:hover {
  background-color: #f0f2f6 !important;
}

[data-testid="stSidebarNavItems"] [role="radiogroup"] label:has(input:checked) {
  background-color: #E3F2FD !important;
  border-left-color: #002147 !important;
  color: #002147 !important;
  font-weight: 600 !important;
}

/* Base Web may set aria-checked on an inner node instead of :has(checked) */
[data-testid="stSidebarNavItems"] [aria-checked="true"] {
  background-color: rgba(30, 136, 229, 0.1) !important;
  border-left: 5px solid #002147 !important;
  font-weight: 700 !important;
  color: #002147 !important;
}

[data-testid="stSidebarNavItems"] > li {
  margin: 0 !important;
  padding: 0 !important;
  list-style: none !important;
}

[data-testid="stSidebarNavLinkContainer"] {
  width: 100% !important;
  max-width: 100% !important;
  box-sizing: border-box !important;
}

[data-testid="stSidebarNavItems"] a[href] {
  display: flex !important;
  flex-direction: row !important;
  align-items: center !important;
  width: 100% !important;
  max-width: 100% !important;
  box-sizing: border-box !important;
  border-radius: 8px !important;
  padding: 0.62rem 0.75rem !important;
  margin: 0.12rem 0 !important;
  border-left: 4px solid transparent !important;
  text-decoration: none !important;
  transition: background-color 0.15s ease, border-color 0.15s ease !important;
  font-weight: 500 !important;
  font-size: 0.94rem !important;
  color: inherit !important;
}

[data-testid="stSidebarNavItems"] a[href]:hover {
  background-color: #f0f2f6 !important;
}

[data-testid="stSidebarNavItems"] a[href][aria-current="page"] {
  background-color: #E3F2FD !important;
  border-left-color: #002147 !important;
  font-weight: 600 !important;
  color: #002147 !important;
}

[data-testid="stSidebarNavItems"] svg {
  flex-shrink: 0 !important;
}

/* Sidebar: sticky compact logo (nav list scrolls beneath) */
[data-testid="stSidebar"] .farnda-sidebar-sticky-head {
  position: sticky !important;
  top: 0 !important;
  z-index: 100 !important;
  background: #ffffff !important;
  padding-bottom: 0 !important;
  margin: 0 !important;
  border-bottom: none !important;
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

/* Login / register: wordmark if image files are absent */
.farnda-auth-wordmark-fallback {
  text-align: center !important;
  font-weight: 700 !important;
  font-size: 1.75rem !important;
  color: #002147 !important;
  margin: 0.25rem 0 0.5rem 0 !important;
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
  color: #113C7A;
  margin: 0;
  line-height: 1.2;
}
.farnda-metric-card .farnda-metric-delta {
  font-size: 0.875rem;
  margin-top: 0.35rem;
  font-weight: 600;
}
"""


def apply_custom_styles() -> None:
    """
    Ensure brand heading CSS is present once per session.

    When :func:`inject_farnda_global_styles_once` has already run, rules are in the main
    ``inject_style_block`` bundle and this is a no-op. Standalone pages should call this
    (or ``inject_farnda_global_styles_once``) so ``st.markdown`` heading divs are styled —
    ``st.markdown``-only ``<style>`` injection is unreliable on Streamlit 1.54+.
    """
    if st.session_state.get(_BRAND_STYLE_SESSION_KEY):
        return
    st.session_state[_BRAND_STYLE_SESSION_KEY] = True
    if st.session_state.get(_SESSION_FLAG):
        return
    inject_style_block(_BRAND_HEADER_CSS.strip())


def _markdown_brand(html_fragment: str, *, sidebar: bool) -> None:
    if sidebar:
        st.sidebar.markdown(html_fragment, unsafe_allow_html=True)
    else:
        st.markdown(html_fragment, unsafe_allow_html=True)


def _main_header_inline_style_attr() -> str:
    """Inline CSS so navy/size/weight survive Streamlit markdown sanitization (class-based rules often never apply)."""
    return (
        f"color:{BRAND_NAVY} !important;-webkit-text-fill-color:{BRAND_NAVY} !important;"
        f"font-size:{_MAIN_HEADER_FONT_PX}px !important;font-weight:{_MAIN_HEADER_FONT_WEIGHT} !important;"
        f"text-align:left !important;margin:0 !important;padding:0 0 0.5rem 0 !important;"
        f"line-height:1.15 !important;letter-spacing:0.02em !important;"
        f"box-sizing:border-box !important;-webkit-font-smoothing:antialiased !important;"
        f"display:block !important;background:transparent !important;"
    )


def _sub_header_inline_style_attr() -> str:
    """Inline navy + scale for level-2 headings (matches ``_BRAND_HEADER_CSS``)."""
    return (
        f"color:{BRAND_NAVY} !important;-webkit-text-fill-color:{BRAND_NAVY} !important;"
        f"font-size:{_SUB_HEADER_FONT_PX}px !important;font-weight:{_SUB_HEADER_FONT_WEIGHT} !important;"
        f"text-align:left !important;margin:0 !important;padding:0.35rem 0 0.25rem 0 !important;"
        f"line-height:1.3 !important;box-sizing:border-box !important;-webkit-font-smoothing:antialiased !important;"
        f"display:block !important;background:transparent !important;"
    )


def _sub_sub_header_inline_style_attr() -> str:
    """Inline navy + scale for level-3 headings (same px/weight/padding as level-2 / Admin “Users”)."""
    return (
        f"color:{BRAND_NAVY} !important;-webkit-text-fill-color:{BRAND_NAVY} !important;"
        f"font-size:{_SUB_SUB_HEADER_FONT_PX}px !important;font-weight:{_SUB_SUB_HEADER_FONT_WEIGHT} !important;"
        f"text-align:left !important;margin:0 !important;padding:0.35rem 0 0.25rem 0 !important;"
        f"line-height:1.3 !important;box-sizing:border-box !important;-webkit-font-smoothing:antialiased !important;"
        f"display:block !important;background:transparent !important;"
    )


def _emit_brand_heading_html(fragment: str, *, sidebar: bool) -> None:
    dg = st.sidebar if sidebar else st
    html_m = getattr(dg, "html", None)
    if html_m is not None:
        html_m(fragment, unsafe_allow_javascript=True)
    else:
        _markdown_brand(fragment, sidebar=sidebar)


def render_main_header(text: str, *, uppercase: bool = False, sidebar: bool = False) -> None:
    """Brand level-1 heading: navy; size/weight from ``_MAIN_HEADER_*`` (``st.html`` + inline CSS)."""
    raw = str(text).strip()
    if uppercase:
        raw = raw.upper()
    safe = html.escape(raw)
    fragment = (
        f'<div class="main-header" role="heading" aria-level="1" '
        f'style="{_main_header_inline_style_attr()}">{safe}</div>'
    )
    _emit_brand_heading_html(fragment, sidebar=sidebar)


def render_sub_header(text: str, *, sidebar: bool = False) -> None:
    """Brand level-2 heading: navy; size/weight from ``_SUB_HEADER_*`` (``st.html`` + inline CSS when available)."""
    safe = html.escape(str(text).strip())
    fragment = (
        f'<div class="sub-header" role="heading" aria-level="2" '
        f'style="{_sub_header_inline_style_attr()}">{safe}</div>'
    )
    _emit_brand_heading_html(fragment, sidebar=sidebar)


def render_sub_sub_header(text: str, *, sidebar: bool = False) -> None:
    """Brand level-3 heading: same navy/size/weight as :func:`render_sub_header`; ``aria-level=\"3\"`` preserved."""
    safe = html.escape(str(text).strip())
    fragment = (
        f'<div class="sub-sub-header" role="heading" aria-level="3" '
        f'style="{_sub_sub_header_inline_style_attr()}">{safe}</div>'
    )
    _emit_brand_heading_html(fragment, sidebar=sidebar)


def inject_style_block(css_rules: str) -> None:
    """
    Inject a ``<style>`` block using ``st.html``.

    Streamlit 1.54+ (DOMPurify): (1) ``st.markdown`` can strip ``<style>`` or show CSS as text.
    (2) **Style-only** ``st.html`` is sent via the event container **without**
    ``unsafe_allow_javascript``, so ``<style>`` is removed and **no global CSS applies** (sidebar
    radios stay visible, theme lost). (3) Non-style-only HTML + ``unsafe_allow_javascript=True``
    uses a sanitizer profile that **keeps** ``<style>``. We add a hidden span so the payload is
    not "style-only", then pass ``unsafe_allow_javascript=True`` (trusted static CSS only).
    """
    text = css_rules.strip()
    if not text:
        return
    body = (
        f"<style>\n{text}\n</style>"
        '<span style="display:none" aria-hidden="true"></span>'
    )
    html_fn = getattr(st, "html", None)
    if html_fn is not None:
        html_fn(body, unsafe_allow_javascript=True)
    else:
        st.markdown(f"<style>\n{text}\n</style>", unsafe_allow_html=True)


def inject_farnda_global_styles_once() -> None:
    """Inject fonts, FA CDN, and global CSS when the bundle version changes (session-scoped)."""
    if st.session_state.get(_SESSION_FLAG) == _FARNDA_CSS_BUNDLE_VERSION:
        return
    st.session_state[_SESSION_FLAG] = _FARNDA_CSS_BUNDLE_VERSION
    _font_fa = (
        '@import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap");\n'
        '@import url("https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css");\n'
    )
    inject_style_block(_font_fa + FARNDA_GLOBAL_CSS.strip() + "\n" + _BRAND_HEADER_CSS.strip())
    apply_custom_styles()


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
    "Subscription": "\U0001F4B3",
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


def render_main_page_title(section_key: str) -> None:
    """Main canvas section title for sidebar nav: brand ``.main-header``, uppercase."""
    render_main_header(str(section_key).strip(), uppercase=True)


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
    "BRAND_GREEN",
    "BRAND_NAVY",
    "BRAND_TEXT_BODY",
    "BRAND_TEXT_MUTED",
    "BRAND_TEXT_SOFT",
    "TABS_UNDERLINE_ACTIVE",
    "TABS_UNDERLINE_TRACK",
    "TAB_UNDERLINE_WIDTH_PX",
    "LM_SEGMENT_UNDERLINE_PX",
    "apply_custom_styles",
    "create_card",
    "CURSOR_DEFAULT",
    "CURSOR_POINTER",
    "FARNDA_GLOBAL_CSS",
    "format_navigation_label",
    "inject_farnda_global_styles_once",
    "inject_style_block",
    "NAV_LABEL_ICONS",
    "render_main_header",
    "render_main_page_title",
    "render_sub_header",
    "render_sub_sub_header",
]
