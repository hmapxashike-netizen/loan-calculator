"""Streamlit UX helpers for long-running synchronous work in a single rerun.

Use :func:`run_with_spinner` for operations that often exceed ~300ms or are an explicit
user-triggered "run" (DB writes, reports, batch jobs). Do not wrap trivial validation-only
paths or instant UI toggles — avoid spinner noise.

For work that spans reruns or runs outside the Streamlit process, use session state,
progress widgets, polling, or messages instead; spinners only cover blocking work until
``fn`` returns.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

import streamlit as st

T = TypeVar("T")


def run_with_spinner(label: str, fn: Callable[[], T]) -> T:
    """Run ``fn`` while showing ``st.spinner(label)``; return ``fn()``'s result."""
    with st.spinner(label):
        return fn()
