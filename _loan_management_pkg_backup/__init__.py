"""
Loan management package: persistence for loans, schedules, repayments.

Stage 1: implementation lives in ``_legacy`` (former monolithic module). Same import
paths as before: ``from loan_management import record_repayment``, etc.
"""

from __future__ import annotations

from . import _legacy

# Re-export every implementation name (including leading-underscore symbols used
# by sibling modules, e.g. ``_connection``, ``_merge_cash_gl_into_payload``).
for _name in dir(_legacy):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_legacy, _name)

del _name

__all__ = sorted(n for n in dir(_legacy) if not n.startswith("__"))
