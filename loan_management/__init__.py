"""
Loan management package: persistence for loans, schedules, repayments.

Implementation lives in ``_legacy`` (monolithic module body). Same import paths
as ``from loan_management import record_repayment``, etc.
"""

from __future__ import annotations

from . import _legacy

for _name in dir(_legacy):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_legacy, _name)

del _name

__all__ = sorted(n for n in dir(_legacy) if not n.startswith("__"))
