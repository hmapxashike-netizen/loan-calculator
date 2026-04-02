"""
Loan management package: persistence for loans, schedules, repayments.

The stable API is re-exported from :mod:`loan_management.facade` (barrel imports).
Use ``from loan_management import record_repayment``, etc., unchanged.
"""

from __future__ import annotations

from . import facade

for _name in dir(facade):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(facade, _name)

del _name

__all__ = sorted(n for n in dir(facade) if not n.startswith("__"))
