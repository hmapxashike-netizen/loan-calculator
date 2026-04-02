"""
Backend accounting: COA, journals, PostgreSQL DAL, posting service, fiscal periods.

Import concrete modules to avoid loading the full stack on startup, e.g.::

    from accounting.service import AccountingService
    from accounting.core import MappingRegistry
    from accounting.dal import get_conn

The package root stays light: ``import accounting`` does not eagerly import ``service``.
"""

from __future__ import annotations

__all__ = [
    "builtin_defaults",
    "core",
    "dal",
    "defaults_loader",
    "periods",
    "service",
]
