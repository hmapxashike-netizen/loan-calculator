"""DB helpers for creditor loans (same connection pattern as loan_management)."""

from loan_management.db import RealDictCursor, _connection

__all__ = ["RealDictCursor", "_connection"]
