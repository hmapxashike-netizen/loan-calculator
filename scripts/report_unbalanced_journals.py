"""
CLI: list journal headers where sum(debits) != sum(credits).

Usage (from project root):
  python scripts/report_unbalanced_journals.py

Repair a specific LOAN_APPROVAL after fixing posting logic (interactive / one-off):
  python -c "from accounting_service import AccountingService; AccountingService().repost_loan_approval_journal(6)"
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow running as script without installing package
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from accounting_service import AccountingService  # noqa: E402


def main() -> int:
    svc = AccountingService()
    rows = svc.list_unbalanced_journal_entries()
    if not rows:
        print("No unbalanced journal headers found.")
        return 0
    print(f"Found {len(rows)} unbalanced journal header(s):\n")
    for r in rows:
        print(
            f"  id={r.get('id')} date={r.get('entry_date')} ref={r.get('reference')!r} "
            f"event_id={r.get('event_id')!r} event_tag={r.get('event_tag')!r} "
            f"debits={r.get('total_debit')} credits={r.get('total_credit')}"
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
