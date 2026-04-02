"""Reallocate receipt ids then run single-loan EOD (hard-coded ids/dates for local debugging)."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from eod.core import run_single_loan_eod
from loan_management import load_system_config_from_db, reallocate_repayment


def main() -> None:
    cfg = load_system_config_from_db()
    for rep_id in [10, 11, 12, 13, 14]:
        print(f"Reallocating {rep_id}...")
        reallocate_repayment(rep_id, system_config=cfg)

    run_single_loan_eod(1, date(2025, 9, 1), sys_cfg=cfg)
    print("Done.")


if __name__ == "__main__":
    main()
