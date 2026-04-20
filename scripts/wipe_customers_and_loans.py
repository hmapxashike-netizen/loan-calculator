"""
Truncate operational customer/loan/GL/EOD data (same as ``wipe_fresh_start.py``).

Prefer:
  python scripts/wipe_fresh_start.py --dry-run
  python scripts/wipe_fresh_start.py -y

This module forwards to ``wipe_fresh_start.main()`` for backward compatibility.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


def main() -> None:
    p = Path(__file__).resolve().parent / "wipe_fresh_start.py"
    spec = importlib.util.spec_from_file_location("wipe_fresh_start", p)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load wipe_fresh_start.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()


if __name__ == "__main__":
    main()
