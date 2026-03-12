"""
Run EOD for a single date with the current loan to get baseline timing measurements.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date
from eod import run_eod_for_date

# Re-run Oct 31 EOD (idempotent, no state change). Oct 31 is a due date so it exercises
# all code paths (arrears, grace, period-to-date resets).
import time
t0 = time.time()
result = run_eod_for_date(date(2025, 10, 31), skip_reallocate_after_reversals=True)
elapsed = round((time.time() - t0) * 1000, 2)
print(f"EOD done: loans_processed={result.loans_processed}  elapsed={elapsed}ms")
