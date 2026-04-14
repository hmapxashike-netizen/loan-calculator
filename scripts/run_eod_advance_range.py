"""
Repeatedly run the same EOD step as the Streamlit **EOD Date advance** button
(``Run EOD now``): ``run_eod_process()`` → canonical EOD for *current* system
date, then advance ``current_system_date`` by +1 on success.

Use this to close one business day after another until a target date is processed.

**Precondition:** ``system_business_config.current_system_date`` must already equal
``start_date`` before you run (set it in the app or DB). The script refuses to
start if the live system date does not match ``start_date`` (unless
``--ignore-start-check``).

Run from project root:

  python scripts/run_eod_advance_range.py 2025-06-17 2025-06-20
  python scripts/run_eod_advance_range.py 2025-06-17 2025-06-20 --quiet
  python scripts/run_eod_advance_range.py 2025-06-17 2025-06-20 --dry-run
  # Exactly 46 successful EOD days then exit (no 47th run):
  python scripts/run_eod_advance_range.py 2025-06-17 2025-12-31 --max-runs 46
  # Or set _EDITABLE_MAX_RUNS = 46 near the top of this file (optional cap without CLI).
  # While a long run is going: after the day you want completes, create the stop
  # file; the next loop iteration exits before starting another EOD:
  #   Windows: New-Item -ItemType File .eod_advance_range_stop
  #   Unix:    touch .eod_advance_range_stop
"""
from __future__ import annotations

import argparse
import os
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

_DEFAULT_STOP_FILE = os.path.join(_PROJECT_ROOT, ".eod_advance_range_stop")

# Optional cap on successful EOD advances in one invocation (no CLI flag needed).
# None = use only --max-runs (default 3660). If set, the effective cap is
# min(--max-runs, this value), so e.g. 46 here stops after run 46 even when
# end_date would allow more days.
_EDITABLE_MAX_RUNS: int | None = None


def _parse_date(s: str):
    from datetime import datetime

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid date: {s!r}. Use YYYY-MM-DD, DD.MM.YYYY, or DD/MM/YYYY.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run EOD Date advance (run_eod_process) once per calendar day from "
            "start_date through end_date inclusive. Same as clicking the app button each time."
        )
    )
    parser.add_argument(
        "start_date",
        help="Expected current system business date before the first run (must match DB).",
    )
    parser.add_argument(
        "end_date",
        help="Last business date to process (inclusive). After success, system date will be end_date + 1 day.",
    )
    parser.add_argument(
        "--ignore-start-check",
        action="store_true",
        help=(
            "Do not require system date == start_date. Starts from whatever "
            "current_system_date is; still stops after processing end_date."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned iterations only; do not call run_eod_process.",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Less output (one line per successful day, errors always printed).",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=3660,
        metavar="N",
        help=(
            "Run at most N successful EOD advances in this process, then exit 3 "
            "(before starting the (N+1)th). Example: --max-runs 46 stops after run 46 completes."
        ),
    )
    parser.add_argument(
        "--stop-file",
        metavar="PATH",
        nargs="?",
        const=_DEFAULT_STOP_FILE,
        default=None,
        help=(
            "If set (default path when flag is bare: .eod_advance_range_stop in project root), "
            "before each EOD the script checks this path; if the file exists, it is removed and "
            "the script exits 0 without running that EOD. Create the file after the last day you "
            "want has finished printing."
        ),
    )
    args = parser.parse_args()

    effective_max_runs = int(args.max_runs)
    if _EDITABLE_MAX_RUNS is not None:
        effective_max_runs = min(effective_max_runs, int(_EDITABLE_MAX_RUNS))

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)
    if start > end:
        start, end = end, start

    from datetime import timedelta

    from eod.system_business_date import get_system_business_config, run_eod_process

    cfg = get_system_business_config()
    cur = cfg["current_system_date"]
    if not args.ignore_start_check and cur != start:
        print(
            f"Refusing: current system business date is {cur.isoformat()}, "
            f"but start_date is {start.isoformat()}.\n"
            "Align **system_business_config.current_system_date** to start_date first "
            "(EOD UI or database), or pass --ignore-start-check to run from the current date.",
            file=sys.stderr,
        )
        sys.exit(2)

    if not args.ignore_start_check:
        planned_first = start
    else:
        planned_first = cur

    if planned_first > end:
        print(f"Nothing to do: first day to process ({planned_first}) is after end_date ({end}).")
        return

    n_days = (end - planned_first).days + 1
    capped = min(n_days, effective_max_runs)
    if not args.quiet:
        span_note = (
            f"up to {capped} run(s) (calendar span {n_days} day(s), cap {effective_max_runs})"
            if capped < n_days
            else f"{n_days} time(s)"
        )
        print(
            f"Will run EOD advance {span_note}: "
            f"process {planned_first.isoformat()} … {end.isoformat()} "
            f"(system date would become {(end + timedelta(days=1)).isoformat()} if the cap is not hit)."
        )

    if args.dry_run:
        print("Dry run: no EOD executed.")
        return

    stop_file = args.stop_file
    runs = 0
    while True:
        cfg = get_system_business_config()
        cur = cfg["current_system_date"]
        if cur > end:
            break
        if stop_file and os.path.isfile(stop_file):
            try:
                os.remove(stop_file)
            except OSError:
                pass
            print(
                f"Stop file was present ({stop_file}); exiting before next EOD. "
                f"Completed {runs} run(s) in this session; current system date {cur.isoformat()}.",
                file=sys.stderr,
            )
            sys.exit(0)
        if runs >= effective_max_runs:
            why = f"--max-runs {args.max_runs}"
            if _EDITABLE_MAX_RUNS is not None:
                why = f"min(--max-runs {args.max_runs}, _EDITABLE_MAX_RUNS {_EDITABLE_MAX_RUNS}) = {effective_max_runs}"
            print(f"Stopped: hit run cap ({why}).", file=sys.stderr)
            sys.exit(3)

        result = run_eod_process()
        runs += 1
        if not result.get("success"):
            err = result.get("error") or "unknown error"
            print(
                f"EOD FAILED at system date {cur.isoformat()} (run {runs}): {err}",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.quiet:
            print(
                f"OK {result.get('as_of_date')} → {result.get('new_system_date')} "
                f"(loans_processed={result.get('loans_processed', 0)})"
            )
        else:
            print(
                f"Run {runs}: as_of={result.get('as_of_date')} "
                f"new_system_date={result.get('new_system_date')} "
                f"loans_processed={result.get('loans_processed', 0)} "
                f"duration_s={result.get('duration_seconds', 0):.2f}"
            )

    if not args.quiet:
        print(f"Finished {runs} EOD advance(s). Current system date: {get_system_business_config()['current_system_date']}.")


if __name__ == "__main__":
    main()
