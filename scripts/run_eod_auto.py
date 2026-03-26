"""
Run EOD automatically when is_auto_eod_enabled and current time >= eod_auto_run_time.

Use with cron, Windows Task Scheduler, or similar. Example (cron, daily at 23:05):
  5 23 * * * cd /path/to/FarndaCred && python scripts/run_eod_auto.py

When run manually, run_eod_process() still advances current_system_date by +1 day on success.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from system_business_date import get_system_business_config, run_eod_process, should_trigger_auto_eod


def main():
    cfg = get_system_business_config()
    print(f"Current system date: {cfg['current_system_date']}")
    print(f"Auto EOD enabled: {cfg['is_auto_eod_enabled']}")
    print(f"EOD auto-run time: {cfg['eod_auto_run_time']}")

    if not cfg["is_auto_eod_enabled"]:
        print("Auto EOD is disabled. Run with --force to run anyway.")
        if "--force" in sys.argv:
            result = run_eod_process()
        else:
            sys.exit(0)
    elif should_trigger_auto_eod() or "--force" in sys.argv:
        print("Running EOD process...")
        result = run_eod_process()
        if result["success"]:
            print(f"EOD completed. New system date: {result['new_system_date']}")
        else:
            print(f"EOD failed: {result.get('error')}")
            sys.exit(1)
    else:
        print("Current time has not yet reached eod_auto_run_time. Use --force to run anyway.")
        if "--force" in sys.argv:
            result = run_eod_process()
            if result["success"]:
                print(f"EOD completed. New system date: {result['new_system_date']}")
            else:
                print(f"EOD failed: {result.get('error')}")
                sys.exit(1)


if __name__ == "__main__":
    main()
