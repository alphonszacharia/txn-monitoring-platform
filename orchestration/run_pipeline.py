"""Run the whole pipeline through Dagster, in the right order.

    python -m orchestration.run_pipeline

Same three steps as in the UI, run one after another so each finishes before the
next starts. Runs are recorded in Dagster's history, so they show up in the UI.
"""
import subprocess
import sys
from datetime import date, timedelta

START = date(2026, 10, 1)
DAYS = 7
MODULE = ["-m", "orchestration.definitions"]


def materialize(select, partition=None):
    cmd = ["dagster", "asset", "materialize", *MODULE, "--select", select]
    if partition:
        cmd += ["--partition", partition]
    label = select + (f" [{partition}]" if partition else "")
    print(f"\n=== {label}", flush=True)
    if subprocess.run(cmd).returncode != 0:
        sys.exit(f"Failed: {label}")


def main():
    # 1. Reference data (not partitioned): customers and accounts.
    materialize("raw_customers,raw_accounts")
    # 2. One run per day: generate the files, then load them.
    for offset in range(DAYS):
        day = (START + timedelta(days=offset)).isoformat()
        materialize("daily_batch_files,raw_transactions,raw_ground_truth", day)
    # 3. Build and test the warehouse once, on all days.
    materialize("group:default")
    print("\nPipeline finished.", flush=True)


if __name__ == "__main__":
    main()
