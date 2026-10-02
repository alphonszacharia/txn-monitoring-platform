"""Dagster definitions: assets, jobs, a daily schedule and a file sensor.

Run from the project root:
    dagster dev -m orchestration.definitions
"""
from datetime import date

import dagster as dg
from dagster_dbt import DbtCliResource

from data_generator.generate import DATA_DIR
from orchestration.assets import (
    daily_batch_files,
    daily_partitions,
    dbt_project,
    raw_ground_truth,
    raw_reference_data,
    raw_transactions,
    warehouse_models,
)

# Everything: generate a day, load it, rebuild and test the warehouse.
daily_pipeline = dg.define_asset_job(
    "daily_pipeline",
    selection=dg.AssetSelection.all(),
    description="Generate, load and transform one day of data.",
)

# Used when a batch file already exists (dropped by an external system).
ingest_and_transform = dg.define_asset_job(
    "ingest_and_transform",
    selection=dg.AssetSelection.assets(raw_transactions, raw_ground_truth).downstream(),
    description="Load an existing batch file, then rebuild and test the warehouse.",
)


@dg.schedule(
    job=daily_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Dublin",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def daily_pipeline_schedule(context: dg.ScheduleEvaluationContext):
    """Every morning at 06:00 process that day's partition."""
    day = context.scheduled_execution_time.date().isoformat()
    return dg.RunRequest(run_key=day, partition_key=day)


@dg.sensor(
    job=ingest_and_transform,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def new_batch_file_sensor(context: dg.SensorEvaluationContext):
    """Load any batch file that exists on disk but is not in the warehouse yet."""
    loaded = context.instance.get_materialized_partitions(raw_transactions.key)
    for path in sorted((DATA_DIR / "transactions").glob("transactions_*.csv")):
        day = path.stem.replace("transactions_", "")
        if day in loaded or not daily_partitions.has_partition_key(day):
            continue
        yield dg.RunRequest(run_key=f"{day}-{path.stat().st_mtime_ns}", partition_key=day)


defs = dg.Definitions(
    assets=[
        raw_reference_data,
        daily_batch_files,
        raw_transactions,
        raw_ground_truth,
        warehouse_models,
    ],
    jobs=[daily_pipeline, ingest_and_transform],
    schedules=[daily_pipeline_schedule],
    sensors=[new_batch_file_sensor],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
    # Run steps one after another inside a single process. This pipeline is small,
    # and it avoids slow, fragile per-step subprocess start-up on Windows.
    executor=dg.in_process_executor,
)
