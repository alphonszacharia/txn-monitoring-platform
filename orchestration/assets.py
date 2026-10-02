"""Dagster assets for the transaction monitoring platform.

Asset graph:

    daily_batch_files ──► raw_transactions ──┐
                    └───► raw_ground_truth ──┤
    raw_customers / raw_accounts ────────────┴──► dbt models (staging → marts)

The ingestion assets reuse the same functions as the command-line tools in
data_generator/ and ingestion/, so the CLI and Dagster behave identically.
"""
from datetime import date

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from dotenv import load_dotenv

from data_generator.generate import build_reference, write_day
from ingestion import load_raw as lr

# Make .env values visible to dbt subprocesses too (profiles.yml reads env vars).
load_dotenv(lr.ROOT / ".env", override=True)

SEED = 42
N_CUSTOMERS = 500

# end_offset=1 makes today's partition available as well as past days.
daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-10-01", end_offset=1)


def _load(table, columns, path, batch_id, ddl=None):
    """Idempotently load one CSV into raw.<table> under a batch id."""
    conn = lr.connect()
    try:
        with conn, conn.cursor() as cur:
            if ddl:
                cur.execute(ddl)
            return lr.load_file(cur, table, columns, path, batch_id)
    finally:
        conn.close()


@dg.multi_asset(
    specs=[
        dg.AssetSpec("raw_customers", group_name="ingestion", kinds={"python", "postgres"}),
        dg.AssetSpec("raw_accounts", group_name="ingestion", kinds={"python", "postgres"}),
    ],
)
def raw_reference_data():
    """Customers and accounts: generated once, then loaded into raw."""
    build_reference(N_CUSTOMERS, SEED)
    for table, (cols, path) in lr.REFERENCE_TABLES.items():
        n = _load(table, cols, path, "reference")
        yield dg.MaterializeResult(
            asset_key=f"raw_{table}", metadata={"rows_loaded": n}
        )


@dg.asset(partitions_def=daily_partitions, group_name="ingestion", kinds={"python"})
def daily_batch_files(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Synthetic source system: writes one day of transactions plus ground truth."""
    day = date.fromisoformat(context.partition_key)
    _, accounts = build_reference(N_CUSTOMERS, SEED)
    n_txns, n_planted = write_day(day, accounts, SEED)
    return dg.MaterializeResult(
        metadata={"transactions": n_txns, "planted_pattern_rows": n_planted}
    )


@dg.asset(
    partitions_def=daily_partitions,
    deps=[daily_batch_files],
    group_name="ingestion",
    kinds={"python", "postgres"},
)
def raw_transactions(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Load one day's transactions into raw (replaces that batch if re-run)."""
    day = context.partition_key
    path = lr.DATA_DIR / "transactions" / f"transactions_{day}.csv"
    n = _load("transactions", lr.TXN_COLUMNS, path, day)
    return dg.MaterializeResult(metadata={"rows_loaded": n, "batch_id": day})


@dg.asset(
    partitions_def=daily_partitions,
    deps=[daily_batch_files],
    group_name="ingestion",
    kinds={"python", "postgres"},
)
def raw_ground_truth(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Load the planted-pattern answers used to evaluate the alert rules."""
    day = context.partition_key
    path = lr.GROUND_TRUTH_DIR / f"ground_truth_{day}.csv"
    n = _load("ground_truth", lr.GT_COLUMNS, path, day, ddl=lr.GT_DDL)
    return dg.MaterializeResult(metadata={"rows_loaded": n, "batch_id": day})


# ---------------------------------------------------------------- dbt
dbt_project = DbtProject(
    project_dir=lr.ROOT / "dbt_project",
    profiles_dir=lr.ROOT / "dbt_project",
)

# Build dbt's manifest.json once, only if it is missing. We deliberately do NOT
# use prepare_if_dev(): it re-runs `dbt parse` in every process that imports this
# module, and on Windows several step subprocesses parsing at once collide on
# dbt's target/ and logs/ files and crash. If you change dbt models, refresh
# the manifest with:  cd dbt_project && dbt parse --profiles-dir .
if not dbt_project.manifest_path.exists():
    dbt_project.preparer.prepare(dbt_project)

# dbt sources (raw.*) are mapped onto the ingestion assets above,
# so the lineage graph connects Python ingestion to dbt models.
SOURCE_TO_ASSET = {
    "customers": "raw_customers",
    "accounts": "raw_accounts",
    "transactions": "raw_transactions",
    "ground_truth": "raw_ground_truth",
}


class WarehouseTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return dg.AssetKey(SOURCE_TO_ASSET[dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=WarehouseTranslator(),
)
def warehouse_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Seeds, models and tests: dbt build."""
    yield from dbt.cli(["build"], context=context).stream()
