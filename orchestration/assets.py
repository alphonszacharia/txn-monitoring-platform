"""Dagster assets for the transaction monitoring platform.

Asset graph:

    daily_batch_files ──► raw_transactions ──┐
                    └───► raw_ground_truth ──┤
    raw_customers / raw_accounts ────────────┴──► dbt models (staging → marts)

The ingestion assets reuse the same functions as the command-line tools in
data_generator/ and ingestion/, so the CLI and Dagster behave identically.
"""
import hashlib
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
daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-10-01", end_offset=7)


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

def _dbt_source_hash():
    """Fingerprint of every dbt source file (content, not timestamps)."""
    root = dbt_project.project_dir
    digest = hashlib.sha256()
    files = [root / "dbt_project.yml"]
    for sub in ("models", "tests", "macros", "seeds", "analyses"):
        files += sorted(p for p in (root / sub).rglob("*") if p.is_file())
    for path in files:
        if path.exists():
            digest.update(str(path.relative_to(root)).replace("\\", "/").encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


def _ensure_fresh_manifest():
    """Run `dbt parse` only when the manifest is missing or the dbt files changed.

    Not prepare_if_dev(): that re-parses in every process that imports this module,
    and on Windows several processes parsing at once collide on dbt's target/ files.
    Comparing content hashes (not file times) also works after unzipping new files,
    whose timestamps can be older than the existing manifest. A stale manifest makes
    the UI and the run process disagree about which assets and checks exist.
    """
    marker = dbt_project.manifest_path.parent / "source_hash.txt"
    current = _dbt_source_hash()
    if (dbt_project.manifest_path.exists() and marker.exists()
            and marker.read_text().strip() == current):
        return
    dbt_project.preparer.prepare(dbt_project)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(current)


_ensure_fresh_manifest()

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
