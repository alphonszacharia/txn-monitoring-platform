# Transaction Monitoring Data Platform

A local, end-to-end data engineering project: synthetic banking transactions
are ingested, modelled and tested, then flagged by AML-style monitoring rules.

> Status: stages 1-4 done (generator, raw load, dbt staging, star schema,
> alert rules with evaluation, Dagster orchestration). CI and dashboard next.

## Architecture (target)

```
generator -> raw (bronze) -> staging (silver) -> marts (gold) -> dashboard
              Postgres        dbt                dbt
                        orchestrated by Dagster, tested in CI
```

## Quick start

```bash
cp .env.example .env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make up                 # start Postgres
make generate           # 7 days of synthetic data
make load               # load reference data + each daily batch
```

Verify:

```bash
docker exec -it txn_postgres psql -U warehouse -d txn_monitoring \
  -c "SELECT _batch_id, count(*) FROM raw.transactions GROUP BY 1 ORDER BY 1;"
```

## Build the warehouse layers

```bash
python -m ingestion.load_raw --ground-truth   # planted answers, for evaluation
cd dbt_project
dbt seed --profiles-dir .                     # high-risk country list
dbt run  --profiles-dir .
dbt test --profiles-dir .
```

## Model layers

| Layer | Models |
|---|---|
| raw (bronze) | `customers`, `accounts`, `transactions`, `ground_truth` |
| staging (silver) | `stg_customers`, `stg_accounts`, `stg_transactions`, `stg_ground_truth` |
| marts (gold) | `dim_customer`, `dim_account`, `dim_date`, `fct_transactions`, `fct_alerts`, `eval_alert_performance` |

## Alert rules

| Rule | Logic (parameters live in `dbt_project.yml`) |
|---|---|
| `structuring` | 3+ cash deposits in one day, each 90-100% of the 10,000 threshold |
| `rapid_in_out` | Inbound transfer of 10,000+ followed within 24h by an outbound transfer of 80-100% of it |
| `high_risk_country` | Any activity with a counterparty on the high-risk list (`seeds/high_risk_countries.csv`) |

`eval_alert_performance` scores the first two against the planted patterns
(precision and recall per rule), and a dbt test fails if either drops below
the gates in `dbt_project.yml`.

> Note: the synthetic background traffic contains no look-alike activity, so
> the rules currently score perfectly. Adding decoys (legitimate near-threshold
> deposits, genuine large transfers) is the planned way to make the evaluation
> meaningful.

## Orchestration (Dagster)

```bash
mkdir .dagster_home
export DAGSTER_HOME="$PWD/.dagster_home"      # Windows: set DAGSTER_HOME=%CD%\.dagster_home
dagster dev -m orchestration.definitions      # then open http://localhost:3000
```

Asset graph: `daily_batch_files` -> `raw_transactions` / `raw_ground_truth` ->
dbt models (seeds, staging, marts and all tests via `dbt build`);
`raw_customers` / `raw_accounts` feed the same dbt models.

| Component | Purpose |
|---|---|
| Daily partitions | One partition per day, starting 2026-10-01; re-running a day replaces it |
| `daily_pipeline` job + schedule | Generate, load and transform one day (06:00 Europe/Dublin, off by default) |
| `ingest_and_transform` job + sensor | If a batch file appears on disk but is not loaded, load it and rebuild the warehouse |

If you change a dbt model, refresh Dagster's view of it with
`cd dbt_project && dbt parse --profiles-dir .` and restart `dagster dev`.
Steps run sequentially in one process, which is plenty for this data size.

The ingestion assets call the same functions as the CLI tools, and dbt sources
are mapped to Dagster assets so lineage is one connected graph.

## Design decisions

- **Synthetic data only:** no real customer data, so the repo is safe to publish.
- **Planted patterns + ground truth:** structuring and rapid in/out flows are
  inserted on purpose, with answers saved separately, so alert rules can be
  measured for precision and recall.
- **Idempotent loads:** every row carries `_batch_id` and `_loaded_at`;
  re-running a batch replaces it rather than duplicating it.
- **Raw as text:** the bronze layer stores values as received; typing and
  cleaning happen in dbt staging models.

## Roadmap

- [x] Data generator and raw load
- [x] dbt staging models and tests
- [x] Star schema and alert rules
- [x] Dagster orchestration
- [ ] Data quality checks and CI
- [ ] Dashboard and write-up
