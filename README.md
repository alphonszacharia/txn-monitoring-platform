# Transaction Monitoring Data Platform

A local, end-to-end data engineering project: synthetic banking transactions
are ingested, modelled and tested, then flagged by AML-style monitoring rules.

> Status: stages 1-5 done (generator with decoys, raw load, dbt staging, star
> schema, tuned alert rules with evaluation, Dagster, CI, Streamlit dashboard).

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
the gates in `dbt_project.yml`. The gate only applies once at least 7 days are
loaded (`gate_min_days`), because the recurring-account refinement needs
several days of history to work.

### Decoys and tuning

The generator also plants **decoys**: legitimate activity that looks like the
suspicious patterns but is *not* in the ground truth, so alerting on it counts
as a false positive.

- Cash-intensive businesses lodging several just-under-threshold deposits
  (12 do so most days, 5 only occasionally).
- Large inbound transfers forwarded within hours, usually to a domestic
  account, occasionally to CY/AE.

Measured on 7 days of data (seed 42, 21 structuring and 28 rapid in/out
planted cases; recall is 1.0 in both runs):

| Rule | Baseline precision | Tuned precision | Refinement |
|---|---|---|---|
| `structuring` | 0.273 (56 false positives) | 0.808 (5) | Ignore accounts that trigger on 3+ different days |
| `rapid_in_out` | 0.571 (21 false positives) | 0.933 (2) | Outbound leg must go to an elevated/high-risk country |

Reproduce the baseline with
`dbt build --vars '{structuring_recurring_days: 0, rapid_require_risky_outbound: false, min_precision: 0, min_recall: 0}'`.

Caveats: the decoys and the refinements were designed together on synthetic
data, so the tuned figures show the tuning workflow works, not how the rules
would perform on real bank data. The recurring-days refinement also depends on
how much history is loaded.

## Dashboard

```bash
pip install -r requirements-dashboard.txt
streamlit run dashboard/app.py          # opens http://localhost:8501
```

Reads only from the marts and staging layers. Three tabs: **Alerts** (daily
trend by rule, filterable table with each alert's outcome against the synthetic
ground truth), **Rule performance** (precision and recall per rule), and
**Investigate** (pick an alert and see the account's transactions around it).
Sidebar filters cover rule, date, customer KYC risk and a false-positives-only view.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request against a
Postgres service container: ruff lint, generator unit tests (pytest), then
generate, load, `dbt build` (50 data tests plus the precision/recall gate),
source freshness and Dagster definition validation.

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
- **Planted patterns, decoys and ground truth:** suspicious flows and
  look-alike legitimate ones are inserted on purpose, with only the suspicious
  ones recorded as answers, so alert rules can be scored for precision and
  recall.
- **Idempotent loads:** every row carries `_batch_id` and `_loaded_at`;
  re-running a batch replaces it rather than duplicating it.
- **Raw as text:** the bronze layer stores values as received; typing and
  cleaning happen in dbt staging models.

## Roadmap

- [x] Data generator and raw load
- [x] dbt staging models and tests
- [x] Star schema and alert rules
- [x] Dagster orchestration
- [x] Data quality checks and CI
- [x] Dashboard
- [ ] Write-up (architecture diagram and design notes in `docs/`)
