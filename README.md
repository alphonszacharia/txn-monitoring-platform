# Transaction Monitoring Data Platform

A local, end-to-end data engineering project: synthetic banking transactions
are ingested, modelled and tested, then flagged by AML-style monitoring rules.

> Status: stage 1 (generator + raw load). dbt, Dagster, data quality and
> dashboard come next.

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
- [ ] dbt staging models and tests
- [ ] Star schema and alert rules
- [ ] Dagster orchestration
- [ ] Data quality checks and CI
- [ ] Dashboard and write-up
