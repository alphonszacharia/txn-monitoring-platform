-- Schemas for the medallion layers
CREATE SCHEMA IF NOT EXISTS raw;       -- bronze: data exactly as received
CREATE SCHEMA IF NOT EXISTS staging;   -- silver: cleaned and typed (dbt)
CREATE SCHEMA IF NOT EXISTS marts;     -- gold: star schema and alerts (dbt)

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id      TEXT,
    full_name        TEXT,
    country          TEXT,
    kyc_risk_rating  TEXT,
    onboarded_date   TEXT,
    _batch_id        TEXT NOT NULL,
    _loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.accounts (
    account_id    TEXT,
    customer_id   TEXT,
    account_type  TEXT,
    opened_date   TEXT,
    _batch_id     TEXT NOT NULL,
    _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.transactions (
    txn_id               TEXT,
    account_id           TEXT,
    txn_ts               TEXT,
    amount               TEXT,
    currency             TEXT,
    txn_type             TEXT,
    counterparty_country TEXT,
    _batch_id            TEXT NOT NULL,
    _loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_txn_batch ON raw.transactions (_batch_id);

CREATE TABLE IF NOT EXISTS raw.ground_truth (
    txn_id      TEXT,
    account_id  TEXT,
    txn_date    TEXT,
    pattern     TEXT,
    _batch_id   TEXT NOT NULL,
    _loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
