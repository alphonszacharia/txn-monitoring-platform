"""Load generated CSV files into the raw schema.

Idempotent: each file is loaded under a batch ID. Re-running a batch deletes
that batch's rows first, then reloads, all inside one transaction.

Usage:
    python -m ingestion.load_raw --reference
    python -m ingestion.load_raw --date 2026-10-01
"""
import argparse
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

DATA_DIR = Path("data/raw")

REFERENCE_TABLES = {
    "customers": ("customer_id, full_name, country, kyc_risk_rating, onboarded_date",
                  DATA_DIR / "reference" / "customers.csv"),
    "accounts": ("account_id, customer_id, account_type, opened_date",
                 DATA_DIR / "reference" / "accounts.csv"),
}
TXN_COLUMNS = ("txn_id, account_id, txn_ts, amount, currency, "
               "txn_type, counterparty_country")


def connect():
    load_dotenv(override=True)
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "txn_monitoring"),
        user=os.getenv("POSTGRES_USER", "warehouse"),
        password=os.getenv("POSTGRES_PASSWORD", "warehouse"),
    )


def load_file(cur, table, columns, path, batch_id):
    """Replace one batch of one raw table from a CSV file."""
    cur.execute(f"DELETE FROM raw.{table} WHERE _batch_id = %s", (batch_id,))
    # Stage the CSV in a temp table holding only the source columns,
    # then insert with batch metadata attached.
    cur.execute(f"CREATE TEMP TABLE _stg ON COMMIT DROP AS "
                f"SELECT {columns} FROM raw.{table} WITH NO DATA")
    with open(path, encoding="utf-8") as f:
        cur.copy_expert(
            f"COPY _stg ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)", f)
    cur.execute(f"INSERT INTO raw.{table} ({columns}, _batch_id) "
                f"SELECT {columns}, %s FROM _stg", (batch_id,))
    loaded = cur.rowcount
    cur.execute("DROP TABLE _stg")
    return loaded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Load the transactions file for YYYY-MM-DD")
    parser.add_argument("--reference", action="store_true",
                        help="Load customers and accounts")
    args = parser.parse_args()
    if not args.date and not args.reference:
        parser.error("Provide --date and/or --reference")

    conn = connect()
    try:
        with conn, conn.cursor() as cur:  # one transaction; rolls back on error
            if args.reference:
                for table, (cols, path) in REFERENCE_TABLES.items():
                    n = load_file(cur, table, cols, path, "reference")
                    print(f"raw.{table}: {n} rows (batch=reference)")
            if args.date:
                path = DATA_DIR / "transactions" / f"transactions_{args.date}.csv"
                n = load_file(cur, "transactions", TXN_COLUMNS, path, args.date)
                print(f"raw.transactions: {n} rows (batch={args.date})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
