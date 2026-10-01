"""Generate synthetic banking data.

Reference data (customers, accounts) is created once and reused.
Transactions are generated per day, as an incremental batch file.

Usage:
    python -m data_generator.generate --date 2026-10-01
    python -m data_generator.generate --date 2026-10-01 --days 7
"""
import argparse
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

from data_generator.patterns import PATTERNS

DATA_DIR = Path("data/raw")
COUNTRIES = ["IE", "GB", "DE", "FR", "ES", "NL", "IT", "US", "IN", "PL"]
ACCOUNT_TYPES = ["current", "savings", "business"]
NORMAL_TXN_TYPES = ["card_payment", "transfer_out", "transfer_in",
                    "cash_deposit", "cash_withdrawal", "direct_debit"]


def build_reference(n_customers, seed):
    ref_dir = DATA_DIR / "reference"
    cust_path, acct_path = ref_dir / "customers.csv", ref_dir / "accounts.csv"
    if cust_path.exists() and acct_path.exists():
        return pd.read_csv(cust_path), pd.read_csv(acct_path)

    ref_dir.mkdir(parents=True, exist_ok=True)
    fake, rng = Faker("en_IE"), random.Random(seed)
    Faker.seed(seed)

    customers, accounts = [], []
    for i in range(1, n_customers + 1):
        cid = f"C{i:06d}"
        customers.append({
            "customer_id": cid,
            "full_name": fake.name(),
            "country": rng.choices(COUNTRIES, weights=[60, 8, 5, 4, 4, 4, 4, 4, 4, 3])[0],
            "kyc_risk_rating": rng.choices(["low", "medium", "high"], weights=[70, 24, 6])[0],
            "onboarded_date": fake.date_between(start_date="-8y", end_date="-30d").isoformat(),
        })
        for j in range(rng.choices([1, 2, 3], weights=[60, 30, 10])[0]):
            accounts.append({
                "account_id": f"A{i:06d}{j + 1}",
                "customer_id": cid,
                "account_type": rng.choice(ACCOUNT_TYPES),
                "opened_date": fake.date_between(start_date="-8y", end_date="-30d").isoformat(),
            })

    cust_df, acct_df = pd.DataFrame(customers), pd.DataFrame(accounts)
    cust_df.to_csv(cust_path, index=False)
    acct_df.to_csv(acct_path, index=False)
    return cust_df, acct_df


def generate_day(day, accounts, seed, txns_per_account=2.0, pattern_rate=0.01):
    # Deterministic per day, so re-running a date gives the same file.
    rng = random.Random(f"{seed}-{day.isoformat()}")
    counter = {"n": 0}

    def next_id():
        counter["n"] += 1
        return f"T{day.strftime('%Y%m%d')}{counter['n']:07d}"

    account_ids = accounts["account_id"].tolist()
    rows, truth = [], []

    for acct in account_ids:
        for _ in range(max(0, int(rng.gauss(txns_per_account, 1)))):
            ts = datetime.combine(day, datetime.min.time()) + timedelta(
                seconds=rng.randint(0, 86_399))
            rows.append({
                "txn_id": next_id(), "account_id": acct,
                "txn_ts": ts.isoformat(),
                "amount": round(min(rng.lognormvariate(4.5, 1.0), 5_000), 2),
                "currency": "EUR",
                "txn_type": rng.choice(NORMAL_TXN_TYPES),
                "counterparty_country": rng.choices(COUNTRIES, weights=[70, 8, 4, 4, 3, 3, 3, 2, 2, 1])[0],
            })

    n_planted = max(1, int(len(account_ids) * pattern_rate))
    for acct in rng.sample(account_ids, n_planted):
        pattern = rng.choice(PATTERNS)
        txns, label = pattern(acct, day, rng, next_id)
        rows.extend(txns)
        truth.extend({"txn_id": t["txn_id"], "account_id": acct,
                      "txn_date": day.isoformat(), "pattern": label} for t in txns)

    return pd.DataFrame(rows), pd.DataFrame(truth)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (first day)")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    _, accounts = build_reference(args.customers, args.seed)
    start = date.fromisoformat(args.date)
    out_dir = DATA_DIR / "transactions"
    truth_dir = Path("data/ground_truth")
    out_dir.mkdir(parents=True, exist_ok=True)
    truth_dir.mkdir(parents=True, exist_ok=True)

    for offset in range(args.days):
        day = start + timedelta(days=offset)
        txns, truth = generate_day(day, accounts, args.seed)
        txns.to_csv(out_dir / f"transactions_{day.isoformat()}.csv", index=False)
        truth.to_csv(truth_dir / f"ground_truth_{day.isoformat()}.csv", index=False)
        print(f"{day}: {len(txns)} transactions ({len(truth)} planted)")


if __name__ == "__main__":
    main()
