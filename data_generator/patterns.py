"""Planted suspicious patterns.

Ground truth is written to a separate file so that, later, the dbt alert
rules can be evaluated against known answers (precision/recall).
"""
from datetime import datetime, timedelta

# Illustrative reporting threshold used for the structuring pattern.
REPORTING_THRESHOLD = 10_000

HIGH_RISK_COUNTRIES = ["IR", "KP", "MM", "SY", "AF"]


def structuring(account_id, day, rng, next_id):
    """3-5 cash deposits just under the threshold on the same day."""
    txns = []
    for _ in range(rng.randint(3, 5)):
        ts = datetime.combine(day, datetime.min.time()) + timedelta(
            hours=rng.randint(9, 17), minutes=rng.randint(0, 59)
        )
        txns.append({
            "txn_id": next_id(),
            "account_id": account_id,
            "txn_ts": ts.isoformat(),
            "amount": round(rng.uniform(0.90, 0.99) * REPORTING_THRESHOLD, 2),
            "currency": "EUR",
            "txn_type": "cash_deposit",
            "counterparty_country": "IE",
        })
    return txns, "structuring"


def rapid_in_out(account_id, day, rng, next_id):
    """Large inbound transfer followed within hours by an outbound transfer."""
    amount = round(rng.uniform(15_000, 60_000), 2)
    start = datetime.combine(day, datetime.min.time()) + timedelta(
        hours=rng.randint(8, 12), minutes=rng.randint(0, 59)
    )
    country = rng.choice(HIGH_RISK_COUNTRIES + ["CY", "AE", "PA"])
    txns = [
        {
            "txn_id": next_id(), "account_id": account_id,
            "txn_ts": start.isoformat(), "amount": amount,
            "currency": "EUR", "txn_type": "transfer_in",
            "counterparty_country": country,
        },
        {
            "txn_id": next_id(), "account_id": account_id,
            "txn_ts": (start + timedelta(hours=rng.randint(1, 4))).isoformat(),
            "amount": round(amount * rng.uniform(0.90, 0.98), 2),
            "currency": "EUR", "txn_type": "transfer_out",
            "counterparty_country": rng.choice(HIGH_RISK_COUNTRIES),
        },
    ]
    return txns, "rapid_in_out"


PATTERNS = [structuring, rapid_in_out]
