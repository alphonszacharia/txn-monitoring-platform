"""Database access for the dashboard. Reads only from the marts/staging layers."""
import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if not os.getenv("RUNNING_IN_DOCKER"):  # in a container, compose sets the env
    load_dotenv(ROOT / ".env", override=True)


def run(sql, params=None):
    """Run a read-only query and return a DataFrame."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "txn_monitoring"),
        user=os.getenv("POSTGRES_USER", "warehouse"),
        password=os.getenv("POSTGRES_PASSWORD", "warehouse"),
    )
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


OVERVIEW = """
select
    count(*)                        as transactions,
    sum(amount)                     as volume_eur,
    count(distinct account_id)      as accounts,
    min(txn_date)                   as first_day,
    max(txn_date)                   as last_day
from marts.fct_transactions
"""

# One row per alert, joined to who it concerns. `outcome` compares the alert to the
# synthetic ground truth (only the two rules that have planted patterns are scored).
ALERTS = """
with truth as (
    select account_id, txn_date, string_agg(distinct pattern, ', ') as patterns
    from staging.stg_ground_truth
    group by account_id, txn_date
)
select
    a.alert_id,
    a.rule_name,
    a.alert_date,
    a.account_id,
    c.full_name,
    c.country_code,
    c.kyc_risk_rating,
    ac.account_type,
    a.txn_count,
    a.total_amount,
    case
        when a.rule_name not in ('structuring', 'rapid_in_out') then 'not evaluated'
        when position(a.rule_name in coalesce(t.patterns, '')) > 0 then 'confirmed'
        else 'false positive'
    end                             as outcome
from marts.fct_alerts       as a
join marts.dim_account      as ac on ac.account_id  = a.account_id
join marts.dim_customer     as c  on c.customer_id  = a.customer_id
left join truth             as t  on t.account_id = a.account_id
                                 and t.txn_date   = a.alert_date
order by a.alert_date desc, a.total_amount desc
"""

EVALUATION = "select * from marts.eval_alert_performance order by rule_name"

DAILY_VOLUME = """
select txn_date, count(*) as transactions, sum(amount) as volume_eur
from marts.fct_transactions
group by txn_date
order by txn_date
"""

ACCOUNT_ACTIVITY = """
select
    txn_ts, txn_type, amount, counterparty_country_code, counterparty_risk_level
from marts.fct_transactions
where account_id = %(account_id)s
  and txn_date between %(day)s::date - 1 and %(day)s::date + 1
order by txn_ts
"""
