-- All alert rules in one table: one row per rule, account and day.
with unioned as (

    select * from {{ ref('alert_structuring') }}
    union all
    select * from {{ ref('alert_rapid_in_out') }}
    union all
    select * from {{ ref('alert_high_risk_country') }}

)

select
    md5(rule_name || '|' || account_id || '|' || alert_date::text)  as alert_id,
    rule_name,
    account_id,
    customer_id,
    alert_date,
    to_char(alert_date, 'YYYYMMDD')::int                            as date_key,
    txn_count,
    total_amount
from unioned
