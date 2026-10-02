-- Fact table: one row per transaction, keyed to the dimensions.
select
    t.txn_id,
    t.account_id,
    a.customer_id,
    to_char(t.txn_date, 'YYYYMMDD')::int                    as date_key,
    t.txn_date,
    t.txn_ts,
    t.amount,
    t.currency,
    t.txn_type,
    t.counterparty_country_code,
    t.txn_type in ('cash_deposit', 'cash_withdrawal')       as is_cash,
    coalesce(h.risk_level, 'standard')                      as counterparty_risk_level
from {{ ref('stg_transactions') }}   as t
left join {{ ref('stg_accounts') }}  as a on a.account_id = t.account_id
left join {{ ref('high_risk_countries') }} as h
       on h.country_code = t.counterparty_country_code
