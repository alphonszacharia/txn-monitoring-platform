-- Silver: typed transactions, deduplicated on txn_id.
-- If the same transaction arrives in more than one batch, the latest load wins.
with source as (

    select * from {{ source('raw', 'transactions') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by txn_id
            order by _loaded_at desc, _batch_id desc
        ) as rn
    from source

)

select
    txn_id,
    account_id,
    txn_ts::timestamp                       as txn_ts,
    txn_ts::timestamp::date                 as txn_date,
    amount::numeric(18, 2)                  as amount,
    upper(trim(currency))                   as currency,
    lower(trim(txn_type))                   as txn_type,
    upper(trim(counterparty_country))       as counterparty_country_code,
    _batch_id,
    _loaded_at
from ranked
where rn = 1
