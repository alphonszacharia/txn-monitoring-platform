-- Silver: typed and standardised accounts.
with source as (

    select * from {{ source('raw', 'accounts') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by account_id
            order by _loaded_at desc, _batch_id desc
        ) as rn
    from source

)

select
    account_id,
    customer_id,
    lower(trim(account_type))   as account_type,
    opened_date::date           as opened_date,
    _batch_id,
    _loaded_at
from ranked
where rn = 1
