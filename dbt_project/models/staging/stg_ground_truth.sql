-- Silver: planted patterns (answers) from the synthetic data generator.
with source as (

    select * from {{ source('raw', 'ground_truth') }}

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
    txn_date::date              as txn_date,
    lower(trim(pattern))        as pattern,
    _batch_id,
    _loaded_at
from ranked
where rn = 1
