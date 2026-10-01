-- Silver: typed and standardised customers.
-- Keeps only the most recently loaded row per customer.
with source as (

    select * from {{ source('raw', 'customers') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by _loaded_at desc, _batch_id desc
        ) as rn
    from source

)

select
    customer_id,
    trim(full_name)                 as full_name,
    upper(trim(country))            as country_code,
    lower(trim(kyc_risk_rating))    as kyc_risk_rating,
    onboarded_date::date            as onboarded_date,
    _batch_id,
    _loaded_at
from ranked
where rn = 1
