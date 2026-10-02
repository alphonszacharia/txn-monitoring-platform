-- One row per calendar day covering all loaded transactions.
with bounds as (

    select
        min(txn_date) as first_day,
        max(txn_date) as last_day
    from {{ ref('stg_transactions') }}

),

days as (

    select generate_series(first_day, last_day, interval '1 day')::date as date_day
    from bounds

)

select
    to_char(date_day, 'YYYYMMDD')::int          as date_key,
    date_day                                    as full_date,
    extract(isodow from date_day)::int          as iso_day_of_week,
    to_char(date_day, 'Day')                    as day_name,
    extract(isodow from date_day) in (6, 7)     as is_weekend,
    extract(month from date_day)::int           as month_number,
    extract(quarter from date_day)::int         as quarter_number,
    extract(year from date_day)::int            as year_number
from days
