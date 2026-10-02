-- Rule: a large inbound transfer followed quickly by an outbound transfer
-- of a similar amount from the same account (pass-through behaviour).
with inbound as (

    select * from {{ ref('fct_transactions') }}
    where txn_type = 'transfer_in'
      and amount >= {{ var('rapid_min_amount') }}

),

outbound as (

    select * from {{ ref('fct_transactions') }}
    where txn_type = 'transfer_out'

),

matched as (

    select
        i.account_id,
        i.customer_id,
        i.txn_date,
        i.txn_id        as in_txn_id,
        i.amount        as in_amount,
        o.amount        as out_amount
    from inbound as i
    join outbound as o
      on  o.account_id = i.account_id
      and o.txn_ts >  i.txn_ts
      and o.txn_ts <= i.txn_ts + interval '{{ var("rapid_window_hours") }} hours'
      and o.amount >= i.amount * {{ var('rapid_out_ratio') }}
      and o.amount <= i.amount

)

select
    'rapid_in_out'                  as rule_name,
    account_id,
    customer_id,
    txn_date                        as alert_date,
    count(distinct in_txn_id)       as txn_count,
    sum(in_amount)                  as total_amount
from matched
group by account_id, customer_id, txn_date
