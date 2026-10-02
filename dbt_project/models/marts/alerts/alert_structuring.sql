-- Rule: several cash deposits just below the reporting threshold in one day.
select
    'structuring'                   as rule_name,
    account_id,
    customer_id,
    txn_date                        as alert_date,
    count(*)                        as txn_count,
    sum(amount)                     as total_amount
from {{ ref('fct_transactions') }}
where txn_type = 'cash_deposit'
  and amount <  {{ var('reporting_threshold') }}
  and amount >= {{ var('reporting_threshold') }} * {{ var('structuring_lower_pct') }}
group by account_id, customer_id, txn_date
having count(*) >= {{ var('structuring_min_deposits') }}
