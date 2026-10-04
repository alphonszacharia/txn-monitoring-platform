-- Rule: several cash deposits just below the reporting threshold in one day.
--
-- Refinement (structuring_recurring_days): a cash-intensive business banks
-- near-threshold takings again and again, so an account that triggers on
-- N or more different days is treated as routine, not suspicious.
-- Set structuring_recurring_days to 0 to switch the refinement off.
with candidate_days as (

    select
        account_id,
        customer_id,
        txn_date,
        count(*)        as txn_count,
        sum(amount)     as total_amount
    from {{ ref('fct_transactions') }}
    where txn_type = 'cash_deposit'
      and amount <  {{ var('reporting_threshold') }}
      and amount >= {{ var('reporting_threshold') }} * {{ var('structuring_lower_pct') }}
    group by account_id, customer_id, txn_date
    having count(*) >= {{ var('structuring_min_deposits') }}

),

with_history as (

    select
        *,
        count(*) over (partition by account_id) as days_triggered
    from candidate_days

)

select
    'structuring'       as rule_name,
    account_id,
    customer_id,
    txn_date            as alert_date,
    txn_count,
    total_amount
from with_history
{% if var('structuring_recurring_days') > 0 %}
where days_triggered < {{ var('structuring_recurring_days') }}
{% endif %}
