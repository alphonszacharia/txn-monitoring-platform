-- Rule: any activity with a counterparty in a high-risk country.
-- A screening rule: it has no planted ground truth of its own.
select
    'high_risk_country'             as rule_name,
    account_id,
    customer_id,
    txn_date                        as alert_date,
    count(*)                        as txn_count,
    sum(amount)                     as total_amount
from {{ ref('fct_transactions') }}
where counterparty_risk_level = 'high'
group by account_id, customer_id, txn_date
