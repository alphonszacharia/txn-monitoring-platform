-- Scores each alert rule against the planted patterns, at account-day level.
-- Only rules with ground truth are evaluated (structuring, rapid_in_out).
with truth as (

    select distinct
        pattern     as rule_name,
        account_id,
        txn_date    as alert_date
    from {{ ref('stg_ground_truth') }}

),

alerts as (

    select distinct rule_name, account_id, alert_date
    from {{ ref('fct_alerts') }}
    where rule_name in (select distinct rule_name from truth)

),

joined as (

    select
        coalesce(a.rule_name, t.rule_name)  as rule_name,
        a.account_id is not null            as alerted,
        t.account_id is not null            as planted
    from alerts as a
    full outer join truth as t
      on  a.rule_name  = t.rule_name
      and a.account_id = t.account_id
      and a.alert_date = t.alert_date

),

counts as (

    select
        rule_name,
        count(*) filter (where alerted and planted)      as true_positives,
        count(*) filter (where alerted and not planted)  as false_positives,
        count(*) filter (where not alerted and planted)  as false_negatives
    from joined
    group by rule_name

)

select
    rule_name,
    true_positives,
    false_positives,
    false_negatives,
    round(true_positives::numeric
          / nullif(true_positives + false_positives, 0), 3) as precision,
    round(true_positives::numeric
          / nullif(true_positives + false_negatives, 0), 3) as recall
from counts
