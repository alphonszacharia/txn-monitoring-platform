-- Fails if any evaluated rule drops below the precision/recall gates.
select *
from {{ ref('eval_alert_performance') }}
where precision < {{ var('min_precision') }}
   or recall    < {{ var('min_recall') }}
