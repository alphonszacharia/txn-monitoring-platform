-- Fails if any evaluated rule drops below the precision/recall gates.
--
-- The gate only applies once enough days are loaded (gate_min_days). The
-- recurring-account refinement needs several days of history, so a one-day
-- build would fail for a reason that has nothing to do with rule quality.
select *
from {{ ref('eval_alert_performance') }}
where days_loaded >= {{ var('gate_min_days') }}
  and (precision < {{ var('min_precision') }}
    or recall    < {{ var('min_recall') }})
