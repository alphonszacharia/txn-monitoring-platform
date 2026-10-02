select
    a.account_id,
    a.customer_id,
    a.account_type,
    a.opened_date
from {{ ref('stg_accounts') }} as a
