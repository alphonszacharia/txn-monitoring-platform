-- Fails if any transaction has a zero or negative amount.
select txn_id, amount
from {{ ref('stg_transactions') }}
where amount <= 0
