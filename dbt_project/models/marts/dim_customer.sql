select
    customer_id,
    full_name,
    country_code,
    kyc_risk_rating,
    onboarded_date
from {{ ref('stg_customers') }}
